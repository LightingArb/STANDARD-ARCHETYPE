"""
_lib/fill_simulator.py — 通用吃單模擬器

設計原則：
  1. 純函式：不讀檔、不打 API、不碰 config。所有參數由呼叫端傳入。
  2. Side-generic：支援 buy_asks（買入）和 sell_bids（賣出），第一版外部只用 buy_asks。
  3. 一套邏輯，三種停止條件：
     - best_only  ：只吃第一層
     - fixed_depth：累積 notional >= fixed_depth_usd（最後一層可部分吃）
     - sweet_spot ：marginal_ev <= 0 就停（第一個就停，不跳層）

排序假設（呼叫端負責）：
  buy_asks  → 由低到高（最便宜先吃）
  sell_bids → 由高到低（最貴先賣）

sweet_spot 停止規則前提：
  asks 已排序，cost 對價格單調不下降。
  未來若做 bids / exit / 不同費率模型，需重新檢查此前提。
"""

from dataclasses import dataclass, field
from typing import List, Optional


# ============================================================
# 資料結構
# ============================================================

@dataclass
class FillLevel:
    """一層的吃單結果"""
    price: float
    size: float                  # 實際吃的量（fixed_depth 最後一層可能是部分）
    notional: float              # price × size（USD）
    fee_per_share: float         # fee = price × fee_rate × (price × (1-price))^exp
    cost_per_share: float        # buy: price + fee；sell: price - fee
    marginal_ev: float           # buy: p - cost；sell: cost - (1-p)
    cumulative_shares: float
    cumulative_notional: float
    cumulative_ev: float


@dataclass
class FillResult:
    """fill simulation 的完整結果"""
    mode: str                     # "best_only" / "fixed_depth" / "sweet_spot"
    side: str                     # "buy_asks" / "sell_bids"
    total_shares: float
    total_notional: float         # USD（不含 fee）
    avg_fill_price: float
    total_fee: float
    total_cost: float             # buy: notional + fee；sell: notional - fee
    cumulative_ev: float
    depth_exhausted: bool         # sweet_spot/fixed_depth: 吃完 book 還沒滿足停止條件
    levels: List[FillLevel] = field(default_factory=list)
    levels_consumed: int = 0


# ============================================================
# 核心模擬器
# ============================================================

def simulate_fill(
    orderbook_levels: List[dict],
    p: float,
    fee_rate: float,
    fee_exponent: float,
    mode: str = "best_only",
    fixed_depth_usd: Optional[float] = None,
    side: str = "buy_asks",
) -> FillResult:
    """
    通用吃單模擬器。

    orderbook_levels: [{"price": float-or-str, "size": float-or-str}, ...]
      buy_asks  → 必須已由低到高排序
      sell_bids → 必須已由高到低排序

    mode:
      "best_only"   → 只吃第一層，忽略 fixed_depth_usd
      "fixed_depth" → 吃到累積 notional >= fixed_depth_usd（最後層部分吃）
      "sweet_spot"  → 吃到第一個 marginal_ev <= 0 就停

    回傳空的 FillResult（levels=[]）若 orderbook_levels 為空。
    """
    if not orderbook_levels:
        return FillResult(
            mode=mode, side=side,
            total_shares=0.0, total_notional=0.0, avg_fill_price=0.0,
            total_fee=0.0, total_cost=0.0, cumulative_ev=0.0,
            depth_exhausted=True, levels=[], levels_consumed=0,
        )

    levels: List[FillLevel] = []
    levels_consumed = 0
    cumulative_shares = 0.0
    cumulative_notional = 0.0
    cumulative_ev = 0.0
    budget_reached = False  # for fixed_depth: True when budget was reached

    for level_data in orderbook_levels:
        price = float(level_data["price"])
        available_size = float(level_data["size"])

        if price <= 0 or available_size <= 0:
            continue  # skip degenerate levels

        # ── Fee & marginal EV ────────────────────────────────────
        try:
            fee_per_share = price * fee_rate * (price * (1.0 - price)) ** fee_exponent
        except (ValueError, ZeroDivisionError):
            fee_per_share = 0.0

        if side == "buy_asks":
            cost_per_share = price + fee_per_share
            marginal_ev = p - cost_per_share
        else:  # sell_bids
            cost_per_share = price - fee_per_share  # net receipt per share
            marginal_ev = cost_per_share - (1.0 - p)  # EV of selling YES

        # ── Stopping conditions (before consuming this level) ────
        if mode == "best_only" and levels_consumed >= 1:
            break
        if mode == "sweet_spot" and marginal_ev <= 0:
            break
        if mode == "fixed_depth":
            budget = fixed_depth_usd or 0.0
            if cumulative_notional >= budget - 1e-9:
                budget_reached = True
                break

        # ── Determine actual size to consume ────────────────────
        if mode == "fixed_depth" and fixed_depth_usd is not None:
            remaining_budget = fixed_depth_usd - cumulative_notional
            size = min(available_size, remaining_budget / price)
        else:
            size = available_size

        notional = price * size
        cumulative_shares += size
        cumulative_notional += notional
        cumulative_ev += size * marginal_ev

        level_record = FillLevel(
            price=round(price, 6),
            size=round(size, 6),
            notional=round(notional, 6),
            fee_per_share=round(fee_per_share, 8),
            cost_per_share=round(cost_per_share, 8),
            marginal_ev=round(marginal_ev, 8),
            cumulative_shares=round(cumulative_shares, 6),
            cumulative_notional=round(cumulative_notional, 6),
            cumulative_ev=round(cumulative_ev, 8),
        )
        levels.append(level_record)
        levels_consumed += 1

        # After consuming: has fixed_depth budget been reached?
        if mode == "fixed_depth":
            budget = fixed_depth_usd or 0.0
            if cumulative_notional >= budget - 1e-9:
                budget_reached = True
                break

    # ── depth_exhausted ─────────────────────────────────────────
    n_levels = len(orderbook_levels)
    if mode == "fixed_depth":
        # True = ran out of book BEFORE filling the requested budget
        depth_exhausted = (levels_consumed == n_levels and not budget_reached)
    elif mode == "sweet_spot":
        # True = consumed ALL levels without hitting EV ≤ 0
        depth_exhausted = (levels_consumed == n_levels)
    else:
        # best_only: never exhausted (stopped by design)
        depth_exhausted = False

    # ── Aggregate results ────────────────────────────────────────
    total_shares = cumulative_shares
    total_notional = cumulative_notional
    avg_fill_price = (cumulative_notional / cumulative_shares) if cumulative_shares > 0 else 0.0
    total_fee = sum(lv.fee_per_share * lv.size for lv in levels)
    if side == "buy_asks":
        total_cost = total_notional + total_fee
    else:
        total_cost = total_notional - total_fee

    return FillResult(
        mode=mode,
        side=side,
        total_shares=round(total_shares, 6),
        total_notional=round(total_notional, 6),
        avg_fill_price=round(avg_fill_price, 6),
        total_fee=round(total_fee, 8),
        total_cost=round(total_cost, 6),
        cumulative_ev=round(cumulative_ev, 8),
        depth_exhausted=depth_exhausted,
        levels=levels,
        levels_consumed=levels_consumed,
    )


# ============================================================
# Regression test
# ============================================================

def test_fill_simulator() -> bool:
    """
    固定 orderbook + p + fee → 驗算每層結果。
    回傳 True = 全部通過。
    """
    book = [
        {"price": 0.03, "size": 200},
        {"price": 0.05, "size": 150},
        {"price": 0.08, "size": 100},
        {"price": 0.12, "size": 300},
    ]
    p = 0.20
    fee_rate = 0.025
    fee_exp = 0.5
    all_pass = True

    def _check(label: str, cond: bool, msg: str = "") -> None:
        nonlocal all_pass
        status = "PASS" if cond else "FAIL"
        print(f"  [{status}] {label}" + (f": {msg}" if msg else ""))
        if not cond:
            all_pass = False

    print("=== fill_simulator regression tests ===")

    # ── best_only ─────────────────────────────────────────────
    r1 = simulate_fill(book, p, fee_rate, fee_exp, mode="best_only")
    _check("best_only: levels_consumed == 1", r1.levels_consumed == 1)
    _check("best_only: total_shares == 200", abs(r1.total_shares - 200) < 0.001)
    _check("best_only: avg_fill_price ≈ 0.03", abs(r1.avg_fill_price - 0.03) < 0.001)
    _check("best_only: depth_exhausted == False", not r1.depth_exhausted)

    # 向後相容：best_only edge 應等於 p - price（不含 fee）
    old_edge = p - 0.03
    new_edge_no_fee = p - r1.avg_fill_price
    _check("best_only: edge backward-compat",
           abs(old_edge - new_edge_no_fee) < 0.001,
           f"old={old_edge:.4f} new={new_edge_no_fee:.4f}")

    # ── sweet_spot ───────────────────────────────────────────────
    # p=0.20, all costs < 0.20 (prices 0.03/0.05/0.08/0.12) → should consume all
    r2 = simulate_fill(book, p, fee_rate, fee_exp, mode="sweet_spot")
    _check("sweet_spot: levels_consumed >= 1", r2.levels_consumed >= 1)
    _check("sweet_spot: depth_exhausted == True (p > all costs)",
           r2.depth_exhausted,
           f"levels_consumed={r2.levels_consumed}")
    _check("sweet_spot: cumulative_ev > 0", r2.cumulative_ev > 0)

    # Now with low p: sweet_spot should stop early
    r2b = simulate_fill(book, 0.04, fee_rate, fee_exp, mode="sweet_spot")
    _check("sweet_spot (p=0.04): stops at first level with EV≤0",
           r2b.levels_consumed >= 1,
           f"levels={r2b.levels_consumed}")

    # ── fixed_depth ───────────────────────────────────────────────
    r3 = simulate_fill(book, p, fee_rate, fee_exp, mode="fixed_depth", fixed_depth_usd=10.0)
    _check("fixed_depth: total_notional <= 10.01",
           r3.total_notional <= 10.01,
           f"got={r3.total_notional:.4f}")
    _check("fixed_depth: depth_exhausted == False (budget reachable)",
           not r3.depth_exhausted)
    _check("fixed_depth: levels_consumed >= 1", r3.levels_consumed >= 1)

    # Very large budget → depth exhausted
    r3b = simulate_fill(book, p, fee_rate, fee_exp, mode="fixed_depth", fixed_depth_usd=999.0)
    _check("fixed_depth (large budget): depth_exhausted == True",
           r3b.depth_exhausted,
           f"levels={r3b.levels_consumed}")
    _check("fixed_depth (large budget): levels_consumed == len(book)",
           r3b.levels_consumed == len(book))

    # ── three modes should give different results ─────────────────
    _check("best_only vs sweet_spot: different shares",
           r1.total_shares != r2.total_shares or r1.levels_consumed != r2.levels_consumed)

    # ── empty book ───────────────────────────────────────────────
    r_empty = simulate_fill([], p, fee_rate, fee_exp, mode="best_only")
    _check("empty book: levels_consumed == 0", r_empty.levels_consumed == 0)
    _check("empty book: depth_exhausted == True", r_empty.depth_exhausted)

    print("=== fill_simulator tests done ===")
    return all_pass


if __name__ == "__main__":
    ok = test_fill_simulator()
    import sys
    sys.exit(0 if ok else 1)
