"""
16_position_manager.py — 持倉追蹤（STEP 13）

職責：
  1. 讀寫 data/positions.json（持倉記錄）
  2. 進場/平倉立即落盤
  3. edge/pnl 更新節流落盤（每 30 秒）
  4. 不接 Polymarket 交易 API（第一版手動記錄）

設計原則：
  - 原子寫入（.tmp → os.replace）
  - 進場/平倉：立即落盤（write-through）
  - mark/edge 更新：記憶體更新，flush_edges() 節流
  - position_id 格式：pos_YYYYMMDD_HHMMSS_XXX（XXX = 隨機 3 碼）
  - PnL 用持有 token 的 best bid 計算（不管 YES 還是 NO）
  - unrealized_pnl_gross（不扣 sell fee）和 unrealized_pnl_net（扣預估 sell fee）分開

positions.json schema（schema_version: positions_v1）：
  見 STEP 13 spec。
"""

import json
import logging
import os
import random
import string
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

PROJ_DIR = Path(__file__).resolve().parent


# ============================================================
# 工具函數
# ============================================================

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_utc_str() -> str:
    return _now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")


def _atomic_write_json(path: Path, data: dict) -> None:
    """原子寫入 JSON：先寫 .tmp → os.replace() 覆蓋。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", dir=str(path.parent), suffix=".tmp", delete=False, encoding="utf-8"
    ) as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        tmp = f.name
    os.replace(tmp, str(path))


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


# ============================================================
# PositionManager
# ============================================================

class PositionManager:
    """
    管理所有持倉（data/positions.json）。

    落盤規則：
      - add_position / close_position → 立即落盤（write-through）
      - update_mark → 記憶體更新，設 _dirty_edges = True
      - flush_edges → 節流落盤（預設每 30 秒最多寫一次）
    """

    # Polymarket sell fee 估算參數（與 fill_simulator 一致）
    FEE_RATE = 0.025
    FEE_EXPONENT = 0.5

    EMPTY_SCHEMA = {
        "schema_version": "positions_v1",
        "updated_at_utc": "",
        "positions": [],
    }

    def __init__(self, path: str = "data/positions.json"):
        self.path = PROJ_DIR / path
        self._data: Optional[dict] = None
        self._dirty_edges: bool = False
        self._last_edge_flush: Optional[datetime] = None

    # ── 讀寫 ──────────────────────────────────────────────────

    def load(self) -> dict:
        """載入 positions.json；不存在時初始化空結構。"""
        if not self.path.exists():
            self._data = dict(self.EMPTY_SCHEMA)
            self._data["positions"] = []
            return self._data
        try:
            self._data = json.loads(self.path.read_text(encoding="utf-8"))
            if "positions" not in self._data:
                self._data["positions"] = []
        except Exception as e:
            log.warning(f"PositionManager.load: {e} — starting with empty positions")
            self._data = dict(self.EMPTY_SCHEMA)
            self._data["positions"] = []
        return self._data

    def _ensure_loaded(self) -> None:
        if self._data is None:
            self.load()

    def save(self) -> None:
        """原子寫入 positions.json。"""
        self._ensure_loaded()
        if self._data is None:
            raise RuntimeError("PositionManager: _data is None after load")
        self._data["updated_at_utc"] = _now_utc_str()
        try:
            _atomic_write_json(self.path, self._data)
        except Exception as e:
            log.error(f"PositionManager.save failed: {e}")

    # ── 持倉操作（立即落盤）──────────────────────────────────

    def add_position(
        self,
        market_id: str,
        token_id: str,
        city: str,
        market_date: str,
        contract_label: str,
        side: str,
        entry_price: float,
        shares: float,
        entry_fee_per_share: float,
        entry_edge: float,
        entry_ev: float,
        signal_action: str,
    ) -> str:
        """
        新增持倉，回傳 position_id。立即落盤。

        side: "YES" 或 "NO"
        entry_cost_total = (entry_price + entry_fee_per_share) * shares

        Raises:
            ValueError: 若 market_id 或 token_id 為空（交易模組下單需要有效 token_id）。
        """
        if not market_id:
            raise ValueError("add_position: market_id is required")
        if not token_id:
            raise ValueError(
                f"add_position: token_id is required (market_id={market_id}, side={side}). "
                "Check market_master.csv has yes_token_id/no_token_id for this market."
            )
        self._ensure_loaded()
        position_id = self._generate_id()
        entry_cost_total = (entry_price + entry_fee_per_share) * shares

        position = {
            "position_id": position_id,
            "market_id": market_id,
            "token_id": token_id,
            "city": city,
            "market_date": market_date,
            "contract_label": contract_label,
            "side": side,
            "entry_price": entry_price,
            "shares": shares,
            "entry_fee_per_share": entry_fee_per_share,
            "entry_cost_total": round(entry_cost_total, 6),
            "entry_time_utc": _now_utc_str(),
            "entry_edge": entry_edge,
            "entry_ev": entry_ev,
            "entry_signal_action": signal_action,
            "status": "open",
            "mark_price": None,
            "current_edge": None,
            "current_ev": None,
            "unrealized_pnl_gross": None,
            "unrealized_pnl_net": None,
            "last_checked_utc": None,
            "exit_reason": None,
            "exit_time_utc": None,
            "exit_price": None,
            "exit_fee_estimate": None,
            "exit_signal_action_at_close": None,
            "close_method": None,
            "pnl_gross": None,
            "pnl_net": None,
            "status_reason": None,
            "notes": "",
        }
        if self._data is None:
            raise RuntimeError("PositionManager: _data is None after load")
        self._data["positions"].append(position)
        self.save()
        log.info(
            f"PositionManager: opened {position_id} "
            f"({city} {contract_label} {side} × {shares} @ {entry_price})"
        )
        return position_id

    def close_position(
        self,
        position_id: str,
        exit_price: float,
        exit_fee_estimate: float = 0.0,
        exit_reason: str = "manual",
        exit_signal_action: Optional[str] = None,
    ) -> bool:
        """
        關閉持倉。立即落盤。回傳是否成功。

        pnl_gross = shares × exit_price - entry_cost_total
        pnl_net   = shares × exit_price - exit_fee_estimate - entry_cost_total
        """
        pos = self._find(position_id)
        if pos is None:
            log.warning(f"close_position: position_id={position_id!r} not found")
            return False
        if pos.get("status") != "open":
            log.warning(f"close_position: {position_id} already {pos.get('status')}")
            return False

        shares = _safe_float(pos.get("shares"))
        entry_cost_total = _safe_float(pos.get("entry_cost_total"))
        pnl_gross = shares * exit_price - entry_cost_total
        pnl_net = shares * exit_price - exit_fee_estimate - entry_cost_total

        pos["status"] = "closed"
        pos["exit_price"] = exit_price
        pos["exit_fee_estimate"] = exit_fee_estimate
        pos["exit_time_utc"] = _now_utc_str()
        pos["exit_reason"] = exit_reason
        pos["exit_signal_action_at_close"] = exit_signal_action
        pos["close_method"] = "manual"
        pos["pnl_gross"] = round(pnl_gross, 6)
        pos["pnl_net"] = round(pnl_net, 6)
        self.save()
        log.info(
            f"PositionManager: closed {position_id} "
            f"pnl_gross={pnl_gross:+.4f} pnl_net={pnl_net:+.4f}"
        )
        return True

    # ── 即時更新（節流落盤）──────────────────────────────────

    def update_mark(
        self,
        position_id: str,
        mark_price: float,
        current_edge: Optional[float],
        current_ev: Optional[float],
    ) -> None:
        """
        更新持倉的即時資訊（記憶體）。不立即落盤。

        mark_price = 持有 token 的 best bid（賣出方向）
        unrealized_pnl_gross = shares × mark_price - entry_cost_total
        unrealized_pnl_net   = shares × mark_price - estimated_sell_fee - entry_cost_total
        """
        pos = self._find(position_id)
        if pos is None:
            return
        shares = _safe_float(pos.get("shares"))
        entry_cost_total = _safe_float(pos.get("entry_cost_total"))

        pos["mark_price"] = mark_price
        pos["current_edge"] = current_edge
        pos["current_ev"] = current_ev
        pos["last_checked_utc"] = _now_utc_str()

        pos["unrealized_pnl_gross"] = round(shares * mark_price - entry_cost_total, 6)

        sell_fee = self._estimate_sell_fee(mark_price, shares)
        pos["unrealized_pnl_net"] = round(
            shares * mark_price - sell_fee - entry_cost_total, 6
        )

        self._dirty_edges = True

    def flush_edges(self, min_interval_seconds: float = 30.0) -> None:
        """
        節流落盤：只有距上次 flush 超過 min_interval_seconds 才寫。
        進場/平倉已即時落盤，這裡只處理 edge/pnl 更新。
        """
        if not self._dirty_edges:
            return
        now = _now_utc()
        if (
            self._last_edge_flush is not None
            and (now - self._last_edge_flush).total_seconds() < min_interval_seconds
        ):
            return
        self.save()
        self._dirty_edges = False
        self._last_edge_flush = now
        log.debug("PositionManager: flushed edge updates")

    def _estimate_sell_fee(self, price: float, shares: float) -> float:
        """預估 sell fee（USDC）。Polymarket sell fee 以 USDC 收取。"""
        inner = price * (1 - price)
        if inner < 0:
            inner = 0.0
        fee_per_share = price * self.FEE_RATE * (inner ** self.FEE_EXPONENT)
        return fee_per_share * shares

    # ── 查詢 ──────────────────────────────────────────────────

    def get_open_positions(self) -> list:
        """回傳所有 status=open 的持倉（最新進場在前）。"""
        self._ensure_loaded()
        if self._data is None:
            raise RuntimeError("PositionManager: _data is None after load")
        positions = [
            p for p in self._data.get("positions", [])
            if p.get("status") == "open"
        ]
        # 最新進場在前
        positions.sort(key=lambda p: p.get("entry_time_utc", ""), reverse=True)
        return positions

    def get_closed_positions(self) -> list:
        """回傳所有 status=closed 的持倉（最新平倉在前）。"""
        self._ensure_loaded()
        if self._data is None:
            raise RuntimeError("PositionManager: _data is None after load")
        positions = [
            p for p in self._data.get("positions", [])
            if p.get("status") == "closed"
        ]
        positions.sort(key=lambda p: p.get("exit_time_utc", ""), reverse=True)
        return positions

    def get_position(self, position_id: str) -> Optional[dict]:
        """按 position_id 查詢（找不到回傳 None）。"""
        return self._find(position_id)

    # ── 私有工具 ──────────────────────────────────────────────

    def _generate_id(self) -> str:
        """pos_YYYYMMDD_HHMMSS_XXX（XXX = 隨機 3 碼英數）"""
        ts = _now_utc().strftime("%Y%m%d_%H%M%S")
        suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=3))
        return f"pos_{ts}_{suffix}"

    def _find(self, position_id: str) -> Optional[dict]:
        """在 positions list 中找到 position_id；找不到回傳 None。"""
        self._ensure_loaded()
        if self._data is None:
            raise RuntimeError("PositionManager: _data is None after load")
        for p in self._data.get("positions", []):
            if p.get("position_id") == position_id:
                return p
        return None
