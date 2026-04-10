"""
11_ev_engine.py — EV 與交易信號計算

⚠️ D1-only MVP baseline：基於少量樣本，僅供管線驗證，不可用於交易決策

輸入：
  data/results/probability/{city}/event_probability.csv   （10 輸出）
  config/trading_params.yaml

價格輸入（手動）：
  --yes-price / --no-price   對所有市場套用同一組價格
  --prices-csv               每個 market_id 個別指定（columns: market_id, yes_price, no_price）

沒有提供價格時：只輸出 p_yes / p_no / naive_fair，不計算 edge / EV / signal。

輸出：
  data/results/ev_signals/{city}/ev_signals.csv
"""

import argparse
import csv
import json
import logging
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ── fill_simulator（optional，STEP 9 深度分析）──────────────────
_lib_dir = PROJ_DIR / "_lib"
if str(_lib_dir) not in sys.path:
    sys.path.insert(0, str(_lib_dir))
try:
    from fill_simulator import simulate_fill as _simulate_fill  # type: ignore[import]
    _HAS_FILL_SIM = True
except ImportError:
    _HAS_FILL_SIM = False
    log.warning("fill_simulator not found — depth analysis columns will be empty")

MODEL_SCOPE = "D1-only MVP"

OUTPUT_FIELDS = [
    "market_id",
    "city",
    "market_date_local",
    "market_type",
    "threshold",
    "range_low",
    "range_high",
    "temp_unit",
    "model_name",
    "model_scope",
    "predicted_daily_high",
    "lead_day",
    "lead_hours_to_settlement",
    "p_yes",
    "p_no",
    "yes_ask_price",
    "no_ask_price",
    "yes_fee",
    "no_fee",
    "yes_cost",
    "no_cost",
    "yes_edge",
    "no_edge",
    "yes_ev",
    "no_ev",
    "naive_fair_yes",
    "naive_fair_no",
    "signal",           # 向後相容，值 = signal_action
    "kelly_fraction_yes",
    "kelly_fraction_no",
    "kelly_amount",
    "fee_rate_used",
    "fee_exponent_used",
    # STEP 5 新增欄位
    "fee_mode",
    "fee_basis",
    "fee_status",
    "price_status",
    "price_age_seconds",
    "book_source",
    "signal_status",
    "signal_action",
    # STEP 9 深度分析欄位（有 book_state 時才有值，否則空字串）
    "yes_depth_usd",
    "yes_sweet_shares",
    "yes_sweet_usd",
    "yes_sweet_avg_price",
    "yes_sweet_ev",
    "yes_sweet_exhausted",
    "yes_fixed_shares",
    "yes_fixed_usd",
    "yes_fixed_avg_price",
    "yes_fixed_ev",
    "yes_fixed_exhausted",
    "no_depth_usd",
    "no_sweet_shares",
    "no_sweet_usd",
    "no_sweet_avg_price",
    "no_sweet_ev",
    "no_sweet_exhausted",
    "no_fixed_shares",
    "no_fixed_usd",
    "no_fixed_avg_price",
    "no_fixed_ev",
    "no_fixed_exhausted",
    # 即時觀測邏輯裁剪欄位
    "observed_high_c",
    "observation_clipped",
    "clip_reason",
    "observation_source",
]

STALE_PRICE_THRESHOLD = 300  # 秒，超過此值 price_status = "stale"
MODEL_STALE_DAYS = 7         # 模型超過此天數視為 stale


# ============================================================
# Seed city timezone loader
# ============================================================

def _load_seed_timezones() -> dict[str, str]:
    """從 config/seed_cities.json 讀 city→timezone mapping。"""
    path = PROJ_DIR / "config" / "seed_cities.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {
            city: info.get("timezone", "")
            for city, info in data.items()
            if isinstance(info, dict) and info.get("timezone")
        }
    except Exception as e:
        log.warning(f"_load_seed_timezones: {e}")
        return {}


def _calc_hours_to_settlement(market_date_str: str, city_tz_str: str) -> Optional[float]:
    """
    計算從現在到 market_date 結算時刻（當地隔天午夜）的小時數。

    market_date_str : "YYYY-MM-DD"（當地日期）
    city_tz_str     : IANA timezone, e.g. "America/New_York"
    回傳 None 代表無法計算（tz 無效、日期格式錯誤等）。
    """
    if not market_date_str or not city_tz_str:
        return None
    try:
        import zoneinfo
        from datetime import date, timedelta
        tz = zoneinfo.ZoneInfo(city_tz_str)
        market_date = date.fromisoformat(market_date_str)
        # 結算時刻 = market_date 的當地隔天午夜（即 market_date+1 00:00 local）
        settlement_local = datetime(
            market_date.year, market_date.month, market_date.day,
            tzinfo=tz
        ) + timedelta(days=1)
        now_utc = datetime.now(timezone.utc)
        delta = settlement_local - now_utc
        return delta.total_seconds() / 3600
    except Exception as e:
        log.warning(f"_calc_hours_to_settlement({market_date_str}, {city_tz_str}): {e}")
        return None


# ============================================================
# 即時觀測邏輯裁剪
# ============================================================

def _apply_observation_clipping(
    out_row: dict,
    current_obs: dict,
    city_timezones: dict,
) -> None:
    """
    用即時觀測做邏輯裁剪（基於物理事實：最終最高溫 ≥ 目前最高溫）。

    只對「城市當地今天」的市場做裁剪。修改 out_row in-place。

    裁剪規則：
      exact:  threshold < observed → p_yes=0, p_no=1（已超過，不可能剛好等於）
      below:  threshold <= observed → p_yes=0, p_no=1（已達到或超過，不可能低於）
      higher: threshold < observed → p_yes=1, p_no=0（已確定超過門檻）
      range:  range_high < observed → p_yes=0, p_no=1（已超過區間上限）
    """
    city = out_row.get("city", "")
    market_date = out_row.get("market_date_local", "")
    if not city or not market_date:
        return

    city_tz_str = city_timezones.get(city, "")
    if not city_tz_str:
        return

    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(city_tz_str)
        city_today = datetime.now(tz).date().isoformat()
    except Exception as e:
        log.debug(f"_apply_observation_clipping: timezone error for {city}: {e}")
        return

    if market_date != city_today:
        return

    obs_info = current_obs.get(city)
    if not obs_info or "high_c" not in obs_info:
        return

    observed_high_c = float(obs_info["high_c"])
    obs_source = obs_info.get("source", "unknown")

    market_type = out_row.get("market_type", "")
    temp_unit = out_row.get("temp_unit", "C")

    # 統一單位：合約是 F 時，把觀測值（C）轉 F 再比較
    if temp_unit == "F":
        observed_compare = observed_high_c * 9.0 / 5.0 + 32.0
    else:
        observed_compare = observed_high_c

    threshold = safe_float(out_row.get("threshold"))
    range_high = safe_float(out_row.get("range_high"))
    range_low = safe_float(out_row.get("range_low"))

    clipped = False
    clip_reason = ""

    if market_type == "exact":
        if threshold is not None and threshold < observed_compare:
            out_row["p_yes"] = 0.0
            out_row["p_no"] = 1.0
            clipped = True
            clip_reason = f"exact {threshold} < obs {observed_compare:.1f}"

    elif market_type == "below":
        if threshold is not None and threshold <= observed_compare:
            out_row["p_yes"] = 0.0
            out_row["p_no"] = 1.0
            clipped = True
            clip_reason = f"below {threshold} <= obs {observed_compare:.1f}"

    elif market_type == "higher":
        if threshold is not None and threshold < observed_compare:
            out_row["p_yes"] = 1.0
            out_row["p_no"] = 0.0
            clipped = True
            clip_reason = f"higher {threshold} < obs {observed_compare:.1f}"

    elif market_type == "range":
        if range_high is not None and range_high < observed_compare:
            out_row["p_yes"] = 0.0
            out_row["p_no"] = 1.0
            clipped = True
            clip_reason = f"range_high {range_high} < obs {observed_compare:.1f}"

    out_row["observed_high_c"] = round(observed_high_c, 1)
    out_row["observation_source"] = obs_source
    out_row["observation_clipped"] = clipped
    out_row["clip_reason"] = clip_reason

    if clipped:
        log.info(
            f"  OBS CLIP: {city} {market_date} [{market_type}] — {clip_reason}"
        )


# ============================================================
# Price freshness helpers
# ============================================================

def get_price_age_seconds(proj_dir: Path) -> float | None:
    """計算 market_prices.csv 距上次修改的秒數（CSV fallback 用）。不存在則回傳 None。"""
    prices_path = proj_dir / "data" / "raw" / "prices" / "market_prices.csv"
    if not prices_path.exists():
        return None
    return time.time() - prices_path.stat().st_mtime


def calc_price_age_from_utc_str(utc_str: str) -> float | None:
    """從 snapshot_fetch_time_utc 字串計算距今秒數（精確到秒）。解析失敗回傳 None。"""
    if not utc_str:
        return None
    try:
        dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except (ValueError, TypeError):
        return None


def load_book_state(market_id: str, book_state_dir: Path) -> dict | None:
    """讀取 book_state JSON。不存在或解析失敗回傳 None。"""
    json_path = book_state_dir / f"{market_id}.json"
    if not json_path.exists():
        return None
    try:
        return json.loads(json_path.read_text("utf-8"))
    except Exception:
        return None


def get_price_status(age_seconds: float | None) -> str:
    """回傳 'fresh' / 'stale' / 'no_price'。"""
    if age_seconds is None:
        return "no_price"
    return "stale" if age_seconds > STALE_PRICE_THRESHOLD else "fresh"


def get_fee_status(metric_type: str) -> str:
    """Weather 類市場（daily_high）fee 已定案 → 'known'，其他 → 'unknown'。"""
    if metric_type in ("daily_high", "daily_low"):
        return "known"
    return "unknown"


def get_signal_status(
    price_status: str,
    fee_status: str,
    has_price: bool,
    book_complete: bool = True,
) -> str:
    """決定 signal_status（系統狀態）。優先序（高 → 低）：
    no_price > book_incomplete > stale_price > fee_unknown > active
    """
    if not has_price or price_status == "no_price":
        return "no_price"
    if not book_complete:
        return "book_incomplete"
    if price_status == "stale":
        return "stale_price"
    if fee_status == "unknown":
        return "fee_unknown"
    return "active"


def get_signal_action(signal_status: str, raw_signal: str) -> str:
    """只有 signal_status='active' 才輸出正式 action，其他一律 SUPPRESSED。"""
    if signal_status != "active":
        return "SUPPRESSED"
    return raw_signal  # BUY_YES / BUY_NO / NO_TRADE / NO_PRICE


# ============================================================
# Fee regression test
# ============================================================

def run_fee_regression() -> bool:
    """
    驗算官方費率表 3 筆（Weather, 100 shares, fee_rate=0.025, exponent=0.5）。
    來源：https://docs.polymarket.com/trading/fees
    """
    test_cases = [
        (100, 0.10, 0.08),
        (100, 0.50, 0.62),
        (100, 0.90, 0.67),
    ]
    print("Fee regression (Weather, 100 shares, fee_rate=0.025, exponent=0.5):")
    all_pass = True
    for shares, price, expected in test_cases:
        fee = shares * price * 0.025 * (price * (1 - price)) ** 0.5
        got = round(fee, 2)
        ok = abs(got - expected) < 0.01
        status = "PASS" if ok else "FAIL"
        print(f"  p={price:.2f}: fee={fee:.4f} → ${got:.2f}  expected=${expected:.2f}  [{status}]")
        if not ok:
            all_pass = False
    return all_pass


# ============================================================
# Config loader（simple flat YAML parser）
# ============================================================

def load_trading_params(path: Path) -> dict:
    """Parse a simple flat YAML file (no nesting). Returns dict with float values."""
    params: dict = {}
    if not path.exists():
        log.warning(f"trading_params.yaml not found: {path}, using defaults")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                continue
            key, _, val = line.partition(":")
            key = key.strip()
            val = val.split("#")[0].strip()  # Strip inline comments
            try:
                params[key] = float(val)
            except ValueError:
                params[key] = val
    return params


DEFAULT_PARAMS = {
    "fee_rate": 0.025,
    "fee_exponent": 0.5,
    "min_edge": 0.03,
    "kelly_fraction": 0.25,
    "bankroll": 10000.0,
    "hurdle_rate": 0.0,
    "depth_fixed_usd": 200.0,   # STEP 9：fixed_depth 模式預設金額（分析用）
}


def get_param(params: dict, key: str) -> float:
    val = params.get(key, DEFAULT_PARAMS.get(key, 0.0))
    try:
        return float(val)
    except (TypeError, ValueError):
        return float(DEFAULT_PARAMS.get(key, 0.0))


# ============================================================
# EV 計算
# ============================================================

def compute_fee(p: float, fee_rate: float, fee_exponent: float) -> float:
    """
    fee_per_share = p * fee_rate * (p * (1 - p)) ** fee_exponent
    ⚠️ 需人工驗證此公式是否符合當前 Polymarket fee schedule
    """
    try:
        return p * fee_rate * (p * (1.0 - p)) ** fee_exponent
    except (ValueError, ZeroDivisionError):
        return 0.0


def compute_kelly(p: float, cost: float, kelly_frac: float) -> float:
    """Full Kelly = (p - cost) / (1 - cost), scaled by kelly_frac. Returns 0 if EV <= 0."""
    ev = p - cost
    if ev <= 0 or cost >= 1.0:
        return 0.0
    full_kelly = ev / (1.0 - cost)
    return round(full_kelly * kelly_frac, 6)


def compute_ev_row(
    p_yes: float,
    p_no: float,
    yes_ask: float,
    no_ask: float,
    fee_rate: float,
    fee_exponent: float,
    min_edge: float,
    kelly_fraction: float,
    bankroll: float,
) -> dict:
    """Compute EV fields for one row. All values in probability units [0, 1]."""
    yes_fee = round(compute_fee(yes_ask, fee_rate, fee_exponent), 6)
    no_fee = round(compute_fee(no_ask, fee_rate, fee_exponent), 6)
    yes_cost = round(yes_ask + yes_fee, 6)
    no_cost = round(no_ask + no_fee, 6)

    yes_edge = round(p_yes - yes_ask, 6)
    no_edge = round(p_no - no_ask, 6)
    yes_ev = round(p_yes - yes_cost, 6)
    no_ev = round(p_no - no_cost, 6)

    kf_yes = compute_kelly(p_yes, yes_cost, kelly_fraction)
    kf_no = compute_kelly(p_no, no_cost, kelly_fraction)

    # Signal: prefer the side with larger edge above threshold
    if yes_edge >= min_edge and yes_edge >= no_edge:
        signal = "BUY_YES"
        kelly_amount = round(kf_yes * bankroll, 2)
    elif no_edge >= min_edge:
        signal = "BUY_NO"
        kelly_amount = round(kf_no * bankroll, 2)
    else:
        signal = "NO_TRADE"
        kelly_amount = 0.0

    return {
        "yes_fee": yes_fee,
        "no_fee": no_fee,
        "yes_cost": yes_cost,
        "no_cost": no_cost,
        "yes_edge": yes_edge,
        "no_edge": no_edge,
        "yes_ev": yes_ev,
        "no_ev": no_ev,
        "signal": signal,
        "kelly_fraction_yes": kf_yes,
        "kelly_fraction_no": kf_no,
        "kelly_amount": kelly_amount,
    }


# ============================================================
# Data loaders
# ============================================================

def load_probability_csv(city: str) -> list[dict]:
    path = PROJ_DIR / "data" / "results" / "probability" / city / "event_probability.csv"
    if not path.exists():
        log.warning(f"event_probability.csv not found: {path}")
        return []
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))
    return rows


def load_prices_csv(path_str: str) -> dict[str, tuple[float, float]]:
    """Returns {market_id: (yes_price, no_price)} from a manual prices CSV file."""
    path = Path(path_str)
    if not path.exists():
        log.warning(f"prices CSV not found: {path}")
        return {}
    prices: dict[str, tuple[float, float]] = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            mid = row.get("market_id", "").strip()
            try:
                yes = float(row.get("yes_price", ""))
                no = float(row.get("no_price", ""))
                prices[mid] = (yes, no)
            except ValueError:
                pass
    return prices


def load_market_prices_csv() -> dict[str, tuple[float, float, str]]:
    """
    Auto-load from 08's output: data/raw/prices/market_prices.csv.
    Returns {market_id: (yes_best_ask, no_best_ask, source_label)}.
    Rows with missing yes_best_ask or no_best_ask are skipped.
    """
    path = PROJ_DIR / "data" / "raw" / "prices" / "market_prices.csv"
    if not path.exists():
        return {}
    prices: dict[str, tuple[float, float, str]] = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            mid = row.get("market_id", "").strip()
            try:
                yes_ask = float(row.get("yes_best_ask", ""))
                no_ask = float(row.get("no_best_ask", ""))
                prices[mid] = (yes_ask, no_ask, "market_prices.csv")
            except (ValueError, TypeError):
                pass
    return prices


def safe_float(val) -> float | None:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ============================================================
# Main logic
# ============================================================

def run(
    cities: str,
    model_name: str,
    yes_price_global: float | None,
    no_price_global: float | None,
    prices_csv_path: str | None,
    min_edge_override: float | None,
    verbose: bool,
    book_source: str = "json",
    books_in_memory: dict | None = None,
    current_obs: dict | None = None,
) -> tuple[bool, list[dict]]:
    """
    book_source:
    - "json"（預設）：從 data/raw/prices/book_state/{market_id}.json 讀（REST 模式）
    - "memory"：從 books_in_memory dict 讀（WS 模式，每個 value 同 book_state JSON schema）
    books_in_memory: market_id → book_state_dict（WS 模式時由 signal_main 傳入）
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=" * 55)
    log.info("11_ev_engine: EV 與交易信號計算")
    log.info(f"⚠️  {MODEL_SCOPE} — 僅供管線驗證，不可用於交易決策")
    log.info("=" * 55)

    cities_filter = {c.strip() for c in cities.split(",") if c.strip()} if cities else set()

    params = load_trading_params(PROJ_DIR / "config" / "trading_params.yaml")
    fee_rate = get_param(params, "fee_rate")
    fee_exponent = get_param(params, "fee_exponent")
    min_edge = min_edge_override if min_edge_override is not None else get_param(params, "min_edge")
    kelly_fraction = get_param(params, "kelly_fraction")
    bankroll = get_param(params, "bankroll")
    fee_mode = params.get("fee_mode", "manual_hardcoded")
    fee_basis = params.get("fee_basis", "unknown")

    depth_fixed_usd = get_param(params, "depth_fixed_usd")

    # 即時觀測：載入城市 timezone 供邏輯裁剪用
    city_timezones = _load_seed_timezones()
    _obs = current_obs or {}
    if _obs:
        log.info(f"Observation clipping enabled for: {sorted(_obs.keys())}")

    signal_min_settlement_hours = float(params.get("signal_min_settlement_hours", 8))
    signal_extreme_price_threshold = float(params.get("signal_extreme_price_threshold", 0.95))
    log.info(f"Params: fee_rate={fee_rate}, fee_exponent={fee_exponent}, min_edge={min_edge}, depth_fixed_usd={depth_fixed_usd}")
    log.info(f"Safety gates: min_settlement_hours={signal_min_settlement_hours}, extreme_price_threshold={signal_extreme_price_threshold}")
    log.info(f"Fee basis: {fee_mode} / {fee_basis}")

    # book_state dir（STEP 6）
    book_state_dir = PROJ_DIR / "data" / "raw" / "prices" / "book_state"
    has_book_state = book_state_dir.exists()
    log.info(f"book_state dir: {'found' if has_book_state else 'not found'} — {book_state_dir.relative_to(PROJ_DIR)}")

    # CSV fallback：預先計算 mtime-based price_age（book_state 找不到時才用）
    csv_price_age_sec = get_price_age_seconds(PROJ_DIR)
    csv_price_status = get_price_status(csv_price_age_sec)

    # Priority 2 (CSV fallback): auto-load from 08's market_prices.csv
    auto_prices = load_market_prices_csv()
    if auto_prices:
        log.info(f"Auto-loaded {len(auto_prices)} market prices from market_prices.csv (CSV fallback)"
                 f" — age={round(csv_price_age_sec, 1) if csv_price_age_sec else '?'}s")
    else:
        log.info("market_prices.csv not found or empty — will use manual price inputs if provided")

    # Priority 2: manual --prices-csv
    manual_csv_prices: dict[str, tuple[float, float]] = {}
    if prices_csv_path:
        manual_csv_prices = load_prices_csv(prices_csv_path)
        log.info(f"Loaded {len(manual_csv_prices)} manual prices from {prices_csv_path}")

    has_global_prices = (yes_price_global is not None and no_price_global is not None)

    # Find cities to process
    prob_root = PROJ_DIR / "data" / "results" / "probability"
    if not prob_root.exists():
        log.warning("data/results/probability/ does not exist — run 10_event_probability first")
        return (True, [])

    city_dirs = sorted(d for d in prob_root.iterdir() if d.is_dir())
    if not city_dirs:
        log.warning("No probability city dirs found")
        return (True, [])

    all_rows: list[dict] = []  # in-memory results across all cities (STEP 10)

    for city_dir in city_dirs:
        city = city_dir.name
        if cities_filter and city not in cities_filter:
            continue

        prob_rows = load_probability_csv(city)
        log.info(f"--- {city}: {len(prob_rows)} probability rows ---")

        output_rows: list[dict] = []

        for prow in prob_rows:
            market_id = prow.get("market_id", "")
            p_yes = safe_float(prow.get("p_yes"))
            p_no = safe_float(prow.get("p_no"))
            if p_yes is None or p_no is None:
                continue

            # ── Price loading（三層 fallback，per-market）──────────────────
            # Priority 1: book_state JSON（snapshot_fetch_time_utc 精確計時）
            # Priority 2: market_prices.csv（mtime 近似計時）
            # Priority 3: CLI 手動輸入
            price_source = None
            yes_ask = None
            no_ask = None
            price_age_sec_market: float | None = None
            book_source_market: str = ""
            book_complete_market: bool = True  # default: assume complete (CSV fallback has no info)
            yes_asks_list: list[dict] = []  # for fill_simulator (STEP 9)
            no_asks_list: list[dict] = []

            if book_source == "memory" and books_in_memory is not None:
                book = books_in_memory.get(market_id)
                if book is None:
                    log.warning(f"WS mode but no in-memory book for {market_id} — skipping price")
            else:
                book = load_book_state(market_id, book_state_dir) if has_book_state else None
            yes_best_bid: Optional[float] = None
            no_best_bid: Optional[float] = None
            if book and book.get("fetch_status") == "ok":
                yes_ask = book.get("yes_best_ask")
                no_ask = book.get("no_best_ask")
                yes_best_bid = book.get("yes_best_bid")
                no_best_bid = book.get("no_best_bid")
                if yes_ask is not None and no_ask is not None:
                    price_source = "book_state"
                    book_source_market = book.get("source", "rest_snapshot")
                    price_age_sec_market = calc_price_age_from_utc_str(
                        book.get("snapshot_fetch_time_utc", "")
                    )
                    book_complete_market = book.get("book_complete", True)
                    # Extract ordered ask levels for fill_simulator
                    yes_asks_list = [
                        {"price": float(a["price"]), "size": float(a["size"])}
                        for a in book.get("yes_asks", [])
                        if a.get("price") and a.get("size")
                    ]
                    no_asks_list = [
                        {"price": float(a["price"]), "size": float(a["size"])}
                        for a in book.get("no_asks", [])
                        if a.get("price") and a.get("size")
                    ]
                    # Ensure sorted: buy_asks low→high
                    yes_asks_list.sort(key=lambda x: x["price"])
                    no_asks_list.sort(key=lambda x: x["price"])

            if price_source is None and market_id in auto_prices:
                yes_ask, no_ask, _ = auto_prices[market_id]
                price_source = "market_prices.csv"
                book_source_market = "csv_fallback"
                price_age_sec_market = csv_price_age_sec

            if price_source is None and market_id in manual_csv_prices:
                yes_ask, no_ask = manual_csv_prices[market_id]
                price_source = "manual --prices-csv"
                book_source_market = "manual"
                price_age_sec_market = None  # 無時間資訊

            if price_source is None and has_global_prices:
                yes_ask, no_ask = yes_price_global, no_price_global  # type: ignore[assignment]
                price_source = "manual --yes-price/--no-price"
                book_source_market = "manual"
                price_age_sec_market = None

            # per-market price_status
            price_status_market = get_price_status(price_age_sec_market)
            price_age_display_market = round(price_age_sec_market, 1) if price_age_sec_market is not None else ""

            if price_source and yes_ask is not None:
                log.info(f"  {city} {market_id[:12]}...: source={price_source} "
                         f"yes_ask={yes_ask} no_ask={no_ask} age={price_age_display_market}s")
            else:
                log.info(f"  {city} {market_id[:12]}...: no price available, output naive_fair only")

            metric_type = prow.get("metric_type", "daily_high")
            row_fee_status = get_fee_status(metric_type)

            out_row: dict = {
                "market_id": market_id,
                "city": city,
                "market_date_local": prow.get("market_date_local", ""),
                "market_type": prow.get("market_type", ""),
                "threshold": prow.get("threshold", ""),
                "range_low": prow.get("range_low", ""),
                "range_high": prow.get("range_high", ""),
                "temp_unit": prow.get("temp_unit", "C"),
                "model_name": prow.get("model_name", model_name),
                "model_scope": MODEL_SCOPE,
                "predicted_daily_high": prow.get("predicted_daily_high", ""),
                "lead_day": prow.get("lead_day", ""),
                "p_yes": p_yes,
                "p_no": p_no,
                "naive_fair_yes": p_yes,
                "naive_fair_no": p_no,
                "fee_rate_used": fee_rate,
                "fee_exponent_used": fee_exponent,
                "fee_mode": fee_mode,
                "fee_basis": fee_basis,
                "fee_status": row_fee_status,
                "price_status": price_status_market,
                "price_age_seconds": price_age_display_market,
                "book_source": book_source_market,
            }

            # 即時計算 lead_hours_to_settlement（不再依賴 probability CSV 欄位）
            _city_tz = city_timezones.get(city, "")
            _market_date_str = out_row.get("market_date_local", "")
            lead_hours_to_settlement = _calc_hours_to_settlement(_market_date_str, _city_tz)
            out_row["lead_hours_to_settlement"] = (
                round(lead_hours_to_settlement, 2) if lead_hours_to_settlement is not None else ""
            )

            if yes_best_bid is not None:
                out_row["yes_best_bid"] = round(yes_best_bid, 6)
            if no_best_bid is not None:
                out_row["no_best_bid"] = round(no_best_bid, 6)

            # ── 即時觀測邏輯裁剪（在 EV 計算之前）────────────────────────
            out_row["observed_high_c"] = ""
            out_row["observation_source"] = ""
            out_row["observation_clipped"] = False
            out_row["clip_reason"] = ""
            _apply_observation_clipping(out_row, _obs, city_timezones)
            # 更新 local p_yes/p_no（裁剪後可能已改變）
            p_yes = out_row["p_yes"]
            p_no = out_row["p_no"]

            if yes_ask is not None and no_ask is not None:
                out_row["yes_ask_price"] = round(yes_ask, 6)
                out_row["no_ask_price"] = round(no_ask, 6)
                ev_fields = compute_ev_row(
                    p_yes=p_yes,
                    p_no=p_no,
                    yes_ask=yes_ask,
                    no_ask=no_ask,
                    fee_rate=fee_rate,
                    fee_exponent=fee_exponent,
                    min_edge=min_edge,
                    kelly_fraction=kelly_fraction,
                    bankroll=bankroll,
                )
                out_row.update(ev_fields)
                sig_status = get_signal_status(
                    price_status_market, row_fee_status,
                    has_price=True, book_complete=book_complete_market,
                )
                sig_action = get_signal_action(sig_status, out_row.get("signal", "NO_TRADE"))

                # ── 安全閘門 1：距結算時間過短 ─────────────────────────────
                if (sig_action != "SUPPRESSED"
                        and lead_hours_to_settlement is not None
                        and lead_hours_to_settlement < signal_min_settlement_hours):
                    sig_status = "too_close_to_settlement"
                    sig_action = "SUPPRESSED"

                # ── 安全閘門 2：市場極端價格（已定局）────────────────────────
                if sig_action != "SUPPRESSED":
                    if (yes_ask > signal_extreme_price_threshold
                            or no_ask > signal_extreme_price_threshold):
                        sig_status = "market_extreme"
                        sig_action = "SUPPRESSED"

                out_row["signal_status"] = sig_status
                out_row["signal_action"] = sig_action
                out_row["signal"] = sig_action  # 向後相容

                # ── STEP 9：深度分析（需 book_state + fill_simulator）──────────
                if _HAS_FILL_SIM and yes_asks_list and no_asks_list:
                    try:
                        yes_sweet = _simulate_fill(yes_asks_list, p_yes, fee_rate, fee_exponent, mode="sweet_spot")
                        yes_fixed = _simulate_fill(yes_asks_list, p_yes, fee_rate, fee_exponent, mode="fixed_depth", fixed_depth_usd=depth_fixed_usd)
                        no_sweet = _simulate_fill(no_asks_list, p_no, fee_rate, fee_exponent, mode="sweet_spot")
                        no_fixed = _simulate_fill(no_asks_list, p_no, fee_rate, fee_exponent, mode="fixed_depth", fixed_depth_usd=depth_fixed_usd)

                        yes_depth_usd = sum(a["price"] * a["size"] for a in yes_asks_list)
                        no_depth_usd = sum(a["price"] * a["size"] for a in no_asks_list)

                        out_row.update({
                            "yes_depth_usd": round(yes_depth_usd, 2),
                            "yes_sweet_shares": yes_sweet.total_shares,
                            "yes_sweet_usd": yes_sweet.total_notional,
                            "yes_sweet_avg_price": yes_sweet.avg_fill_price,
                            "yes_sweet_ev": yes_sweet.cumulative_ev,
                            "yes_sweet_exhausted": yes_sweet.depth_exhausted,
                            "yes_fixed_shares": yes_fixed.total_shares,
                            "yes_fixed_usd": yes_fixed.total_notional,
                            "yes_fixed_avg_price": yes_fixed.avg_fill_price,
                            "yes_fixed_ev": yes_fixed.cumulative_ev,
                            "yes_fixed_exhausted": yes_fixed.depth_exhausted,
                            "no_depth_usd": round(no_depth_usd, 2),
                            "no_sweet_shares": no_sweet.total_shares,
                            "no_sweet_usd": no_sweet.total_notional,
                            "no_sweet_avg_price": no_sweet.avg_fill_price,
                            "no_sweet_ev": no_sweet.cumulative_ev,
                            "no_sweet_exhausted": no_sweet.depth_exhausted,
                            "no_fixed_shares": no_fixed.total_shares,
                            "no_fixed_usd": no_fixed.total_notional,
                            "no_fixed_avg_price": no_fixed.avg_fill_price,
                            "no_fixed_ev": no_fixed.cumulative_ev,
                            "no_fixed_exhausted": no_fixed.depth_exhausted,
                        })
                    except Exception as e:
                        log.warning(f"  fill_simulator failed for {market_id}: {e}")
            else:
                sig_status = get_signal_status(
                    price_status_market, row_fee_status, has_price=False,
                )
                out_row.update({
                    "yes_ask_price": "",
                    "no_ask_price": "",
                    "yes_fee": "",
                    "no_fee": "",
                    "yes_cost": "",
                    "no_cost": "",
                    "yes_edge": "",
                    "no_edge": "",
                    "yes_ev": "",
                    "no_ev": "",
                    "signal": "SUPPRESSED",
                    "kelly_fraction_yes": "",
                    "kelly_fraction_no": "",
                    "kelly_amount": "",
                    "signal_status": sig_status,
                    "signal_action": "SUPPRESSED",
                })

            output_rows.append(out_row)

        if output_rows:
            _write_output(city, output_rows)
        else:
            log.warning(f"  {city}: 0 EV rows — skipping write (preserving existing)")
        all_rows.extend(output_rows)  # STEP 10：收集 in-memory 結果
        log.info(f"  {city}: {len(output_rows)} EV signal rows")

        # Summary
        if output_rows:
            buy_yes = sum(1 for r in output_rows if r.get("signal_action") == "BUY_YES")
            buy_no = sum(1 for r in output_rows if r.get("signal_action") == "BUY_NO")
            no_trade = sum(1 for r in output_rows if r.get("signal_action") == "NO_TRADE")
            suppressed = sum(1 for r in output_rows if r.get("signal_action") == "SUPPRESSED")
            if buy_yes + buy_no + no_trade > 0:
                log.info(f"  Signals: BUY_YES={buy_yes}, BUY_NO={buy_no}, NO_TRADE={no_trade}, SUPPRESSED={suppressed}")
            elif suppressed > 0:
                log.info(f"  All signals SUPPRESSED ({suppressed} rows) — signal_status: {output_rows[0].get('signal_status', '?')}")

    log.info("11_ev_engine done.")
    return (True, all_rows)


def _write_output(city: str, rows: list[dict]) -> None:
    out_dir = PROJ_DIR / "data" / "results" / "ev_signals" / city
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "ev_signals.csv"
    with tempfile.NamedTemporaryFile(
        "w", dir=str(out_dir), suffix=".tmp", delete=False, encoding="utf-8", newline=""
    ) as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        tmp = f.name
    os.replace(tmp, str(out_path))
    log.info(f"  Written: {out_path.relative_to(PROJ_DIR)} ({len(rows)} rows)")


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="EV and trading signal engine (11_ev_engine)")
    p.add_argument("--cities", type=str, default="", help="城市 filter（逗號分隔）")
    p.add_argument("--model", type=str, default="empirical",
                   help="使用哪個模型（預設: empirical）")
    p.add_argument("--yes-price", type=float, default=None, dest="yes_price",
                   help="手動 YES ask price（對所有市場套用）")
    p.add_argument("--no-price", type=float, default=None, dest="no_price",
                   help="手動 NO ask price（對所有市場套用）")
    p.add_argument("--prices-csv", type=str, default=None, dest="prices_csv",
                   help="每市場個別價格 CSV（columns: market_id, yes_price, no_price）")
    p.add_argument("--min-edge", type=float, default=None, dest="min_edge",
                   help="最小進場門檻（可選，預設從 config 讀）")
    p.add_argument("--fee-regression", action="store_true", dest="fee_regression",
                   help="跑 fee regression test（驗算官方費率表 3 筆）並退出")
    p.add_argument("--verbose", action="store_true")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    if args.fee_regression:
        ok = run_fee_regression()
        sys.exit(0 if ok else 1)
    ok, _ = run(
        cities=args.cities,
        model_name=args.model,
        yes_price_global=args.yes_price,
        no_price_global=args.no_price,
        prices_csv_path=args.prices_csv,
        min_edge_override=args.min_edge,
        verbose=args.verbose,
    )
    sys.exit(0 if ok else 1)
