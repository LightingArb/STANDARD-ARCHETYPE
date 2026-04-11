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

# ── _lib path（ecdf_query, fill_simulator 等）──────────────────
_lib_dir = PROJ_DIR / "_lib"
if str(_lib_dir) not in sys.path:
    sys.path.insert(0, str(_lib_dir))

# ── Realtime ECDF：module-level lazy cache（跨 run() 呼叫持久）──
_empirical_cache: dict[str, dict] = {}
_empirical_mtime: dict[str, float] = {}


def _get_empirical_model(city: str) -> dict | None:
    """
    Lazy-load empirical_model.json，mtime 有變化時才重讀。
    Module-level cache 在 signal_main in-process 載入時跨整個進程生命週期持久。
    """
    from _lib.ecdf_query import load_empirical_model as _load_em
    model_path = (
        PROJ_DIR / "data" / "models" / "empirical" / city / "empirical_model.json"
    )
    if not model_path.exists():
        return None
    try:
        current_mtime = model_path.stat().st_mtime
    except OSError:
        return None
    if city in _empirical_cache and current_mtime == _empirical_mtime.get(city, 0):
        return _empirical_cache[city]
    model = _load_em(city)
    if model:
        _empirical_cache[city] = model
        _empirical_mtime[city] = current_mtime
    return model
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
    "precision_half",   # 原始單位（供 obs clipping 用）
    "model_name",
    "model_scope",
    "predicted_daily_high",
    "lead_day",
    "lead_hours_to_settlement",
    "bucket_used",
    "bucket_level_used",
    "bucket_sample_count",
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
    "fair_exit_yes",    # p_yes 扣出場手續費（給 Trailing TP 用的理論出場價）
    "fair_exit_no",     # p_no 扣出場手續費
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
    "obs_time_utc",
    "observation_clipped",
    "clip_reason",
    "observation_source",
    # Remaining Gain 機率模式（v7.5）
    "probability_mode",             # empirical / remaining_gain / empirical_fallback
    "remaining_gain_hour_used",     # 查詢用的 local_hour（診斷用）
    "remaining_gain_sample_count",  # bucket 樣本數（診斷用）
    # 版本追蹤（P0-6：偵測 probability/ev_signals 脫鉤）
    "generated_utc",           # 本輪 11 產生此 row 的 UTC 時間
    "upstream_generated_utc",  # 10 產生 probability 的 UTC 時間（從 prow 帶上來）
    # Phase 1-B：Lock Range + 三模式信號
    "error_q05",               # 預報誤差 5th percentile（C，來自 10 的插值桶）
    "error_q95",               # 預報誤差 95th percentile（C，來自 10 的插值桶）
    "bucket_interpolated",     # 是否用插值桶（True/False，來自 10）
    "lock_range_low",          # predicted + error_q05（C）
    "lock_range_high",         # predicted + error_q95（C）
    "lock_direction",          # YES / NO / NONE（90% 區間是否完全落在一側）
    "signal_action_scatter",   # 散彈模式 = signal_action（現有邏輯不變）
    "signal_action_sniper",    # 點射模式：lock_direction 確認方向才動作
    "signal_action_precision", # 精準模式：有方向鎖定才動作（scatter + 方向過濾）
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
# Remaining Gain 模型（v7.5）
# ============================================================

def _load_remaining_gain_models() -> dict[str, dict]:
    """讀取所有城市的 remaining_gain model JSON。回傳 {city: model_dict}。"""
    rg_dir = PROJ_DIR / "data" / "models" / "remaining_gain"
    models: dict[str, dict] = {}
    if not rg_dir.exists():
        return models
    for city_dir in rg_dir.iterdir():
        if not city_dir.is_dir():
            continue
        model_path = city_dir / "remaining_gain_model.json"
        if model_path.exists():
            try:
                models[city_dir.name] = json.loads(model_path.read_text(encoding="utf-8"))
            except Exception as e:
                log.warning(f"_load_remaining_gain_models: {city_dir.name}: {e}")
    return models


def _calc_remaining_gain_p(
    rg_model: dict,
    local_hour: int,
    current_max_c: float,
    threshold_c: float,
    event_type: str,
    precision_half_c: float,
    range_low_c: float = 0.0,
    range_high_c: float = 0.0,
    min_bucket_samples: int = 30,
) -> Optional[tuple[float, int]]:
    """
    用 remaining_gain ECDF 計算 p_yes。
    回傳 (p_yes, n_samples)；None = 無法計算（bucket 不存在或樣本不夠）。

    所有溫度參數均為攝氏。
    """
    bucket = rg_model.get("buckets", {}).get(str(local_hour))
    if not bucket:
        return None

    sorted_gains: list[float] = bucket.get("sorted_gains", [])
    n = len(sorted_gains)
    if n < min_bucket_samples:
        return None

    def p_exceed(need_gain: float) -> float:
        """P(remaining_gain >= need_gain)"""
        if need_gain <= 0:
            return 1.0
        count = sum(1 for g in sorted_gains if g >= need_gain)
        return count / n

    if event_type == "higher":
        need = (threshold_c - precision_half_c) - current_max_c
        p = p_exceed(need)

    elif event_type == "below":
        need = (threshold_c + precision_half_c) - current_max_c
        p = 1.0 - p_exceed(need)

    elif event_type == "exact":
        low_need = (threshold_c - precision_half_c) - current_max_c
        high_need = (threshold_c + precision_half_c) - current_max_c
        p = p_exceed(low_need) - p_exceed(high_need)

    elif event_type == "range":
        low_need = (range_low_c - precision_half_c) - current_max_c
        high_need = (range_high_c + precision_half_c) - current_max_c
        p = p_exceed(low_need) - p_exceed(high_need)

    else:
        return None

    return (max(0.0, min(1.0, p)), n)


def _is_obs_fresh(
    obs_info: dict,
    *,
    fetch_max_minutes: Optional[float] = None,
    obs_time_max_minutes: Optional[float] = None,
) -> bool:
    """
    統一 freshness 檢查（C-fix-3）。接受 epoch 或 ISO（透過 parse_obs_time_utc）。

    fetch_max_minutes: None → 跳過 fetched_at 檢查。
    obs_time_max_minutes: None → 跳過 obs_time_utc 檢查。
    任一啟用的檢查失敗 → False。
    """
    from _lib.obs_time_utils import parse_obs_time_utc
    now = datetime.now(timezone.utc)

    if fetch_max_minutes is not None:
        fetched = obs_info.get("fetched_at_utc") or obs_info.get("fetched_at", "")
        ft = parse_obs_time_utc(fetched)
        if ft is None or (now - ft).total_seconds() > fetch_max_minutes * 60:
            return False

    if obs_time_max_minutes is not None:
        obs_time = obs_info.get("obs_time_utc") or obs_info.get("obs_time", "")
        if obs_time:
            ot = parse_obs_time_utc(obs_time)
            if ot is not None and (now - ot).total_seconds() > obs_time_max_minutes * 60:
                return False

    return True


def _is_obs_fresh_for_rg(obs_info: dict, params: dict) -> bool:
    """
    remaining_gain 用的雙重 freshness 檢查。委派給 _is_obs_fresh()。
    """
    return _is_obs_fresh(
        obs_info,
        fetch_max_minutes=float(params.get("remaining_gain_obs_max_age_minutes", 20)),
        obs_time_max_minutes=float(params.get("remaining_gain_obs_time_max_age_minutes", 30)),
    )


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

    重點：邊界要考慮 precision_half，與 10_event_probability 的機率計算對齊。
    precision_half 由 10 寫入 CSV（原始單位 F 或 C），這裡直接使用。

    YES 窗口（與 10 的 compute_p_yes 一致）：
      exact X:   actual ∈ [X-h, X+h)     → clip NO 當 observed >= X+h
      range L-H: actual ∈ [L-h, H+h)     → clip NO 當 observed >= H+h
      higher X:  actual >= X-h           → clip YES 當 observed >= X-h
      below X:   actual <  X+h           → clip NO 當 observed >= X+h
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
    if not obs_info or obs_info.get("high_c") is None:
        return

    # P1-1：TTL 檢查 — 不管資料多舊都 clip 是危險的。
    # 即使物理上「最大溫只會增加」讓 stale 資料偏向 under-clip（安全方向），
    # 但跨日殘留 / schema bug 造成的 stale 可能完全錯方向。
    # 條件：
    #   1. status == "stale"（collector_main 標記的）→ 直接跳過
    #   2. obs_time_utc 超過 2 小時 → 跳過
    if str(obs_info.get("status", "ok")).lower() == "stale":
        log.debug(f"  obs clip skip: {city} status=stale")
        return
    if not _is_obs_fresh(obs_info, obs_time_max_minutes=120.0):
        log.debug(f"  obs clip skip: {city} obs_time_utc stale (>2h)")
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
    # precision_half 由 10 寫入（原始單位，F 或 C），fallback 0.5 = 1° precision
    half = safe_float(out_row.get("precision_half"))
    if half is None:
        half = 0.5

    clipped = False
    clip_reason = ""

    if market_type == "exact":
        # YES 窗口上界 = threshold + half。observed >= 上界 → 不可能落入窗口 → clip NO
        if threshold is not None and observed_compare >= threshold + half:
            out_row["p_yes"] = 0.0
            out_row["p_no"] = 1.0
            clipped = True
            clip_reason = f"exact {threshold}±{half} < obs {observed_compare:.1f}"

    elif market_type == "below":
        # YES 窗口上界 = threshold + half
        if threshold is not None and observed_compare >= threshold + half:
            out_row["p_yes"] = 0.0
            out_row["p_no"] = 1.0
            clipped = True
            clip_reason = f"below {threshold}+{half} <= obs {observed_compare:.1f}"

    elif market_type == "higher":
        # YES 下界 = threshold - half。observed >= 下界 → 確定落入 YES → clip YES
        if threshold is not None and observed_compare >= threshold - half:
            out_row["p_yes"] = 1.0
            out_row["p_no"] = 0.0
            clipped = True
            clip_reason = f"higher {threshold}-{half} <= obs {observed_compare:.1f}"

    elif market_type == "range":
        # YES 窗口上界 = range_high + half
        if range_high is not None and observed_compare >= range_high + half:
            out_row["p_yes"] = 0.0
            out_row["p_no"] = 1.0
            clipped = True
            clip_reason = f"range_high {range_high}+{half} < obs {observed_compare:.1f}"

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
# Phase 1-B：Lock Range + Direction + Signal Modes
# ============================================================

def compute_lock_range(
    predicted_c: float,
    error_q05: float,
    error_q95: float,
) -> tuple[float, float]:
    """
    90% 預測區間（攝氏）。
    lock_range_low  = predicted + error_q05（誤差 5th pct，通常為負）
    lock_range_high = predicted + error_q95（誤差 95th pct，通常為正）
    """
    return round(predicted_c + error_q05, 4), round(predicted_c + error_q95, 4)


def compute_lock_direction(
    market_type: str,
    threshold: float | None,
    range_low: float | None,
    range_high: float | None,
    precision_half: float,
    lock_low_c: float,
    lock_high_c: float,
    temp_unit: str = "C",
) -> str:
    """
    判斷 90% 預測區間是否完全落在市場的 YES 或 NO 一側。
    lock_low_c / lock_high_c 為攝氏；若合約單位為 F 則自動轉換後比較。
    回傳 "YES" / "NO" / "NONE"。

    YES 窗口定義（與 10_event_probability 和 obs clipping 一致）：
      higher X:  actual >= X - precision_half
      below  X:  actual <  X + precision_half
      exact  X:  actual in [X - half, X + half)
      range L-H: actual in [L - half, H + half)
    """
    def _to_market(c: float) -> float:
        return c * 9.0 / 5.0 + 32.0 if temp_unit == "F" else c

    lo = _to_market(lock_low_c)
    hi = _to_market(lock_high_c)

    if market_type == "higher":
        if threshold is None:
            return "NONE"
        yes_lo = threshold - precision_half
        if lo >= yes_lo:
            return "YES"
        if hi < yes_lo:
            return "NO"

    elif market_type == "below":
        if threshold is None:
            return "NONE"
        yes_hi = threshold + precision_half
        if hi < yes_hi:
            return "YES"
        if lo >= yes_hi:
            return "NO"

    elif market_type == "exact":
        if threshold is None:
            return "NONE"
        yes_lo = threshold - precision_half
        yes_hi = threshold + precision_half
        if lo >= yes_hi or hi < yes_lo:
            return "NO"
        if lo >= yes_lo and hi < yes_hi:
            return "YES"

    elif market_type == "range":
        if range_low is None or range_high is None:
            return "NONE"
        yes_lo = range_low - precision_half
        yes_hi = range_high + precision_half
        if lo >= yes_hi or hi < yes_lo:
            return "NO"
        if lo >= yes_lo and hi < yes_hi:
            return "YES"

    return "NONE"


def _apply_signal_modes(base_signal: str, lock_direction: str) -> tuple[str, str, str]:
    """
    三模式信號計算。

    scatter  （散彈）：= base_signal，等同現有邏輯，方向最寬。
    precision（精準）：有方向鎖定（lock_direction != NONE）才動作，否則 SUPPRESSED。
    sniper   （點射）：lock_direction 明確確認與下注方向一致才動作；不確認 → NO_TRADE。

    優先序寬→窄：scatter > precision > sniper。
    """
    scatter = base_signal

    # precision：需要有方向鎖定
    if base_signal == "SUPPRESSED" or lock_direction == "NONE":
        precision = "SUPPRESSED"
    else:
        precision = base_signal

    # sniper：lock_direction 必須與下注側吻合
    if base_signal == "SUPPRESSED":
        sniper = "SUPPRESSED"
    elif base_signal == "BUY_YES" and lock_direction == "YES":
        sniper = "BUY_YES"
    elif base_signal == "BUY_NO" and lock_direction == "NO":
        sniper = "BUY_NO"
    else:
        sniper = "NO_TRADE"

    return scatter, sniper, precision


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

    # P0-6：本輪 run 的時間戳（所有 out_row 共用）
    run_generated_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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

    signal_suppress_hours = float(params.get("signal_suppress_hours", 6))
    signal_warning_hours = float(params.get("signal_warning_hours", 8))
    signal_extreme_price_threshold = float(params.get("signal_extreme_price_threshold", 0.95))
    log.info(f"Params: fee_rate={fee_rate}, fee_exponent={fee_exponent}, min_edge={min_edge}, depth_fixed_usd={depth_fixed_usd}")
    log.info(f"Safety gates: suppress_hours={signal_suppress_hours}, warning_hours={signal_warning_hours}, extreme_price_threshold={signal_extreme_price_threshold}")
    log.info(f"Fee basis: {fee_mode} / {fee_basis}")

    # Remaining Gain 參數
    rg_enabled = str(params.get("remaining_gain_enabled", "false")).lower() == "true"
    rg_max_lead = float(params.get("remaining_gain_max_lead_hours", 6))
    rg_min_hour = int(float(params.get("remaining_gain_min_local_hour", 7)))
    rg_min_bucket_samples = int(float(params.get("remaining_gain_min_bucket_samples", 30)))
    rg_models = _load_remaining_gain_models() if rg_enabled else {}
    if rg_enabled:
        log.info(f"Remaining gain enabled: max_lead={rg_max_lead}h, min_local_hour={rg_min_hour}, models_loaded={len(rg_models)}")

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
                "precision_half": prow.get("precision_half", ""),
                "model_name": prow.get("model_name", model_name),
                "model_scope": MODEL_SCOPE,
                "predicted_daily_high": prow.get("predicted_daily_high", ""),
                "lead_day": prow.get("lead_day", ""),
                "bucket_used": prow.get("bucket_used", ""),
                "bucket_level_used": prow.get("bucket_level_used", ""),
                "bucket_sample_count": prow.get("bucket_sample_count", ""),
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
                "generated_utc": run_generated_utc,
                "upstream_generated_utc": prow.get("generated_utc", ""),
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

            # ── Phase 1-B：Lock Range & Direction（不依賴價格，早於 EV 計算）──
            _pred_c = safe_float(prow.get("predicted_daily_high"))
            _eq05 = safe_float(prow.get("error_q05"))
            _eq95 = safe_float(prow.get("error_q95"))
            out_row["error_q05"] = _eq05 if _eq05 is not None else ""
            out_row["error_q95"] = _eq95 if _eq95 is not None else ""
            out_row["bucket_interpolated"] = prow.get("bucket_interpolated", "")

            if _pred_c is not None and _eq05 is not None and _eq95 is not None:
                _lock_lo, _lock_hi = compute_lock_range(_pred_c, _eq05, _eq95)
                _lock_dir = compute_lock_direction(
                    market_type=out_row.get("market_type", ""),
                    threshold=safe_float(out_row.get("threshold")),
                    range_low=safe_float(out_row.get("range_low")),
                    range_high=safe_float(out_row.get("range_high")),
                    precision_half=safe_float(out_row.get("precision_half")) or 0.5,
                    lock_low_c=_lock_lo,
                    lock_high_c=_lock_hi,
                    temp_unit=out_row.get("temp_unit", "C"),
                )
                out_row["lock_range_low"] = _lock_lo
                out_row["lock_range_high"] = _lock_hi
                out_row["lock_direction"] = _lock_dir
                log.debug(
                    f"  LockRange: {city} predicted={_pred_c:.2f}°C "
                    f"[{_lock_lo:.2f}, {_lock_hi:.2f}] → {_lock_dir}"
                )
            else:
                out_row["lock_range_low"] = ""
                out_row["lock_range_high"] = ""
                out_row["lock_direction"] = "NONE"

            # ── 即時觀測邏輯裁剪（在 EV 計算之前）────────────────────────
            out_row["observed_high_c"] = ""
            out_row["obs_time_utc"] = ""
            out_row["observation_source"] = ""
            out_row["observation_clipped"] = False
            out_row["clip_reason"] = ""
            _apply_observation_clipping(out_row, _obs, city_timezones)
            # obs_time_utc：從即時觀測 cache 補入（不在 clipping 函式內，避免修改簽名）
            _city_obs = _obs.get(city)
            if _city_obs:
                out_row["obs_time_utc"] = _city_obs.get("obs_time_utc", "")
            # 更新 local p_yes/p_no（裁剪後可能已改變）
            p_yes = out_row["p_yes"]
            p_no = out_row["p_no"]

            # ── Realtime ECDF 即時機率（Phase 1.5）────────────────────────────
            # use_realtime_probability: true → 每輪重算 ECDF p_yes（不依賴 batch 固定值）
            # 失敗時 fallback 到 batch_ecdf（prow 的固定值）
            probability_mode = "batch_ecdf"

            _use_rt = str(params.get("use_realtime_probability", "false")).lower() == "true"
            if _use_rt:
                _rt_model = _get_empirical_model(city)
                if _rt_model is not None:
                    from _lib.ecdf_query import (
                        get_bucket_interpolated as _gbi,
                        compute_p_yes as _cpy,
                        _f_to_c as _ftc,
                        _percentile_from_sorted as _pfs,
                    )
                    # lead_hours 即時算，lead_day 讀 prow（避免邊界 bug）
                    _rt_lead_hours = lead_hours_to_settlement
                    _rt_lead_day = int(safe_float(prow.get("lead_day")) or 1)

                    # predicted_daily_high 永遠是 °C，不轉換
                    _rt_predicted = safe_float(prow.get("predicted_daily_high"))

                    # 合約門檻只轉 F→C
                    _rt_temp_unit = prow.get("temp_unit", "C")
                    _rt_threshold = safe_float(prow.get("threshold"))
                    _rt_range_low = safe_float(prow.get("range_low"))
                    _rt_range_high = safe_float(prow.get("range_high"))
                    _rt_half_w = safe_float(prow.get("precision_half")) or 0.5
                    if _rt_temp_unit == "F":
                        if _rt_threshold is not None:
                            _rt_threshold = _ftc(_rt_threshold)
                        if _rt_range_low is not None:
                            _rt_range_low = _ftc(_rt_range_low)
                        if _rt_range_high is not None:
                            _rt_range_high = _ftc(_rt_range_high)
                        _rt_half_w = _rt_half_w * 5.0 / 9.0

                    # 三層插值 bucket 查找
                    _rt_bucket, _rt_key, _rt_interp, _rt_meta = _gbi(
                        _rt_model, _rt_lead_day, _rt_lead_hours
                    )
                    _rt_sorted_errors = _rt_bucket.get("sorted_errors", []) if _rt_bucket else []

                    if _rt_sorted_errors and _rt_predicted is not None:
                        _rt_p_yes = _cpy(
                            _rt_sorted_errors,
                            _rt_predicted,
                            prow.get("market_type", ""),
                            _rt_threshold,
                            _rt_range_low,
                            _rt_range_high,
                            _rt_half_w,
                        )
                        _rt_p_no = round(1.0 - _rt_p_yes, 6)
                        # 同步更新 out_row 及本地變數（RG 仍可覆蓋）
                        out_row["p_yes"] = _rt_p_yes
                        out_row["p_no"] = _rt_p_no
                        p_yes = _rt_p_yes
                        p_no = _rt_p_no
                        # 更新 error percentiles（即時 bucket 的插值結果）
                        out_row["error_q05"] = round(_pfs(_rt_sorted_errors, 0.05), 4)
                        out_row["error_q95"] = round(_pfs(_rt_sorted_errors, 0.95), 4)
                        probability_mode = "realtime_ecdf"
                        log.debug(
                            f"  RT-ECDF: {city} lead_h={_rt_lead_hours} "
                            f"ld={_rt_lead_day} p_yes={_rt_p_yes:.4f}"
                        )
                    else:
                        log.debug(
                            f"  RT-ECDF: {city} no bucket/predicted — fallback batch_ecdf"
                        )
                else:
                    log.debug(f"  RT-ECDF: {city} model not found — fallback batch_ecdf")

            # ── Remaining Gain 機率修正（v7.5）────────────────────────────────
            # 條件：enabled + 距結算 <= rg_max_lead + 未被 obs clip + 有 RG 模型 + 有觀測
            # RG 優先序高於 realtime_ecdf（覆蓋）
            rg_hour_used = ""
            rg_sample_count = ""

            if (rg_enabled
                    and lead_hours_to_settlement is not None
                    and lead_hours_to_settlement <= rg_max_lead
                    and not out_row.get("observation_clipped", False)
                    and city in rg_models
                    and _obs.get(city) is not None):

                _city_obs_rg = _obs[city]
                _city_tz_str = city_timezones.get(city, "")
                local_hour = -1
                if _city_tz_str:
                    try:
                        import zoneinfo as _zi
                        local_hour = datetime.now(_zi.ZoneInfo(_city_tz_str)).hour
                    except Exception:
                        pass

                if local_hour >= rg_min_hour and _is_obs_fresh_for_rg(_city_obs_rg, params):
                    try:
                        current_max_c = float(_city_obs_rg.get("high_c", 0) or 0)
                        temp_unit = out_row.get("temp_unit", "C")

                        def _to_c(val):
                            """合約門檻轉攝氏（原本是 F 時）"""
                            v = safe_float(val)
                            if v is None:
                                return None
                            return (v - 32.0) * 5.0 / 9.0 if temp_unit == "F" else v

                        def _half_to_c(val):
                            """precision_half 轉攝氏（1°F = 5/9°C）"""
                            v = safe_float(val)
                            if v is None:
                                return 0.5
                            return v * 5.0 / 9.0 if temp_unit == "F" else v

                        market_type = out_row.get("market_type", "")
                        threshold_c = _to_c(out_row.get("threshold"))
                        half_c = _half_to_c(out_row.get("precision_half"))
                        range_low_c = _to_c(out_row.get("range_low")) or 0.0
                        range_high_c = _to_c(out_row.get("range_high")) or 0.0

                        if threshold_c is not None or market_type == "range":
                            rg_result = _calc_remaining_gain_p(
                                rg_models[city],
                                local_hour,
                                current_max_c,
                                threshold_c or 0.0,
                                market_type,
                                half_c,
                                range_low_c,
                                range_high_c,
                                rg_min_bucket_samples,
                            )
                            if rg_result is not None:
                                rg_p_yes, rg_n = rg_result
                                out_row["p_yes"] = round(rg_p_yes, 6)
                                out_row["p_no"] = round(1.0 - rg_p_yes, 6)
                                p_yes = out_row["p_yes"]
                                p_no = out_row["p_no"]
                                probability_mode = "remaining_gain"
                                rg_hour_used = local_hour
                                rg_sample_count = rg_n
                                log.debug(
                                    f"  RG: {city} h={local_hour} "
                                    f"cur={current_max_c:.1f}°C thr={threshold_c} "
                                    f"p_yes={rg_p_yes:.4f} (n={rg_n})"
                                )
                            # else: probability_mode 保留 realtime_ecdf / batch_ecdf
                        # else: probability_mode 保留 realtime_ecdf / batch_ecdf
                    except Exception as e:
                        log.warning(f"  RG calc error {city}: {e}")
                        # probability_mode 保留 realtime_ecdf / batch_ecdf
                # else: probability_mode 保留 realtime_ecdf / batch_ecdf

            out_row["probability_mode"] = probability_mode
            out_row["remaining_gain_hour_used"] = rg_hour_used
            out_row["remaining_gain_sample_count"] = rg_sample_count

            # ── fair_exit：扣出場 fee 後的理論出場價（供交易模組 Trailing TP 用）──
            # 同時同步 naive_fair 為裁剪後的值（原本建構時是裁剪前的 p_yes/p_no）
            out_row["naive_fair_yes"] = p_yes
            out_row["naive_fair_no"] = p_no
            out_row["fair_exit_yes"] = round(
                max(0.0, p_yes - compute_fee(p_yes, fee_rate, fee_exponent)), 6
            )
            out_row["fair_exit_no"] = round(
                max(0.0, p_no - compute_fee(p_no, fee_rate, fee_exponent)), 6
            )

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
                if sig_action != "SUPPRESSED" and lead_hours_to_settlement is not None:
                    if lead_hours_to_settlement < signal_suppress_hours:
                        if probability_mode == "remaining_gain":
                            # remaining_gain 模式：不 suppress，這個窗口正是 RG 設計的目標
                            pass
                        else:
                            # < 6h：完全壓制
                            sig_status = "too_close_to_settlement"
                            sig_action = "SUPPRESSED"
                    elif lead_hours_to_settlement < signal_warning_hours:
                        # 6-8h：顯示但標記最後預報警告（sig_action 保持正常）
                        sig_status = "last_forecast_warning"

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

            # ── Phase 1-B：三模式信號（scatter / sniper / precision）──────────
            _base_sig = out_row.get("signal_action", "SUPPRESSED")
            _lock_dir_sig = out_row.get("lock_direction", "NONE")
            _sig_scatter, _sig_sniper, _sig_precision = _apply_signal_modes(
                _base_sig, _lock_dir_sig
            )
            out_row["signal_action_scatter"] = _sig_scatter
            out_row["signal_action_sniper"] = _sig_sniper
            out_row["signal_action_precision"] = _sig_precision

            output_rows.append(out_row)

        if output_rows:
            _write_output(city, output_rows)
        else:
            # P0-6：0 rows 時的脫鉤偵測
            # 若上游 probability 已更新（generated_utc 更新），而現存 ev_signals 還帶舊版本，
            # 保留舊檔會造成下游（Bot/交易模組）讀到不一致的資料。
            # 這種情況下強制寫空檔，讓 ev_signals 的 mtime 追上 probability。
            upstream_now = ""
            if prob_rows:
                upstream_now = prob_rows[0].get("generated_utc", "")
            existing_upstream = _read_existing_upstream_version(city)
            if upstream_now and existing_upstream and upstream_now != existing_upstream:
                log.warning(
                    f"  {city}: 0 EV rows AND upstream drift detected "
                    f"(existing upstream={existing_upstream}, current={upstream_now}) "
                    f"→ force-writing empty ev_signals to avoid stale data"
                )
                _write_output(city, [])
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


def _read_existing_upstream_version(city: str) -> str:
    """讀取現存 ev_signals.csv 的第一列 upstream_generated_utc。不存在或失敗則回傳空字串。

    供 P0-6 的脫鉤偵測使用：比對目前 probability 的 generated_utc 與現存 ev_signals 的
    upstream_generated_utc，若不一致代表上游已更新但下游還帶舊版本。
    """
    path = PROJ_DIR / "data" / "results" / "ev_signals" / city / "ev_signals.csv"
    if not path.exists():
        return ""
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            first = next(reader, None)
            if first is None:
                return ""
            return first.get("upstream_generated_utc", "") or ""
    except Exception:
        return ""


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
