"""
_lib/ecdf_query.py — ECDF 查詢與計算函式庫

從 10_event_probability.py 抽出，供 10 與 11 共用。

包含：
  - _percentile_from_sorted    : 排序列表的分位數計算
  - _interpolate_sorted_errors : 1D Wasserstein barycenter 插值
  - _c_to_f / _f_to_c          : 溫度單位轉換
  - compute_p_yes              : ECDF 直接計數機率
  - load_empirical_model       : 讀取 empirical_model.json
  - get_bucket                 : 查找 error bucket（不插值）
  - get_bucket_interpolated    : 三層插值 bucket 查找（Phase 1-C）
  - safe_float                 : 安全的 float 轉換
  - precision_half_width       : precision 字串轉半寬值
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)


# ============================================================
# Quantile interpolation helpers（Phase 1-C）
# ============================================================

def _percentile_from_sorted(sorted_vals: list[float], p: float) -> float:
    """Linear interpolation at quantile p ∈ [0, 1] from pre-sorted list."""
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    if n == 1:
        return sorted_vals[0]
    idx = p * (n - 1)
    lo = int(idx)
    hi = min(lo + 1, n - 1)
    frac = idx - lo
    return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


def _interpolate_sorted_errors(
    errors_a: list[float],
    errors_b: list[float],
    weight_b: float,
) -> list[float]:
    """
    1D Wasserstein barycenter（quantile function interpolation）。

    Q_interp(p) = (1 - weight_b) * Q_a(p) + weight_b * Q_b(p)

    n_output = min(200, (len_a + len_b) // 2)，至少 20 點。
    回傳已排序的 list[float]。
    """
    n_output = min(200, (len(errors_a) + len(errors_b)) // 2)
    n_output = max(n_output, 20)
    result = []
    for i in range(n_output):
        p = i / max(n_output - 1, 1)
        qa = _percentile_from_sorted(errors_a, p)
        qb = _percentile_from_sorted(errors_b, p)
        result.append(round(qa * (1 - weight_b) + qb * weight_b, 4))
    return sorted(result)


# ============================================================
# 溫度單位轉換
# ============================================================

def _c_to_f(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0

def _f_to_c(f: float) -> float:
    return (f - 32.0) * 5.0 / 9.0


# ============================================================
# ECDF 機率計算
# ============================================================

def compute_p_yes(
    sorted_errors: list[float],
    predicted_high: float,
    contract_type: str,
    threshold: float | None = None,
    range_low: float | None = None,
    range_high: float | None = None,
    precision_half: float = 0.5,
) -> float:
    """
    Compute P(YES) using ECDF (direct count of sorted_errors).

    For each contract_type:
      exact:   P(actual rounds to threshold) → P(error in [threshold ± precision_half - predicted])
      range:   P(actual in [range_low, range_high]) → P(error in [lo - pred, hi - pred])
      higher:  P(actual > threshold) → P(error > threshold - predicted)
      below:   P(actual < threshold) → P(error < threshold - predicted)

    注意：所有參數（predicted_high, threshold, range_*）必須使用相同單位（°C）。
    呼叫前需完成 F→C 轉換。
    """
    n = len(sorted_errors)
    if n == 0:
        return 0.0

    if contract_type == "exact" and threshold is not None:
        lo = threshold - precision_half - predicted_high
        hi = threshold + precision_half - predicted_high
        count = sum(1 for e in sorted_errors if lo <= e <= hi)

    elif contract_type == "range" and range_low is not None and range_high is not None:
        # 擴展邊界 ±precision_half，與 exact 保持一致（相鄰 bin 之間不留縫隙）
        lo = range_low - precision_half - predicted_high
        hi = range_high + precision_half - predicted_high
        count = sum(1 for e in sorted_errors if lo <= e <= hi)

    elif contract_type == "higher" and threshold is not None:
        # 「X° or higher」= actual >= X - precision_half（與相鄰 exact bin 無縫銜接）
        thresh_err = (threshold - precision_half) - predicted_high
        count = sum(1 for e in sorted_errors if e >= thresh_err)

    elif contract_type == "below" and threshold is not None:
        # 「X° or below」= actual < X + precision_half（與相鄰 exact bin 無縫銜接）
        thresh_err = (threshold + precision_half) - predicted_high
        count = sum(1 for e in sorted_errors if e < thresh_err)

    else:
        log.warning(f"Unknown contract_type={contract_type!r} or missing params, returning 0")
        return 0.0

    return round(count / n, 6)


# ============================================================
# Model loader
# ============================================================

def load_empirical_model(city: str, proj_dir: Path | None = None) -> dict | None:
    """
    讀取 data/models/empirical/{city}/empirical_model.json。

    proj_dir 預設為此模組所在目錄的父目錄（即沙盒根目錄）。
    """
    if proj_dir is None:
        proj_dir = Path(__file__).resolve().parent.parent
    path = proj_dir / "data" / "models" / "empirical" / city / "empirical_model.json"
    if not path.exists():
        log.warning(f"Empirical model not found: {path}")
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================
# Bucket lookup
# ============================================================

def get_bucket(
    model: dict,
    lead_day: int,
    lead_hours: float | None = None,
) -> tuple[dict | None, str | None]:
    """
    Look up error bucket.
    Priority: lead_hours_{N} (6h granularity) → lead_day_{N} (exact) → nearest lead_day fallback.
    lead_hours bucket is only used if it exists AND n >= 100 (built by 09 with MIN_HOURS_BUCKET_SAMPLES).
    """
    buckets = model.get("buckets", {})

    # 1. Try lead_hours bucket (more granular, only present when enough samples)
    if lead_hours is not None:
        bh = (int(lead_hours) // 6) * 6
        hours_key = f"lead_hours_{bh}"
        if hours_key in buckets:
            log.debug(f"Bucket: {hours_key} (lead_hours={lead_hours:.1f}h)")
            return buckets[hours_key], hours_key

    # 2. Exact lead_day bucket
    key = f"lead_day_{lead_day}"
    if key in buckets:
        return buckets[key], key

    # 3. Nearest available lead_day fallback
    available = []
    for k, b in buckets.items():
        if not k.startswith("lead_day_"):
            continue
        ld = b.get("lead_day")
        if ld is not None:
            available.append((abs(int(ld) - lead_day), k, b))
    if not available:
        return None, None
    available.sort()
    _, fallback_key, fallback_bucket = available[0]
    log.debug(f"Bucket fallback: lead_day_{lead_day} → {fallback_key}")
    return fallback_bucket, fallback_key


def get_bucket_interpolated(
    model: dict,
    lead_day: int,
    lead_hours: float | None = None,
) -> tuple[dict | None, str | None, bool, dict]:
    """
    Three-level bucket lookup with quantile interpolation（Phase 1-C）。

    Priority 1: lead_hours 插值
      lead_hours_X 和 lead_hours_X+6 都存在 → 在兩個 6h bucket 之間插值

    Priority 2: lead_day 插值
      lead_hours bucket 不可用 → 根據 lead_hours 計算 fractional lead_day，
      在相鄰整數 lead_day 之間插值

    Priority 3: 現有 fallback（不插值，委派給 get_bucket()）

    回傳 (bucket, key, is_interpolated, interp_meta)
    interp_meta = {"from", "to", "weight", "level"} 或 {}
    """
    buckets = model.get("buckets", {})

    # ── Priority 1: lead_hours interpolation ──
    if lead_hours is not None:
        bh_lo = (int(lead_hours) // 6) * 6
        bh_hi = bh_lo + 6
        lo_key = f"lead_hours_{bh_lo}"
        hi_key = f"lead_hours_{bh_hi}"
        b_lo = buckets.get(lo_key)
        b_hi = buckets.get(hi_key)

        if b_lo is not None and b_hi is not None:
            weight = (lead_hours - bh_lo) / 6.0
            if weight <= 0.0:
                # 整除，直接用 lo bucket，不插值
                return b_lo, lo_key, False, {}
            errors_a = b_lo.get("sorted_errors", [])
            errors_b = b_hi.get("sorted_errors", [])
            interp_errors = _interpolate_sorted_errors(errors_a, errors_b, weight)
            fake_bucket = dict(b_lo)
            fake_bucket["sorted_errors"] = interp_errors
            fake_bucket["sample_count"] = len(interp_errors)
            log.debug(f"Interpolated {lo_key}→{hi_key} weight={weight:.3f}")
            return fake_bucket, lo_key, True, {
                "from": lo_key,
                "to": hi_key,
                "weight": round(weight, 4),
                "level": "lead_hours",
            }
        elif b_lo is not None:
            # 只有 lo，直接用，不插值
            return b_lo, lo_key, False, {}

    # ── Priority 2: lead_day interpolation ──
    if lead_hours is not None:
        ld_float = lead_hours / 24.0
        ld_lo = int(ld_float)
        ld_hi = ld_lo + 1
        weight = round(ld_float - ld_lo, 6)
        lo_key = f"lead_day_{ld_lo}"
        hi_key = f"lead_day_{ld_hi}"
        b_lo = buckets.get(lo_key)
        b_hi = buckets.get(hi_key)

        if b_lo is not None and b_hi is not None and weight > 0.0:
            errors_a = b_lo.get("sorted_errors", [])
            errors_b = b_hi.get("sorted_errors", [])
            interp_errors = _interpolate_sorted_errors(errors_a, errors_b, weight)
            fake_bucket = dict(b_lo)
            fake_bucket["sorted_errors"] = interp_errors
            fake_bucket["sample_count"] = len(interp_errors)
            log.debug(f"Interpolated {lo_key}→{hi_key} weight={weight:.4f}")
            return fake_bucket, lo_key, True, {
                "from": lo_key,
                "to": hi_key,
                "weight": round(weight, 4),
                "level": "lead_day",
            }

    # ── Priority 3: fallback（現有邏輯，不插值）──
    bucket, key = get_bucket(model, lead_day, lead_hours)
    return bucket, key, False, {}


# ============================================================
# 通用工具
# ============================================================

def safe_float(val) -> float | None:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def precision_half_width(precision_str: str) -> float:
    """Parse precision string to half-bin width in the native unit."""
    s = str(precision_str).strip().lower()
    if s.startswith("0.5"):
        return 0.25
    return 0.5  # Default: 1° precision → 0.5 half-width
