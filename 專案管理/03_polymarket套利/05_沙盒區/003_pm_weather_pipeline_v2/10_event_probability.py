"""
10_event_probability.py — 事件機率計算（ECDF-based）

⚠️ D1-only MVP baseline：基於少量樣本，僅供管線驗證，不可用於交易決策

核心邏輯：
  真實溫度 ≈ predicted_daily_high + error
  每個歷史 error 樣本 = 一個可能的真實溫度
  p_yes = 落在目標區間的 error 樣本比例（直接計數 ECDF，不插值）

輸入：
  data/models/empirical/{city}/empirical_model.json   （09 輸出）
  data/market_master.csv                              （合約條件）
  data/processed/forecast_daily_high/{city}/forecast_daily_high.csv

輸出：
  data/results/probability/{city}/event_probability.csv

溫度單位：London / Paris 均為 °C，第一版不做單位轉換。
"""

import argparse
import csv
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

PROJ_DIR = Path(__file__).resolve().parent

from _lib.ecdf_query import (  # noqa: E402
    _percentile_from_sorted,
    _interpolate_sorted_errors,
    _c_to_f,
    _f_to_c,
    compute_p_yes,
    load_empirical_model,
    get_bucket,
    get_bucket_interpolated,
    safe_float,
    precision_half_width,
)

# 本次 run() 共用的時間戳；run() 開頭會覆寫（供 11 檢測上游是否有更新）
_RUN_GENERATED_UTC: str = ""

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

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
    "precision_half",         # 原始單位的邊界半寬（供 11 的觀測裁剪使用）
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
    "sum_p_all_bins",
    "generated_utc",          # 本次執行的 UTC 時間戳（供 11 檢測上游更新）
    # Phase 1-C：預報收斂插值（C-P1）
    "error_q05",              # signed error 5th percentile（lock_range 用）
    "error_q95",              # signed error 95th percentile（lock_range 用）
    "bucket_interpolated",    # true/false
    "interp_from",            # 插值來源 bucket
    "interp_to",              # 插值目標 bucket
    "interp_weight",          # 插值權重 0-1
    "interp_level",           # "lead_hours" / "lead_day" / ""
]

# ============================================================
# Data loaders
# ============================================================

def load_market_master() -> list[dict]:
    path = PROJ_DIR / "data" / "market_master.csv"
    if not path.exists():
        return []
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))
    return rows


def load_forecast_map(city: str) -> dict[tuple, list[dict]]:
    """
    Returns {(station_id, market_date_local): [rows sorted by lead_day asc]}
    """
    path = (PROJ_DIR / "data" / "processed" / "forecast_daily_high"
            / city / "forecast_daily_high.csv")
    if not path.exists():
        return {}
    result: dict[tuple, list[dict]] = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            sid = row.get("station_id", "")
            mdate = row.get("market_date_local", "")
            if sid and mdate:
                if not row.get("lead_day"):
                    log.warning(f"Skipping forecast row with missing lead_day: market_date={mdate}")
                    continue
                result.setdefault((sid, mdate), []).append(row)
    for key in result:
        # 先按 snapshot_time_utc 降序（最新在前），再穩定按 lead_day 升序
        # 這樣同 lead_day 時保證拿到最新 snapshot
        result[key].sort(key=lambda r: r.get("snapshot_time_utc", ""), reverse=True)
        result[key].sort(key=lambda r: int(r.get("lead_day") or 99))
    return result


def get_forecast_row(
    forecast_map: dict,
    station_id: str,
    market_date: str,
    lead_day_override: int | None,
) -> dict | None:
    candidates = forecast_map.get((station_id, market_date), [])
    if not candidates:
        return None
    if lead_day_override is not None:
        matching = [r for r in candidates if int(r.get("lead_day") or -1) == lead_day_override]
        return matching[0] if matching else None
    return candidates[0]  # Minimum lead_day = most recent forecast


# ============================================================
# Main logic
# ============================================================

def run(
    cities: str,
    model_name: str,
    forecast_override: float | None,
    lead_day_override: int | None,
    verbose: bool,
) -> bool:
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=" * 55)
    log.info("10_event_probability: 事件機率計算（ECDF）")
    log.info(f"⚠️  {MODEL_SCOPE} — 僅供管線驗證，不可用於交易決策")
    log.info("=" * 55)

    # 本次執行的時間戳（整輪共用，供 11 檢測上游更新）
    global _RUN_GENERATED_UTC
    _RUN_GENERATED_UTC = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Feature flag：預報收斂插值（Phase 1-C）
    _params_path = PROJ_DIR / "config" / "trading_params.yaml"
    _use_interp = False
    if _params_path.exists():
        for _line in _params_path.read_text(encoding="utf-8").splitlines():
            _line = _line.strip()
            if _line.startswith("use_convergence_interpolation"):
                _val = _line.partition(":")[2].split("#")[0].strip().lower()
                _use_interp = _val == "true"
                break
    log.info(f"use_convergence_interpolation={_use_interp}")

    cities_filter = {c.strip() for c in cities.split(",") if c.strip()} if cities else set()

    # B1: strict_mode — --cities 指定時啟用；任何假成功路徑都改 return False
    strict_mode = bool(cities_filter)

    # Only empirical is fully implemented for p_t; OU/QR not yet supported
    if model_name not in ("empirical", "all"):
        log.warning(f"model={model_name!r}: OU/QR p_t calculation not yet implemented, using empirical")
    effective_model = "empirical"

    master_rows = load_market_master()
    if not master_rows:
        # B2: strict_mode → 真實失敗，不假裝成功
        if strict_mode:
            log.error(
                f"market_master.csv empty or not found — no markets to process "
                f"(strict_mode, requested: {sorted(cities_filter)})"
            )
            return False
        log.warning("market_master.csv empty or not found — no markets to process")
        # Still write empty output CSVs per city（非 strict 向後相容）
        for city in (cities_filter or []):
            _write_output(city, [])
        return True

    # Filter active markets
    active_markets = [
        r for r in master_rows
        if r.get("market_enabled", "").lower() == "true"
        and r.get("parse_status", "") == "ok"
        and (not cities_filter or r.get("city", "") in cities_filter)
    ]

    if not active_markets:
        # B3: strict_mode → 真實失敗
        if strict_mode:
            log.error(
                f"No active markets after filtering (strict_mode, requested: {sorted(cities_filter)})"
            )
            return False
        log.warning("No active markets after filtering")
        return True

    log.info(f"Active markets: {len(active_markets)}")

    # Group active markets by city
    cities_in_scope: set[str] = {r["city"] for r in active_markets}

    all_output_rows: list[dict] = []
    written_cities: set[str] = set()  # B4: 追蹤實際寫出 CSV 的城市

    for city in sorted(cities_in_scope):
        if cities_filter and city not in cities_filter:
            continue

        model = load_empirical_model(city)
        if model is None:
            log.warning(f"{city}: empirical model not found, skipping")
            continue

        forecast_map = load_forecast_map(city)
        city_markets = [r for r in active_markets if r["city"] == city]
        log.info(f"--- {city}: {len(city_markets)} active markets ---")

        city_rows: list[dict] = []

        for mkt in city_markets:
            market_id = mkt.get("market_id", "")
            market_date = mkt.get("market_date_local", "")
            market_type = mkt.get("market_type", "")
            station_id = mkt.get("station_id", "")
            temp_unit = mkt.get("temp_unit", "C")
            precision_str = mkt.get("precision", "1C")
            half_w_orig = precision_half_width(precision_str)  # 原始單位（F 或 C）
            half_w = half_w_orig                                # 給 compute_p_yes 用（會轉 °C）

            threshold = safe_float(mkt.get("threshold"))
            range_low = safe_float(mkt.get("range_low"))
            range_high = safe_float(mkt.get("range_high"))

            # 保留原始值供 CSV 輸出（下游 11/bot 用原始單位顯示）
            threshold_orig = threshold
            range_low_orig = range_low
            range_high_orig = range_high

            # ── Unit conversion: F → C (model errors are always in °C) ──
            if temp_unit == "F":
                if threshold is not None:
                    threshold = _f_to_c(threshold)
                if range_low is not None:
                    range_low = _f_to_c(range_low)
                if range_high is not None:
                    range_high = _f_to_c(range_high)
                # precision_half 也要轉成 °C（range_low/high 已轉 C，單位要一致）
                half_w = half_w * 5.0 / 9.0
                log.debug(
                    f"  F→C conversion: threshold={threshold} "
                    f"range=[{range_low}, {range_high}] half_w={half_w:.4f}°C"
                )

            # ── Get forecast ──
            _lead_hours_to_settlement = ""
            if forecast_override is not None:
                predicted = forecast_override
                actual_lead_day = lead_day_override if lead_day_override is not None else 1
            else:
                frow = get_forecast_row(forecast_map, station_id, market_date, lead_day_override)
                if frow is None:
                    log.debug(f"  No forecast for {city} {market_date}, skipping market {market_id}")
                    continue
                predicted = safe_float(frow.get("predicted_daily_high"))
                if predicted is None:
                    continue
                actual_lead_day = int(frow.get("lead_day") or 1)
                _lead_hours_to_settlement = frow.get("lead_hours_to_settlement", "")

            # ── Get bucket（with optional quantile interpolation）──
            _lead_hours_val = safe_float(_lead_hours_to_settlement) if _lead_hours_to_settlement else None
            if _use_interp:
                bucket, bucket_key, _is_interp, _interp_meta = get_bucket_interpolated(
                    model, actual_lead_day, lead_hours=_lead_hours_val
                )
            else:
                bucket, bucket_key = get_bucket(model, actual_lead_day, lead_hours=_lead_hours_val)
                _is_interp, _interp_meta = False, {}

            # Derive bucket level for diagnostics
            if _is_interp:
                _bucket_level = f"interp_{_interp_meta.get('level', '')}"
            elif bucket_key is not None and bucket_key.startswith("lead_hours_"):
                _bucket_level = "lead_hours"
            elif bucket_key == f"lead_day_{actual_lead_day}":
                _bucket_level = "lead_day_exact"
            else:
                _bucket_level = "lead_day_nearest"
            if bucket is None:
                log.warning(f"  No bucket for {city} lead_day={actual_lead_day}, skipping market {market_id}")
                continue

            sorted_errors = bucket.get("sorted_errors", [])
            if not sorted_errors:
                log.warning(f"  Empty sorted_errors for {city} lead_day={actual_lead_day}")
                continue

            # error percentiles for lock_range（Phase 1-C → Phase 1-B）
            _error_q05 = round(_percentile_from_sorted(sorted_errors, 0.05), 4)
            _error_q95 = round(_percentile_from_sorted(sorted_errors, 0.95), 4)

            # ── Compute p_yes ──
            p_yes = compute_p_yes(
                sorted_errors=sorted_errors,
                predicted_high=predicted,
                contract_type=market_type,
                threshold=threshold,
                range_low=range_low,
                range_high=range_high,
                precision_half=half_w,
            )
            p_no = round(1.0 - p_yes, 6)

            city_rows.append({
                "market_id": market_id,
                "city": city,
                "market_date_local": market_date,
                "market_type": market_type,
                "threshold": threshold_orig if threshold_orig is not None else "",
                "range_low": range_low_orig if range_low_orig is not None else "",
                "range_high": range_high_orig if range_high_orig is not None else "",
                "temp_unit": temp_unit,
                "precision_half": round(half_w_orig, 6),  # 原始單位
                "model_name": effective_model,
                "model_scope": MODEL_SCOPE,
                "predicted_daily_high": round(predicted, 4),
                "lead_day": actual_lead_day,
                "lead_hours_to_settlement": _lead_hours_to_settlement,
                "bucket_used": bucket_key,
                "bucket_level_used": _bucket_level,
                "bucket_sample_count": bucket.get("sample_count", len(sorted_errors)),
                "p_yes": p_yes,
                "p_no": p_no,
                "sum_p_all_bins": "",  # Filled in post-processing below
                "generated_utc": _RUN_GENERATED_UTC,
                # Phase 1-C
                "error_q05": _error_q05,
                "error_q95": _error_q95,
                "bucket_interpolated": "true" if _is_interp else "false",
                "interp_from": _interp_meta.get("from", ""),
                "interp_to": _interp_meta.get("to", ""),
                "interp_weight": _interp_meta.get("weight", ""),
                "interp_level": _interp_meta.get("level", ""),
            })

        # ── sum_p_all_bins sanity check ──
        date_sums: dict[str, float] = {}
        for row in city_rows:
            mdate = row["market_date_local"]
            date_sums[mdate] = round(date_sums.get(mdate, 0.0) + row["p_yes"], 6)

        for row in city_rows:
            total = date_sums[row["market_date_local"]]
            row["sum_p_all_bins"] = round(total, 6)
            if abs(total - 1.0) > 0.05:
                log.debug(
                    f"  sum_p_all_bins={total:.4f} for {city} {row['market_date_local']} "
                    f"(expected ≈1.0 only if bins are exhaustive)"
                )

        log.info(f"  {city}: {len(city_rows)} probability rows")
        all_output_rows.extend(city_rows)
        if city_rows:
            _write_output(city, city_rows)
            written_cities.add(city)  # B4
        else:
            if strict_mode:
                # B4: strict_mode 下 0 行是錯誤，不保留舊檔
                log.error(
                    f"  {city}: 0 probability rows (strict_mode) "
                    f"— model/forecast data may be missing"
                )
            else:
                log.warning(f"  {city}: 0 probability rows — skipping write (preserving existing)")

    total = len(all_output_rows)
    log.info(f"10_event_probability done. Total rows: {total}")

    # B4: strict_mode — 所有 requested 城市都必須有輸出
    if strict_mode:
        missing_output = sorted(cities_filter - written_cities)
        if missing_output:
            log.error(f"strict_mode: no output written for: {missing_output}")
            return False

    return True


def _write_output(city: str, rows: list[dict]) -> None:
    out_dir = PROJ_DIR / "data" / "results" / "probability" / city
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "event_probability.csv"
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
    p = argparse.ArgumentParser(description="Event probability calculation (10_event_probability)")
    p.add_argument("--cities", type=str, default="", help="城市 filter（逗號分隔）")
    p.add_argument("--model", type=str, default="empirical",
                   choices=["empirical", "ou", "qr", "all"],
                   help="使用哪個模型（預設: empirical）")
    p.add_argument("--forecast", type=float, default=None,
                   help="手動指定 forecast 溫度（°C）（可選）")
    p.add_argument("--lead-day", type=int, default=None,
                   dest="lead_day", help="手動指定 lead_day（可選，否則用最新可用）")
    p.add_argument("--verbose", action="store_true")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    ok = run(
        cities=args.cities,
        model_name=args.model,
        forecast_override=args.forecast,
        lead_day_override=args.lead_day,
        verbose=args.verbose,
    )
    sys.exit(0 if ok else 1)
