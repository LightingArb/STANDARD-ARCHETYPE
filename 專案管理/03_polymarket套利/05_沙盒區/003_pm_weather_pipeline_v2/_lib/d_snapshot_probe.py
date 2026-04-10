"""
d_snapshot_probe.py — D availability probe module (Phase 1)

探測 city × model 的 safe_start_date，供 Phase 2 fetch 使用。
判定規則：8 層 hourly 變數全部完整才算 available。

輸出：
  probe_availability.csv  — 每個 city × model 的可用性結果
  probe_status.json       — 本次探測的 run status
  probe_run.log           — 詳細日誌
"""

from __future__ import annotations

import csv
import json
import logging
import time
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import requests

from _lib import (
    D_HOURLY_VARS,
    OM_HISTORICAL_FORECAST_URL,
    now_utc,
    resolve_active_d_models,
)

log = logging.getLogger(__name__)

# --- 常數 ---

SCHEMA_VERSION = "d_availability_v1"
DEFAULT_HORIZON_BASIS_HOURS = 192
ANCHOR_TIME_LOCAL = "00:00"
MIN_PROBE_YEAR = 2020

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2
API_DELAY = 0.3

OUTPUT_FIELDS = [
    "schema_version",
    "city",
    "model",
    "tz",
    "available",
    "safe_start_date",
    "safe_start_month",
    "probe_rule",
    "horizon_basis_hours",
    "anchor_time_local",
    "last_probe_time_utc",
    "probe_request_count",
    "note",
]


# ============================================================
# City CSV helpers
# ============================================================

def load_city_csv(csv_path: Path) -> list[dict]:
    if not csv_path.exists():
        raise FileNotFoundError(f"找不到 city csv: {csv_path}")
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def city_probe_ready(row: dict) -> tuple[bool, str]:
    city = row.get("city", "").strip() or "<unknown>"
    missing = []
    for key in ("lat", "lon", "timezone"):
        if not str(row.get(key, "")).strip():
            missing.append(key)

    enabled = str(row.get("city_enabled", "")).strip().lower()
    if enabled not in ("true", "1", "yes"):
        return False, f"{city}: city_enabled is not true"
    if missing:
        return False, f"{city}: missing metadata: {', '.join(missing)}"
    return True, ""


def select_cities(
    rows: list[dict],
    city_filter: Optional[list[str]] = None,
) -> list[dict]:
    requested = set(city_filter or [])
    selected: list[dict] = []
    seen: set[str] = set()

    for row in rows:
        city = row.get("city", "").strip()
        if not city:
            continue
        if requested and city not in requested:
            continue
        seen.add(city)
        ready, note = city_probe_ready(row)
        if not ready:
            log.warning(f"skip city for probe: {note}")
            continue
        selected.append(row)

    if requested:
        for city in sorted(requested - seen):
            log.warning(f"skip city for probe: not found in 01_city.csv: {city}")

    return selected


# ============================================================
# Probe API helpers
# ============================================================

def probe_hourly_block(
    lat: float,
    lon: float,
    tz: str,
    model: str,
    start_date: date,
    end_date: date,
) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(D_HOURLY_VARS),
        "start_date": str(start_date),
        "end_date": str(end_date),
        "models": model,
        "timezone": tz,
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                OM_HISTORICAL_FORECAST_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                return {"ok": True, "data": resp.json(), "note": "ok", "fatal": False}

            try:
                payload = resp.json()
            except Exception:
                payload = {}
            reason = (
                payload.get("reason")
                or payload.get("error")
                or f"HTTP {resp.status_code}"
            )
            last_error = f"http_{resp.status_code}: {reason}"

            # HTTP 400 = 模型資料範圍限制，不可恢復，立即回傳
            if resp.status_code == 400:
                log.warning(
                    f"probe fatal (400): {model} {start_date}..{end_date} "
                    f"-> {last_error}"
                )
                return {"ok": False, "data": {}, "note": last_error, "fatal": True}

            log.warning(
                f"probe request failed: {model} {start_date}..{end_date} "
                f"(attempt {attempt}/{MAX_RETRIES}) -> {last_error}"
            )
        except Exception as exc:
            last_error = f"request_error: {str(exc)[:200]}"
            log.warning(
                f"probe request error: {model} {start_date}..{end_date} "
                f"(attempt {attempt}/{MAX_RETRIES}) -> {last_error}"
            )

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)

    return {"ok": False, "data": {}, "note": last_error or "unknown_error", "fatal": False}


# ============================================================
# Completeness checks
# ============================================================

def check_forecast_day_completeness(
    hourly: dict,
    target_date: str,
) -> tuple[bool, str]:
    times = hourly.get("time", [])
    if not times:
        return False, "no_hourly_time"

    date_indices = [i for i, v in enumerate(times) if v[:10] == target_date]
    expected_count = len(date_indices)
    if expected_count == 0:
        return False, f"no_timestamps_for_{target_date}"

    missing_vars = []
    incomplete_vars = []

    for var in D_HOURLY_VARS:
        values = hourly.get(var)
        if values is None or len(values) == 0:
            short = var.replace("temperature_2m_", "") if "previous" in var else "day0"
            missing_vars.append(short)
            continue

        non_null = sum(
            1 for i in date_indices if i < len(values) and values[i] is not None
        )
        if non_null != expected_count:
            short = var.replace("temperature_2m_", "") if "previous" in var else "day0"
            incomplete_vars.append(f"{short}({non_null}/{expected_count})")

    if missing_vars:
        return False, f"missing_vars={','.join(missing_vars)}"
    if incomplete_vars:
        return False, f"incomplete_hourly: {','.join(incomplete_vars)}"
    return True, f"complete: 8_layers x {expected_count}h"


def build_daily_completeness_map(hourly: dict) -> dict[str, dict]:
    times = hourly.get("time", [])
    if not times:
        return {}

    date_indices: dict[str, list[int]] = {}
    for index, value in enumerate(times):
        date_indices.setdefault(value[:10], []).append(index)

    result: dict[str, dict] = {}
    for date_str in sorted(date_indices.keys()):
        indices = date_indices[date_str]
        expected = len(indices)
        missing_layers = []
        incomplete_layers = []
        min_non_null = expected
        present_count = 0

        for var in D_HOURLY_VARS:
            values = hourly.get(var)
            if values is None or len(values) == 0:
                short = var.replace("temperature_2m_", "") if "previous" in var else "day0"
                missing_layers.append(short)
                min_non_null = 0
                continue

            present_count += 1
            non_null = sum(
                1 for i in indices if i < len(values) and values[i] is not None
            )
            min_non_null = min(min_non_null, non_null)
            if non_null != expected:
                short = var.replace("temperature_2m_", "") if "previous" in var else "day0"
                incomplete_layers.append(short)

        is_complete = not missing_layers and not incomplete_layers
        if missing_layers:
            note = f"missing_vars={','.join(missing_layers)}"
        elif incomplete_layers:
            note = f"incomplete_hourly: {','.join(incomplete_layers)}"
        else:
            note = "complete"

        result[date_str] = {
            "is_complete": is_complete,
            "expected_hourly_count": expected,
            "present_layer_count": present_count,
            "min_non_null_count": min_non_null,
            "note": note,
        }

    return result


# ============================================================
# Safe start date search
# ============================================================

def probe_single_day(
    lat: float,
    lon: float,
    tz: str,
    model: str,
    target_date: date,
) -> tuple[bool, str, bool]:
    """回傳 (ok, note, fatal)。"""
    response = probe_hourly_block(
        lat=lat, lon=lon, tz=tz, model=model,
        start_date=target_date, end_date=target_date,
    )
    if not response["ok"]:
        return False, response["note"], response.get("fatal", False)

    hourly = response["data"].get("hourly", {})
    ok, note = check_forecast_day_completeness(hourly, str(target_date))
    return ok, note, False


def find_first_complete_date_in_year(
    lat: float,
    lon: float,
    tz: str,
    model: str,
    target_year: int,
) -> tuple[Optional[str], str, int, bool]:
    """回傳 (safe_start_date, note, request_count, fatal)。"""
    request_count = 0
    last_note = ""

    for month in range(1, 13):
        start_date = date(target_year, month, 1)
        end_date = date(target_year, month, monthrange(target_year, month)[1])
        response = probe_hourly_block(
            lat=lat, lon=lon, tz=tz, model=model,
            start_date=start_date, end_date=end_date,
        )
        request_count += 1
        last_note = response["note"]

        if not response["ok"]:
            if response.get("fatal"):
                log.info(
                    f"probe fast-fail: {model} {target_year}-{month:02d} "
                    f"fatal error, aborting year scan -> {last_note}"
                )
                return None, last_note, request_count, True
            time.sleep(API_DELAY)
            continue

        completeness_map = build_daily_completeness_map(
            response["data"].get("hourly", {})
        )
        complete_days = [d for d, info in completeness_map.items() if info["is_complete"]]
        if complete_days:
            first_day = complete_days[0]
            info = completeness_map[first_day]
            return first_day, f"month_probe_found: {first_day} ({info['note']})", request_count, False

        time.sleep(API_DELAY)

    return None, last_note or f"no_complete_day_in_{target_year}", request_count, False


def find_safe_start_date(
    lat: float,
    lon: float,
    tz: str,
    model: str,
) -> tuple[bool, str, int, str]:
    """
    回傳 (available, safe_start_date, request_count, note)。
    從 current_year 開始往回探測 Jan 1，找到最早完整日期。
    """
    current_year = datetime.now(ZoneInfo(tz)).year
    request_count = 0
    last_note = ""
    last_good_year: Optional[int] = None

    for year in range(current_year, MIN_PROBE_YEAR - 1, -1):
        ok, note, fatal = probe_single_day(
            lat=lat, lon=lon, tz=tz, model=model,
            target_date=date(year, 1, 1),
        )
        request_count += 1
        last_note = note
        log.info(
            f"probe year boundary: model={model} tz={tz} "
            f"target_date={year}-01-01 ok={ok} fatal={fatal} note={note}"
        )
        if fatal:
            log.info(f"probe fast-fail: {model} fatal at {year}-01-01, marking unavailable")
            return False, "", request_count, last_note
        time.sleep(API_DELAY)
        if ok:
            last_good_year = year
            continue
        break

    if last_good_year is None:
        safe_start, month_note, month_reqs, fatal = find_first_complete_date_in_year(
            lat=lat, lon=lon, tz=tz, model=model, target_year=current_year,
        )
        request_count += month_reqs
        if fatal:
            return False, "", request_count, month_note or last_note
        if safe_start:
            return True, safe_start, request_count, month_note
        return False, "", request_count, month_note or last_note

    boundary_year = last_good_year - 1
    if boundary_year < MIN_PROBE_YEAR:
        return (
            True,
            f"{last_good_year}-01-01",
            request_count,
            "year_floor_reached_with_complete_jan01",
        )

    safe_start, month_note, month_reqs, fatal = find_first_complete_date_in_year(
        lat=lat, lon=lon, tz=tz, model=model, target_year=boundary_year,
    )
    request_count += month_reqs
    if safe_start:
        return True, safe_start, request_count, month_note

    return True, f"{last_good_year}-01-01", request_count, month_note or last_note


# ============================================================
# Output helpers
# ============================================================

def build_output_row(
    city: str,
    model: str,
    tz: str,
    horizon_basis_hours: int,
    available: bool,
    safe_start_date: str,
    probe_request_count: int,
    note: str,
) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "city": city,
        "model": model,
        "tz": tz,
        "available": str(available).lower(),
        "safe_start_date": safe_start_date,
        "safe_start_month": safe_start_date[:7] if safe_start_date else "",
        "probe_rule": "8_layer_strict_day_basis",
        "horizon_basis_hours": str(horizon_basis_hours),
        "anchor_time_local": ANCHOR_TIME_LOCAL,
        "last_probe_time_utc": now_utc(),
        "probe_request_count": str(probe_request_count),
        "note": note,
    }


def write_availability_csv(rows: list[dict], csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def attach_run_log(log_path: Path) -> logging.Handler:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    logging.getLogger().addHandler(handler)
    return handler


# ============================================================
# Main entry: run_probe
# ============================================================

def run_probe(
    city_csv: Path,
    output_dir: Path,
    cities: Optional[list[str]] = None,
    models: Optional[list[str]] = None,
    horizon_basis_hours: int = DEFAULT_HORIZON_BASIS_HOURS,
    verbose: bool = False,
) -> dict:
    """
    Phase 1 主入口。

    輸入：
      city_csv          — data/01_city.csv 路徑
      output_dir        — 輸出目錄（logs/08_snapshot/probe/）
      cities            — 城市過濾（None = 全部 enabled）
      models            — 模型過濾（可用 D1~D19 alias 或完整名）
      horizon_basis_hours — availability 基準時距（預設 192）

    輸出：
      {output_dir}/probe_availability.csv
      {output_dir}/probe_status.json
      {output_dir}/probe_run.log

    回傳：run status dict
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    availability_csv = output_dir / "probe_availability.csv"
    status_path = output_dir / "probe_status.json"
    log_handler = attach_run_log(output_dir / "probe_run.log")
    started_at = now_utc()

    try:
        resolved_models, skipped_disabled = resolve_active_d_models(models or [])
        city_rows = load_city_csv(Path(city_csv))
        selected = select_cities(city_rows, city_filter=cities)

        log.info("Phase 1: D availability probe")
        log.info(f"  city_csv={city_csv}")
        log.info(f"  output_dir={output_dir}")
        log.info(f"  horizon_basis_hours={horizon_basis_hours}")
        log.info(f"  cities={cities if cities else '<all enabled>'}")
        log.info(f"  models={resolved_models}")
        if models:
            for item in skipped_disabled:
                log.warning(
                    "  disabled model skipped: "
                    f"{item['token']} -> {item['model']} ({item['reason']})"
                )
        elif skipped_disabled:
            log.info(
                "  default-disabled models excluded: "
                + ", ".join(f"{item['alias']}={item['model']}" for item in skipped_disabled)
            )

        rows: list[dict] = []
        for city_row in selected:
            city = city_row["city"].strip()
            lat = float(city_row["lat"])
            lon = float(city_row["lon"])
            tz = city_row["timezone"].strip()

            for model in resolved_models:
                log.info(
                    f"probe: city={city} model={model} "
                    f"horizon_basis_hours={horizon_basis_hours}"
                )
                available, safe_start_date, req_count, note = find_safe_start_date(
                    lat=lat, lon=lon, tz=tz, model=model,
                )
                rows.append(
                    build_output_row(
                        city=city,
                        model=model,
                        tz=tz,
                        horizon_basis_hours=horizon_basis_hours,
                        available=available,
                        safe_start_date=safe_start_date,
                        probe_request_count=req_count,
                        note=note,
                    )
                )

        rows = sorted(rows, key=lambda r: (r["city"], r["model"]))
        write_availability_csv(rows, availability_csv)

        status = {
            "schema_version": SCHEMA_VERSION,
            "started_at_utc": started_at,
            "finished_at_utc": now_utc(),
            "city_csv": str(city_csv),
            "output_csv": str(availability_csv),
            "selected_city_count": len(selected),
            "resolved_models": resolved_models,
            "horizon_basis_hours": horizon_basis_hours,
            "row_count": len(rows),
            "available_count": sum(1 for r in rows if r["available"] == "true"),
            "unavailable_count": sum(1 for r in rows if r["available"] != "true"),
        }
        status_path.write_text(
            json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        log.info(f"probe availability written: {availability_csv} ({len(rows)} rows)")
        log.info(f"probe status written: {status_path}")
        return status

    finally:
        logging.getLogger().removeHandler(log_handler)
        log_handler.close()
