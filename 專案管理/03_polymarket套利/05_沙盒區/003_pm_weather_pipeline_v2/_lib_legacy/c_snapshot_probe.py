"""
c_snapshot_probe.py — C availability probe module (Phase 1)

探測 city × date range 的 ERA5 archive 可用性。
C 源只有一個 "model"（ERA5），且資料範圍遠比 D 源穩定（~1940 至 today-5d），
因此不需要像 D 一樣做 year-boundary 掃描。

判定規則：
  - 對 start_date 發一次 hourly 請求
  - hourly.time 有值 且 temperature_2m 非全 null → available
  - HTTP 400 / 空資料 / 全 null → unavailable

輸出：
  probe_availability.csv  — 每個 city 的可用性結果
  probe_status.json       — 本次探測的 run status
  probe_run.log           — 詳細日誌
"""

from __future__ import annotations

import csv
import json
import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import requests

from _lib import C_HOURLY_VARS, OM_ARCHIVE_URL, now_utc

log = logging.getLogger(__name__)

# --- 常數 ---

SCHEMA_VERSION = "c_availability_v1"
SOURCE_LABEL = "C_ERA5"

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2
API_DELAY = 0.3

OUTPUT_FIELDS = [
    "schema_version",
    "city",
    "source",
    "tz",
    "available",
    "probe_start_date",
    "probe_rule",
    "anchor_time_local",
    "last_probe_time_utc",
    "probe_request_count",
    "note",
]


# ============================================================
# City CSV helpers (same pattern as d_snapshot_probe)
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
# Probe API
# ============================================================

def probe_archive_block(
    lat: float,
    lon: float,
    tz: str,
    start_date: date,
    end_date: date,
) -> dict:
    """
    向 Open-Meteo Archive API 發 hourly temperature_2m 請求。
    回傳 {"ok": bool, "data": dict, "note": str, "fatal": bool}。
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(C_HOURLY_VARS),
        "start_date": str(start_date),
        "end_date": str(end_date),
        "timezone": tz,
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                OM_ARCHIVE_URL, params=params, timeout=REQUEST_TIMEOUT,
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

            if resp.status_code == 400:
                log.warning(f"probe fatal (400): C ERA5 {start_date}..{end_date} -> {last_error}")
                return {"ok": False, "data": {}, "note": last_error, "fatal": True}

            log.warning(
                f"probe request failed: C ERA5 {start_date}..{end_date} "
                f"(attempt {attempt}/{MAX_RETRIES}) -> {last_error}"
            )
        except Exception as exc:
            last_error = f"request_error: {str(exc)[:200]}"
            log.warning(
                f"probe request error: C ERA5 {start_date}..{end_date} "
                f"(attempt {attempt}/{MAX_RETRIES}) -> {last_error}"
            )

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)

    return {"ok": False, "data": {}, "note": last_error or "unknown_error", "fatal": False}


# ============================================================
# Completeness check
# ============================================================

def check_archive_completeness(
    hourly: dict,
    target_date: str,
) -> tuple[bool, str]:
    """檢查 ERA5 在 target_date 是否有非 null 的 hourly 資料。"""
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])

    if not times:
        return False, "no_hourly_time"

    date_indices = [i for i, v in enumerate(times) if v[:10] == target_date]
    if not date_indices:
        return False, f"no_timestamps_for_{target_date}"

    non_null = sum(
        1 for i in date_indices
        if i < len(temps) and temps[i] is not None
    )
    if non_null == 0:
        return False, f"all_null_for_{target_date}"

    return True, f"complete: {non_null}/{len(date_indices)} hourly values"


# ============================================================
# Probe logic
# ============================================================

def probe_city(
    lat: float,
    lon: float,
    tz: str,
    probe_date: date,
) -> tuple[bool, int, str]:
    """
    對一個城市做 C 源 availability probe。
    回傳 (available, request_count, note)。
    """
    response = probe_archive_block(
        lat=lat, lon=lon, tz=tz,
        start_date=probe_date, end_date=probe_date,
    )
    if not response["ok"]:
        return False, 1, response["note"]

    hourly = response["data"].get("hourly", {})
    ok, note = check_archive_completeness(hourly, str(probe_date))
    return ok, 1, note


# ============================================================
# Output helpers
# ============================================================

def build_output_row(
    city: str,
    tz: str,
    probe_start_date: str,
    available: bool,
    probe_request_count: int,
    note: str,
) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "city": city,
        "source": SOURCE_LABEL,
        "tz": tz,
        "available": str(available).lower(),
        "probe_start_date": probe_start_date,
        "probe_rule": "hourly_non_null",
        "anchor_time_local": "00:00",
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
    probe_date: str,
    cities: Optional[list[str]] = None,
    verbose: bool = False,
) -> dict:
    """
    Phase 1 主入口。

    輸入���
      city_csv    — data/01_city.csv 路徑
      output_dir  — 輸出目錄（logs/08_snapshot/probe_c/）
      probe_date  — 用來探測可用性的日期 (YYYY-MM-DD)
      cities      — 城市過濾（None = 全部 enabled）

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
        probe_d = datetime.strptime(probe_date, "%Y-%m-%d").date()

        city_rows = load_city_csv(Path(city_csv))
        selected = select_cities(city_rows, city_filter=cities)

        log.info("Phase 1: C availability probe (ERA5 Archive)")
        log.info(f"  city_csv={city_csv}")
        log.info(f"  output_dir={output_dir}")
        log.info(f"  probe_date={probe_date}")
        log.info(f"  cities={cities if cities else '<all enabled>'}")

        rows: list[dict] = []
        for city_row in selected:
            city = city_row["city"].strip()
            lat = float(city_row["lat"])
            lon = float(city_row["lon"])
            tz = city_row["timezone"].strip()

            log.info(f"probe: city={city} source=C_ERA5 probe_date={probe_date}")
            available, req_count, note = probe_city(
                lat=lat, lon=lon, tz=tz, probe_date=probe_d,
            )
            rows.append(
                build_output_row(
                    city=city,
                    tz=tz,
                    probe_start_date=probe_date,
                    available=available,
                    probe_request_count=req_count,
                    note=note,
                )
            )
            time.sleep(API_DELAY)

        rows = sorted(rows, key=lambda r: r["city"])
        write_availability_csv(rows, availability_csv)

        status = {
            "schema_version": SCHEMA_VERSION,
            "started_at_utc": started_at,
            "finished_at_utc": now_utc(),
            "city_csv": str(city_csv),
            "output_csv": str(availability_csv),
            "probe_date": probe_date,
            "selected_city_count": len(selected),
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
