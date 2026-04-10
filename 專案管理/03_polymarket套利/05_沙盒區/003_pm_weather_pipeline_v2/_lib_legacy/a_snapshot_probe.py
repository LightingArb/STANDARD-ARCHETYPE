"""
a_snapshot_probe.py — A availability probe module (Phase 1)

探測 city 的 IEM/METAR 觀測資料可用性。
A 源是觀測站即時資料，不像 D 需要模型掃描，也不像 C 有固定 lag。

判定規則：
  - 對 probe_date 發一次 IEM request（station_code + tmpf）
  - 回傳含有效溫度資料 → available
  - HTTP 404 / 空資料 / station 無效 → unavailable

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
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

from _lib import now_utc

log = logging.getLogger(__name__)

# --- 常數 ---

SCHEMA_VERSION = "a_availability_v1"
SOURCE_LABEL = "A_IEM"

IEM_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2
API_DELAY = 0.3

OUTPUT_FIELDS = [
    "schema_version",
    "city",
    "source",
    "station",
    "tz",
    "available",
    "probe_date",
    "probe_rule",
    "last_probe_time_utc",
    "probe_request_count",
    "hourly_count",
    "note",
]


# ============================================================
# City CSV helpers (same pattern as c/d probe)
# ============================================================

def load_city_csv(csv_path: Path) -> list[dict]:
    if not csv_path.exists():
        raise FileNotFoundError(f"找不到 city csv: {csv_path}")
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def city_probe_ready(row: dict) -> tuple[bool, str]:
    city = row.get("city", "").strip() or "<unknown>"
    missing = []
    for key in ("station_code", "timezone"):
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

def probe_iem_station(
    station: str,
    probe_date: date,
) -> dict:
    """
    向 IEM ASOS API 發請求，確認 station 在 probe_date 有觀測資料。
    回傳 {"ok": bool, "hourly_count": int, "note": str}。
    """
    # IEM end_date is exclusive, so add 1 day
    end_date = probe_date + timedelta(days=1)

    params = {
        "station": station,
        "data": "tmpf",
        "year1": probe_date.year,
        "month1": probe_date.month,
        "day1": probe_date.day,
        "year2": end_date.year,
        "month2": end_date.month,
        "day2": end_date.day,
        "tz": "Etc/UTC",
        "format": "onlycomma",
        "latlon": "no",
        "elev": "no",
        "missing": "M",
        "trace": "T",
        "report_type": "3",
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(IEM_URL, params=params, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 200:
                # Parse and count valid temperature readings
                lines = resp.text.strip().split("\n")
                if len(lines) < 2 or "tmpf" not in lines[0].lower():
                    return {
                        "ok": False, "hourly_count": 0,
                        "note": "empty_or_invalid_response",
                    }

                header = [h.strip().lower() for h in lines[0].split(",")]
                tmpf_idx = header.index("tmpf") if "tmpf" in header else None
                if tmpf_idx is None:
                    return {
                        "ok": False, "hourly_count": 0,
                        "note": "no_tmpf_column",
                    }

                valid_count = 0
                for line in lines[1:]:
                    if not line.strip():
                        continue
                    parts = line.split(",")
                    if len(parts) > tmpf_idx:
                        val = parts[tmpf_idx].strip()
                        if val and val != "M":
                            valid_count += 1

                if valid_count == 0:
                    return {
                        "ok": False, "hourly_count": 0,
                        "note": f"no_valid_temps_for_{probe_date}",
                    }

                return {
                    "ok": True, "hourly_count": valid_count,
                    "note": f"ok: {valid_count} hourly readings",
                }

            if resp.status_code == 404:
                return {
                    "ok": False, "hourly_count": 0,
                    "note": f"station_not_found_{station}",
                }

            last_error = f"http_{resp.status_code}"
            if resp.status_code >= 500:
                log.warning(
                    f"probe IEM: upstream error ({resp.status_code}) "
                    f"station={station} (attempt {attempt}/{MAX_RETRIES})"
                )
            else:
                return {
                    "ok": False, "hourly_count": 0,
                    "note": last_error,
                }

        except requests.exceptions.Timeout:
            last_error = "timeout"
            log.warning(
                f"probe IEM: timeout station={station} "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )
        except requests.exceptions.ConnectionError:
            last_error = "connection_error"
            log.warning(
                f"probe IEM: connection error station={station} "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )
        except Exception as exc:
            last_error = f"error: {str(exc)[:200]}"
            log.warning(
                f"probe IEM: {last_error} station={station} "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)

    return {"ok": False, "hourly_count": 0, "note": last_error or "unknown_error"}


# ============================================================
# Output helpers
# ============================================================

def build_output_row(
    city: str,
    station: str,
    tz: str,
    probe_date: str,
    available: bool,
    hourly_count: int,
    note: str,
) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "city": city,
        "source": SOURCE_LABEL,
        "station": station,
        "tz": tz,
        "available": str(available).lower(),
        "probe_date": probe_date,
        "probe_rule": "iem_tmpf_non_empty",
        "last_probe_time_utc": now_utc(),
        "probe_request_count": "1",
        "hourly_count": str(hourly_count),
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

    輸入：
      city_csv    — data/01_city.csv 路徑
      output_dir  — 輸出目錄（logs/08_snapshot/probe_a/）
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

        log.info("Phase 1: A availability probe (IEM/METAR)")
        log.info(f"  city_csv={city_csv}")
        log.info(f"  output_dir={output_dir}")
        log.info(f"  probe_date={probe_date}")
        log.info(f"  cities={cities if cities else '<all enabled>'}")

        rows: list[dict] = []
        for city_row in selected:
            city = city_row["city"].strip()
            station = city_row["station_code"].strip()
            tz = city_row["timezone"].strip()

            log.info(f"probe: city={city} station={station} source=A_IEM probe_date={probe_date}")
            result = probe_iem_station(station=station, probe_date=probe_d)

            rows.append(
                build_output_row(
                    city=city,
                    station=station,
                    tz=tz,
                    probe_date=probe_date,
                    available=result["ok"],
                    hourly_count=result["hourly_count"],
                    note=result["note"],
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
