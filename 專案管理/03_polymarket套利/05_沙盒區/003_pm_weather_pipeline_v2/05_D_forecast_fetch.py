"""
05_D_forecast_fetch.py — D1 (gfs_seamless) forecast 抓取

改版說明（STEP 3.5）：
  原版用 Historical Forecast API 打 7 次/market_date，但 7 個 snapshot 值完全相同
  （Historical Forecast API 對同一 market_date 只存一份 archived forecast）。

  新版：使用 Historical Forecast API 的 previous_day1~7 欄位，
  1 次 API call/market_date 同時取得 7 個真正不同的 snapshot。
  - temperature_2m_previous_day1 = market_date 前 1 天的 GFS run 的預測
  - temperature_2m_previous_day7 = market_date 前 7 天的 GFS run 的預測
  snapshot_mode = historical_forecast_previous_day

改版說明（Live Forecast 支援）：
  live mode 新增雙 API 模式，依 market_date 與城市本地 today 自動選擇 API：
  - market_date < city_today  → Historical Forecast API（不變）
  - market_date >= city_today → 普通 Forecast API（新增）
    snapshot_mode = live_forecast
    lead_day = (market_date - city_today).days
    snapshot_time_utc = fetch time（不是 model run time）
    cache policy：6 小時內重跑則 skip（LIVE_FORECAST_MAX_CACHE_HOURS）

  historical mode 完全不受影響。

讀取（live mode）：
  data/market_master.csv → enabled markets → dedupe fetch tasks

讀取（historical mode）：
  config/seed_cities.json + --start-date / --end-date → 直接生成日期範圍任務
  不讀 market_master.csv

API：
  Historical: https://historical-forecast-api.open-meteo.com/v1/forecast
    model=gfs_seamless, hourly=temperature_2m_previous_day1,...,7
  Live:       https://api.open-meteo.com/v1/forecast
    model=gfs_seamless, hourly=temperature_2m

輸出：
  data/raw/D/{city}/gfs_seamless/forecast_hourly_{market_date}.csv
  schema 統一：HOURLY_FIELDS（live 與 historical 一致，07 不需修改）

注意：
  - 每次 request 間隔至少 0.5 秒
  - 429 → cooldown 60s → 重試一次；仍 429 → 停止整個 run
  - 5xx → 最多重試 3 次；4xx → 記錄並跳過
  - live mode past dates: market_date < city_today → Historical Forecast API
  - live mode future dates: market_date >= city_today → Forecast API
  - Forecast API horizon：最多 16 天，超過則 log warning + skip

Probe（historical mode）：
  - 啟動時自動偵測每個城市的 previous_day{N} 最早有效日期
  - 避免對無資料的早期日期盲打
  - probe 結果快取 7 天：data/raw/D/{city}/gfs_seamless/probe_earliest_date.json
  - --skip-probe：略過 probe，直接用 --start-date
  - --force-probe：忽略 cache，強制重新偵測
"""

import argparse
import csv
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import requests

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

FORECAST_API = "https://historical-forecast-api.open-meteo.com/v1/forecast"
LIVE_FORECAST_API = "https://api.open-meteo.com/v1/forecast"
MODEL = "gfs_seamless"
REQUEST_INTERVAL = 0.5  # seconds between API requests
PROBE_EARLIEST_YEAR = 2020
PROBE_COMPLETENESS_THRESHOLD = 20  # non-null hourly values required to count as "sufficient"
LIVE_FORECAST_MAX_CACHE_HOURS = 6  # live forecast cache policy (engineering constant, not API update cycle)
LIVE_FORECAST_MAX_HORIZON_DAYS = 16  # Open-Meteo Forecast API max horizon

# lead_day N → API field name
PREV_DAY_FIELDS: dict[int, str] = {
    1: "temperature_2m_previous_day1",
    2: "temperature_2m_previous_day2",
    3: "temperature_2m_previous_day3",
    4: "temperature_2m_previous_day4",
    5: "temperature_2m_previous_day5",
    6: "temperature_2m_previous_day6",
    7: "temperature_2m_previous_day7",
}

HOURLY_FIELDS = [
    "snapshot_time_utc",
    "snapshot_time_local",
    "target_time_utc",
    "target_time_local",
    "market_date_local",
    "lead_hours",
    "lead_day",
    "city",
    "station_id",
    "model",
    "forecast_temp",
    "source_api",
    "snapshot_mode",
    "value_status",
]


class StopRun(Exception):
    pass


# ============================================================
# Probe: detect earliest date with sufficient previous_day data
# ============================================================

def probe_earliest_previous_day_date(
    lat: str,
    lon: str,
    tz_name: str,
    lookback_days: int = 7,
    model: str = MODEL,
) -> tuple[Optional[str], int]:
    """
    Detect the earliest date where temperature_2m_previous_day{lookback_days}
    has >= PROBE_COMPLETENESS_THRESHOLD non-null hourly values.

    Phase 1: year-level coarse scan (current year → PROBE_EARLIEST_YEAR).
      - Jan 1 of each year is tested.
      - First year whose Jan 1 has insufficient data = boundary_year.
      - If all years have data → return 'PROBE_EARLIEST_YEAR-01-01'.

    Phase 2: month-level fine scan within boundary_year.
      - Test the 1st of each month; return the first month that has data.
      - If no month in boundary_year has data → return None.

    Returns (earliest_date_str | None, probe_request_count).
    """
    if lookback_days > 7:
        log.error(f"lookback_days={lookback_days} > 7 not supported; clamped to 7")
        lookback_days = 7

    field_name = f"temperature_2m_previous_day{lookback_days}"
    probe_count = 0
    today = date.today()

    def check_date(test_date: date) -> bool:
        nonlocal probe_count
        if test_date > today:
            return False
        test_str = test_date.isoformat()
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": field_name,
            "models": model,
            "start_date": test_str,
            "end_date": test_str,
            "timezone": tz_name,
        }
        probe_count += 1
        try:
            resp = _do_get(FORECAST_API, params)
            if resp is None or resp.status_code != 200:
                log.info(f"  Testing {test_str}... HTTP error → insufficient")
                time.sleep(REQUEST_INTERVAL)
                return False
            data = resp.json()
            values = data.get("hourly", {}).get(field_name, [])
            non_null = sum(1 for v in values if v is not None)
            total = len(values)
            has_data = non_null >= PROBE_COMPLETENESS_THRESHOLD
            log.info(
                f"  Testing {test_str}... non_null={non_null}/{total} → "
                f"{'sufficient' if has_data else 'insufficient'}"
            )
            time.sleep(REQUEST_INTERVAL)
            return has_data
        except Exception as e:
            log.warning(f"  Probe {test_str} exception: {e}")
            time.sleep(REQUEST_INTERVAL)
            return False

    # Phase 1: year-level coarse scan
    boundary_year = None
    for yr in range(today.year, PROBE_EARLIEST_YEAR - 1, -1):
        test_date = date(yr, 1, 1)
        if test_date > today:
            continue
        if not check_date(test_date):
            boundary_year = yr
            break

    if boundary_year is None:
        # All tested years have data; earliest = PROBE_EARLIEST_YEAR-01-01
        return date(PROBE_EARLIEST_YEAR, 1, 1).isoformat(), probe_count

    # Phase 2: month-level fine scan within boundary_year
    for month in range(1, 13):
        try:
            test_date = date(boundary_year, month, 1)
        except ValueError:
            break
        if test_date > today:
            break
        log.info(f"  Fine scan {boundary_year}: testing {test_date}...")
        if check_date(test_date):
            return test_date.isoformat(), probe_count

    return None, probe_count


# ─── Probe cache ───────────────────────────────────────────────

def _probe_cache_path(city: str, model: str = MODEL) -> Path:
    return PROJ_DIR / "data" / "raw" / "D" / city / model / "probe_earliest_date.json"


def load_probe_cache(
    city: str,
    lookback_days: int,
    max_age_days: int = 7,
    model: str = MODEL,
) -> Optional[str]:
    """Return cached earliest_date string, or None if absent / stale / param-mismatch."""
    path = _probe_cache_path(city, model)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            cache = json.load(f)
        if cache.get("lookback_days") != lookback_days:
            return None
        if cache.get("completeness_threshold") != PROBE_COMPLETENESS_THRESHOLD:
            return None
        probe_time_str = cache.get("probe_time_utc", "")
        if probe_time_str:
            probe_dt = datetime.fromisoformat(probe_time_str.replace("Z", "+00:00"))
            age = datetime.now(timezone.utc) - probe_dt
            if age.days > max_age_days:
                return None
        return cache.get("earliest_date")
    except Exception as e:
        log.warning(f"Could not load probe cache for {city}: {e}")
        return None


def save_probe_cache(
    city: str,
    station_id: str,
    lookback_days: int,
    earliest_date: Optional[str],
    probe_requests: int,
    model: str = MODEL,
) -> None:
    path = _probe_cache_path(city, model)
    path.parent.mkdir(parents=True, exist_ok=True)
    cache = {
        "city": city,
        "station_id": station_id,
        "model": model,
        "probe_field": f"temperature_2m_previous_day{lookback_days}",
        "lookback_days": lookback_days,
        "completeness_threshold": PROBE_COMPLETENESS_THRESHOLD,
        "earliest_date": earliest_date,
        "probe_time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "probe_requests": probe_requests,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)
    log.info(f"  Probe cache saved: {path.relative_to(PROJ_DIR)}")


def get_earliest_date_for_city(
    city: str,
    meta: dict,
    lookback_days: int,
    force_probe: bool,
    model: str = MODEL,
) -> Optional[str]:
    """Return earliest available previous_day date string for city, using cache when valid."""
    station_id = meta.get("station_code", "")
    lat = str(meta.get("lat", ""))
    lon = str(meta.get("lon", ""))
    tz_name = meta.get("timezone", "UTC")

    if not force_probe:
        cached = load_probe_cache(city, lookback_days, model=model)
        if cached is not None:
            log.info(f"  {city}: using cached earliest_date={cached}")
            return cached

    log.info(f"Probing earliest previous_day{lookback_days} date for {city} ({station_id})...")
    earliest, probe_requests = probe_earliest_previous_day_date(
        lat=lat,
        lon=lon,
        tz_name=tz_name,
        lookback_days=lookback_days,
        model=model,
    )
    log.info(f"  {city} earliest_date = {earliest} ({probe_requests} probe requests)")
    save_probe_cache(city, station_id, lookback_days, earliest, probe_requests, model=model)
    return earliest


# ============================================================
# I/O helpers
# ============================================================

def load_master_csv(path: Path) -> list[dict]:
    if not path.exists():
        log.error(f"market_master.csv not found: {path}")
        return []
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))
    log.info(f"Loaded market_master: {len(rows)} rows")
    return rows


def load_seed_cities(path: Path) -> dict:
    if not path.exists():
        log.error(f"seed_cities.json not found: {path}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Strip meta key
    return {k: v for k, v in data.items() if not k.startswith("_")}


def build_live_tasks(
    master_rows: list[dict],
    active_cities: set[str],
    start_date: Optional[date],
    end_date: Optional[date],
) -> list[dict]:
    """
    Live mode: dedupe enabled markets → unique (station_id, market_date_local) tasks.

    Classifies each task by _api_type (used by run() to choose fetch path):
      'historical'    → market_date < city_today → Historical Forecast API
      'live_forecast' → market_date >= city_today → Forecast API

    city_today is determined per task using the city's timezone.
    """
    seen: set[tuple] = set()
    tasks: list[dict] = []
    historical_count = 0
    live_count = 0

    for row in master_rows:
        if row.get("market_enabled") != "true":
            continue
        city = row.get("city", "")
        if active_cities and city not in active_cities:
            continue
        mdate_str = row.get("market_date_local", "")
        if not mdate_str:
            continue
        try:
            mdate = date.fromisoformat(mdate_str)
        except ValueError:
            continue
        if start_date and mdate < start_date:
            continue
        if end_date and mdate > end_date:
            continue

        tz_name = row.get("timezone", "UTC")
        try:
            city_today = datetime.now(ZoneInfo(tz_name)).date()
        except Exception:
            city_today = date.today()

        key = (row.get("station_id", ""), mdate_str)
        if key in seen:
            continue
        seen.add(key)

        api_type = "historical" if mdate < city_today else "live_forecast"
        if api_type == "historical":
            historical_count += 1
        else:
            live_count += 1

        tasks.append({
            "station_id": row.get("station_id", ""),
            "city": city,
            "lat": row.get("lat", ""),
            "lon": row.get("lon", ""),
            "timezone": tz_name,
            "market_date_local": mdate_str,
            "_api_type": api_type,
            "_city_today": city_today.isoformat(),
        })

    log.info(
        f"Fetch tasks: {len(tasks)} "
        f"(cities={sorted({t['city'] for t in tasks})}, "
        f"historical={historical_count}, live_forecast={live_count})"
    )
    return tasks


def build_historical_tasks(
    seed_cities: dict,
    active_cities: set[str],
    start_date: date,
    end_date: date,
    per_city_start: Optional[dict] = None,
) -> list[dict]:
    """
    Historical mode: generate one task per (city, date) in date range from seed_cities.json.

    per_city_start: if provided, the effective start date is taken per city from this dict.
      Cities absent from per_city_start are skipped entirely (probe found no data).
    """
    today = date.today()
    tasks: list[dict] = []
    skipped_future = 0
    skipped_no_probe_data = 0

    for city, meta in seed_cities.items():
        if active_cities and city not in active_cities:
            continue
        # Probe result: city not in per_city_start → skip
        if per_city_start is not None and city not in per_city_start:
            log.warning(f"  {city}: skipped (no valid previous_day probe data)")
            skipped_no_probe_data += 1
            continue
        effective_start = per_city_start[city] if per_city_start else start_date
        curr = effective_start
        while curr <= end_date:
            if curr >= today:
                skipped_future += 1
                curr += timedelta(days=1)
                continue
            tasks.append({
                "station_id": meta.get("station_code", ""),
                "city": city,
                "lat": str(meta.get("lat", "")),
                "lon": str(meta.get("lon", "")),
                "timezone": meta.get("timezone", "UTC"),
                "market_date_local": curr.isoformat(),
            })
            curr += timedelta(days=1)

    log.info(
        f"Fetch tasks (historical): {len(tasks)} "
        f"(cities={sorted({t['city'] for t in tasks})}, "
        f"skipped_future={skipped_future}, skipped_no_probe_data={skipped_no_probe_data})"
    )
    return tasks


def output_path_for(task: dict) -> Path:
    return (
        PROJ_DIR / "data" / "raw" / "D"
        / task["city"] / MODEL
        / f"forecast_hourly_{task['market_date_local']}.csv"
    )


def load_existing_ok_keys(csv_path: Path) -> set[tuple]:
    """
    Return set of (station_id, market_date_local, lead_day, target_time_local, model)
    where value_status == 'ok'.
    """
    keys: set[tuple] = set()
    if not csv_path.exists():
        return keys
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                if row.get("value_status") == "ok":
                    keys.add((
                        row.get("station_id", ""),
                        row.get("market_date_local", ""),
                        row.get("lead_day", ""),
                        row.get("target_time_local", ""),
                        row.get("model", ""),
                    ))
    except Exception as e:
        log.warning(f"Could not load existing keys from {csv_path}: {e}")
    return keys


def append_rows(csv_path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not csv_path.exists()
    with open(csv_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HOURLY_FIELDS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def write_rows(csv_path: Path, rows: list[dict]) -> None:
    """Overwrite CSV with rows (used for live_forecast to replace stale snapshots)."""
    if not rows:
        return
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HOURLY_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def should_refetch_live(csv_path: Path, max_age_hours: float = LIVE_FORECAST_MAX_CACHE_HOURS) -> bool:
    """
    Return True if live forecast CSV should be re-fetched.
    Uses file mtime as approximation of snapshot age.
    First version: mtime-based, does not distinguish historical/live write sources.
    """
    if not csv_path.exists():
        return True
    mtime = csv_path.stat().st_mtime
    age_hours = (time.time() - mtime) / 3600
    return age_hours > max_age_hours


# ============================================================
# Time helpers
# ============================================================

def local_str_to_utc_str(time_str: str, tz_name: str) -> str:
    """'YYYY-MM-DDTHH:MM' local → 'YYYY-MM-DDTHH:MM:SSZ' UTC"""
    tz = ZoneInfo(tz_name)
    dt_local = datetime.fromisoformat(time_str).replace(tzinfo=tz)
    dt_utc = dt_local.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_snapshot_utc_dt(snapshot_date: date) -> datetime:
    return datetime(
        snapshot_date.year, snapshot_date.month, snapshot_date.day,
        0, 0, 0, tzinfo=timezone.utc,
    )


# ============================================================
# API helpers
# ============================================================

def _do_get(url: str, params: dict) -> Optional[requests.Response]:
    try:
        return requests.get(url, params=params, timeout=30)
    except Exception as e:
        log.warning(f"Request exception: {e}")
        return None


def api_get(url: str, params: dict) -> tuple[Optional[dict], str]:
    """
    Returns (json|None, status_str).
    Raises StopRun on unrecoverable 429.
    status: 'ok' | 'failed' | 'skip_4xx_NNN'
    """
    resp = _do_get(url, params)

    if resp is None:
        for _ in range(3):
            time.sleep(2)
            resp = _do_get(url, params)
            if resp is not None:
                break
        if resp is None:
            return None, "failed"

    if resp.status_code == 200:
        try:
            return resp.json(), "ok"
        except Exception:
            return None, "failed"

    if resp.status_code == 429:
        log.warning("429 rate limit — cooldown 60s, retry once...")
        time.sleep(60)
        resp2 = _do_get(url, params)
        if resp2 and resp2.status_code == 200:
            try:
                return resp2.json(), "ok"
            except Exception:
                return None, "failed"
        code2 = resp2.status_code if resp2 else "None"
        raise StopRun(f"429 after cooldown (second attempt code={code2})")

    if resp.status_code >= 500:
        for _ in range(3):
            time.sleep(2)
            resp = _do_get(url, params)
            if resp and resp.status_code == 200:
                try:
                    return resp.json(), "ok"
                except Exception:
                    return None, "failed"
        log.error(f"5xx after retries: {resp.status_code if resp else 'None'}")
        return None, "failed"

    # 4xx (not 429)
    log.warning(f"4xx response {resp.status_code}, skipping")
    return None, f"skip_4xx_{resp.status_code}"


# ============================================================
# Fetch live forecast (market_date >= city_today)
# ============================================================

def fetch_live_forecast(task: dict, city_today: date) -> list[dict]:
    """
    Fetch GFS seamless forecast for market_date >= city_today using Forecast API.

    lead_day = (market_date - city_today).days
      today's market → 0, tomorrow's → 1, etc.

    snapshot_time_utc = fetch time (not model run initialization time).
    snapshot_mode = "live_forecast".

    Returns [] if market_date exceeds forecast horizon (logs warning).
    """
    market_date_str = task["market_date_local"]
    mdate = date.fromisoformat(market_date_str)
    tz_name = task["timezone"]
    station_id = task["station_id"]

    lead_day = (mdate - city_today).days

    if lead_day > LIVE_FORECAST_MAX_HORIZON_DAYS:
        log.warning(
            f"  {task['city']} {market_date_str}: lead_day={lead_day} "
            f"exceeds forecast horizon ({LIVE_FORECAST_MAX_HORIZON_DAYS}d), skipping"
        )
        return []

    params = {
        "latitude": task["lat"],
        "longitude": task["lon"],
        "hourly": "temperature_2m",
        "models": MODEL,
        "start_date": market_date_str,
        "end_date": market_date_str,
        "timezone": tz_name,
    }

    now_utc = datetime.now(timezone.utc)
    snapshot_time_utc = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        snap_local_str = now_utc.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        snap_local_str = snapshot_time_utc

    data, status = api_get(LIVE_FORECAST_API, params)
    time.sleep(REQUEST_INTERVAL)

    rows: list[dict] = []

    if status != "ok" or data is None:
        rows.append({
            "snapshot_time_utc": snapshot_time_utc,
            "snapshot_time_local": snap_local_str,
            "target_time_utc": "",
            "target_time_local": "",
            "market_date_local": market_date_str,
            "lead_hours": "",
            "lead_day": lead_day,
            "city": task["city"],
            "station_id": station_id,
            "model": MODEL,
            "forecast_temp": "",
            "source_api": "Open-Meteo forecast",
            "snapshot_mode": "live_forecast",
            "value_status": "failed",
        })
        return rows

    hourly = data.get("hourly", {})
    times_list = hourly.get("time", [])
    temps_list = hourly.get("temperature_2m", [])

    if not times_list or not temps_list:
        rows.append({
            "snapshot_time_utc": snapshot_time_utc,
            "snapshot_time_local": snap_local_str,
            "target_time_utc": "",
            "target_time_local": "",
            "market_date_local": market_date_str,
            "lead_hours": "",
            "lead_day": lead_day,
            "city": task["city"],
            "station_id": station_id,
            "model": MODEL,
            "forecast_temp": "",
            "source_api": "Open-Meteo forecast",
            "snapshot_mode": "live_forecast",
            "value_status": "no_data",
        })
        return rows

    for time_str, temp in zip(times_list, temps_list):
        try:
            target_utc_str = local_str_to_utc_str(time_str, tz_name)
            target_dt_utc = datetime.fromisoformat(target_utc_str.replace("Z", "+00:00"))
            lead_hours = int((target_dt_utc - now_utc).total_seconds() / 3600)
        except Exception:
            target_utc_str = ""
            lead_hours = ""

        if temp is None:
            value_status = "null"
            forecast_temp_val = ""
        else:
            value_status = "ok"
            forecast_temp_val = temp

        rows.append({
            "snapshot_time_utc": snapshot_time_utc,
            "snapshot_time_local": snap_local_str,
            "target_time_utc": target_utc_str,
            "target_time_local": time_str,
            "market_date_local": market_date_str,
            "lead_hours": lead_hours,
            "lead_day": lead_day,
            "city": task["city"],
            "station_id": station_id,
            "model": MODEL,
            "forecast_temp": forecast_temp_val,
            "source_api": "Open-Meteo forecast",
            "snapshot_mode": "live_forecast",
            "value_status": value_status,
        })

    return rows


# ============================================================
# Fetch one market_date (all lead_days in a single API call)
# ============================================================

def fetch_market_date(
    task: dict,
    existing_ok_keys: set[tuple],
    lookback_days: int,
) -> list[dict]:
    """
    1 API call per market_date.
    Requests temperature_2m_previous_day1 ... previous_day{lookback_days}.
    Each field = GFS model run from (market_date - N days) predicting market_date.
    Returns list of row dicts to append.
    """
    market_date_str = task["market_date_local"]
    mdate = date.fromisoformat(market_date_str)
    tz_name = task["timezone"]
    station_id = task["station_id"]

    # Determine which lead_days need fetching
    lead_days_to_fetch = []
    for ld in range(1, lookback_days + 1):
        ld_str = str(ld)
        cached_count = sum(
            1 for k in existing_ok_keys
            if k[0] == station_id
            and k[1] == market_date_str
            and k[2] == ld_str
            and k[4] == MODEL
        )
        if cached_count < 24:
            lead_days_to_fetch.append(ld)

    if not lead_days_to_fetch:
        log.debug(f"  All {lookback_days} lead_days cached, skip API call")
        return []

    # Build the hourly fields string for all needed lead_days
    hourly_fields = ",".join(
        PREV_DAY_FIELDS[ld] for ld in range(1, lookback_days + 1)
        if ld in PREV_DAY_FIELDS
    )

    params = {
        "latitude": task["lat"],
        "longitude": task["lon"],
        "hourly": hourly_fields,
        "start_date": market_date_str,
        "end_date": market_date_str,
        "models": MODEL,
        "timezone": tz_name,
    }

    data, status = api_get(FORECAST_API, params)
    time.sleep(REQUEST_INTERVAL)

    rows: list[dict] = []

    if status != "ok" or data is None:
        # Write one failed row per needed lead_day
        for ld in lead_days_to_fetch:
            snapshot_date = mdate - timedelta(days=ld)
            snap_utc_str = make_snapshot_utc_dt(snapshot_date).strftime("%Y-%m-%dT%H:%M:%SZ")
            rows.append({
                "snapshot_time_utc": snap_utc_str,
                "snapshot_time_local": "",
                "target_time_utc": "",
                "target_time_local": "",
                "market_date_local": market_date_str,
                "lead_hours": "",
                "lead_day": ld,
                "city": task["city"],
                "station_id": station_id,
                "model": MODEL,
                "forecast_temp": "",
                "source_api": "Open-Meteo historical forecast",
                "snapshot_mode": "historical_forecast_previous_day",
                "value_status": "failed",
            })
        return rows

    hourly = data.get("hourly", {})
    times_list = hourly.get("time", [])

    for ld in range(1, lookback_days + 1):
        if ld not in PREV_DAY_FIELDS:
            continue
        field_key = PREV_DAY_FIELDS[ld]
        temps_list = hourly.get(field_key, [])

        snapshot_date = mdate - timedelta(days=ld)
        snap_utc_dt = make_snapshot_utc_dt(snapshot_date)
        snap_utc_str = snap_utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            snap_local_str = snap_utc_dt.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            snap_local_str = snap_utc_str

        ld_str = str(ld)

        if not times_list or not temps_list:
            rows.append({
                "snapshot_time_utc": snap_utc_str,
                "snapshot_time_local": snap_local_str,
                "target_time_utc": "",
                "target_time_local": "",
                "market_date_local": market_date_str,
                "lead_hours": "",
                "lead_day": ld,
                "city": task["city"],
                "station_id": station_id,
                "model": MODEL,
                "forecast_temp": "",
                "source_api": "Open-Meteo historical forecast",
                "snapshot_mode": "historical_forecast_previous_day",
                "value_status": "no_data",
            })
            continue

        for time_str, temp in zip(times_list, temps_list):
            dedup_key = (station_id, market_date_str, ld_str, time_str, MODEL)
            if dedup_key in existing_ok_keys and temp is not None:
                continue

            try:
                target_utc_str = local_str_to_utc_str(time_str, tz_name)
                target_dt_utc = datetime.fromisoformat(target_utc_str.replace("Z", "+00:00"))
                lead_hours = int((target_dt_utc - snap_utc_dt).total_seconds() / 3600)
            except Exception:
                target_utc_str = ""
                lead_hours = ""

            if temp is None:
                value_status = "null"
                forecast_temp_val = ""
            else:
                value_status = "ok"
                forecast_temp_val = temp

            rows.append({
                "snapshot_time_utc": snap_utc_str,
                "snapshot_time_local": snap_local_str,
                "target_time_utc": target_utc_str,
                "target_time_local": time_str,
                "market_date_local": market_date_str,
                "lead_hours": lead_hours,
                "lead_day": ld,
                "city": task["city"],
                "station_id": station_id,
                "model": MODEL,
                "forecast_temp": forecast_temp_val,
                "source_api": "Open-Meteo historical forecast",
                "snapshot_mode": "historical_forecast_previous_day",
                "value_status": value_status,
            })

    return rows


# ============================================================
# Main logic
# ============================================================

def run(
    cities: str,
    start_date_str: str,
    end_date_str: str,
    lookback_days: int,
    mode: str,
    dry_run: bool,
    verbose: bool,
    skip_probe: bool = False,
    force_probe: bool = False,
) -> bool:
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=" * 50)
    log.info("05_D_forecast_fetch: D1 gfs_seamless forecast")
    log.info("=" * 50)

    active_cities = {c.strip() for c in cities.split(",") if c.strip()} if cities else set()
    start_date = date.fromisoformat(start_date_str) if start_date_str else None
    end_date = date.fromisoformat(end_date_str) if end_date_str else None

    if mode == "historical":
        if not start_date or not end_date:
            log.error("historical mode requires --start-date and --end-date")
            return False
        seed_path = PROJ_DIR / "config" / "seed_cities.json"
        seed_cities = load_seed_cities(seed_path)
        if not seed_cities:
            return False

        # ── Probe: detect earliest valid date per city ──────────────────
        per_city_start: dict[str, date] = {}
        for city_name, meta in seed_cities.items():
            if active_cities and city_name not in active_cities:
                continue
            if skip_probe:
                per_city_start[city_name] = start_date
                log.info(f"  {city_name}: --skip-probe, using requested start_date {start_date}")
                continue
            earliest_str = get_earliest_date_for_city(
                city=city_name,
                meta=meta,
                lookback_days=lookback_days,
                force_probe=force_probe,
            )
            if earliest_str is None:
                log.warning(
                    f"{city_name}: no valid previous_day{lookback_days} data found "
                    f"between {PROBE_EARLIEST_YEAR}-01-01 and now; skipping city"
                )
                continue  # city absent from per_city_start → skipped by build_historical_tasks
            earliest_date = date.fromisoformat(earliest_str)
            if start_date < earliest_date:
                log.info(
                    f"  Clamping start_date: {start_date} → {earliest_date} "
                    f"(earliest available previous_day data)"
                )
                per_city_start[city_name] = earliest_date
            else:
                log.info(
                    f"  Requested start_date {start_date} is within valid range "
                    f"(earliest={earliest_date}); no clamp needed"
                )
                per_city_start[city_name] = start_date

        tasks = build_historical_tasks(
            seed_cities, active_cities, start_date, end_date, per_city_start=per_city_start
        )
    else:
        master_path = PROJ_DIR / "data" / "market_master.csv"
        master_rows = load_master_csv(master_path)
        if not master_rows:
            log.error("No master rows loaded")
            return False
        tasks = build_live_tasks(master_rows, active_cities, start_date, end_date)

    if not tasks:
        log.info("No tasks to fetch (all future or filtered out)")
        return True

    total_rows_written = 0

    try:
        for i, task in enumerate(tasks, 1):
            api_type = task.get("_api_type", "historical")
            city_today_str = task.get("_city_today", date.today().isoformat())
            city_today = date.fromisoformat(city_today_str)

            log.info(
                f"[{i}/{len(tasks)}] {task['city']} {task['market_date_local']}"
                f"  station={task['station_id']}  api={api_type}"
            )
            out_path = output_path_for(task)

            if dry_run:
                if api_type == "live_forecast":
                    lead_day = (date.fromisoformat(task["market_date_local"]) - city_today).days
                    log.info(f"  [DRY RUN] live_forecast lead_day={lead_day}")
                else:
                    for ld in range(1, lookback_days + 1):
                        snapshot_date = date.fromisoformat(task["market_date_local"]) - timedelta(days=ld)
                        log.info(f"  [DRY RUN] lead_day={ld} snapshot={snapshot_date}")
                continue

            if api_type == "live_forecast":
                live_lead_day = (date.fromisoformat(task["market_date_local"]) - city_today).days
                if live_lead_day > LIVE_FORECAST_MAX_HORIZON_DAYS:
                    log.info(
                        f"  Skipping {task['city']} {task['market_date_local']}: "
                        f"lead_day={live_lead_day} > {LIVE_FORECAST_MAX_HORIZON_DAYS}, beyond GFS range"
                    )
                    continue
                if not should_refetch_live(out_path):
                    age_h = (time.time() - out_path.stat().st_mtime) / 3600
                    log.info(f"  Cached (age={age_h:.1f}h < {LIVE_FORECAST_MAX_CACHE_HOURS}h), skip")
                    continue
                new_rows = fetch_live_forecast(task, city_today)
                if new_rows:
                    write_rows(out_path, new_rows)
                    total_rows_written += len(new_rows)
                    ok_count = sum(1 for r in new_rows if r.get("value_status") == "ok")
                    log.info(
                        f"  → {len(new_rows)} rows (ok={ok_count}) "
                        f"→ {out_path.relative_to(PROJ_DIR)}"
                    )
                else:
                    log.info("  No rows returned (horizon exceeded or error)")
            else:
                existing_ok = load_existing_ok_keys(out_path)
                new_rows = fetch_market_date(task, existing_ok, lookback_days)
                if new_rows:
                    append_rows(out_path, new_rows)
                    total_rows_written += len(new_rows)
                    ok_count = sum(1 for r in new_rows if r.get("value_status") == "ok")
                    log.info(
                        f"  → {len(new_rows)} rows (ok={ok_count}) "
                        f"→ {out_path.relative_to(PROJ_DIR)}"
                    )
                else:
                    log.info("  No new rows (all cached)")

    except StopRun as e:
        log.error(f"Run stopped: {e}")
        return False

    log.info(f"05_D_forecast_fetch done. Total rows written: {total_rows_written}")
    return True


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="D1 gfs_seamless forecast fetch")
    p.add_argument(
        "--cities", type=str, default="",
        help="城市 filter（逗號分隔，空白=從 market_master 讀取所有 enabled）",
    )
    p.add_argument("--start-date", type=str, default="", help="market_date 起始日 YYYY-MM-DD")
    p.add_argument("--end-date", type=str, default="", help="market_date 結束日 YYYY-MM-DD")
    p.add_argument("--lookback-days", type=int, default=7, help="往前推幾個 lead_day（預設 7）")
    p.add_argument(
        "--mode", type=str, default="live", choices=["live", "historical"],
        help="live=從 market_master 讀日期；historical=直接用 start/end date + seed_cities",
    )
    p.add_argument("--dry-run", action="store_true", help="只印計畫不實際抓取")
    p.add_argument(
        "--skip-probe", action="store_true", dest="skip_probe",
        help="跳過 probe，直接用 --start-date（已知資料範圍時省時間）",
    )
    p.add_argument(
        "--force-probe", action="store_true", dest="force_probe",
        help="忽略 cache，強制重新偵測 earliest date",
    )
    p.add_argument("--verbose", action="store_true")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    ok = run(
        cities=args.cities,
        start_date_str=args.start_date,
        end_date_str=args.end_date,
        lookback_days=args.lookback_days,
        mode=args.mode,
        dry_run=args.dry_run,
        verbose=args.verbose,
        skip_probe=args.skip_probe,
        force_probe=args.force_probe,
    )
    sys.exit(0 if ok else 1)
