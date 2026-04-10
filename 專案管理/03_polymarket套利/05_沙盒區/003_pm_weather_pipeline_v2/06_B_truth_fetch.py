"""
06_B_truth_fetch.py — WU 真值抓取 (experimental)

改版說明（STEP 3.5）：
  新增 --mode historical，可直接用 start/end date + seed_cities.json 生成任務，
  不依賴 market_master.csv（供歷史回補使用）。

⚠️ 資料源風險：
  使用 WU 頁面嵌入的 hidden API key 打 api.weather.com。
  - 非官方公開 API，可能隨時失效
  - 429 / 403 → 停止整個 run（被封風險較高）
  - 每次 request 間隔至少 1 秒

讀取：
  data/market_master.csv → enabled markets

API Key 雙軌機制：
  1. config/wu_api_key.txt（優先）
  2. 從 WU 頁面 HTML 提取 apiKey= 或 SUN_API_KEY=（fallback）

API：
  https://api.weather.com/v1/location/{station}:9:{country}/observations/historical.json
  params: apiKey, units=e, startDate=YYYYMMDD, endDate=YYYYMMDD
  daily_high_f = max(observations[*].temp)
  daily_high_c = (°F - 32) × 5 / 9

輸出：
  data/raw/B/{city}/truth_daily_high.csv（append 模式，已存在日期不重抓）
"""

import argparse
import csv
import json
import logging
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

WU_API_BASE = "https://api.weather.com/v1/location/{station}:9:{country}/observations/historical.json"
REQUEST_INTERVAL = 1  # seconds between API requests

TRUTH_FIELDS = [
    "market_date_local",
    "city",
    "station_id",
    "country",
    "actual_daily_high_f",
    "actual_daily_high_c",
    "observation_count",
    "truth_status",
    "source_name",
    "source_contract_type",
    "wu_api_url",
    "fetch_time_utc",
]


class StopRun(Exception):
    pass


# ============================================================
# API Key management
# ============================================================

def load_api_key_from_file(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    key = path.read_text(encoding="utf-8").strip()
    if key:
        log.info(f"WU API key loaded from {path.name}")
        return key
    return None


def extract_api_key_from_wu_html(settlement_url: str) -> Optional[str]:
    """
    Fallback: fetch WU page HTML and extract embedded API key.
    Tries patterns: apiKey=XXX and SUN_API_KEY=XXX
    Note: WU pages are JS-rendered; extraction is best-effort.
    """
    if not settlement_url:
        return None
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
        resp = requests.get(settlement_url, headers=headers, timeout=20)
        if resp.status_code != 200:
            log.warning(f"WU page returned {resp.status_code}: {settlement_url}")
            return None
        html = resp.text
        patterns = [
            r'["\s]apiKey["\s]*[:=]["\s]*([A-Za-z0-9]{20,})',
            r'SUN_API_KEY["\s]*[:=]["\s]*([A-Za-z0-9]{20,})',
            r'"key"\s*:\s*"([A-Za-z0-9]{20,})"',
        ]
        for pat in patterns:
            m = re.search(pat, html)
            if m:
                key = m.group(1)
                log.info(f"WU API key extracted from HTML (pattern matched)")
                return key
        log.warning("Could not extract WU API key from HTML")
    except Exception as e:
        log.warning(f"HTML key extraction failed: {e}")
    return None


def get_api_key(master_rows: list[dict]) -> Optional[str]:
    """
    Try file first, then fallback to HTML extraction using first enabled city's settlement_url.
    """
    key_path = PROJ_DIR / "config" / "wu_api_key.txt"
    key = load_api_key_from_file(key_path)
    if key:
        return key

    log.info("wu_api_key.txt not found, attempting HTML extraction...")
    for row in master_rows:
        if row.get("market_enabled") != "true":
            continue
        url = row.get("settlement_url", "")
        if "wunderground.com" in url:
            key = extract_api_key_from_wu_html(url)
            if key:
                return key
            break

    if not key:
        log.error(
            "Could not obtain WU API key. "
            "Create config/wu_api_key.txt with the key, or ensure WU page is accessible."
        )
    return key


# ============================================================
# I/O helpers
# ============================================================

def load_seed_cities(path: Path) -> dict:
    if not path.exists():
        log.error(f"seed_cities.json not found: {path}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {k: v for k, v in data.items() if not k.startswith("_")}


def build_historical_tasks(
    seed_cities: dict,
    active_cities: set[str],
    start_date: date,
    end_date: date,
) -> list[dict]:
    """Historical mode: generate one task per (city, date) from seed_cities.json"""
    today = date.today()
    tasks: list[dict] = []
    for city, meta in seed_cities.items():
        if active_cities and city not in active_cities:
            continue
        curr = start_date
        while curr <= end_date:
            mdate = curr
            tasks.append({
                "station_id": meta.get("station_code", ""),
                "city": city,
                "country": meta.get("country", ""),
                "timezone": meta.get("timezone", "UTC"),
                "market_date_local": mdate.isoformat(),
                "market_date_obj": mdate,
                "settlement_url": meta.get("settlement_url", ""),
                "is_future": mdate >= today,
            })
            curr += timedelta(days=1)
    log.info(
        f"Fetch tasks (historical): {len(tasks)} "
        f"(cities={sorted({t['city'] for t in tasks})})"
    )
    return tasks


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


def build_fetch_tasks(
    master_rows: list[dict],
    active_cities: set[str],
    start_date: Optional[date],
    end_date: Optional[date],
) -> list[dict]:
    """dedupe enabled markets → unique (station_id, country, market_date_local) tasks"""
    today = date.today()
    seen: set[tuple] = set()
    tasks: list[dict] = []

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

        key = (row.get("station_id", ""), row.get("country", ""), mdate_str)
        if key in seen:
            continue
        seen.add(key)
        tasks.append({
            "station_id": row.get("station_id", ""),
            "city": city,
            "country": row.get("country", ""),
            "timezone": row.get("timezone", ""),
            "market_date_local": mdate_str,
            "market_date_obj": mdate,
            "settlement_url": row.get("settlement_url", ""),
            "is_future": mdate >= today,
        })

    log.info(
        f"Fetch tasks: {len(tasks)} "
        f"(cities={sorted({t['city'] for t in tasks})})"
    )
    return tasks


def output_path_for(city: str) -> Path:
    return PROJ_DIR / "data" / "raw" / "B" / city / "truth_daily_high.csv"


STATUS_PRIORITY = {"complete": 4, "partial": 3, "missing": 2, "future": 1}


def load_existing_date_status(csv_path: Path) -> dict[str, dict]:
    """
    Read existing truth CSV and return per-date status.
    Returns dict[date_str, {"truth_status": str, "observation_count": str}].
    Duplicate dates: keep the row with higher STATUS_PRIORITY.
    Rows missing truth_status or date: excluded (conservative → won't skip).
    CSV read failure: log warning, return {} (won't skip anything).
    """
    result: dict[str, dict] = {}
    if not csv_path.exists():
        return result
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                d = row.get("market_date_local", "").strip()
                status = row.get("truth_status", "").strip()
                if not d or not status:
                    continue
                if d in result:
                    existing_priority = STATUS_PRIORITY.get(result[d]["truth_status"], 0)
                    new_priority = STATUS_PRIORITY.get(status, 0)
                    if new_priority <= existing_priority:
                        continue
                result[d] = {
                    "truth_status": status,
                    "observation_count": row.get("observation_count", ""),
                }
    except Exception as e:
        log.warning(f"Could not load existing date status from {csv_path}: {e}")
        return {}
    return result


def should_skip_date(date_str: str, existing_status: dict[str, dict]) -> bool:
    """Only skip dates confirmed stable: truth_status == 'complete'."""
    entry = existing_status.get(date_str)
    if entry is None:
        return False
    return entry.get("truth_status") == "complete"


def append_single_row(csv_path: Path, row: dict) -> None:
    """Append one row immediately; flush to disk after write."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = csv_path.exists() and csv_path.stat().st_size > 0
    with open(csv_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=TRUTH_FIELDS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
        f.flush()


def dedupe_csv_if_needed(csv_path: Path, key_field: str = "market_date_local") -> int:
    """
    If the CSV has duplicate key_field values, rewrite keeping the last row per key.
    Returns number of duplicate rows removed (0 = nothing changed, file untouched).
    """
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return 0
    try:
        rows: list[dict] = []
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
    except Exception as e:
        log.warning(f"dedupe: could not read {csv_path}: {e}")
        return 0

    # Keep last occurrence per key (append semantics: last write wins)
    seen: dict[str, dict] = {}
    for row in rows:
        k = row.get(key_field, "")
        seen[k] = row  # later rows overwrite earlier ones

    deduped = list(seen.values())
    removed = len(rows) - len(deduped)
    if removed == 0:
        return 0

    log.info(f"  dedupe: removing {removed} duplicate rows from {csv_path.name}")
    try:
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=TRUTH_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(deduped)
    except Exception as e:
        log.warning(f"dedupe: rewrite failed: {e}")
        return 0
    return removed


# ============================================================
# Temperature conversion
# ============================================================

def f_to_c(f: float) -> float:
    return round((f - 32) * 5 / 9, 2)


# ============================================================
# WU API fetch
# ============================================================

def _do_get(url: str, params: dict) -> Optional[requests.Response]:
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        return requests.get(url, params=params, headers=headers, timeout=30)
    except Exception as e:
        log.warning(f"Request exception: {e}")
        return None


def fetch_truth_day(
    station_id: str,
    country: str,
    market_date: date,
    api_key: str,
) -> tuple[Optional[dict], str]:
    """
    Fetch WU hourly observations for one day.
    Returns (result_dict, status_str). Raises StopRun on 429/403.

    result_dict keys: daily_high_f, daily_high_c, observation_count
    status: 'ok' | 'missing' | 'failed'
    """
    date_str = market_date.strftime("%Y%m%d")
    url = WU_API_BASE.format(station=station_id, country=country)
    params = {
        "apiKey": api_key,
        "units": "e",
        "startDate": date_str,
        "endDate": date_str,
    }
    # URL for audit (without apiKey)
    audit_url = f"{url}?units=e&startDate={date_str}&endDate={date_str}"

    resp = _do_get(url, params)
    if resp is None:
        return None, "failed"

    if resp.status_code in (429, 403):
        raise StopRun(
            f"WU API returned {resp.status_code} — stopping run "
            f"(station={station_id}, date={market_date})"
        )

    if resp.status_code != 200:
        log.warning(f"WU API {resp.status_code} for {station_id} {market_date}")
        return {"audit_url": audit_url}, "failed"

    try:
        data = resp.json()
    except Exception:
        return {"audit_url": audit_url}, "failed"

    observations = data.get("observations", [])
    if not observations:
        return {
            "daily_high_f": None,
            "daily_high_c": None,
            "observation_count": 0,
            "audit_url": audit_url,
        }, "missing"

    temps = [o.get("temp") for o in observations if o.get("temp") is not None]
    observation_count = len(observations)

    if not temps:
        return {
            "daily_high_f": None,
            "daily_high_c": None,
            "observation_count": observation_count,
            "audit_url": audit_url,
        }, "missing"

    daily_high_f = max(temps)
    daily_high_c = f_to_c(daily_high_f)

    return {
        "daily_high_f": daily_high_f,
        "daily_high_c": daily_high_c,
        "observation_count": observation_count,
        "audit_url": audit_url,
    }, "ok"


def compute_truth_status(observation_count: int) -> str:
    if observation_count >= 20:
        return "complete"
    elif observation_count >= 10:
        return "partial"
    else:
        return "missing"


# ============================================================
# Main logic
# ============================================================

def run(
    cities: str,
    start_date_str: str,
    end_date_str: str,
    mode: str,
    dry_run: bool,
    verbose: bool,
) -> bool:
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=" * 50)
    log.info("06_B_truth_fetch: WU daily high truth (experimental)")
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
        tasks = build_historical_tasks(seed_cities, active_cities, start_date, end_date)
        # For API key fallback, we still need some WU URL; build a minimal master_rows proxy
        master_rows = [
            {"market_enabled": "true", "settlement_url": meta.get("settlement_url", "")}
            for meta in seed_cities.values()
            if "wunderground.com" in meta.get("settlement_url", "")
        ]
    else:
        master_path = PROJ_DIR / "data" / "market_master.csv"
        master_rows = load_master_csv(master_path)
        if not master_rows:
            log.error("No master rows loaded")
            return False
        tasks = build_fetch_tasks(master_rows, active_cities, start_date, end_date)
    if not tasks:
        log.info("No tasks to fetch")
        return True

    if dry_run:
        for t in tasks:
            status = "future→skip" if t["is_future"] else "would fetch"
            log.info(f"  [DRY RUN] {t['city']} {t['market_date_local']} ({status})")
        return True

    # Obtain API key once
    api_key = get_api_key(master_rows)
    if not api_key:
        return False

    # Group tasks by city (for per-city output file)
    by_city: dict[str, list[dict]] = {}
    for t in tasks:
        by_city.setdefault(t["city"], []).append(t)

    total_written = 0

    try:
        for city, city_tasks in by_city.items():
            out_path = output_path_for(city)

            # Step 1: dedupe before reading status
            dedupe_csv_if_needed(out_path)

            # Step 2: load status after dedupe
            existing_status = load_existing_date_status(out_path)
            fetch_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            # Step 3: pre-classify for summary logging
            stable_cached = 0
            nonfinal_refetch = 0
            future_skipped = 0
            to_fetch: list[dict] = []

            for task in city_tasks:
                mdate_str = task["market_date_local"]
                if task["is_future"]:
                    future_skipped += 1
                    continue
                if should_skip_date(mdate_str, existing_status):
                    stable_cached += 1
                    continue
                if mdate_str in existing_status:
                    nonfinal_refetch += 1
                to_fetch.append(task)

            log.info(f"  {city}: {len(city_tasks)} total dates")
            log.info(f"    stable cached (complete): {stable_cached}")
            log.info(f"    non-final to refetch (future/partial/missing): {nonfinal_refetch}")
            log.info(f"    future skipped: {future_skipped}")
            log.info(f"    to fetch: {len(to_fetch)}")

            # Step 4: fetch and append per-row immediately
            city_written = 0
            for task in to_fetch:
                mdate_str = task["market_date_local"]
                mdate = task["market_date_obj"]

                log.info(f"  Fetching {city} {mdate_str} (station={task['station_id']})")
                result, status = fetch_truth_day(
                    task["station_id"], task["country"], mdate, api_key
                )
                time.sleep(REQUEST_INTERVAL)

                # Do not append on fetch failure (result is None, status == "failed" with no result)
                if result is None:
                    log.warning(f"  {city} {mdate_str}: fetch failed, not written")
                    continue

                obs_count = result.get("observation_count", 0) or 0
                if status == "ok":
                    truth_status = compute_truth_status(obs_count)
                else:
                    truth_status = "missing"

                row = {
                    "market_date_local": mdate_str,
                    "city": city,
                    "station_id": task["station_id"],
                    "country": task["country"],
                    "actual_daily_high_f": result.get("daily_high_f") or "",
                    "actual_daily_high_c": result.get("daily_high_c") or "",
                    "observation_count": obs_count,
                    "truth_status": truth_status,
                    "source_name": "wu_hidden_api",
                    "source_contract_type": "experimental",
                    "wu_api_url": result.get("audit_url", ""),
                    "fetch_time_utc": fetch_time_utc,
                }
                append_single_row(out_path, row)
                # Update in-memory cache so same-run duplicates are caught
                existing_status[mdate_str] = {
                    "truth_status": truth_status,
                    "observation_count": str(obs_count),
                }
                city_written += 1
                log.info(f"    → {truth_status} (obs={obs_count})")

            total_written += city_written
            if city_written:
                log.info(f"  {city}: wrote {city_written} rows → {out_path.relative_to(PROJ_DIR)}")

    except StopRun as e:
        log.error(f"Run stopped: {e}")
        return False

    log.info(f"06_B_truth_fetch done. Total rows written: {total_written}")
    return True


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="WU truth fetch (experimental) → data/raw/B/"
    )
    p.add_argument("--cities", type=str, default="", help="城市 filter（逗號分隔）")
    p.add_argument("--start-date", type=str, default="", help="market_date 起始日 YYYY-MM-DD")
    p.add_argument("--end-date", type=str, default="", help="market_date 結束日 YYYY-MM-DD")
    p.add_argument(
        "--mode", type=str, default="live", choices=["live", "historical"],
        help="live=從 market_master 讀日期；historical=直接用 start/end date + seed_cities",
    )
    p.add_argument("--dry-run", action="store_true", help="只印計畫不實際抓取")
    p.add_argument("--verbose", action="store_true")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    ok = run(
        cities=args.cities,
        start_date_str=args.start_date,
        end_date_str=args.end_date,
        mode=args.mode,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
    sys.exit(0 if ok else 1)
