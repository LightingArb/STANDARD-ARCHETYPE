"""
07_daily_high_pipeline.py — Daily High 主表生成

不做任何 API 呼叫，只讀本地 CSV。

輸入：
  data/raw/D/{city}/gfs_seamless/forecast_hourly_{market_date}.csv（05 輸出）
  data/raw/B/{city}/truth_daily_high.csv（06 輸出）
  data/market_master.csv（讀取 enabled markets + station_id→timezone 映射）

Step A：forecast_hourly_raw → forecast_daily_high_snapshot（表 2）
  按 (station_id, snapshot_time_utc, market_date_local) 分組
  max(forecast_temp) → predicted_daily_high
  輸出 data/processed/forecast_daily_high/{city}/forecast_daily_high.csv

Step B：raw/B → truth_daily_high_b canonical 表（表 3）
  dedupe on (station_id, market_date_local)，取最新 fetch_time_utc
  輸出 data/processed/truth_daily_high/truth_daily_high_b.csv

Step C：join A+B → market_day_error_table（表 4）
  join key: (station_id, market_date_local)
  error = actual_daily_high_c - predicted_daily_high
  只保留 truth_status==complete AND forecast_status==complete
  輸出 data/processed/error_table/{city}/market_day_error_table.csv
"""

import argparse
import csv
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ============================================================
# Output field definitions
# ============================================================

FORECAST_DAILY_FIELDS = [
    "snapshot_time_utc",
    "snapshot_time_local",
    "market_date_local",
    "lead_day",
    "lead_hours_to_settlement",
    "city",
    "station_id",
    "model",
    "predicted_daily_high",
    "hourly_count",
    "expected_hourly_count",
    "forecast_status",
    "snapshot_mode",
]

TRUTH_CANONICAL_FIELDS = [
    "market_date_local",
    "city",
    "station_id",
    "country",
    "actual_daily_high_c",
    "actual_daily_high_f",
    "observation_count",
    "truth_status",
    "source_name",
    "source_contract_type",
    "fetch_time_utc",
]

ERROR_TABLE_FIELDS = [
    "snapshot_time_utc",
    "market_date_local",
    "lead_day",
    "lead_hours_to_settlement",
    "city",
    "station_id",
    "model",
    "predicted_daily_high",
    "actual_daily_high",
    "error",
    "truth_status",
    "forecast_status",
    "source_name",
    "source_contract_type",
]


# ============================================================
# Helper: load market_master for timezone/city mapping
# ============================================================

def load_master_csv(path: Path) -> list[dict]:
    if not path.exists():
        log.error(f"market_master.csv not found: {path}")
        return []
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))
    return rows


def build_station_meta(master_rows: list[dict]) -> dict[str, dict]:
    """station_id → {city, timezone, country} (first occurrence)"""
    meta: dict[str, dict] = {}
    for row in master_rows:
        sid = row.get("station_id", "")
        if sid and sid not in meta:
            meta[sid] = {
                "city": row.get("city", ""),
                "timezone": row.get("timezone", ""),
                "country": row.get("country", ""),
            }
    return meta


def build_station_meta_from_seed(path: Path) -> dict[str, dict]:
    """Fallback: load station_id → {city, timezone, country} from seed_cities.json"""
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        log.warning(f"Could not load seed_cities.json: {e}")
        return {}
    meta: dict[str, dict] = {}
    for city, info in data.items():
        if city.startswith("_"):
            continue
        station_id = info.get("station_code", "")
        if station_id:
            meta[station_id] = {
                "city": city,
                "timezone": info.get("timezone", "UTC"),
                "country": info.get("country", ""),
            }
    return meta


# ============================================================
# Time helpers
# ============================================================

def utc_str_to_dt(s: str) -> Optional[datetime]:
    """Parse 'YYYY-MM-DDTHH:MM:SSZ' or 'YYYY-MM-DDTHH:MM:SS+00:00' → UTC datetime"""
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def settlement_end_utc(market_date_str: str, tz_name: str) -> Optional[datetime]:
    """
    Returns UTC datetime for midnight at END of market_date_local
    (i.e., start of market_date_local + 1 day in city timezone).
    """
    try:
        tz = ZoneInfo(tz_name)
        next_day = date.fromisoformat(market_date_str) + timedelta(days=1)
        local_midnight = datetime(
            next_day.year, next_day.month, next_day.day,
            0, 0, 0, tzinfo=tz,
        )
        return local_midnight.astimezone(timezone.utc)
    except Exception:
        return None


def utc_to_local_str(utc_str: str, tz_name: str) -> str:
    """Convert UTC ISO string to local datetime string."""
    try:
        dt_utc = utc_str_to_dt(utc_str)
        if dt_utc is None:
            return ""
        dt_local = dt_utc.astimezone(ZoneInfo(tz_name))
        return dt_local.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return ""


# ============================================================
# STEP A: forecast_hourly_raw → forecast_daily_high_snapshot
# ============================================================

def _safe_float(val) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def determine_forecast_status(hourly_count: int, expected: int) -> str:
    if hourly_count == 0:
        return "failed"
    if hourly_count == expected:
        return "complete"
    if hourly_count >= 16:
        return "partial"
    return "missing"


def run_step_a(
    cities_filter: set[str],
    station_meta: dict[str, dict],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> dict[str, list[dict]]:
    """
    Read all forecast_hourly_*.csv files under data/raw/D/,
    aggregate to daily high per (station_id, snapshot_time_utc, market_date_local).

    Returns {city: [forecast_daily_row, ...]}
    """
    raw_d_root = PROJ_DIR / "data" / "raw" / "D"
    if not raw_d_root.exists():
        log.warning(f"data/raw/D/ does not exist, skipping Step A")
        return {}

    # Collect all hourly CSV paths
    hourly_files = list(raw_d_root.glob("*/gfs_seamless/forecast_hourly_*.csv"))
    log.info(f"Step A: found {len(hourly_files)} forecast_hourly CSV files")

    # Build aggregation: key = (station_id, snapshot_time_utc, market_date_local)
    # value = {temps: [...], lead_day: str, model: str, snapshot_mode: str, city: str}
    agg: dict[tuple, dict] = {}

    for fpath in hourly_files:
        # city = fpath.parts[-3] (raw/D/{city}/gfs_seamless/...)
        city_from_path = fpath.parts[-3]
        if cities_filter and city_from_path not in cities_filter:
            continue

        try:
            with open(fpath, "r", encoding="utf-8", newline="") as f:
                for row in csv.DictReader(f):
                    if row.get("value_status") not in ("ok", "null"):
                        continue
                    station_id = row.get("station_id", "")
                    snap_utc = row.get("snapshot_time_utc", "")
                    mdate = row.get("market_date_local", "")
                    if not (station_id and snap_utc and mdate):
                        continue
                    if start_date or end_date:
                        try:
                            mdate_parsed = date.fromisoformat(mdate)
                            if start_date and mdate_parsed < start_date:
                                continue
                            if end_date and mdate_parsed > end_date:
                                continue
                        except ValueError:
                            pass

                    key = (station_id, snap_utc, mdate)
                    if key not in agg:
                        agg[key] = {
                            "temps": [],
                            "lead_day": row.get("lead_day", ""),
                            "model": row.get("model", ""),
                            "snapshot_mode": row.get("snapshot_mode", ""),
                            "city": city_from_path,
                        }
                    temp = _safe_float(row.get("forecast_temp"))
                    if temp is not None:
                        agg[key]["temps"].append(temp)
        except Exception as e:
            log.warning(f"Could not read {fpath}: {e}")

    # Build output rows
    result_by_city: dict[str, list[dict]] = {}
    expected_hourly = 24

    for (station_id, snap_utc, mdate), info in agg.items():
        city = info["city"]
        temps = info["temps"]
        hourly_count = len(temps)
        predicted_daily_high = round(max(temps), 2) if temps else ""
        forecast_status = determine_forecast_status(hourly_count, expected_hourly)

        tz_name = (station_meta.get(station_id) or {}).get("timezone", "UTC")

        # snapshot_time_local
        snap_local = utc_to_local_str(snap_utc, tz_name)

        # lead_hours_to_settlement
        lead_hrs_to_settle = ""
        snap_dt = utc_str_to_dt(snap_utc)
        settle_end = settlement_end_utc(mdate, tz_name)
        if snap_dt and settle_end:
            delta = settle_end - snap_dt
            lead_hrs_to_settle = round(delta.total_seconds() / 3600)

        row_out = {
            "snapshot_time_utc": snap_utc,
            "snapshot_time_local": snap_local,
            "market_date_local": mdate,
            "lead_day": info["lead_day"],
            "lead_hours_to_settlement": lead_hrs_to_settle,
            "city": city,
            "station_id": station_id,
            "model": info["model"],
            "predicted_daily_high": predicted_daily_high,
            "hourly_count": hourly_count,
            "expected_hourly_count": expected_hourly,
            "forecast_status": forecast_status,
            "snapshot_mode": info["snapshot_mode"],
        }
        result_by_city.setdefault(city, []).append(row_out)

    for city, rows in result_by_city.items():
        log.info(f"  Step A: {city} → {len(rows)} forecast_daily rows")
    return result_by_city


def _merge_write_csv(
    new_rows: list[dict],
    out_path: Path,
    fieldnames: list[str],
    date_col: str = "market_date_local",
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    replace_cities: Optional[set] = None,
) -> int:
    """
    Merge-write：保留 [start_date, end_date] 範圍外的舊行，用 new_rows 替換範圍內的行。
    replace_cities 指定時，只替換屬於這些城市的行；其他城市即使在日期範圍內也保留。
    未指定日期範圍時退化為全量覆寫。回傳寫入總行數。
    """
    existing_rows: list[dict] = []
    if out_path.exists() and (start_date or end_date):
        try:
            with open(out_path, "r", encoding="utf-8", newline="") as f:
                for r in csv.DictReader(f):
                    d_str = r.get(date_col, "")
                    try:
                        d = date.fromisoformat(d_str)
                        in_range = (
                            (start_date is None or d >= start_date) and
                            (end_date is None or d <= end_date)
                        )
                        if not in_range:
                            existing_rows.append(r)  # 範圍外：保留
                        elif replace_cities is not None and r.get("city", "") not in replace_cities:
                            existing_rows.append(r)  # 範圍內但不在目標城市：保留
                        # 其他範圍內的行：丟棄（由 new_rows 替換）
                    except ValueError:
                        existing_rows.append(r)  # 日期無法解析：保留
        except Exception as e:
            log.warning(f"Could not read {out_path.name} for merge: {e}")

    all_rows = existing_rows + new_rows
    all_rows.sort(key=lambda r: r.get(date_col, ""))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)
    return len(all_rows)


def write_step_a(
    rows_by_city: dict[str, list[dict]],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> None:
    for city, rows in rows_by_city.items():
        out_path = (
            PROJ_DIR / "data" / "processed" / "forecast_daily_high"
            / city / "forecast_daily_high.csv"
        )
        total = _merge_write_csv(rows, out_path, FORECAST_DAILY_FIELDS, start_date=start_date, end_date=end_date)
        log.info(f"  Written: {out_path.relative_to(PROJ_DIR)} ({total} rows, {len(rows)} new/updated)")


# ============================================================
# STEP B: raw/B → truth_daily_high_b canonical table
# ============================================================

def run_step_b(
    cities_filter: set[str],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> list[dict]:
    """
    Read all raw B CSVs, deduplicate on (station_id, market_date_local),
    keeping latest fetch_time_utc.
    Returns canonical rows.
    """
    raw_b_root = PROJ_DIR / "data" / "raw" / "B"
    if not raw_b_root.exists():
        log.warning("data/raw/B/ does not exist, skipping Step B")
        return []

    truth_files = list(raw_b_root.glob("*/truth_daily_high.csv"))
    log.info(f"Step B: found {len(truth_files)} raw truth CSV files")

    # Dedupe: (station_id, market_date_local) → latest row
    best: dict[tuple, dict] = {}

    for fpath in truth_files:
        city_from_path = fpath.parts[-2]
        if cities_filter and city_from_path not in cities_filter:
            continue
        try:
            with open(fpath, "r", encoding="utf-8", newline="") as f:
                for row in csv.DictReader(f):
                    sid = row.get("station_id", "")
                    mdate = row.get("market_date_local", "")
                    if not (sid and mdate):
                        continue
                    if start_date or end_date:
                        try:
                            mdate_parsed = date.fromisoformat(mdate)
                            if start_date and mdate_parsed < start_date:
                                continue
                            if end_date and mdate_parsed > end_date:
                                continue
                        except ValueError:
                            pass
                    key = (sid, mdate)
                    existing = best.get(key)
                    if existing is None:
                        best[key] = dict(row)
                    else:
                        # Keep latest fetch_time_utc
                        if row.get("fetch_time_utc", "") > existing.get("fetch_time_utc", ""):
                            best[key] = dict(row)
        except Exception as e:
            log.warning(f"Could not read {fpath}: {e}")

    canonical_rows = list(best.values())
    log.info(f"  Step B: {len(canonical_rows)} canonical truth rows after dedup")
    return canonical_rows


def write_step_b(
    rows: list[dict],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    cities_filter: Optional[set] = None,
) -> None:
    out_path = PROJ_DIR / "data" / "processed" / "truth_daily_high" / "truth_daily_high_b.csv"
    total = _merge_write_csv(
        rows, out_path, TRUTH_CANONICAL_FIELDS,
        start_date=start_date, end_date=end_date,
        replace_cities=cities_filter if cities_filter else None,
    )
    log.info(f"  Written: {out_path.relative_to(PROJ_DIR)} ({total} rows, {len(rows)} new/updated)")


# ============================================================
# STEP C: join A + B → market_day_error_table
# ============================================================

def run_step_c(
    forecast_by_city: dict[str, list[dict]],
    truth_rows: list[dict],
) -> dict[str, list[dict]]:
    """
    Join forecast_daily_high and truth_daily_high_b on (station_id, market_date_local).
    Only keep rows where truth_status==complete AND forecast_status==complete.
    Returns {city: [error_table_row, ...]}
    """
    # Build truth lookup: (station_id, market_date_local) → truth_row
    truth_lookup: dict[tuple, dict] = {
        (r.get("station_id", ""), r.get("market_date_local", "")): r
        for r in truth_rows
    }

    result_by_city: dict[str, list[dict]] = {}

    for city, f_rows in forecast_by_city.items():
        error_rows: list[dict] = []
        for fr in f_rows:
            if fr.get("forecast_status") != "complete":
                continue
            key = (fr.get("station_id", ""), fr.get("market_date_local", ""))
            tr = truth_lookup.get(key)
            if tr is None:
                continue
            if tr.get("truth_status") != "complete":
                continue

            predicted = _safe_float(fr.get("predicted_daily_high"))
            actual = _safe_float(tr.get("actual_daily_high_c"))
            if predicted is None or actual is None:
                continue

            error_val = round(actual - predicted, 4)

            error_rows.append({
                "snapshot_time_utc": fr.get("snapshot_time_utc", ""),
                "market_date_local": fr.get("market_date_local", ""),
                "lead_day": fr.get("lead_day", ""),
                "lead_hours_to_settlement": fr.get("lead_hours_to_settlement", ""),
                "city": city,
                "station_id": fr.get("station_id", ""),
                "model": fr.get("model", ""),
                "predicted_daily_high": predicted,
                "actual_daily_high": actual,
                "error": error_val,
                "truth_status": tr.get("truth_status", ""),
                "forecast_status": fr.get("forecast_status", ""),
                "source_name": tr.get("source_name", ""),
                "source_contract_type": tr.get("source_contract_type", ""),
            })

        log.info(f"  Step C: {city} → {len(error_rows)} error table rows")
        result_by_city[city] = error_rows

    return result_by_city


def write_step_c(
    rows_by_city: dict[str, list[dict]],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> None:
    for city, rows in rows_by_city.items():
        out_path = (
            PROJ_DIR / "data" / "processed" / "error_table"
            / city / "market_day_error_table.csv"
        )
        total = _merge_write_csv(rows, out_path, ERROR_TABLE_FIELDS, start_date=start_date, end_date=end_date)
        log.info(f"  Written: {out_path.relative_to(PROJ_DIR)} ({total} rows, {len(rows)} new/updated)")


# ============================================================
# Main logic
# ============================================================

def run(
    cities: str,
    start_date_str: str,
    end_date_str: str,
    verbose: bool,
) -> bool:
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=" * 50)
    log.info("07_daily_high_pipeline: forecast + truth → error table")
    log.info("=" * 50)

    cities_filter = {c.strip() for c in cities.split(",") if c.strip()} if cities else set()

    start_date_dt = date.fromisoformat(start_date_str) if start_date_str else None
    end_date_dt = date.fromisoformat(end_date_str) if end_date_str else None
    if start_date_dt or end_date_dt:
        log.info(f"Date filter: {start_date_dt} → {end_date_dt}")

    # Load station metadata for timezone lookups.
    # Priority: market_master.csv (per-station); seed_cities.json fills gaps for new cities
    # not yet in market_master (e.g., during backfill of freshly-discovered cities).
    master_path = PROJ_DIR / "data" / "market_master.csv"
    master_rows = load_master_csv(master_path)
    station_meta = build_station_meta(master_rows)
    seed_meta = build_station_meta_from_seed(PROJ_DIR / "config" / "seed_cities.json")
    for sid, smeta in seed_meta.items():
        if sid not in station_meta:
            station_meta[sid] = smeta
    log.info(
        f"Station meta: {len(station_meta)} total "
        f"(market_master={len(build_station_meta(master_rows))}, seed_fallback fills gaps)"
    )

    # ── Step A ──
    log.info("Step A: forecast_hourly_raw → forecast_daily_high_snapshot...")
    forecast_by_city = run_step_a(cities_filter, station_meta, start_date_dt, end_date_dt)
    write_step_a(forecast_by_city, start_date=start_date_dt, end_date=end_date_dt)

    # ── Step B ──
    log.info("Step B: raw/B → truth_daily_high_b canonical...")
    truth_rows = run_step_b(cities_filter, start_date_dt, end_date_dt)
    write_step_b(truth_rows, start_date=start_date_dt, end_date=end_date_dt,
                 cities_filter=cities_filter if cities_filter else None)

    # ── Step C ──
    log.info("Step C: join → market_day_error_table...")
    error_by_city = run_step_c(forecast_by_city, truth_rows)
    write_step_c(error_by_city, start_date=start_date_dt, end_date=end_date_dt)

    total_errors = sum(len(v) for v in error_by_city.values())
    log.info(f"07_daily_high_pipeline done. Error table rows: {total_errors}")
    return True


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Daily high pipeline: forecast + truth → error table"
    )
    p.add_argument("--cities", type=str, default="", help="城市 filter（逗號分隔）")
    p.add_argument("--start-date", type=str, default="", help="market_date 起始日 YYYY-MM-DD")
    p.add_argument("--end-date", type=str, default="", help="market_date 結束日 YYYY-MM-DD")
    p.add_argument("--verbose", action="store_true")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    ok = run(
        cities=args.cities,
        start_date_str=args.start_date,
        end_date_str=args.end_date,
        verbose=args.verbose,
    )
    sys.exit(0 if ok else 1)
