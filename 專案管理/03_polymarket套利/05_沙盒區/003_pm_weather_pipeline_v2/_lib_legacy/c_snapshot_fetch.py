"""
c_snapshot_fetch.py — C snapshot batch fetch module (Phase 2)

讀取 Phase 1 產出的 probe_availability.csv，
對每個 city × snapshot_date 發 Open-Meteo Archive API 取得 hourly temperature_2m，
逐小時展開到 horizon_hours，落地為 batch CSV。

語意說明：
  C 源 (ERA5) 是 reanalysis，不是預報。
  snapshot_date 代表「那天的 ERA5 歷史觀測紀錄」，
  horizon_hour 代表 target_time 相對 snapshot_date 00:00 的偏移，
  用途是與 D 源預報逐小時對齊，做 D-C 偏差分析。

  每 row = 一個 city × snapshot_date × horizon_hour 的 ERA5 temperature_2m 值。

輸出結構：
  {output_root}/{city}/snapshot_batch__{start}__{end}__h{hours}.csv
  {log_dir}/batch_status.json
  {log_dir}/job_summary.csv
  {log_dir}/fetch_run.log
"""

from __future__ import annotations

import csv
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import requests

from _lib import C_HOURLY_VARS, OM_ARCHIVE_URL, now_utc

log = logging.getLogger(__name__)

# --- 常數 ---

SOURCE_LABEL = "C_ERA5"
AVAILABILITY_SCHEMA_VERSION = "c_availability_v1"

REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_DELAY = 3
API_DELAY = 0.3
DAILY_LIMIT_COOLDOWN = 3600  # 撞到 daily limit 後的集中休眠秒數（1 小時）


def _is_daily_limit_error(note: str) -> bool:
    """判定是否為 Open-Meteo daily API limit 429"""
    lower = note.lower()
    return "http_429" in note and "daily" in lower and "limit" in lower

LATEST_STRATEGIES = {"local_today", "local_today_minus_1"}

JOB_SUMMARY_FIELDS = [
    "city", "source", "latest_snapshot_strategy",
    "requested_start_date", "effective_start_date",
    "requested_end_date", "effective_end_date",
    "horizon_hours",
    "end_clamped",
    "snapshot_dates_total", "snapshot_dates_success", "snapshot_dates_failed",
    "rows_written", "value_ok_count", "value_null_count", "value_fail_count",
    "output_csv", "job_status", "note",
]

BATCH_CSV_FIELDS = [
    "snapshot_date_local", "snapshot_time_local", "snapshot_time_utc",
    "target_time_local", "target_time_utc", "horizon_hour",
    "city", "source", "temperature_2m", "value_status",
    "source_api", "fetch_time_utc", "note",
    "timezone", "latitude", "longitude",
]


# ============================================================
# Helpers
# ============================================================

def format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_date_str(value: str, field_name: str) -> date:
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except Exception as exc:
        raise ValueError(f"{field_name} 格式錯誤 (需 YYYY-MM-DD): {value}") from exc


def daterange(start_date: date, end_date: date) -> list[date]:
    days: list[date] = []
    current = start_date
    while current <= end_date:
        days.append(current)
        current += timedelta(days=1)
    return days


def attach_run_log(log_path: Path) -> logging.Handler:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    logging.getLogger().addHandler(handler)
    return handler


# ============================================================
# City metadata
# ============================================================

def load_city_metadata(city_csv: Path) -> dict[str, dict]:
    if not city_csv.exists():
        raise FileNotFoundError(f"找不到 city csv: {city_csv}")

    result: dict[str, dict] = {}
    with city_csv.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            city = row.get("city", "").strip()
            if not city:
                continue
            result[city] = {
                "city": city,
                "lat": row.get("lat", "").strip(),
                "lon": row.get("lon", "").strip(),
                "tz": row.get("timezone", "").strip(),
                "city_enabled": row.get("city_enabled", "").strip(),
            }
    return result


def city_meta_ready(meta: Optional[dict]) -> tuple[bool, str]:
    if not meta:
        return False, "city not found in 01_city.csv"

    missing = []
    for key in ("lat", "lon", "tz"):
        if not str(meta.get(key, "")).strip():
            missing.append(key)
    if missing:
        return False, f"missing city metadata: {', '.join(missing)}"

    enabled = str(meta.get("city_enabled", "")).strip().lower()
    if enabled not in ("true", "1", "yes"):
        return False, "city_enabled is not true"

    try:
        ZoneInfo(meta["tz"])
    except Exception:
        return False, f"invalid timezone: {meta['tz']}"

    return True, ""


# ============================================================
# Availability CSV loader
# ============================================================

class AvailabilityFileError(RuntimeError):
    pass


def load_availability_index(csv_path: Path) -> dict[str, dict]:
    """載入 C 源 probe_availability.csv，以 city 為 key。"""
    if not csv_path.exists():
        raise AvailabilityFileError(f"availability.csv 不存在: {csv_path}")

    try:
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            index: dict[str, dict] = {}
            for row in reader:
                city = row.get("city", "").strip()
                if not city:
                    continue
                index[city] = dict(row)

            if not index:
                raise AvailabilityFileError("availability.csv 無資料列")
            return index

    except AvailabilityFileError:
        raise
    except Exception as exc:
        raise AvailabilityFileError(
            f"availability.csv 無法讀取: {csv_path} ({exc})"
        ) from exc


# ============================================================
# Snapshot fetch core
# ============================================================

def compute_latest_snapshot_date(tz_name: str, strategy: str) -> date:
    """ERA5 通常延遲 ~5 天，但用 local_today_minus_1 保守策略。"""
    local_today = datetime.now(ZoneInfo(tz_name)).date()
    if strategy == "local_today":
        return local_today
    return local_today - timedelta(days=1)


def build_batch_csv_path(
    output_root: Path,
    city: str,
    start_date: date,
    end_date: date,
    horizon_hours: int,
) -> Path:
    return (
        output_root / city
        / f"snapshot_batch__{start_date}__{end_date}__h{horizon_hours}.csv"
    )


def fetch_archive_response(
    lat: float,
    lon: float,
    tz: str,
    snapshot_local: datetime,
    horizon_hours: int,
) -> tuple[bool, dict, str]:
    """
    向 Open-Meteo Archive API 請求 hourly temperature_2m。
    回傳 (ok, data, note)。
    """
    first_target = snapshot_local + timedelta(hours=1)
    last_target = snapshot_local + timedelta(hours=horizon_hours)
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(C_HOURLY_VARS),
        "start_date": str(first_target.date()),
        "end_date": str(last_target.date()),
        "timezone": tz,
    }

    last_error = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                OM_ARCHIVE_URL, params=params, timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                return True, resp.json(), "ok"

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
            log.warning(
                f"C ERA5 request failed: {snapshot_local.date()} "
                f"(attempt {attempt}/{MAX_RETRIES}) -> {last_error}"
            )
        except Exception as exc:
            last_error = f"request_error: {str(exc)[:200]}"
            log.warning(
                f"C ERA5 request error: {snapshot_local.date()} "
                f"(attempt {attempt}/{MAX_RETRIES}) -> {last_error}"
            )

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)

    return False, {}, last_error or "unknown_error"


def build_row_template(
    city: str,
    tz: str,
    lat: float,
    lon: float,
    snapshot_local: datetime,
    target_local: datetime,
    horizon_hour: int,
    fetch_time_utc: str,
) -> dict:
    return {
        "snapshot_date_local": snapshot_local.strftime("%Y-%m-%d"),
        "snapshot_time_local": snapshot_local.strftime("%Y-%m-%d %H:%M"),
        "snapshot_time_utc": format_utc(snapshot_local),
        "target_time_local": target_local.strftime("%Y-%m-%d %H:%M"),
        "target_time_utc": format_utc(target_local),
        "horizon_hour": str(horizon_hour),
        "city": city,
        "source": SOURCE_LABEL,
        "temperature_2m": "",
        "value_status": "",
        "source_api": OM_ARCHIVE_URL,
        "fetch_time_utc": fetch_time_utc,
        "note": "",
        "timezone": tz,
        "latitude": str(lat),
        "longitude": str(lon),
    }


def build_success_rows(
    city: str,
    tz: str,
    lat: float,
    lon: float,
    snapshot_local: datetime,
    horizon_hours: int,
    hourly: dict,
    fetch_time_utc: str,
) -> list[dict]:
    """從 API 回傳的 hourly 資料，展開成 h1~hN rows。"""
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    time_index = {v: i for i, v in enumerate(times)}
    rows: list[dict] = []

    for horizon_hour in range(1, horizon_hours + 1):
        target_local = snapshot_local + timedelta(hours=horizon_hour)
        row = build_row_template(
            city=city, tz=tz, lat=lat, lon=lon,
            snapshot_local=snapshot_local, target_local=target_local,
            horizon_hour=horizon_hour, fetch_time_utc=fetch_time_utc,
        )

        time_key = target_local.strftime("%Y-%m-%dT%H:%M")
        idx = time_index.get(time_key)

        if idx is None:
            row["value_status"] = "target_time_not_returned_by_api"
            row["note"] = "target_time_not_returned_by_api"
        elif idx >= len(temps):
            row["value_status"] = "target_index_out_of_range"
            row["note"] = "target_index_out_of_range"
        else:
            value = temps[idx]
            if value is None:
                row["value_status"] = "temperature_null"
                row["note"] = "temperature_null"
            else:
                row["temperature_2m"] = value
                row["value_status"] = "ok"
                row["note"] = "ok"

        rows.append(row)

    return rows


def build_request_failure_rows(
    city: str,
    tz: str,
    lat: float,
    lon: float,
    snapshot_local: datetime,
    horizon_hours: int,
    error_note: str,
) -> list[dict]:
    rows: list[dict] = []
    for horizon_hour in range(1, horizon_hours + 1):
        target_local = snapshot_local + timedelta(hours=horizon_hour)
        row = build_row_template(
            city=city, tz=tz, lat=lat, lon=lon,
            snapshot_local=snapshot_local, target_local=target_local,
            horizon_hour=horizon_hour, fetch_time_utc=now_utc(),
        )
        row["value_status"] = "request_failed"
        row["note"] = error_note
        rows.append(row)
    return rows


def fetch_snapshot_date_rows(
    city: str,
    tz: str,
    lat: float,
    lon: float,
    snapshot_date_local: date,
    horizon_hours: int,
) -> tuple[list[dict], bool, str]:
    snapshot_local = datetime.combine(
        snapshot_date_local, datetime.min.time(), tzinfo=ZoneInfo(tz)
    )
    fetch_time_utc = now_utc()

    log.info(
        f"C ERA5 fetch: city={city} "
        f"snapshot_date={snapshot_date_local} horizon_hours={horizon_hours}"
    )

    ok, data, note = fetch_archive_response(
        lat=lat, lon=lon, tz=tz,
        snapshot_local=snapshot_local, horizon_hours=horizon_hours,
    )

    if not ok:
        return (
            build_request_failure_rows(
                city=city, tz=tz, lat=lat, lon=lon,
                snapshot_local=snapshot_local, horizon_hours=horizon_hours,
                error_note=f"request_failed:{note}",
            ),
            False,
            note,
        )

    rows = build_success_rows(
        city=city, tz=tz, lat=lat, lon=lon,
        snapshot_local=snapshot_local, horizon_hours=horizon_hours,
        hourly=data.get("hourly", {}),
        fetch_time_utc=fetch_time_utc,
    )
    return rows, True, "ok"


# ============================================================
# CSV output
# ============================================================

def write_batch_csv(rows: list[dict], csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=BATCH_CSV_FIELDS, extrasaction="ignore"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_job_summary(rows: list[dict], csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=JOB_SUMMARY_FIELDS, extrasaction="ignore"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


# ============================================================
# Main entry: run_fetch
# ============================================================

def run_fetch(
    start_date: str,
    end_date: str,
    city_csv: Path,
    availability_csv: Path,
    output_root: Path,
    log_dir: Path,
    cities: Optional[list[str]] = None,
    horizon_hours: int = 48,
    latest_snapshot_strategy: str = "local_today_minus_1",
    verbose: bool = False,
) -> tuple[int, dict]:
    """
    Phase 2 主入口。

    輸入：
      start_date / end_date — snapshot 日期範圍 (YYYY-MM-DD)
      city_csv              — data/01_city.csv
      availability_csv      — Phase 1 產出的 probe_availability.csv
      output_root           — batch CSV 輸出根目錄 (08_snapshot/C/)
      log_dir               — 日誌輸��目錄 (logs/08_snapshot/fetch_c/)
      cities                — 過濾
      horizon_hours         — 每個 snapshot 往後抓幾小時（預設 48）
      latest_snapshot_strategy — end date clamp 策略

    輸出：
      {output_root}/{city}/snapshot_batch__{start}__{end}__h{hours}.csv
      {log_dir}/batch_status.json
      {log_dir}/job_summary.csv
      {log_dir}/fetch_run.log

    回傳：(exit_code, status_dict)
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if latest_snapshot_strategy not in LATEST_STRATEGIES:
        raise ValueError(f"未知 latest_snapshot_strategy: {latest_snapshot_strategy}")

    requested_start = parse_date_str(start_date, "start-date")
    requested_end = parse_date_str(end_date, "end-date")
    if requested_start > requested_end:
        raise ValueError("start-date 不可晚於 end-date")

    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_handler = attach_run_log(log_dir / "fetch_run.log")
    status_path = log_dir / "batch_status.json"
    job_summary_path = log_dir / "job_summary.csv"

    started_at = now_utc()
    job_rows: list[dict] = []

    batch_status: dict = {
        "started_at_utc": started_at,
        "finished_at_utc": None,
        "status": "running",
        "source": SOURCE_LABEL,
        "requested_start_date": str(requested_start),
        "requested_end_date": str(requested_end),
        "horizon_hours": horizon_hours,
        "latest_snapshot_strategy": latest_snapshot_strategy,
        "availability_csv": str(availability_csv),
        "output_root": str(output_root),
    }

    try:
        log.info("Phase 2: C ERA5 snapshot batch fetch")
        log.info(f"  start_date={requested_start}")
        log.info(f"  end_date={requested_end}")
        log.info(f"  horizon_hours={horizon_hours}")
        log.info(f"  strategy={latest_snapshot_strategy}")
        log.info(f"  availability_csv={availability_csv}")
        log.info(f"  output_root={output_root}")

        try:
            city_meta = load_city_metadata(Path(city_csv))
            avail_index = load_availability_index(Path(availability_csv))
        except AvailabilityFileError as exc:
            batch_status["status"] = "failed_availability_file"
            batch_status["error_message"] = str(exc)
            batch_status["finished_at_utc"] = now_utc()
            write_job_summary(job_rows, job_summary_path)
            status_path.write_text(
                json.dumps(batch_status, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            log.error(str(exc))
            return 1, batch_status

        selected_cities = cities or sorted(city_meta.keys())
        total_rows_written = 0

        for city in selected_cities:
            meta = city_meta.get(city)
            meta_ok, meta_note = city_meta_ready(meta)

            summary: dict = {
                "city": city,
                "source": SOURCE_LABEL,
                "latest_snapshot_strategy": latest_snapshot_strategy,
                "requested_start_date": str(requested_start),
                "effective_start_date": str(requested_start),
                "requested_end_date": str(requested_end),
                "effective_end_date": "",
                "horizon_hours": str(horizon_hours),
                "end_clamped": "false",
                "snapshot_dates_total": "0",
                "snapshot_dates_success": "0",
                "snapshot_dates_failed": "0",
                "rows_written": "0",
                "value_ok_count": "0",
                "value_null_count": "0",
                "value_fail_count": "0",
                "output_csv": "",
                "job_status": "",
                "note": "",
            }

            if not meta_ok:
                summary["job_status"] = "skipped_missing_city_meta"
                summary["note"] = meta_note
                job_rows.append(summary)
                log.warning(f"skip job: {city} -> {meta_note}")
                continue

            # Check availability
            avail_row = avail_index.get(city)
            if avail_row is None:
                summary["job_status"] = "skipped_availability_missing"
                summary["note"] = "city not found in availability.csv"
                job_rows.append(summary)
                log.warning(f"skip job: {city} -> availability row missing")
                continue

            avail_ready = str(avail_row.get("available", "")).strip().lower() == "true"
            if not avail_ready:
                summary["job_status"] = "skipped_unavailable"
                summary["note"] = avail_row.get("note", "unavailable")
                job_rows.append(summary)
                log.warning(f"skip job: {city} -> unavailable")
                continue

            # Compute effective date range
            latest_date = compute_latest_snapshot_date(
                meta["tz"], latest_snapshot_strategy
            )
            effective_end = min(requested_end, latest_date)
            effective_start = requested_start
            summary["effective_start_date"] = str(effective_start)
            summary["effective_end_date"] = str(effective_end)
            summary["end_clamped"] = str(effective_end != requested_end).lower()

            if effective_start > effective_end:
                summary["job_status"] = "skipped_date_range_empty"
                summary["note"] = (
                    f"effective_start {effective_start} > "
                    f"effective_end {effective_end}"
                )
                job_rows.append(summary)
                log.warning(f"skip job: {city} -> {summary['note']}")
                continue

            # ── Skip if output CSV already exists ──
            output_csv_check = build_batch_csv_path(
                output_root=Path(output_root),
                city=city,
                start_date=effective_start, end_date=effective_end,
                horizon_hours=horizon_hours,
            )
            if output_csv_check.exists() and output_csv_check.stat().st_size > 0:
                summary["job_status"] = "skipped_already_exists"
                summary["output_csv"] = str(output_csv_check)
                summary["note"] = f"output CSV already exists: {output_csv_check.name}"
                job_rows.append(summary)
                log.info(f"skip job (already exists): {city} -> {output_csv_check.name}")
                continue

            # Fetch each snapshot date
            batch_rows: list[dict] = []
            snapshot_dates = daterange(effective_start, effective_end)
            success_count = 0
            failed_count = 0
            daily_limit_cooldown_used = False

            for snap_date in snapshot_dates:
                rows, ok, note = fetch_snapshot_date_rows(
                    city=city,
                    tz=meta["tz"],
                    lat=float(meta["lat"]),
                    lon=float(meta["lon"]),
                    snapshot_date_local=snap_date,
                    horizon_hours=horizon_hours,
                )

                # 偵測 daily limit：cooldown 一次後重試，若仍失敗則停止此 job
                if not ok and not daily_limit_cooldown_used and _is_daily_limit_error(note):
                    daily_limit_cooldown_used = True
                    log.warning(
                        f"Daily API limit detected ({note}). "
                        f"Cooldown {DAILY_LIMIT_COOLDOWN}s before retry..."
                    )
                    time.sleep(DAILY_LIMIT_COOLDOWN)
                    log.info(f"Cooldown complete. Retrying snapshot_date={snap_date}...")
                    rows, ok, note = fetch_snapshot_date_rows(
                        city=city,
                        tz=meta["tz"],
                        lat=float(meta["lat"]),
                        lon=float(meta["lon"]),
                        snapshot_date_local=snap_date,
                        horizon_hours=horizon_hours,
                    )
                    if not ok and _is_daily_limit_error(note):
                        log.warning(
                            f"Still rate limited after cooldown — "
                            f"stopping job early: {city}"
                        )
                        batch_rows.extend(rows)
                        failed_count += 1
                        break

                batch_rows.extend(rows)
                if ok:
                    success_count += 1
                else:
                    failed_count += 1
                    log.warning(
                        f"request failure: {city} "
                        f"snapshot_date={snap_date} note={note}"
                    )
                time.sleep(API_DELAY)

            output_csv = build_batch_csv_path(
                output_root=Path(output_root),
                city=city,
                start_date=effective_start,
                end_date=effective_end,
                horizon_hours=horizon_hours,
            )

            try:
                write_batch_csv(batch_rows, output_csv)
            except Exception as exc:
                summary["job_status"] = "failed_write"
                summary["note"] = f"failed_write: {str(exc)[:200]}"
                job_rows.append(summary)
                log.error(f"failed write: {city} -> {exc}")
                continue

            rows_written = len(batch_rows)
            total_rows_written += rows_written

            v_ok = sum(1 for r in batch_rows if r.get("value_status") == "ok")
            v_null = sum(
                1 for r in batch_rows
                if r.get("value_status") == "temperature_null"
            )
            v_fail = rows_written - v_ok - v_null

            summary["snapshot_dates_total"] = str(len(snapshot_dates))
            summary["snapshot_dates_success"] = str(success_count)
            summary["snapshot_dates_failed"] = str(failed_count)
            summary["rows_written"] = str(rows_written)
            summary["value_ok_count"] = str(v_ok)
            summary["value_null_count"] = str(v_null)
            summary["value_fail_count"] = str(v_fail)
            summary["output_csv"] = str(output_csv)

            if failed_count > 0:
                summary["job_status"] = "failed_request"
                summary["note"] = "one or more snapshot dates failed"
            elif summary["end_clamped"] == "true":
                summary["job_status"] = "success_with_clamp"
                summary["note"] = "completed with date clamp"
            else:
                summary["job_status"] = "success"
                summary["note"] = "completed"

            job_rows.append(summary)
            log.info(
                f"job done: {city} status={summary['job_status']} "
                f"rows={rows_written} csv={output_csv}"
            )

        write_job_summary(job_rows, job_summary_path)

        counts: dict[str, int] = {}
        for row in job_rows:
            counts[row["job_status"]] = counts.get(row["job_status"], 0) + 1

        batch_status.update({
            "status": "success",
            "finished_at_utc": now_utc(),
            "job_count": len(job_rows),
            "rows_written": total_rows_written,
            "job_status_counts": counts,
            "jobs": job_rows,
        })
        status_path.write_text(
            json.dumps(batch_status, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log.info(f"job summary: {job_summary_path}")
        log.info(f"batch status: {status_path}")
        return 0, batch_status

    finally:
        logging.getLogger().removeHandler(log_handler)
        log_handler.close()
