"""
d_snapshot_fetch.py — D snapshot batch fetch module (Phase 2)

讀取 Phase 1 產出的 probe_availability.csv，
對每個 city × model × snapshot_date 發 API 取得 hourly 預報，
逐小時展開到 horizon_hours，落地為 batch CSV。

輸出結構：
  {output_root}/{city}/{model}/snapshot_batch__{start}__{end}__h{hours}.csv
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

from _lib import (
    D_HOURLY_VARS,
    OM_HISTORICAL_FORECAST_URL,
    now_utc,
    resolve_active_d_models,
)

log = logging.getLogger(__name__)

# --- 常數 ---

REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_DELAY = 3
API_DELAY = 0.3
DAILY_LIMIT_COOLDOWN = 3600  # 撞到 daily limit 後的集中休眠秒數（1 小時）


def _is_daily_limit_error(note: str) -> bool:
    """判定是否為 Open-Meteo daily API limit 429"""
    lower = note.lower()
    return "http_429" in note and "daily" in lower and "limit" in lower

AVAILABILITY_SCHEMA_VERSION = "d_availability_v1"
LATEST_STRATEGIES = {"local_today", "local_today_minus_1"}

AVAILABILITY_REQUIRED_FIELDS = {
    "schema_version", "city", "model", "tz", "available",
    "safe_start_date", "safe_start_month", "probe_rule",
    "horizon_basis_hours", "anchor_time_local",
    "last_probe_time_utc", "probe_request_count", "note",
}

JOB_SUMMARY_FIELDS = [
    "city", "model", "latest_snapshot_strategy",
    "requested_start_date", "effective_start_date",
    "requested_end_date", "effective_end_date",
    "horizon_hours", "availability_horizon_basis_hours",
    "start_clamped", "end_clamped",
    "snapshot_dates_total", "snapshot_dates_success", "snapshot_dates_failed",
    "rows_written", "value_ok_count", "value_null_count", "value_fail_count",
    "output_csv", "job_status", "note",
]

BATCH_CSV_FIELDS = [
    "snapshot_date_local", "snapshot_time_local", "snapshot_time_utc",
    "target_time_local", "target_time_utc", "horizon_hour",
    "city", "model", "temperature_2m", "value_status",
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


def load_availability_index(csv_path: Path) -> dict[tuple[str, str], dict]:
    if not csv_path.exists():
        raise AvailabilityFileError(f"availability.csv 不存在: {csv_path}")

    try:
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = set(reader.fieldnames or [])
            missing_fields = sorted(AVAILABILITY_REQUIRED_FIELDS - fieldnames)
            if missing_fields:
                raise AvailabilityFileError(
                    "availability.csv schema 不符，缺少欄位: "
                    + ", ".join(missing_fields)
                )

            index: dict[tuple[str, str], dict] = {}
            for row in reader:
                schema_ver = str(row.get("schema_version", "")).strip()
                if schema_ver != AVAILABILITY_SCHEMA_VERSION:
                    raise AvailabilityFileError(
                        f"schema_version 不符: {schema_ver} "
                        f"(expected {AVAILABILITY_SCHEMA_VERSION})"
                    )

                city = row.get("city", "").strip()
                model = row.get("model", "").strip()
                if not city or not model:
                    raise AvailabilityFileError("含空白 city/model")

                index[(city, model)] = dict(row)

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
    local_today = datetime.now(ZoneInfo(tz_name)).date()
    if strategy == "local_today":
        return local_today
    return local_today - timedelta(days=1)


def build_batch_csv_path(
    output_root: Path,
    city: str,
    model: str,
    start_date: date,
    end_date: date,
    horizon_hours: int,
) -> Path:
    return (
        output_root / city / model
        / f"snapshot_batch__{start_date}__{end_date}__h{horizon_hours}.csv"
    )


def build_layer_var(horizon_hour: int) -> str | None:
    layer = (horizon_hour - 1) // 24
    if layer == 0:
        return "temperature_2m"
    if 1 <= layer <= 7:
        return f"temperature_2m_previous_day{layer}"
    return None


def build_request_params(
    lat: float,
    lon: float,
    tz: str,
    model: str,
    snapshot_local: datetime,
    horizon_hours: int,
) -> dict:
    first_target = snapshot_local + timedelta(hours=1)
    last_target = snapshot_local + timedelta(hours=horizon_hours)
    return {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(D_HOURLY_VARS),
        "start_date": str(first_target.date()),
        "end_date": str(last_target.date()),
        "models": model,
        "timezone": tz,
    }


def fetch_snapshot_response(
    lat: float,
    lon: float,
    tz: str,
    model: str,
    snapshot_local: datetime,
    horizon_hours: int,
) -> tuple[bool, dict, str, dict]:
    params = build_request_params(
        lat=lat, lon=lon, tz=tz, model=model,
        snapshot_local=snapshot_local, horizon_hours=horizon_hours,
    )

    last_error = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                OM_HISTORICAL_FORECAST_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                return True, resp.json(), "ok", params

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
                f"snapshot request failed: model={model} "
                f"snapshot_date={snapshot_local.date()} "
                f"(attempt {attempt}/{MAX_RETRIES}) -> {last_error}"
            )
        except Exception as exc:
            last_error = f"request_error: {str(exc)[:200]}"
            log.warning(
                f"snapshot request error: model={model} "
                f"snapshot_date={snapshot_local.date()} "
                f"(attempt {attempt}/{MAX_RETRIES}) -> {last_error}"
            )

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)

    return False, {}, last_error or "unknown_error", params


def build_row_template(
    city: str,
    model: str,
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
        "model": model,
        "temperature_2m": "",
        "value_status": "",
        "source_api": OM_HISTORICAL_FORECAST_URL,
        "fetch_time_utc": fetch_time_utc,
        "note": "",
        "timezone": tz,
        "latitude": str(lat),
        "longitude": str(lon),
    }


def build_success_rows(
    city: str,
    model: str,
    tz: str,
    lat: float,
    lon: float,
    snapshot_local: datetime,
    horizon_hours: int,
    hourly: dict,
    fetch_time_utc: str,
) -> list[dict]:
    times = hourly.get("time", [])
    time_index = {v: i for i, v in enumerate(times)}
    rows: list[dict] = []

    for horizon_hour in range(1, horizon_hours + 1):
        target_local = snapshot_local + timedelta(hours=horizon_hour)
        row = build_row_template(
            city=city, model=model, tz=tz, lat=lat, lon=lon,
            snapshot_local=snapshot_local, target_local=target_local,
            horizon_hour=horizon_hour, fetch_time_utc=fetch_time_utc,
        )

        layer_var = build_layer_var(horizon_hour)
        notes = [f"layer_var={layer_var or 'unsupported'}"]

        if layer_var is None:
            row["value_status"] = "unsupported_horizon_gt_192h"
            notes.append("unsupported_horizon_gt_192h")
            row["note"] = ";".join(notes)
            rows.append(row)
            continue

        time_key = target_local.strftime("%Y-%m-%dT%H:%M")
        idx = time_index.get(time_key)
        values = hourly.get(layer_var, [])

        if idx is None:
            row["value_status"] = "target_time_not_returned_by_api"
            notes.append("target_time_not_returned_by_api")
        elif idx >= len(values):
            row["value_status"] = "target_index_out_of_var_range"
            notes.append("target_index_out_of_var_range")
        else:
            value = values[idx]
            if value is None:
                row["value_status"] = "temperature_null"
                notes.append("temperature_null")
            else:
                row["temperature_2m"] = value
                row["value_status"] = "ok"
                notes.append("ok")

        row["note"] = ";".join(notes)
        rows.append(row)

    return rows


def build_request_failure_rows(
    city: str,
    model: str,
    tz: str,
    lat: float,
    lon: float,
    snapshot_local: datetime,
    horizon_hours: int,
    fetch_time_utc: str,
    error_note: str,
) -> list[dict]:
    rows: list[dict] = []
    for horizon_hour in range(1, horizon_hours + 1):
        target_local = snapshot_local + timedelta(hours=horizon_hour)
        row = build_row_template(
            city=city, model=model, tz=tz, lat=lat, lon=lon,
            snapshot_local=snapshot_local, target_local=target_local,
            horizon_hour=horizon_hour, fetch_time_utc=now_utc(),
        )
        row["value_status"] = "request_failed"
        row["note"] = error_note
        rows.append(row)
    return rows


def fetch_snapshot_date_rows(
    city: str,
    model: str,
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
        f"snapshot fetch: city={city} model={model} "
        f"snapshot_date={snapshot_date_local} horizon_hours={horizon_hours}"
    )

    ok, data, note, params = fetch_snapshot_response(
        lat=lat, lon=lon, tz=tz, model=model,
        snapshot_local=snapshot_local, horizon_hours=horizon_hours,
    )

    if not ok:
        return (
            build_request_failure_rows(
                city=city, model=model, tz=tz, lat=lat, lon=lon,
                snapshot_local=snapshot_local, horizon_hours=horizon_hours,
                fetch_time_utc=fetch_time_utc,
                error_note=f"request_failed:{note}",
            ),
            False,
            note,
        )

    rows = build_success_rows(
        city=city, model=model, tz=tz, lat=lat, lon=lon,
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
    models: Optional[list[str]] = None,
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
      output_root           — batch CSV 輸出根目錄 (08_snapshot/D/)
      log_dir               — 日誌輸出目錄 (logs/08_snapshot/fetch/)
      cities / models       — 過濾
      horizon_hours         — 每個 snapshot 往後抓幾小時（預設 48）
      latest_snapshot_strategy — end date clamp 策略

    輸出：
      {output_root}/{city}/{model}/snapshot_batch__{start}__{end}__h{hours}.csv
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

    resolved_models, skipped_disabled = resolve_active_d_models(models or [])

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
        "requested_start_date": str(requested_start),
        "requested_end_date": str(requested_end),
        "resolved_models": resolved_models,
        "horizon_hours": horizon_hours,
        "latest_snapshot_strategy": latest_snapshot_strategy,
        "availability_csv": str(availability_csv),
        "output_root": str(output_root),
    }

    try:
        log.info("Phase 2: D snapshot batch fetch")
        log.info(f"  start_date={requested_start}")
        log.info(f"  end_date={requested_end}")
        log.info(f"  horizon_hours={horizon_hours}")
        log.info(f"  strategy={latest_snapshot_strategy}")
        log.info(f"  availability_csv={availability_csv}")
        log.info(f"  output_root={output_root}")
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

        if not resolved_models:
            batch_status["status"] = "failed_disabled_model_selection"
            batch_status["error_message"] = (
                "No active D models remain after disabled-model filtering"
            )
            batch_status["finished_at_utc"] = now_utc()
            write_job_summary(job_rows, job_summary_path)
            status_path.write_text(
                json.dumps(batch_status, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            log.error(batch_status["error_message"])
            return 1, batch_status

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

            for model in resolved_models:
                summary: dict = {
                    "city": city,
                    "model": model,
                    "latest_snapshot_strategy": latest_snapshot_strategy,
                    "requested_start_date": str(requested_start),
                    "effective_start_date": "",
                    "requested_end_date": str(requested_end),
                    "effective_end_date": "",
                    "horizon_hours": str(horizon_hours),
                    "availability_horizon_basis_hours": "",
                    "start_clamped": "false",
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
                    log.warning(f"skip job: {city} × {model} -> {meta_note}")
                    continue

                avail_row = avail_index.get((city, model))
                if avail_row is None:
                    summary["job_status"] = "skipped_availability_missing"
                    summary["note"] = "city×model not found in availability.csv"
                    job_rows.append(summary)
                    log.warning(f"skip job: {city} × {model} -> availability row missing")
                    continue

                avail_basis = int(avail_row["horizon_basis_hours"])
                summary["availability_horizon_basis_hours"] = str(avail_basis)
                if horizon_hours > avail_basis:
                    summary["job_status"] = "skipped_horizon_exceeds_basis"
                    summary["note"] = (
                        f"requested horizon {horizon_hours} exceeds "
                        f"availability basis {avail_basis}"
                    )
                    job_rows.append(summary)
                    log.warning(f"skip job: {city} × {model} -> {summary['note']}")
                    continue

                latest_date = compute_latest_snapshot_date(
                    meta["tz"], latest_snapshot_strategy
                )
                effective_end = min(requested_end, latest_date)
                summary["effective_end_date"] = str(effective_end)
                summary["end_clamped"] = str(effective_end != requested_end).lower()

                safe_start_raw = avail_row.get("safe_start_date", "").strip()
                avail_ready = str(avail_row.get("available", "")).strip().lower() == "true"
                if not avail_ready or not safe_start_raw:
                    summary["job_status"] = "skipped_before_safe_start"
                    summary["note"] = "unavailable or missing safe_start_date"
                    job_rows.append(summary)
                    log.warning(f"skip job: {city} × {model} -> {summary['note']}")
                    continue

                safe_start = parse_date_str(safe_start_raw, "safe_start_date")
                effective_start = max(requested_start, safe_start)
                summary["effective_start_date"] = str(effective_start)
                summary["start_clamped"] = str(
                    effective_start != requested_start
                ).lower()

                if effective_start > effective_end:
                    summary["job_status"] = "skipped_before_safe_start"
                    summary["note"] = (
                        f"effective_start {effective_start} > "
                        f"effective_end {effective_end}"
                    )
                    job_rows.append(summary)
                    log.warning(f"skip job: {city} × {model} -> {summary['note']}")
                    continue

                # ── Skip if output CSV already exists ──
                output_csv_check = build_batch_csv_path(
                    output_root=Path(output_root),
                    city=city, model=model,
                    start_date=effective_start, end_date=effective_end,
                    horizon_hours=horizon_hours,
                )
                if output_csv_check.exists() and output_csv_check.stat().st_size > 0:
                    summary["job_status"] = "skipped_already_exists"
                    summary["output_csv"] = str(output_csv_check)
                    summary["note"] = f"output CSV already exists: {output_csv_check.name}"
                    job_rows.append(summary)
                    log.info(
                        f"skip job (already exists): {city} × {model} "
                        f"-> {output_csv_check.name}"
                    )
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
                        model=model,
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
                            model=model,
                            tz=meta["tz"],
                            lat=float(meta["lat"]),
                            lon=float(meta["lon"]),
                            snapshot_date_local=snap_date,
                            horizon_hours=horizon_hours,
                        )
                        if not ok and _is_daily_limit_error(note):
                            log.warning(
                                f"Still rate limited after cooldown — "
                                f"stopping job early: {city} × {model}"
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
                            f"request failure: {city} × {model} "
                            f"snapshot_date={snap_date} note={note}"
                        )
                    time.sleep(API_DELAY)

                output_csv = build_batch_csv_path(
                    output_root=Path(output_root),
                    city=city, model=model,
                    start_date=effective_start, end_date=effective_end,
                    horizon_hours=horizon_hours,
                )

                try:
                    write_batch_csv(batch_rows, output_csv)
                except Exception as exc:
                    summary["job_status"] = "failed_write"
                    summary["note"] = f"failed_write: {str(exc)[:200]}"
                    job_rows.append(summary)
                    log.error(f"failed write: {city} × {model} -> {exc}")
                    continue

                rows_written = len(batch_rows)
                total_rows_written += rows_written

                # Count value_status categories
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
                elif summary["start_clamped"] == "true" or summary["end_clamped"] == "true":
                    summary["job_status"] = "success_with_clamp"
                    summary["note"] = "completed with date clamp"
                else:
                    summary["job_status"] = "success"
                    summary["note"] = "completed"

                job_rows.append(summary)
                log.info(
                    f"job done: {city} × {model} status={summary['job_status']} "
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
