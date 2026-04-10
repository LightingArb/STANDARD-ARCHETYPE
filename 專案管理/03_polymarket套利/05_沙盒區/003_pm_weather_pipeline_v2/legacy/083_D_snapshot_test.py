"""
083_D_snapshot_test.py — D 源 (Open-Meteo Forecast) snapshot 正式入口

兩個 phase：
  Phase 1: availability probe — 探測 city × model 的 safe_start_date
  Phase 2: snapshot batch fetch — 依 probe 結果抓取 hourly 預報

實作邏輯：
  _lib/d_snapshot_probe.py (Phase 1)
  _lib/d_snapshot_fetch.py (Phase 2)

輸出路徑：
  Probe:  logs/08_snapshot/probe/
  Fetch:  08_snapshot/D/ (data), logs/08_snapshot/fetch/ (logs)

用法：
  python 083_D_snapshot_test.py
  python 083_D_snapshot_test.py --cities London --models D1 --start-date 2026-04-01 --end-date 2026-04-02
  python 083_D_snapshot_test.py --skip-probe   (跳過 Phase 1，直接用上次 probe 結果)
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from _lib import resolve_active_d_models

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# --- 輸出路徑常數 ---

PROBE_OUTPUT_DIR = PROJ_DIR / "logs" / "08_snapshot" / "probe"
FETCH_OUTPUT_ROOT = PROJ_DIR / "08_snapshot" / "D"
FETCH_LOG_DIR = PROJ_DIR / "logs" / "08_snapshot" / "fetch"
DEFAULT_CITY_CSV = PROJ_DIR / "data" / "01_city.csv"


def main():
    parser = argparse.ArgumentParser(
        description="D-source snapshot test (Phase 1: probe + Phase 2: fetch)"
    )
    parser.add_argument(
        "--cities", type=str, default="London",
        help="城市（逗號分隔，預設: London）",
    )
    parser.add_argument(
        "--models", type=str, default="D1",
        help="模型（D1~D19 alias 或完整名；D13/D14/D18 會被標記 disabled 並略過）",
    )
    parser.add_argument(
        "--start-date", type=str, default="2026-04-01",
        help="Snapshot 起始日期 (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date", type=str, default="2026-04-02",
        help="Snapshot 結束日期 (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--horizon-hours", type=int, default=48,
        help="每個 snapshot 往後幾小時（預設: 48）",
    )
    parser.add_argument(
        "--horizon-basis-hours", type=int, default=192,
        help="Phase 1 probe 的 availability 基準時距（預設: 192）",
    )
    parser.add_argument(
        "--latest-snapshot-strategy", type=str, default="local_today_minus_1",
        choices=["local_today", "local_today_minus_1"],
        help="Phase 2 end date clamp 策略（預設: local_today_minus_1）",
    )
    parser.add_argument(
        "--city-csv", type=str, default=str(DEFAULT_CITY_CSV),
        help="01_city.csv 路徑",
    )
    parser.add_argument(
        "--skip-probe", action="store_true",
        help="跳過 Phase 1，直接用上次 probe 結果",
    )
    parser.add_argument("--verbose", action="store_true", help="詳細 log")
    args = parser.parse_args()

    cities = [c.strip() for c in args.cities.split(",") if c.strip()]
    requested_models = [m.strip() for m in args.models.split(",") if m.strip()]

    try:
        models, skipped_disabled = resolve_active_d_models(requested_models)
    except ValueError as exc:
        log.error(str(exc))
        sys.exit(1)

    log.info("=" * 60)
    log.info("083_D_snapshot_test")
    log.info(f"  cities: {cities}")
    log.info(f"  models_requested: {requested_models}")
    log.info(f"  models_active: {models}")
    for item in skipped_disabled:
        log.warning(
            "  D 模型已停用並略過: "
            f"{item['token']} -> {item['model']} ({item['reason']})"
        )
    log.info(f"  date range: {args.start_date} ~ {args.end_date}")
    log.info(f"  horizon: {args.horizon_hours}h")
    log.info(f"  probe basis: {args.horizon_basis_hours}h")
    log.info(f"  strategy: {args.latest_snapshot_strategy}")
    log.info(f"  skip_probe: {args.skip_probe}")
    log.info("=" * 60)

    if not models:
        log.error("No active D models remain after disabled-model filtering.")
        sys.exit(1)

    availability_csv = PROBE_OUTPUT_DIR / "probe_availability.csv"

    # ============================================================
    # Phase 1: Probe
    # ============================================================

    if not args.skip_probe:
        log.info("Phase 1: D availability probe...")
        from _lib.d_snapshot_probe import run_probe

        probe_status = run_probe(
            city_csv=Path(args.city_csv),
            output_dir=PROBE_OUTPUT_DIR,
            cities=cities,
            models=models,
            horizon_basis_hours=args.horizon_basis_hours,
            verbose=args.verbose,
        )

        log.info(
            f"Phase 1 done: {probe_status['available_count']} available, "
            f"{probe_status['unavailable_count']} unavailable"
        )

        if probe_status["available_count"] == 0:
            log.error("Phase 1: no available city×model. Aborting Phase 2.")
            sys.exit(1)
    else:
        log.info("Phase 1: SKIPPED (--skip-probe)")
        if not availability_csv.exists():
            log.error(
                f"Phase 1 skipped but probe result not found: {availability_csv}"
            )
            sys.exit(1)
        log.info(f"  Using existing: {availability_csv}")

    # ============================================================
    # Phase 2: Fetch
    # ============================================================

    log.info("Phase 2: D snapshot batch fetch...")
    from _lib.d_snapshot_fetch import run_fetch

    exit_code, fetch_status = run_fetch(
        start_date=args.start_date,
        end_date=args.end_date,
        city_csv=Path(args.city_csv),
        availability_csv=availability_csv,
        output_root=FETCH_OUTPUT_ROOT,
        log_dir=FETCH_LOG_DIR,
        cities=cities,
        models=models,
        horizon_hours=args.horizon_hours,
        latest_snapshot_strategy=args.latest_snapshot_strategy,
        verbose=args.verbose,
    )

    # ============================================================
    # Consolidated Summary
    # ============================================================

    log.info("")
    log.info("=" * 80)
    log.info("SNAPSHOT SUMMARY")
    log.info("=" * 80)

    if exit_code == 0:
        jobs = fetch_status.get("jobs", [])
        if jobs:
            # Header
            log.info(
                f"{'city':<18} {'model':<20} {'status':<22} "
                f"{'rows':>5} {'ok':>5} {'null':>5} {'fail':>5} {'note'}"
            )
            log.info("-" * 80)

            for job in jobs:
                city = job.get("city", "")
                model = job.get("model", "")
                status = job.get("job_status", "")
                rows = job.get("rows_written", "0")
                v_ok = job.get("value_ok_count", "0")
                v_null = job.get("value_null_count", "0")
                v_fail = job.get("value_fail_count", "0")
                note = job.get("note", "")

                log.info(
                    f"{city:<18} {model:<20} {status:<22} "
                    f"{rows:>5} {v_ok:>5} {v_null:>5} {v_fail:>5} {note}"
                )

            log.info("-" * 80)

        counts = fetch_status.get("job_status_counts", {})
        log.info(
            f"Total: {fetch_status.get('rows_written', 0)} rows, "
            f"jobs: {json.dumps(counts)}"
        )
    else:
        log.error(f"Phase 2 failed: {fetch_status.get('error_message', 'unknown')}")

    log.info("=" * 80)
    log.info("083_D_snapshot_test: complete")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
