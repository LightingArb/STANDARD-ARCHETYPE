"""
08_snapshot_main.py — Snapshot 測試總入口

調度 082_C / 083_D snapshot 測試，以及 09_dc_bias_analysis。
081_A 尚未實作。

用法：
  python 08_snapshot_main.py --sources D --cities London --models D1
  python 08_snapshot_main.py --sources C,D --cities London,Tokyo --models D1,D2
  python 08_snapshot_main.py --sources C,D --run-analysis
  python 08_snapshot_main.py --sources D --skip-probe
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

from _lib import resolve_d_models
from _lib.freshness_utils import (
    all_exist,
    csv_semantic_signature,
    load_json_file,
    max_mtime,
    min_mtime,
    signatures_match,
)

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

ANALYSIS_OUTPUT_ROOT = PROJ_DIR / "08_snapshot" / "analysis" / "dc_bias"
RANKING_OUTPUT_ROOT = PROJ_DIR / "08_snapshot" / "analysis" / "dc_bias_rankings"
ANALYSIS_LOG_DIR = PROJ_DIR / "logs" / "08_snapshot" / "analysis"
ANALYSIS_BUCKETS = ["overall", "h1_24", "h25_48"]


def build_c_batch_path(city: str, start_date: str, end_date: str, horizon_hours: int) -> Path:
    return (
        PROJ_DIR
        / "08_snapshot" / "C" / city
        / f"snapshot_batch__{start_date}__{end_date}__h{horizon_hours}.csv"
    )


def build_d_batch_path(
    city: str, model: str, start_date: str, end_date: str, horizon_hours: int,
) -> Path:
    return (
        PROJ_DIR
        / "08_snapshot" / "D" / city / model
        / f"snapshot_batch__{start_date}__{end_date}__h{horizon_hours}.csv"
    )


def build_analysis_detail_path(
    city: str, model: str, start_date: str, end_date: str, horizon_hours: int,
) -> Path:
    return (
        ANALYSIS_OUTPUT_ROOT / city / model
        / f"bias_detail__{start_date}__{end_date}__h{horizon_hours}.csv"
    )


def build_analysis_summary_path(
    city: str, model: str, start_date: str, end_date: str, horizon_hours: int,
) -> Path:
    return (
        ANALYSIS_OUTPUT_ROOT / city / model
        / f"bias_summary__{start_date}__{end_date}__h{horizon_hours}.csv"
    )


def build_analysis_outputs(
    cities: list[str], models: list[str], start_date: str, end_date: str, horizon_hours: int,
) -> list[Path]:
    outputs: list[Path] = []
    for city in cities:
        for model in models:
            outputs.append(build_analysis_detail_path(city, model, start_date, end_date, horizon_hours))
            outputs.append(build_analysis_summary_path(city, model, start_date, end_date, horizon_hours))
        for bucket in ANALYSIS_BUCKETS:
            outputs.append(
                RANKING_OUTPUT_ROOT / city
                / f"ranking_{bucket}__{start_date}__{end_date}__h{horizon_hours}.csv"
            )
    outputs.append(
        ANALYSIS_OUTPUT_ROOT
        / f"dc_bias_summary__{start_date}__{end_date}__h{horizon_hours}.csv"
    )
    outputs.append(ANALYSIS_LOG_DIR / "analysis_status.json")
    return outputs


def analysis_outputs_are_fresh(
    cities: list[str], models: list[str], start_date: str, end_date: str, horizon_hours: int,
) -> tuple[bool, str]:
    input_paths: list[Path] = []
    current_signatures: dict[str, dict] = {}
    status_path = ANALYSIS_LOG_DIR / "analysis_status.json"
    status = load_json_file(status_path)

    for city in cities:
        c_path = build_c_batch_path(city, start_date, end_date, horizon_hours)
        if not c_path.exists():
            return False, f"missing C batch: {c_path}"

        c_sig = csv_semantic_signature(c_path, ignore_fields={"fetch_time_utc"})
        for model in models:
            d_path = build_d_batch_path(city, model, start_date, end_date, horizon_hours)
            if not d_path.exists():
                return False, f"missing D batch: {d_path}"
            input_paths.extend([c_path, d_path])
            current_signatures[f"{city}||{model}"] = {
                "c_batch": c_sig,
                "d_batch": csv_semantic_signature(d_path, ignore_fields={"fetch_time_utc"}),
            }

    scope_matches_status = (
        status.get("cities") == cities
        and status.get("models") == models
        and status.get("start_date") == start_date
        and status.get("end_date") == end_date
        and int(status.get("horizon_hours", 0)) == horizon_hours
    )

    if scope_matches_status and status.get("generated_output_paths"):
        output_paths = [Path(path) for path in status.get("generated_output_paths", [])]
        output_paths.append(status_path)
    else:
        output_paths = build_analysis_outputs(cities, models, start_date, end_date, horizon_hours)
    if not all_exist(output_paths):
        missing = next(path for path in output_paths if not path.exists())
        return False, f"missing analysis output: {missing}"

    if scope_matches_status:
        recorded = status.get("job_input_signatures", {})
        if recorded and all(
            signatures_match(current_signatures[key].get("c_batch", {}), recorded.get(key, {}).get("c_batch", {}))
            and signatures_match(current_signatures[key].get("d_batch", {}), recorded.get(key, {}).get("d_batch", {}))
            for key in current_signatures
        ):
            return True, "input fingerprints unchanged"

    if min_mtime(output_paths) >= max_mtime(input_paths):
        return True, "analysis outputs newer than snapshot batches"
    return False, "analysis outputs are stale"


def run_script(script_name: str, args: list[str] = None, label: str = "") -> bool:
    """執行子腳本，回傳是否成功。"""
    script_path = PROJ_DIR / script_name
    if not script_path.exists():
        log.error(f"Script not found: {script_path}")
        return False

    cmd = [sys.executable, str(script_path)] + (args or [])
    display = label or script_name

    log.info(f"{'='*50}")
    log.info(f"Running: {display}")
    log.info(f"  cmd: {' '.join(cmd)}")
    log.info(f"{'='*50}")

    try:
        result = subprocess.run(cmd, cwd=str(PROJ_DIR), capture_output=False, text=True)
        if result.returncode != 0:
            log.error(f"{display} exited with code {result.returncode}")
            return False
        log.info(f"{display}: OK")
        return True
    except Exception as e:
        log.error(f"{display} failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Snapshot test orchestrator")
    parser.add_argument(
        "--sources", type=str, default="D",
        help="Which sources to test: A,C,D (comma-separated, default: D)",
    )
    parser.add_argument("--cities", type=str, default="London")
    parser.add_argument("--models", type=str, default="D1")
    parser.add_argument("--start-date", type=str, default="2026-04-01")
    parser.add_argument("--end-date", type=str, default="2026-04-02")
    parser.add_argument("--horizon-hours", type=int, default=48)
    parser.add_argument("--horizon-basis-hours", type=int, default=192)
    parser.add_argument(
        "--latest-snapshot-strategy", type=str, default="local_today_minus_1",
        choices=["local_today", "local_today_minus_1"],
    )
    parser.add_argument("--skip-probe", action="store_true")
    parser.add_argument(
        "--run-analysis", action="store_true",
        help="C,D 皆成功時自動執行 D-C bias analysis",
    )
    parser.add_argument(
        "--force-analysis", action="store_true",
        help="忽略 freshness 檢查，強制重跑 09_dc_bias_analysis",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    sources = [s.strip().upper() for s in args.sources.split(",")]
    cities = [c.strip() for c in args.cities.split(",") if c.strip()]
    try:
        models = resolve_d_models([m.strip() for m in args.models.split(",") if m.strip()])
    except ValueError as exc:
        log.error(str(exc))
        sys.exit(1)

    log.info("=" * 60)
    log.info("08_snapshot_main: Snapshot Test Orchestrator")
    log.info(f"  sources: {sources}")
    log.info(f"  cities: {cities}")
    log.info(f"  models: {models}")
    log.info(f"  date range: {args.start_date} ~ {args.end_date}")
    log.info(f"  horizon: {args.horizon_hours}h")
    log.info(f"  force_analysis: {args.force_analysis}")
    log.info("=" * 60)

    results = {}

    # --- A source ---
    if "A" in sources:
        a_args = [
            "--cities", args.cities,
            "--start-date", args.start_date,
            "--end-date", args.end_date,
            "--horizon-hours", str(args.horizon_hours),
            "--latest-snapshot-strategy", args.latest_snapshot_strategy,
        ]
        if args.skip_probe:
            a_args.append("--skip-probe")
        if args.verbose:
            a_args.append("--verbose")

        ok = run_script("081_A_snapshot_test.py", args=a_args, label="081_A_snapshot")
        results["A"] = "ok" if ok else "failed"

    # --- C source ---
    if "C" in sources:
        c_args = [
            "--cities", args.cities,
            "--start-date", args.start_date,
            "--end-date", args.end_date,
            "--horizon-hours", str(args.horizon_hours),
            "--latest-snapshot-strategy", args.latest_snapshot_strategy,
        ]
        if args.skip_probe:
            c_args.append("--skip-probe")
        if args.verbose:
            c_args.append("--verbose")

        ok = run_script("082_C_snapshot_test.py", args=c_args, label="082_C_snapshot")
        results["C"] = "ok" if ok else "failed"

    # --- D source ---
    if "D" in sources:
        d_args = [
            "--cities", args.cities,
            "--models", args.models,
            "--start-date", args.start_date,
            "--end-date", args.end_date,
            "--horizon-hours", str(args.horizon_hours),
            "--horizon-basis-hours", str(args.horizon_basis_hours),
            "--latest-snapshot-strategy", args.latest_snapshot_strategy,
        ]
        if args.skip_probe:
            d_args.append("--skip-probe")
        if args.verbose:
            d_args.append("--verbose")

        ok = run_script("083_D_snapshot_test.py", args=d_args, label="083_D_snapshot")
        results["D"] = "ok" if ok else "failed"

    # --- D-C Bias Analysis ---
    if args.run_analysis:
        c_ok = results.get("C") == "ok"
        d_ok = results.get("D") == "ok"
        if c_ok and d_ok:
            is_fresh = False
            fresh_reason = ""
            if not args.force_analysis:
                is_fresh, fresh_reason = analysis_outputs_are_fresh(
                    cities=cities,
                    models=models,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    horizon_hours=args.horizon_hours,
                )

            if is_fresh:
                log.info(f"[09_analysis] skipped: {fresh_reason}")
                results["analysis"] = f"skipped ({fresh_reason})"
            else:
                analysis_args = [
                    "--cities", args.cities,
                    "--models", args.models,
                    "--start-date", args.start_date,
                    "--end-date", args.end_date,
                    "--horizon-hours", str(args.horizon_hours),
                ]
                if args.force_analysis:
                    analysis_args.append("--force")
                if args.verbose:
                    analysis_args.append("--verbose")

                ok = run_script(
                    "09_dc_bias_analysis.py", args=analysis_args,
                    label="09_dc_bias_analysis",
                )
                results["analysis"] = "ok" if ok else "failed"
        else:
            skip_reason = []
            if "C" not in results:
                skip_reason.append("C not requested")
            elif not c_ok:
                skip_reason.append(f"C={results.get('C')}")
            if "D" not in results:
                skip_reason.append("D not requested")
            elif not d_ok:
                skip_reason.append(f"D={results.get('D')}")
            reason_str = ", ".join(skip_reason)
            log.info(f"[09_analysis] skipped: {reason_str}")
            results["analysis"] = f"skipped ({reason_str})"

    # --- Summary ---
    log.info("=" * 60)
    log.info("08_snapshot_main: Summary")
    for src, status in results.items():
        log.info(f"  {src}: {status}")
    log.info("=" * 60)

    failed = any(v == "failed" for v in results.values())
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
