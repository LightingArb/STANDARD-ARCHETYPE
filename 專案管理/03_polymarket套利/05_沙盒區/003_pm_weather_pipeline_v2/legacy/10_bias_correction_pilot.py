"""
10_bias_correction_pilot.py — 偏差校正試驗（第一版）

讀取 09 產出的 bias detail CSV，套用最簡單的 mean-bias 校正：
  corrected_value = d_value - mean_bias

比較校正前後的 MAE / RMSE / median_abs_error / p90 / p95，
以及 bucket-level（h1_24 / h25_48）的表現變化。

額外試驗：
  - overall_mean_bias 統一校正
  - h1_24 / h25_48 各自 mean_bias 分段校正

輸出：
  08_snapshot/analysis/correction_pilot/
    correction_summary__{start}__{end}__h{hours}.csv
  logs/08_snapshot/analysis/correction_pilot/
    correction_status.json

用法：
  python 10_bias_correction_pilot.py
  python 10_bias_correction_pilot.py --cities "London,Tokyo,New York City" --models D1,D2,D7
"""

import argparse
import csv
import json
import logging
import math
import statistics
import sys
from pathlib import Path

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

sys.path.insert(0, str(PROJ_DIR))
from _lib import resolve_d_models, now_utc
from _lib.freshness_utils import (
    all_exist,
    file_signature,
    load_json_file,
    max_mtime,
    min_mtime,
    signatures_match,
)

DETAIL_ROOT = PROJ_DIR / "08_snapshot" / "analysis" / "dc_bias"
OUTPUT_ROOT = PROJ_DIR / "08_snapshot" / "analysis" / "correction_pilot"
LOG_DIR = PROJ_DIR / "logs" / "08_snapshot" / "analysis" / "correction_pilot"

HORIZON_BUCKETS = [
    ("overall", 1, 9999),
    ("h1_24", 1, 24),
    ("h25_48", 25, 48),
]

CORRECTION_METHODS = [
    "overall_mean",   # 用 overall mean_bias 校正全部
    "bucket_mean",    # h1_24 用 h1_24 mean_bias，h25_48 用 h25_48 mean_bias
]

SUMMARY_FIELDS = [
    "city", "model", "correction_method", "bucket",
    "valid_pairs",
    "before_mean_bias", "before_mae", "before_rmse",
    "before_median_ae", "before_p90_ae", "before_p95_ae",
    "after_mean_bias", "after_mae", "after_rmse",
    "after_median_ae", "after_p90_ae", "after_p95_ae",
    "mae_change_pct", "rmse_change_pct", "p90_change_pct",
    "correction_bias_used", "sample_note",
]


def build_detail_path(city: str, model: str, start_date: str, end_date: str, horizon_hours: int) -> Path:
    return (
        DETAIL_ROOT / city / model
        / f"bias_detail__{start_date}__{end_date}__h{horizon_hours}.csv"
    )


def build_summary_path(start_date: str, end_date: str, horizon_hours: int) -> Path:
    return (
        OUTPUT_ROOT
        / f"correction_summary__{start_date}__{end_date}__h{horizon_hours}.csv"
    )


# ============================================================
# Helpers
# ============================================================

def _try_float(s) -> float | None:
    if s is None or str(s).strip() == "":
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _try_int(s) -> int | None:
    if s is None or str(s).strip() == "":
        return None
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def _r4(v: float) -> str:
    return str(round(v, 4))


def _pct_change(before: float, after: float) -> str:
    if before == 0:
        return ""
    return _r4(((after - before) / before) * 100)


def _percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = int(math.ceil(p / 100.0 * len(sorted_vals))) - 1
    idx = max(0, min(idx, len(sorted_vals) - 1))
    return sorted_vals[idx]


def _compute_stats(biases: list[float]) -> dict:
    """Compute stats from a list of bias values (= d - c or corrected_d - c)."""
    n = len(biases)
    if n == 0:
        return {
            "valid_pairs": "0", "mean_bias": "", "mae": "", "rmse": "",
            "median_ae": "", "p90_ae": "", "p95_ae": "",
        }
    abs_errors = sorted(abs(b) for b in biases)
    return {
        "valid_pairs": str(n),
        "mean_bias": _r4(sum(biases) / n),
        "mae": _r4(sum(abs_errors) / n),
        "rmse": _r4(math.sqrt(sum(b * b for b in biases) / n)),
        "median_ae": _r4(statistics.median(abs_errors)),
        "p90_ae": _r4(_percentile(abs_errors, 90)),
        "p95_ae": _r4(_percentile(abs_errors, 95)),
    }


# ============================================================
# Load detail CSV
# ============================================================

def load_detail_csv(city: str, model: str, start_date: str, end_date: str, horizon_hours: int) -> list[dict]:
    csv_path = build_detail_path(city, model, start_date, end_date, horizon_hours)
    if not csv_path.exists():
        log.warning(f"detail CSV not found: {csv_path}")
        return []
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


# ============================================================
# Correction engine
# ============================================================

def run_correction_trial(
    detail_rows: list[dict],
    city: str,
    model: str,
    method: str,
) -> list[dict]:
    """
    Run one correction method on detail rows.
    Returns summary rows (one per bucket).
    """
    # Extract matched rows with valid d/c values
    matched = []
    for r in detail_rows:
        if r.get("match_status") != "matched":
            continue
        d = _try_float(r.get("d_value"))
        c = _try_float(r.get("c_value"))
        h = _try_int(r.get("horizon_hour"))
        if d is not None and c is not None and h is not None:
            matched.append({"d": d, "c": c, "h": h, "bias": d - c})

    if not matched:
        return []

    # Compute correction biases
    if method == "overall_mean":
        overall_bias = sum(m["bias"] for m in matched) / len(matched)
        correction_map = {
            "overall": overall_bias,
            "h1_24": overall_bias,
            "h25_48": overall_bias,
        }
    elif method == "bucket_mean":
        correction_map = {}
        for bucket_name, h_min, h_max in HORIZON_BUCKETS:
            bucket_rows = [m for m in matched if h_min <= m["h"] <= h_max]
            if bucket_rows:
                correction_map[bucket_name] = sum(r["bias"] for r in bucket_rows) / len(bucket_rows)
            else:
                correction_map[bucket_name] = 0.0
    else:
        return []

    results: list[dict] = []

    for bucket_name, h_min, h_max in HORIZON_BUCKETS:
        bucket_rows = [m for m in matched if h_min <= m["h"] <= h_max]
        if not bucket_rows:
            continue

        # Determine which bias to use for correction
        if method == "bucket_mean" and bucket_name != "overall":
            corr_bias = correction_map[bucket_name]
        else:
            corr_bias = correction_map.get(bucket_name, correction_map.get("overall", 0.0))

        before_biases = [r["bias"] for r in bucket_rows]
        after_biases = [r["bias"] - corr_bias for r in bucket_rows]

        before = _compute_stats(before_biases)
        after = _compute_stats(after_biases)

        sample_note = "" if len(bucket_rows) >= 24 else "sample_too_small"

        b_mae = _try_float(before["mae"]) or 0
        a_mae = _try_float(after["mae"]) or 0
        b_rmse = _try_float(before["rmse"]) or 0
        a_rmse = _try_float(after["rmse"]) or 0
        b_p90 = _try_float(before["p90_ae"]) or 0
        a_p90 = _try_float(after["p90_ae"]) or 0

        results.append({
            "city": city,
            "model": model,
            "correction_method": method,
            "bucket": bucket_name,
            "valid_pairs": before["valid_pairs"],
            "before_mean_bias": before["mean_bias"],
            "before_mae": before["mae"],
            "before_rmse": before["rmse"],
            "before_median_ae": before["median_ae"],
            "before_p90_ae": before["p90_ae"],
            "before_p95_ae": before["p95_ae"],
            "after_mean_bias": after["mean_bias"],
            "after_mae": after["mae"],
            "after_rmse": after["rmse"],
            "after_median_ae": after["median_ae"],
            "after_p90_ae": after["p90_ae"],
            "after_p95_ae": after["p95_ae"],
            "mae_change_pct": _pct_change(b_mae, a_mae),
            "rmse_change_pct": _pct_change(b_rmse, a_rmse),
            "p90_change_pct": _pct_change(b_p90, a_p90),
            "correction_bias_used": _r4(corr_bias),
            "sample_note": sample_note,
        })

    return results


# ============================================================
# CSV output
# ============================================================

def write_csv(rows: list[dict], fieldnames: list[str], csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def correction_outputs_are_fresh(
    detail_paths: list[Path],
    summary_path: Path,
    status_path: Path,
    previous_status: dict,
    cities: list[str],
    models: list[str],
    start_date: str,
    end_date: str,
    horizon_hours: int,
) -> tuple[bool, dict[str, dict]]:
    signatures = {str(path): file_signature(path) for path in detail_paths}

    if all_exist([summary_path, status_path]) and min_mtime([summary_path, status_path]) >= max_mtime(detail_paths):
        return True, signatures

    if (
        previous_status.get("cities") == cities
        and previous_status.get("models") == models
        and previous_status.get("start_date") == start_date
        and previous_status.get("end_date") == end_date
        and int(previous_status.get("horizon_hours", 0)) == horizon_hours
    ):
        recorded = previous_status.get("detail_input_signatures", {})
        if recorded and all(
            signatures_match(signatures[path_key], recorded.get(path_key, {}))
            for path_key in signatures
        ):
            return True, signatures

    return False, signatures


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Bias correction pilot (v1)")
    parser.add_argument("--cities", type=str, default="London,Tokyo,New York City")
    parser.add_argument("--models", type=str, default="D1,D2,D7")
    parser.add_argument("--start-date", type=str, default="2026-03-25")
    parser.add_argument("--end-date", type=str, default="2026-03-31")
    parser.add_argument("--horizon-hours", type=int, default=48)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    cities = [c.strip() for c in args.cities.split(",") if c.strip()]
    models = resolve_d_models([m.strip() for m in args.models.split(",") if m.strip()])
    summary_path = build_summary_path(args.start_date, args.end_date, args.horizon_hours)
    status_path = LOG_DIR / "correction_status.json"
    previous_status = load_json_file(status_path)
    detail_paths = [
        build_detail_path(city, model, args.start_date, args.end_date, args.horizon_hours)
        for city in cities
        for model in models
    ]
    missing_detail_inputs = [path for path in detail_paths if not path.exists()]

    log.info("=" * 60)
    log.info("10_bias_correction_pilot v1")
    log.info(f"  cities: {cities}")
    log.info(f"  models: {models}")
    log.info(f"  date range: {args.start_date} ~ {args.end_date}")
    log.info(f"  horizon: {args.horizon_hours}h")
    log.info(f"  force: {args.force}")
    log.info(f"  methods: {CORRECTION_METHODS}")
    log.info("=" * 60)

    if missing_detail_inputs:
        for path in missing_detail_inputs:
            log.warning(f"missing detail input: {path}")

    stats = {
        "skipped_due_to_fresh_output": 0,
        "recomputed": 0,
        "missing_detail_inputs": len(missing_detail_inputs),
    }

    if detail_paths and not missing_detail_inputs and not args.force:
        is_fresh, detail_signatures = correction_outputs_are_fresh(
            detail_paths=detail_paths,
            summary_path=summary_path,
            status_path=status_path,
            previous_status=previous_status,
            cities=cities,
            models=models,
            start_date=args.start_date,
            end_date=args.end_date,
            horizon_hours=args.horizon_hours,
        )
        if is_fresh:
            stats["skipped_due_to_fresh_output"] = 1
            log.info("skip correction: summary/status are fresh for current detail inputs")
            log.info(
                "correction stats: "
                f"skipped_due_to_fresh_output={stats['skipped_due_to_fresh_output']} "
                f"recomputed={stats['recomputed']} "
                f"missing_detail_inputs={stats['missing_detail_inputs']}"
            )
            log.info(f"summary csv: {summary_path}")
            log.info(f"status: {status_path}")
            log.info("=" * 100)
            log.info("10_bias_correction_pilot: skipped")
            return

    all_results: list[dict] = []

    for city in cities:
        for model in models:
            detail = load_detail_csv(
                city, model, args.start_date, args.end_date, args.horizon_hours,
            )
            if not detail:
                continue

            for method in CORRECTION_METHODS:
                results = run_correction_trial(detail, city, model, method)
                all_results.extend(results)

    # Write summary CSV
    write_csv(all_results, SUMMARY_FIELDS, summary_path)
    stats["recomputed"] = 1

    # Console summary — focus on overall_mean method, overall bucket
    log.info("")
    log.info("=" * 120)
    log.info("BIAS CORRECTION PILOT: overall_mean method")
    log.info("=" * 120)

    log.info(
        f"{'city':<16} {'model':<20} {'bucket':<9} {'N':>4} "
        f"{'|before':>7} {'MAE':>6} {'RMSE':>6} {'medAE':>6} {'p90':>5} "
        f"{'|after':>7} {'MAE':>6} {'RMSE':>6} {'medAE':>6} {'p90':>5} "
        f"{'|':>1} {'dMAE%':>7} {'dRMSE%':>7} {'dp90%':>7}"
    )
    log.info("-" * 120)

    for r in all_results:
        if r["correction_method"] != "overall_mean":
            continue
        flag = " *" if r.get("sample_note") else ""
        log.info(
            f"{r['city']:<16} {r['model']:<20} {r['bucket']:<9} {r['valid_pairs']:>4} "
            f"{'|':>1}{r['before_mean_bias']:>6} {r['before_mae']:>6} {r['before_rmse']:>6} "
            f"{r['before_median_ae']:>6} {r['before_p90_ae']:>5} "
            f"{'|':>1}{r['after_mean_bias']:>6} {r['after_mae']:>6} {r['after_rmse']:>6} "
            f"{r['after_median_ae']:>6} {r['after_p90_ae']:>5} "
            f"{'|':>1} {r['mae_change_pct']:>7} {r['rmse_change_pct']:>7} "
            f"{r['p90_change_pct']:>7}{flag}"
        )

    log.info("-" * 120)

    # Bucket-level comparison
    log.info("")
    log.info("=" * 100)
    log.info("BUCKET CORRECTION COMPARISON: overall_mean vs bucket_mean")
    log.info("=" * 100)

    # Build lookup
    lookup: dict[tuple, dict] = {}
    for r in all_results:
        key = (r["city"], r["model"], r["correction_method"], r["bucket"])
        lookup[key] = r

    seen_pairs: set[tuple] = set()
    for r in all_results:
        pair = (r["city"], r["model"])
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)

        for bucket_name in ["h1_24", "h25_48"]:
            om = lookup.get((r["city"], r["model"], "overall_mean", bucket_name))
            bm = lookup.get((r["city"], r["model"], "bucket_mean", bucket_name))
            if not om or not bm:
                continue

            om_mae = om.get("after_mae", "")
            bm_mae = bm.get("after_mae", "")
            om_bias_used = om.get("correction_bias_used", "")
            bm_bias_used = bm.get("correction_bias_used", "")

            log.info(
                f"  {r['city']:<14} {r['model']:<18} {bucket_name:<7} "
                f"overall_mean(bias={om_bias_used})->MAE={om_mae}  "
                f"bucket_mean(bias={bm_bias_used})->MAE={bm_mae}"
            )

    # Write status
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    status = {
        "started_at_utc": now_utc(),
        "cities": cities, "models": models,
        "start_date": args.start_date, "end_date": args.end_date,
        "horizon_hours": args.horizon_hours,
        "force": args.force,
        "methods": CORRECTION_METHODS,
        "detail_input_signatures": {
            str(path): file_signature(path)
            for path in detail_paths
            if path.exists()
        },
        "skipped_due_to_fresh_output": stats["skipped_due_to_fresh_output"],
        "recomputed": stats["recomputed"],
        "missing_detail_inputs": stats["missing_detail_inputs"],
        "result_count": len(all_results),
        "output_csv": str(summary_path),
    }
    status_path.write_text(
        json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    log.info("")
    log.info(
        "correction stats: "
        f"skipped_due_to_fresh_output={stats['skipped_due_to_fresh_output']} "
        f"recomputed={stats['recomputed']} "
        f"missing_detail_inputs={stats['missing_detail_inputs']}"
    )
    log.info(f"summary csv: {summary_path}")
    log.info(f"status: {status_path}")
    log.info("=" * 100)
    log.info("10_bias_correction_pilot: complete")


if __name__ == "__main__":
    main()
