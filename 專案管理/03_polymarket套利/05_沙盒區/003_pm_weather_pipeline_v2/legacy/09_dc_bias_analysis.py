"""
09_dc_bias_analysis.py — D-C 偏差分析（v3）

v3 新增：
  - 穩定性統計：std_bias, median_bias, median_abs_error, p90/p95_abs_error
  - 多視角 ranking：overall / h1_24 / h25_48 各自排名
  - sample_note 標示樣本是否足夠

用法：
  python 09_dc_bias_analysis.py
  python 09_dc_bias_analysis.py --cities London,Tokyo --models D1,D2,D7
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

# --- 路徑常數 ---

D_SNAPSHOT_ROOT = PROJ_DIR / "08_snapshot" / "D"
C_SNAPSHOT_ROOT = PROJ_DIR / "08_snapshot" / "C"
ANALYSIS_OUTPUT_ROOT = PROJ_DIR / "08_snapshot" / "analysis" / "dc_bias"
RANKING_OUTPUT_ROOT = PROJ_DIR / "08_snapshot" / "analysis" / "dc_bias_rankings"
ANALYSIS_LOG_DIR = PROJ_DIR / "logs" / "08_snapshot" / "analysis"

sys.path.insert(0, str(PROJ_DIR))
from _lib import resolve_d_models, now_utc
from _lib.freshness_utils import (
    csv_semantic_signature,
    load_json_file,
    max_mtime,
    min_mtime,
    signatures_match,
)

JOIN_KEYS = ["city", "snapshot_date_local", "horizon_hour"]

HORIZON_BUCKETS = [
    ("overall", 1, 9999),
    ("h1_24", 1, 24),
    ("h25_48", 25, 48),
]

MIN_SAMPLE_SIZE = 24  # 少於此數標示 sample_too_small

# --- 輸出欄位 ---

DETAIL_FIELDS = [
    "city", "model", "snapshot_date_local", "horizon_hour",
    "target_time_local", "target_time_utc",
    "d_value", "c_value", "bias", "abs_error",
    "d_value_status", "c_value_status", "match_status",
    "timezone",
]

BUCKET_STAT_FIELDS = [
    "valid_pairs", "mean_bias", "std_bias",
    "median_bias", "mae", "median_abs_error",
    "rmse", "p90_abs_error", "p95_abs_error",
    "max_bias", "min_bias", "sample_note",
]

SUMMARY_FIELDS = [
    "city", "model",
    "start_date", "end_date", "horizon_hours",
    "total_pairs", "matched_pairs", "d_only", "c_only",
    "d_missing_value", "c_missing_value",
]
for _bn, _, _ in HORIZON_BUCKETS:
    for _sf in BUCKET_STAT_FIELDS:
        SUMMARY_FIELDS.append(f"{_bn}_{_sf}")
SUMMARY_FIELDS.append("analysis_time_utc")

RANKING_FIELDS = [
    "rank", "city", "model", "sort_bucket",
    "overall_valid_pairs", "overall_mean_bias", "overall_std_bias",
    "overall_mae", "overall_rmse",
    "overall_median_abs_error", "overall_p90_abs_error",
    "h1_24_valid_pairs", "h1_24_mae", "h1_24_std_bias", "h1_24_rmse",
    "h25_48_valid_pairs", "h25_48_mae", "h25_48_std_bias", "h25_48_rmse",
    "sample_note", "analysis_time_utc",
]


def build_d_batch_path(city: str, model: str, start_date: str, end_date: str, horizon_hours: int) -> Path:
    filename = f"snapshot_batch__{start_date}__{end_date}__h{horizon_hours}.csv"
    return D_SNAPSHOT_ROOT / city / model / filename


def build_c_batch_path(city: str, start_date: str, end_date: str, horizon_hours: int) -> Path:
    filename = f"snapshot_batch__{start_date}__{end_date}__h{horizon_hours}.csv"
    return C_SNAPSHOT_ROOT / city / filename


def build_detail_output_path(
    city: str, model: str, start_date: str, end_date: str, horizon_hours: int,
) -> Path:
    return (
        ANALYSIS_OUTPUT_ROOT / city / model
        / f"bias_detail__{start_date}__{end_date}__h{horizon_hours}.csv"
    )


def build_summary_output_path(
    city: str, model: str, start_date: str, end_date: str, horizon_hours: int,
) -> Path:
    return (
        ANALYSIS_OUTPUT_ROOT / city / model
        / f"bias_summary__{start_date}__{end_date}__h{horizon_hours}.csv"
    )


# ============================================================
# CSV loaders
# ============================================================

def load_d_batch(city: str, model: str, start_date: str, end_date: str, horizon_hours: int) -> dict:
    csv_path = build_d_batch_path(city, model, start_date, end_date, horizon_hours)
    return _load_batch_csv(csv_path)


def load_c_batch(city: str, start_date: str, end_date: str, horizon_hours: int) -> dict:
    csv_path = build_c_batch_path(city, start_date, end_date, horizon_hours)
    return _load_batch_csv(csv_path)


def _load_batch_csv(csv_path: Path) -> dict:
    if not csv_path.exists():
        log.warning(f"batch CSV not found: {csv_path}")
        return {}
    result = {}
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            key = tuple(row.get(k, "").strip() for k in JOIN_KEYS)
            result[key] = dict(row)
    return result


def load_summary_row(csv_path: Path) -> dict | None:
    if not csv_path.exists():
        return None
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return None
    return dict(rows[0])


# ============================================================
# Statistics helpers
# ============================================================

def _percentile(sorted_vals: list[float], p: float) -> float:
    """Simple nearest-rank percentile on a pre-sorted list."""
    if not sorted_vals:
        return 0.0
    idx = int(math.ceil(p / 100.0 * len(sorted_vals))) - 1
    idx = max(0, min(idx, len(sorted_vals) - 1))
    return sorted_vals[idx]


def _compute_bucket_stats(biases: list[float]) -> dict:
    valid = len(biases)
    empty = {f: "" for f in BUCKET_STAT_FIELDS}
    empty["valid_pairs"] = "0"
    empty["sample_note"] = "no_data"
    if valid == 0:
        return empty

    abs_errors = sorted(abs(b) for b in biases)

    mean_bias = sum(biases) / valid
    mae = sum(abs_errors) / valid
    rmse = math.sqrt(sum(b * b for b in biases) / valid)

    std_bias = statistics.pstdev(biases) if valid >= 2 else 0.0
    median_bias = statistics.median(biases)
    median_ae = statistics.median(abs_errors)
    p90_ae = _percentile(abs_errors, 90)
    p95_ae = _percentile(abs_errors, 95)

    sample_note = "" if valid >= MIN_SAMPLE_SIZE else "sample_too_small"

    return {
        "valid_pairs": str(valid),
        "mean_bias": _r4(mean_bias),
        "std_bias": _r4(std_bias),
        "median_bias": _r4(median_bias),
        "mae": _r4(mae),
        "median_abs_error": _r4(median_ae),
        "rmse": _r4(rmse),
        "p90_abs_error": _r4(p90_ae),
        "p95_abs_error": _r4(p95_ae),
        "max_bias": _r4(max(biases)),
        "min_bias": _r4(min(biases)),
        "sample_note": sample_note,
    }


def _r4(v: float) -> str:
    return str(round(v, 4))


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


# ============================================================
# Bias computation
# ============================================================

def compute_bias_detail(
    d_index: dict, c_index: dict, city: str, model: str,
) -> list[dict]:
    all_keys = sorted(set(d_index.keys()) | set(c_index.keys()))
    rows: list[dict] = []

    for key in all_keys:
        d_row = d_index.get(key)
        c_row = c_index.get(key)

        row = {f: "" for f in DETAIL_FIELDS}
        row["city"] = city
        row["model"] = model
        row["snapshot_date_local"] = key[1]
        row["horizon_hour"] = key[2]

        if d_row:
            row["target_time_local"] = d_row.get("target_time_local", "")
            row["target_time_utc"] = d_row.get("target_time_utc", "")
            row["d_value_status"] = d_row.get("value_status", "")
            row["timezone"] = d_row.get("timezone", "")
            if d_row.get("value_status") == "ok":
                row["d_value"] = d_row.get("temperature_2m", "")

        if c_row:
            row["c_value_status"] = c_row.get("value_status", "")
            if not row["target_time_local"]:
                row["target_time_local"] = c_row.get("target_time_local", "")
            if not row["target_time_utc"]:
                row["target_time_utc"] = c_row.get("target_time_utc", "")
            if not row["timezone"]:
                row["timezone"] = c_row.get("timezone", "")
            if c_row.get("value_status") == "ok":
                row["c_value"] = c_row.get("temperature_2m", "")

        if d_row and c_row:
            d_val = _try_float(row["d_value"])
            c_val = _try_float(row["c_value"])
            if d_val is not None and c_val is not None:
                bias = round(d_val - c_val, 4)
                row["bias"] = str(bias)
                row["abs_error"] = str(round(abs(bias), 4))
                row["match_status"] = "matched"
            elif d_val is None and c_val is not None:
                row["match_status"] = "d_missing_value"
            elif d_val is not None and c_val is None:
                row["match_status"] = "c_missing_value"
            else:
                row["match_status"] = "both_missing_value"
        elif d_row and not c_row:
            row["match_status"] = "c_only_missing"
        elif c_row and not d_row:
            row["match_status"] = "d_only_missing"

        rows.append(row)
    return rows


def compute_bias_summary(
    detail_rows: list[dict], city: str, model: str,
    start_date: str, end_date: str, horizon_hours: int,
) -> dict:
    total = len(detail_rows)
    matched = sum(1 for r in detail_rows if r["match_status"] == "matched")
    d_only = sum(1 for r in detail_rows if r["match_status"] == "c_only_missing")
    c_only = sum(1 for r in detail_rows if r["match_status"] == "d_only_missing")
    d_missing = sum(1 for r in detail_rows if r["match_status"] == "d_missing_value")
    c_missing = sum(1 for r in detail_rows if r["match_status"] == "c_missing_value")

    summary = {
        "city": city, "model": model,
        "start_date": start_date, "end_date": end_date,
        "horizon_hours": str(horizon_hours),
        "total_pairs": str(total), "matched_pairs": str(matched),
        "d_only": str(d_only), "c_only": str(c_only),
        "d_missing_value": str(d_missing), "c_missing_value": str(c_missing),
        "analysis_time_utc": now_utc(),
    }

    for bucket_name, h_min, h_max in HORIZON_BUCKETS:
        bucket_biases = []
        for r in detail_rows:
            h = _try_int(r.get("horizon_hour", ""))
            b = _try_float(r.get("bias", ""))
            if h is not None and b is not None and h_min <= h <= h_max:
                bucket_biases.append(b)
        stats = _compute_bucket_stats(bucket_biases)
        for stat_key, stat_val in stats.items():
            summary[f"{bucket_name}_{stat_key}"] = stat_val

    return summary


# ============================================================
# Ranking (multi-view)
# ============================================================

def _build_ranking_for_bucket(
    summaries: list[dict], city: str, bucket_name: str,
) -> list[dict]:
    """Sort by {bucket}_mae, return ranking rows."""
    mae_key = f"{bucket_name}_mae"
    city_summaries = [
        s for s in summaries
        if s["city"] == city and _try_float(s.get(mae_key, "")) is not None
    ]
    city_summaries.sort(key=lambda s: float(s[mae_key]))

    rows: list[dict] = []
    for rank, s in enumerate(city_summaries, 1):
        sample_notes = []
        for bn, _, _ in HORIZON_BUCKETS:
            sn = s.get(f"{bn}_sample_note", "")
            if sn:
                sample_notes.append(f"{bn}:{sn}")

        rows.append({
            "rank": str(rank),
            "city": city,
            "model": s["model"],
            "sort_bucket": bucket_name,
            "overall_valid_pairs": s.get("overall_valid_pairs", ""),
            "overall_mean_bias": s.get("overall_mean_bias", ""),
            "overall_std_bias": s.get("overall_std_bias", ""),
            "overall_mae": s.get("overall_mae", ""),
            "overall_rmse": s.get("overall_rmse", ""),
            "overall_median_abs_error": s.get("overall_median_abs_error", ""),
            "overall_p90_abs_error": s.get("overall_p90_abs_error", ""),
            "h1_24_valid_pairs": s.get("h1_24_valid_pairs", ""),
            "h1_24_mae": s.get("h1_24_mae", ""),
            "h1_24_std_bias": s.get("h1_24_std_bias", ""),
            "h1_24_rmse": s.get("h1_24_rmse", ""),
            "h25_48_valid_pairs": s.get("h25_48_valid_pairs", ""),
            "h25_48_mae": s.get("h25_48_mae", ""),
            "h25_48_std_bias": s.get("h25_48_std_bias", ""),
            "h25_48_rmse": s.get("h25_48_rmse", ""),
            "sample_note": "; ".join(sample_notes) if sample_notes else "",
            "analysis_time_utc": s.get("analysis_time_utc", ""),
        })
    return rows


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


def build_job_key(city: str, model: str) -> str:
    return f"{city}||{model}"


def job_outputs_are_fresh(
    detail_path: Path,
    summary_path: Path,
    c_batch_path: Path,
    d_batch_path: Path,
    previous_status: dict,
    job_key: str,
    cities: list[str],
    models: list[str],
    start_date: str,
    end_date: str,
    horizon_hours: int,
) -> tuple[bool, dict]:
    if not detail_path.exists() or not summary_path.exists():
        return False, {}

    output_paths = [detail_path, summary_path]
    input_paths = [c_batch_path, d_batch_path]
    current_signatures = {
        "c_batch": csv_semantic_signature(c_batch_path, ignore_fields={"fetch_time_utc"}),
        "d_batch": csv_semantic_signature(d_batch_path, ignore_fields={"fetch_time_utc"}),
    }

    if min_mtime(output_paths) >= max_mtime(input_paths):
        return True, current_signatures

    if (
        previous_status.get("cities") == cities
        and previous_status.get("models") == models
        and previous_status.get("start_date") == start_date
        and previous_status.get("end_date") == end_date
        and int(previous_status.get("horizon_hours", 0)) == horizon_hours
    ):
        recorded = previous_status.get("job_input_signatures", {}).get(job_key, {})
        if (
            signatures_match(current_signatures["c_batch"], recorded.get("c_batch", {}))
            and signatures_match(current_signatures["d_batch"], recorded.get("d_batch", {}))
        ):
            return True, current_signatures

    return False, current_signatures


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="D-C bias analysis (v3)")
    parser.add_argument("--cities", type=str, default="London")
    parser.add_argument("--models", type=str, default="D1")
    parser.add_argument("--start-date", type=str, default="2026-04-01")
    parser.add_argument("--end-date", type=str, default="2026-04-02")
    parser.add_argument("--horizon-hours", type=int, default=48)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    cities = [c.strip() for c in args.cities.split(",") if c.strip()]
    models = resolve_d_models([m.strip() for m in args.models.split(",") if m.strip()])
    status_path = ANALYSIS_LOG_DIR / "analysis_status.json"
    previous_status = load_json_file(status_path)

    log.info("=" * 60)
    log.info("09_dc_bias_analysis: D-C Bias Analysis v3")
    log.info(f"  cities: {cities}")
    log.info(f"  models: {models}")
    log.info(f"  date range: {args.start_date} ~ {args.end_date}")
    log.info(f"  horizon: {args.horizon_hours}h")
    log.info(f"  force: {args.force}")
    log.info(f"  buckets: {[b[0] for b in HORIZON_BUCKETS]}")
    log.info("=" * 60)

    all_summaries: list[dict] = []
    analysis_status = {
        "started_at_utc": now_utc(),
        "cities": cities, "models": models,
        "start_date": args.start_date, "end_date": args.end_date,
        "horizon_hours": args.horizon_hours,
        "horizon_buckets": [b[0] for b in HORIZON_BUCKETS],
        "force": args.force,
        "skipped_jobs": 0,
        "recomputed_jobs": 0,
        "missing_input_jobs": 0,
        "job_input_signatures": {},
        "generated_output_paths": [],
        "jobs": [],
    }

    for city in cities:
        c_batch_path = build_c_batch_path(
            city=city,
            start_date=args.start_date,
            end_date=args.end_date,
            horizon_hours=args.horizon_hours,
        )
        c_index: dict | None = None

        for model in models:
            d_batch_path = build_d_batch_path(
                city=city,
                model=model,
                start_date=args.start_date,
                end_date=args.end_date,
                horizon_hours=args.horizon_hours,
            )
            detail_path = build_detail_output_path(
                city=city,
                model=model,
                start_date=args.start_date,
                end_date=args.end_date,
                horizon_hours=args.horizon_hours,
            )
            summary_path = build_summary_output_path(
                city=city,
                model=model,
                start_date=args.start_date,
                end_date=args.end_date,
                horizon_hours=args.horizon_hours,
            )
            job_key = build_job_key(city, model)

            if not c_batch_path.exists() or not d_batch_path.exists():
                note = []
                if not c_batch_path.exists():
                    note.append("C batch CSV not found")
                if not d_batch_path.exists():
                    note.append("D batch CSV not found")
                note_str = "; ".join(note)
                log.warning(f"skip: city={city} model={model}: {note_str}")
                analysis_status["missing_input_jobs"] += 1
                analysis_status["jobs"].append({
                    "city": city, "model": model, "status": "missing_input",
                    "note": note_str,
                })
                continue

            should_skip = False
            current_signatures = {
                "c_batch": csv_semantic_signature(c_batch_path, ignore_fields={"fetch_time_utc"}),
                "d_batch": csv_semantic_signature(d_batch_path, ignore_fields={"fetch_time_utc"}),
            }
            if not args.force:
                should_skip, current_signatures = job_outputs_are_fresh(
                    detail_path=detail_path,
                    summary_path=summary_path,
                    c_batch_path=c_batch_path,
                    d_batch_path=d_batch_path,
                    previous_status=previous_status,
                    job_key=job_key,
                    cities=cities,
                    models=models,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    horizon_hours=args.horizon_hours,
                )

            if should_skip:
                existing_summary = load_summary_row(summary_path)
                if existing_summary is None:
                    log.warning(
                        f"fresh check passed but summary unreadable; recompute city={city} model={model}"
                    )
                else:
                    all_summaries.append(existing_summary)
                    analysis_status["skipped_jobs"] += 1
                    analysis_status["job_input_signatures"][job_key] = current_signatures
                    analysis_status["generated_output_paths"].extend([
                        str(detail_path),
                        str(summary_path),
                    ])
                    analysis_status["jobs"].append({
                        "city": city,
                        "model": model,
                        "status": "skipped_fresh",
                        "note": "detail/summary outputs are fresh",
                    })
                    log.info(f"skip fresh: city={city} model={model}")
                    continue

            if c_index is None:
                c_index = load_c_batch(
                    city=city,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    horizon_hours=args.horizon_hours,
                )
                log.info(f"C batch loaded: city={city} rows={len(c_index)}")

            d_index = load_d_batch(
                city=city,
                model=model,
                start_date=args.start_date,
                end_date=args.end_date,
                horizon_hours=args.horizon_hours,
            )
            log.info(f"D batch loaded: city={city} model={model} rows={len(d_index)}")

            detail_rows = compute_bias_detail(d_index, c_index, city, model)
            summary = compute_bias_summary(
                detail_rows, city, model,
                args.start_date, args.end_date, args.horizon_hours,
            )
            all_summaries.append(summary)
            analysis_status["recomputed_jobs"] += 1
            analysis_status["job_input_signatures"][job_key] = current_signatures

            write_csv(detail_rows, DETAIL_FIELDS, detail_path)
            write_csv([summary], SUMMARY_FIELDS, summary_path)
            analysis_status["generated_output_paths"].extend([
                str(detail_path),
                str(summary_path),
            ])

            log.info(
                f"  {city} × {model}: "
                f"valid={summary.get('overall_valid_pairs','0')} "
                f"mae={summary.get('overall_mae','')} "
                f"std={summary.get('overall_std_bias','')} "
                f"p90={summary.get('overall_p90_abs_error','')}"
            )

            analysis_status["jobs"].append({
                "city": city, "model": model, "status": "recomputed",
                "overall_valid": summary.get("overall_valid_pairs", "0"),
                "overall_mae": summary.get("overall_mae", ""),
                "overall_std": summary.get("overall_std_bias", ""),
            })

    # --- Consolidated summary table ---
    log.info("")
    log.info("=" * 110)
    log.info("D-C BIAS ANALYSIS SUMMARY")
    log.info("=" * 110)

    if all_summaries:
        log.info(
            f"{'city':<14} {'model':<20} "
            f"{'N':>4} {'bias':>7} {'MAE':>6} {'std':>6} {'medAE':>6} {'p90':>6} {'p95':>6} "
            f"{'|':>1} {'h1-24':>6} {'h25-48':>7}"
        )
        log.info("-" * 110)
        for s in all_summaries:
            note = s.get("overall_sample_note", "")
            flag = " *" if note else ""
            log.info(
                f"{s['city']:<14} {s['model']:<20} "
                f"{s.get('overall_valid_pairs',''):>4} "
                f"{s.get('overall_mean_bias',''):>7} "
                f"{s.get('overall_mae',''):>6} "
                f"{s.get('overall_std_bias',''):>6} "
                f"{s.get('overall_median_abs_error',''):>6} "
                f"{s.get('overall_p90_abs_error',''):>6} "
                f"{s.get('overall_p95_abs_error',''):>6} "
                f"{'|':>1} "
                f"{s.get('h1_24_mae',''):>6} "
                f"{s.get('h25_48_mae',''):>7}{flag}"
            )
        log.info("-" * 110)

    # Write consolidated summary CSV
    if all_summaries:
        consolidated_path = (
            ANALYSIS_OUTPUT_ROOT
            / f"dc_bias_summary__{args.start_date}__{args.end_date}__h{args.horizon_hours}.csv"
        )
        write_csv(all_summaries, SUMMARY_FIELDS, consolidated_path)
        analysis_status["generated_output_paths"].append(str(consolidated_path))
        log.info(f"consolidated summary: {consolidated_path}")

    # --- Rankings per city × per bucket ---
    for city in cities:
        for bucket_name, _, _ in HORIZON_BUCKETS:
            ranking_rows = _build_ranking_for_bucket(all_summaries, city, bucket_name)
            if not ranking_rows:
                continue

            ranking_path = (
                RANKING_OUTPUT_ROOT / city
                / f"ranking_{bucket_name}__{args.start_date}__{args.end_date}__h{args.horizon_hours}.csv"
            )
            write_csv(ranking_rows, RANKING_FIELDS, ranking_path)
            analysis_status["generated_output_paths"].append(str(ranking_path))

            log.info("")
            log.info(f"RANKING: {city} (sorted by {bucket_name} MAE)")
            log.info(
                f"  {'#':>2} {'model':<20} "
                f"{'MAE':>6} {'std':>6} {'RMSE':>6} {'medAE':>6} {'p90':>6} "
                f"{'|':>1} {'h1-24':>6} {'h25-48':>7} {'note'}"
            )
            log.info(f"  {'-'*86}")
            for r in ranking_rows:
                bkt_mae = r.get(f"{bucket_name}_mae", r.get("overall_mae", ""))
                bkt_std = r.get(f"{bucket_name}_std_bias", r.get("overall_std_bias", ""))
                bkt_rmse = r.get(f"{bucket_name}_rmse", r.get("overall_rmse", ""))
                log.info(
                    f"  {r['rank']:>2} {r['model']:<20} "
                    f"{bkt_mae:>6} {bkt_std:>6} {bkt_rmse:>6} "
                    f"{r.get('overall_median_abs_error',''):>6} "
                    f"{r.get('overall_p90_abs_error',''):>6} "
                    f"{'|':>1} "
                    f"{r.get('h1_24_mae',''):>6} "
                    f"{r.get('h25_48_mae',''):>7} "
                    f"{r.get('sample_note','')}"
                )
            log.info(f"  csv: {ranking_path}")

    # Write analysis status
    analysis_status["finished_at_utc"] = now_utc()
    ANALYSIS_LOG_DIR.mkdir(parents=True, exist_ok=True)
    status_path.write_text(
        json.dumps(analysis_status, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info(
        "job stats: "
        f"skipped_jobs={analysis_status['skipped_jobs']} "
        f"recomputed_jobs={analysis_status['recomputed_jobs']} "
        f"missing_input_jobs={analysis_status['missing_input_jobs']}"
    )
    log.info(f"\nanalysis status: {status_path}")
    log.info("=" * 110)
    log.info("09_dc_bias_analysis: complete")


if __name__ == "__main__":
    main()
