"""
09_model_engine.py — 誤差分布建模

⚠️ D1-only MVP baseline：基於少量樣本，僅供管線驗證，不可用於交易決策
  - lead_day 是 previous_day offset（1~7），不是精確的 lead_to_settle_hours
  - 費率參數需人工驗證後才可信任

輸入：data/processed/error_table/{city}/market_day_error_table.csv

輸出（必過）：
  data/models/empirical/{city}/empirical_model.json   — Empirical ECDF

輸出（optional）：
  data/models/ou_ar/{city}/ou_model.json              — OU / AR(1)

輸出（best-effort）：
  data/models/quantile_regression/{city}/qr_model.json — QR（需 statsmodels）
"""

import argparse
import csv
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

QUANTILE_PERCENTILES = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95]
MIN_BUCKET_SAMPLES = 5
MODEL_SCOPE = "D1-only MVP"


# ============================================================
# 工具函數
# ============================================================

def compute_quantile(sorted_vals: list, p_pct: float) -> float:
    """Linear-interpolation quantile. p_pct in 0~100."""
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    if n == 1:
        return round(float(sorted_vals[0]), 4)
    idx = (p_pct / 100.0) * (n - 1)
    lo = int(idx)
    hi = min(lo + 1, n - 1)
    frac = idx - lo
    return round(float(sorted_vals[lo]) + frac * (float(sorted_vals[hi]) - float(sorted_vals[lo])), 4)


def _atomic_write_json(path: Path, data: dict) -> None:
    """原子寫入 JSON：先寫 .tmp → os.replace()，防止讀到半寫狀態。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", dir=str(path.parent), suffix=".tmp", delete=False, encoding="utf-8"
    ) as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        tmp = f.name
    os.replace(tmp, str(path))


def load_error_table(path: Path) -> list[dict]:
    if not path.exists():
        log.warning(f"Error table not found: {path}")
        return []
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))
    return rows


def safe_float(val) -> float | None:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ============================================================
# Model A: Empirical ECDF（必過）
# ============================================================

def _build_empirical_bucket(ld: int, errors: list[float], merged_from: list[int] | None) -> dict:
    n = len(errors)
    sorted_errors = sorted(errors)
    mean_val = round(sum(sorted_errors) / n, 4) if n > 0 else 0.0
    variance = sum((x - mean_val) ** 2 for x in sorted_errors) / n if n > 0 else 0.0
    std_val = round(variance ** 0.5, 4)
    quantiles = {f"q{p:02d}": compute_quantile(sorted_errors, p) for p in QUANTILE_PERCENTILES}
    status = "merged" if merged_from else ("sufficient" if n >= MIN_BUCKET_SAMPLES else "insufficient")
    return {
        "lead_day": ld,
        "sample_count": n,
        "bucket_status": status,
        "merged_from": merged_from,
        "sorted_errors": [round(e, 4) for e in sorted_errors],
        "quantiles": quantiles,
        "mean": mean_val,
        "std": std_val,
    }


def build_empirical_model(city: str, error_rows: list[dict]) -> dict:
    """Build empirical signed-residual ECDF model."""
    # Group errors by lead_day
    groups: dict[int, list[float]] = {}
    for row in error_rows:
        ld = safe_float(row.get("lead_day"))
        err = safe_float(row.get("error"))
        if ld is not None and err is not None:
            groups.setdefault(int(ld), []).append(err)

    buckets: dict[str, dict] = {}
    for ld in sorted(groups.keys()):
        errors = groups[ld]

        if len(errors) >= MIN_BUCKET_SAMPLES:
            bucket = _build_empirical_bucket(ld, errors, merged_from=None)
        else:
            # Merge with ±1 adjacent lead_days
            merged_errors = errors[:]
            merged_from_list: list[int] = []
            for adj in [ld - 1, ld + 1]:
                if adj in groups:
                    merged_errors.extend(groups[adj])
                    merged_from_list.append(adj)

            if len(merged_errors) >= MIN_BUCKET_SAMPLES:
                bucket = _build_empirical_bucket(ld, merged_errors, merged_from=merged_from_list)
            else:
                # Still insufficient after merging
                bucket = _build_empirical_bucket(ld, merged_errors, merged_from=merged_from_list if merged_from_list else None)
                bucket["bucket_status"] = "insufficient"

        buckets[f"lead_day_{ld}"] = bucket

    dates = sorted(r["market_date_local"] for r in error_rows if r.get("market_date_local"))
    return {
        "schema_version": "empirical_v1",
        "model_type": "empirical_signed_residual_ecdf",
        "model_scope": MODEL_SCOPE,
        "city": city,
        "build_time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "train_start": dates[0] if dates else "",
        "train_end": dates[-1] if dates else "",
        "source_rows": len(error_rows),
        "buckets": buckets,
    }


# ============================================================
# Model B: OU / AR(1)（optional）
# ============================================================

def _fit_ar1(vals_temporal: list[float]) -> tuple[float, float, float] | None:
    """Estimate AR(1) parameters (mu, phi, sigma). Returns None if failed."""
    n = len(vals_temporal)
    if n < 3:
        return None
    mu = sum(vals_temporal) / n
    centered = [v - mu for v in vals_temporal]
    numerator = sum(centered[t] * centered[t - 1] for t in range(1, n))
    denominator = sum(centered[t - 1] ** 2 for t in range(1, n))
    if denominator < 1e-10:
        return None
    phi = numerator / denominator
    if abs(phi) >= 1.0:
        return None  # Non-stationary
    residuals = [centered[t] - phi * centered[t - 1] for t in range(1, n)]
    sigma = (sum(r ** 2 for r in residuals) / max(n - 1, 1)) ** 0.5
    return round(mu, 4), round(phi, 4), round(sigma, 4)


def build_ou_model(city: str, error_rows: list[dict]) -> dict | None:
    """Build OU/AR(1) model per (city, lead_day) bucket."""
    # Sort rows by market_date for temporal ordering
    sorted_rows = sorted(error_rows, key=lambda r: r.get("market_date_local", ""))

    groups: dict[int, list[float]] = {}
    for row in sorted_rows:
        ld = safe_float(row.get("lead_day"))
        err = safe_float(row.get("error"))
        if ld is not None and err is not None:
            groups.setdefault(int(ld), []).append(err)

    buckets: dict[str, dict] = {}
    success_count = 0

    for ld in sorted(groups.keys()):
        errors_temporal = groups[ld]
        result = _fit_ar1(errors_temporal)
        key = f"lead_day_{ld}"
        if result is None:
            buckets[key] = {
                "lead_day": ld,
                "sample_count": len(errors_temporal),
                "status": "failed",
                "reason": "insufficient_samples_or_non_stationary",
            }
        else:
            mu, phi, sigma = result
            buckets[key] = {
                "lead_day": ld,
                "sample_count": len(errors_temporal),
                "status": "ok",
                "mu": mu,
                "phi": phi,
                "sigma": sigma,
            }
            success_count += 1

    if success_count == 0:
        return None

    dates = sorted(r["market_date_local"] for r in error_rows if r.get("market_date_local"))
    return {
        "schema_version": "ou_ar1_v1",
        "model_type": "ou_ar1",
        "model_scope": MODEL_SCOPE,
        "city": city,
        "build_time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "train_start": dates[0] if dates else "",
        "train_end": dates[-1] if dates else "",
        "source_rows": len(error_rows),
        "successful_buckets": success_count,
        "buckets": buckets,
    }


# ============================================================
# Model C: Quantile Regression（best-effort，需 statsmodels）
# ============================================================

def build_qr_model(city: str, error_rows: list[dict]) -> dict | None:
    """Build QR model using statsmodels (best-effort, requires pip install statsmodels)."""
    try:
        import numpy as np
        from statsmodels.regression.quantile_regression import QuantReg
    except ImportError as e:
        log.warning(f"QR model skipped ({city}): {e}  →  pip install statsmodels numpy")
        return None

    X_rows, y_vals = [], []
    for row in error_rows:
        pred = safe_float(row.get("predicted_daily_high"))
        ld = safe_float(row.get("lead_day"))
        err = safe_float(row.get("error"))
        if pred is not None and ld is not None and err is not None:
            X_rows.append([1.0, pred, ld])  # intercept + 2 features
            y_vals.append(err)

    if len(X_rows) < 10:
        log.warning(f"QR model ({city}): insufficient rows ({len(X_rows)}), skipping")
        return None

    X = np.array(X_rows)
    y = np.array(y_vals)

    quantile_results: dict[str, dict] = {}
    failed_quantiles: list[int] = []

    for p in QUANTILE_PERCENTILES:
        tau = p / 100.0
        try:
            res = QuantReg(y, X).fit(q=tau, max_iter=1000)
            params = res.params.tolist()
            quantile_results[f"q{p:02d}"] = {
                "tau": tau,
                "intercept": round(params[0], 4),
                "coef_predicted": round(params[1], 4),
                "coef_lead_day": round(params[2], 4),
            }
        except Exception as e:
            log.warning(f"QR fit failed for {city} q{p}: {e}")
            failed_quantiles.append(p)

    if not quantile_results:
        return None

    dates = sorted(r["market_date_local"] for r in error_rows if r.get("market_date_local"))
    return {
        "schema_version": "qr_v1",
        "model_type": "quantile_regression",
        "model_scope": MODEL_SCOPE,
        "city": city,
        "build_time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "train_start": dates[0] if dates else "",
        "train_end": dates[-1] if dates else "",
        "source_rows": len(error_rows),
        "n_samples": len(X_rows),
        "features": ["intercept", "predicted_daily_high", "lead_day"],
        "failed_quantiles": failed_quantiles,
        "quantiles": quantile_results,
    }


# ============================================================
# Main logic
# ============================================================

def run(cities: str, models: str, verbose: bool) -> bool:
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=" * 55)
    log.info("09_model_engine: 誤差分布建模")
    log.info(f"⚠️  {MODEL_SCOPE} — 僅供管線驗證，不可用於交易決策")
    log.info("=" * 55)

    cities_filter = {c.strip() for c in cities.split(",") if c.strip()} if cities else set()
    run_empirical = models in ("all", "empirical")
    run_ou = models in ("all", "ou")
    run_qr = models in ("all", "qr")

    error_root = PROJ_DIR / "data" / "processed" / "error_table"
    if not error_root.exists():
        log.error("data/processed/error_table/ does not exist")
        return False

    city_dirs = sorted(d for d in error_root.iterdir() if d.is_dir())
    if not city_dirs:
        log.error("No city error tables found")
        return False

    any_empirical_success = False

    for city_dir in city_dirs:
        city = city_dir.name
        if cities_filter and city not in cities_filter:
            continue

        error_rows = load_error_table(city_dir / "market_day_error_table.csv")
        if not error_rows:
            log.warning(f"{city}: no error rows, skipping")
            continue

        log.info(f"--- {city}: {len(error_rows)} error rows ---")

        # ── Model A: Empirical（必過）──
        if run_empirical:
            model = build_empirical_model(city, error_rows)
            out_dir = PROJ_DIR / "data" / "models" / "empirical" / city
            out_path = out_dir / "empirical_model.json"
            _atomic_write_json(out_path, model)
            n_buckets = len(model["buckets"])
            log.info(f"  Empirical: {out_path.relative_to(PROJ_DIR)} ({n_buckets} buckets)")
            any_empirical_success = True

        # ── Model B: OU/AR(1)（optional）──
        if run_ou:
            try:
                model = build_ou_model(city, error_rows)
                if model:
                    out_dir = PROJ_DIR / "data" / "models" / "ou_ar" / city
                    out_path = out_dir / "ou_model.json"
                    _atomic_write_json(out_path, model)
                    log.info(f"  OU/AR(1): {out_path.relative_to(PROJ_DIR)} ({model['successful_buckets']} ok)")
                else:
                    log.warning(f"  OU/AR(1) ({city}): all buckets failed, skipping")
            except Exception as e:
                log.warning(f"  OU/AR(1) ({city}) exception: {e}")

        # ── Model C: QR（best-effort）──
        if run_qr:
            try:
                model = build_qr_model(city, error_rows)
                if model:
                    out_dir = PROJ_DIR / "data" / "models" / "quantile_regression" / city
                    out_path = out_dir / "qr_model.json"
                    _atomic_write_json(out_path, model)
                    log.info(f"  QR: {out_path.relative_to(PROJ_DIR)}")
                else:
                    log.warning(f"  QR ({city}): failed or skipped")
            except Exception as e:
                log.warning(f"  QR ({city}) exception: {e}")

    if run_empirical and not any_empirical_success:
        log.error("Empirical model required but no city succeeded")
        return False

    log.info("09_model_engine done.")
    return True


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Error distribution modeling (09_model_engine)")
    p.add_argument("--cities", type=str, default="", help="城市 filter（逗號分隔）")
    p.add_argument("--model", type=str, default="all", choices=["all", "empirical", "ou", "qr"],
                   dest="model", help="要建的模型類型（預設: all）")
    p.add_argument("--verbose", action="store_true")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    ok = run(cities=args.cities, models=args.model, verbose=args.verbose)
    sys.exit(0 if ok else 1)
