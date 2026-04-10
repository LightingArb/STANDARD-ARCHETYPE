"""
14_backfill_manager.py — 歷史回補管理器

對 city_status == "discovered" 的城市跑完整回補流程：
  05（forecast）→ 06（truth）→ 07（pipeline）→ 09（model）

成功條件：error_row_count >= 100 AND empirical_model.json 存在且可讀
→ 成功 → csm.set_ready()
→ 失敗 → csm.set_failed(reason)

設計原則：
  - 一個城市失敗不阻塞其他城市
  - 每完成一個城市立即更新 city_status.json（不是全部做完才更新）
  - 14 不跑 08/10/11（那是 Collector/Signal layer 的事）

CLI：
  python 14_backfill_manager.py                          # 回補所有 discovered
  python 14_backfill_manager.py --cities "Tokyo,Seoul"   # 只回補指定
  python 14_backfill_manager.py --retry-failed            # 重跑 failed
  python 14_backfill_manager.py --start-date 2024-01-01  # 指定起始日
"""

import argparse
import csv
import json
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

MIN_ERROR_ROWS = 730                  # ready 條件：error_table 至少 730 筆（約 2 年）
READY_MAX_FORECAST_AGE_DAYS = 7       # latest error_table date 距今最多幾天
DEFAULT_BACKFILL_START = "2023-01-01" # 回補預設起始日

# 各步驟 timeout（秒）：truth fetch 最久，允許 1194 筆 × ~1.5s
STEP_TIMEOUT: dict[str, int] = {
    "05_D_forecast_fetch.py": 1800,
    "06_B_truth_fetch.py":    3600,
    "07_daily_high_pipeline.py": 600,
    "09_model_engine.py":        600,
    "10_event_probability.py":   600,
    "03_market_catalog.py":      300,
    "04_market_master.py":       300,
}


# ============================================================
# 工具函數
# ============================================================

def run_script(script_name: str, args: list[str] = None, label: str = "") -> bool:
    script_path = PROJ_DIR / script_name
    if not script_path.exists():
        log.error(f"Script not found: {script_path}")
        return False
    cmd = [sys.executable, str(script_path)] + (args or [])
    display_label = label or script_name
    timeout = STEP_TIMEOUT.get(script_name, 600)
    log.info(f"  Running: {display_label}  cmd: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJ_DIR),
            capture_output=False,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            log.error(f"  {display_label} exited with code {result.returncode}")
            return False
        log.info(f"  {display_label}: OK")
        return True
    except subprocess.TimeoutExpired:
        log.error(f"  {display_label} timed out after {timeout}s")
        return False
    except Exception as e:
        log.error(f"  {display_label} failed: {e}")
        return False


def count_error_rows(city: str) -> int:
    """Count rows in market_day_error_table.csv for a city."""
    path = PROJ_DIR / "data" / "processed" / "error_table" / city / "market_day_error_table.csv"
    if not path.exists():
        return 0
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            return sum(1 for _ in reader)
    except Exception:
        return 0


def load_model_json(city: str) -> Optional[dict]:
    """Load empirical_model.json for city. Returns None if not found/invalid."""
    path = PROJ_DIR / "data" / "models" / "empirical" / city / "empirical_model.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return None


def get_date_range_from_error_table(city: str) -> tuple[Optional[str], Optional[str]]:
    """Return (earliest_market_date, latest_market_date) from error_table."""
    path = PROJ_DIR / "data" / "processed" / "error_table" / city / "market_day_error_table.csv"
    if not path.exists():
        return None, None
    try:
        dates = []
        with open(path, "r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                d = row.get("market_date_local", "")
                if d:
                    dates.append(d)
        if not dates:
            return None, None
        return min(dates), max(dates)
    except Exception:
        return None, None


# ============================================================
# Per-city backfill
# ============================================================

def backfill_city(
    city: str,
    csm,
    start_date: str = "",
    end_date: str = "",
) -> bool:
    """
    回補單一城市。回傳 True = 成功（已設為 ready），False = 失敗。
    """
    log.info(f"{'='*55}")
    log.info(f"Backfilling: {city}")
    log.info(f"{'='*55}")

    try:
        csm.set_backfilling(city)
    except ValueError as e:
        log.warning(f"  Cannot start backfill for {city}: {e}")
        return False

    # 05 forecast
    forecast_args = ["--cities", city, "--mode", "historical"]
    if start_date:
        forecast_args += ["--start-date", start_date]
    if end_date:
        forecast_args += ["--end-date", end_date]
    if not run_script("05_D_forecast_fetch.py", args=forecast_args, label="05_D_forecast_fetch"):
        reason = "05_D_forecast_fetch failed"
        log.error(f"  {city}: FAILED — {reason}")
        try:
            csm.set_failed(city, reason=reason)
        except ValueError:
            pass
        return False

    # 05 forecast (live) — 補抓今日預報，確保 ready 後 10/11 立刻可用
    if not run_script(
        "05_D_forecast_fetch.py",
        args=["--cities", city, "--mode", "live"],
        label="05_D_forecast_fetch (live)",
    ):
        log.warning(f"  {city}: 05_D_forecast_fetch live failed (non-blocking)")

    # 06 truth
    truth_args = ["--cities", city, "--mode", "historical"]
    if start_date:
        truth_args += ["--start-date", start_date]
    if end_date:
        truth_args += ["--end-date", end_date]
    if not run_script("06_B_truth_fetch.py", args=truth_args, label="06_B_truth_fetch"):
        reason = "06_B_truth_fetch failed"
        log.error(f"  {city}: FAILED — {reason}")
        try:
            csm.set_failed(city, reason=reason)
        except ValueError:
            pass
        return False

    # 07 pipeline
    pipeline_args = ["--cities", city]
    if start_date:
        pipeline_args += ["--start-date", start_date]
    if end_date:
        pipeline_args += ["--end-date", end_date]
    if not run_script("07_daily_high_pipeline.py", args=pipeline_args, label="07_daily_high_pipeline"):
        reason = "07_daily_high_pipeline failed"
        log.error(f"  {city}: FAILED — {reason}")
        try:
            csm.set_failed(city, reason=reason)
        except ValueError:
            pass
        return False

    # 09 model
    if not run_script("09_model_engine.py", args=["--cities", city], label="09_model_engine"):
        reason = "09_model_engine failed"
        log.error(f"  {city}: FAILED — {reason}")
        try:
            csm.set_failed(city, reason=reason)
        except ValueError:
            pass
        return False

    # 03→04: 更新 market_master（non-blocking，必須在 10 之前，10 需要市場資料）
    # 帶入所有 ready 城市 + 當前城市，避免覆蓋其他城市的市場
    try:
        all_ready = csm.get_cities_by_status("ready")
        all_cities = sorted(set(all_ready + [city]))
        cities_str = ",".join(all_cities)
        log.info(f"  Updating market_master for cities: {all_cities}")
        if not run_script("03_market_catalog.py", args=["--cities", cities_str], label="03_market_catalog"):
            log.warning(f"  {city}: 03_market_catalog failed (non-blocking — market_master not updated)")
        elif not run_script("04_market_master.py", label="04_market_master"):
            log.warning(f"  {city}: 04_market_master failed (non-blocking — market_master not updated)")
    except Exception as e:
        log.warning(f"  {city}: market_master update error: {e} (non-blocking)")

    # 10 event probability
    if not run_script("10_event_probability.py", args=["--cities", city], label="10_event_probability"):
        reason = "10_event_probability failed"
        log.error(f"  {city}: FAILED — {reason}")
        try:
            csm.set_failed(city, reason=reason)
        except ValueError:
            pass
        return False

    # Verify ready conditions
    error_rows = count_error_rows(city)
    model = load_model_json(city)
    earliest, latest = get_date_range_from_error_table(city)

    if error_rows < MIN_ERROR_ROWS:
        reason = f"insufficient error_rows={error_rows} (min={MIN_ERROR_ROWS})"
        log.error(f"  {city}: FAILED — {reason}")
        try:
            csm.set_failed(city, reason=reason)
        except ValueError:
            pass
        return False

    # Freshness check: latest error_table date must be within READY_MAX_FORECAST_AGE_DAYS
    if latest:
        from datetime import date as _date
        try:
            age_days = (_date.today() - _date.fromisoformat(latest)).days
            if age_days > READY_MAX_FORECAST_AGE_DAYS:
                reason = (
                    f"latest_forecast_date={latest} is {age_days}d old"
                    f" (max={READY_MAX_FORECAST_AGE_DAYS})"
                )
                log.error(f"  {city}: FAILED — {reason}")
                try:
                    csm.set_failed(city, reason=reason)
                except ValueError:
                    pass
                return False
        except ValueError:
            pass

    if model is None:
        reason = "empirical_model.json missing or unreadable"
        log.error(f"  {city}: FAILED — {reason}")
        try:
            csm.set_failed(city, reason=reason)
        except ValueError:
            pass
        return False

    # 新增：市場主檔確認（market_master.csv 裡必須有該城市的 enabled 市場）
    master_path = PROJ_DIR / "data" / "market_master.csv"
    if master_path.exists():
        try:
            with open(master_path, "r", encoding="utf-8", newline="") as f:
                city_markets = [
                    r for r in csv.DictReader(f)
                    if r.get("city") == city and r.get("market_enabled", "").lower() == "true"
                ]
            if not city_markets:
                reason = "no enabled markets in market_master — 03/04 may have failed"
                log.error(f"  {city}: FAILED — {reason}")
                try:
                    csm.set_failed(city, reason=reason)
                except ValueError:
                    pass
                return False
            log.info(f"  {city}: {len(city_markets)} enabled market(s) in market_master ✓")
        except Exception as e:
            log.warning(f"  {city}: could not verify market_master: {e} (non-blocking)")
    else:
        log.warning(f"  {city}: market_master.csv not found — skipping market check (non-blocking)")

    # 新增：probability 檔案確認（必須存在且 > 0 行）
    prob_path = PROJ_DIR / "data" / "results" / "probability" / city / "event_probability.csv"
    if prob_path.exists():
        try:
            with open(prob_path, "r", encoding="utf-8", newline="") as f:
                prob_rows = sum(1 for _ in csv.DictReader(f))
            if prob_rows <= 0:
                reason = "event_probability.csv empty — 10 found no active markets"
                log.error(f"  {city}: FAILED — {reason}")
                try:
                    csm.set_failed(city, reason=reason)
                except ValueError:
                    pass
                return False
            log.info(f"  {city}: {prob_rows} probability row(s) ✓")
        except Exception as e:
            log.warning(f"  {city}: could not verify probability: {e} (non-blocking)")
    else:
        reason = "event_probability.csv not found — 10_event_probability did not run or produced no output"
        log.error(f"  {city}: FAILED — {reason}")
        try:
            csm.set_failed(city, reason=reason)
        except ValueError:
            pass
        return False

    # All checks passed → ready
    try:
        csm.set_ready(
            city,
            error_row_count=error_rows,
            earliest_forecast_date=earliest,
            latest_forecast_date=latest,
        )
    except ValueError as e:
        log.error(f"  {city}: set_ready failed: {e}")
        return False

    log.info(f"  {city}: READY (error_rows={error_rows}, earliest={earliest}, latest={latest})")
    return True


# ============================================================
# Main
# ============================================================

def run(
    cities_override: str = "",
    retry_failed: bool = False,
    start_date: str = "",
    end_date: str = "",
) -> bool:
    # 預設起始日（若未指定）
    if not start_date:
        start_date = DEFAULT_BACKFILL_START
    # 預設結束日（若未指定）→ 今天
    if not end_date:
        import datetime as _dt
        end_date = _dt.date.today().isoformat()
    log.info("=" * 55)
    log.info("14_backfill_manager: 歷史回補管理器")
    log.info("=" * 55)

    import importlib.util
    _spec = importlib.util.spec_from_file_location(
        "city_status_manager", PROJ_DIR / "13_city_status_manager.py"
    )
    _mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    CityStatusManager = _mod.CityStatusManager

    csm = CityStatusManager()

    if cities_override:
        target_cities = [c.strip() for c in cities_override.split(",") if c.strip()]
        # Force allow these cities; set them to discovered if they exist as failed
        log.info(f"Cities override: {target_cities}")
    elif retry_failed:
        target_cities = csm.get_cities_by_status("failed")
        log.info(f"Retry-failed mode: {target_cities}")
        # Transition failed → backfilling is allowed via set_backfilling(force=True) approach
        # Actually VALID_TRANSITIONS allows failed → backfilling, so no force needed
    else:
        target_cities = csm.get_cities_by_status("discovered")
        log.info(f"Discovered cities to backfill: {target_cities}")

    if not target_cities:
        log.info("No cities to backfill.")
        return True

    results = {"success": [], "failed": []}

    for city in target_cities:
        ok = backfill_city(
            city=city,
            csm=csm,
            start_date=start_date,
            end_date=end_date,
        )
        (results["success"] if ok else results["failed"]).append(city)

    log.info("=" * 55)
    log.info("=== 14 Backfill Summary ===")
    log.info(f"Success: {results['success']}")
    log.info(f"Failed : {results['failed']}")
    log.info(f"Ready cities now: {csm.get_ready_cities()}")
    log.info("=" * 55)

    return len(results["failed"]) == 0


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="城市歷史回補管理器"
    )
    p.add_argument(
        "--cities", type=str, default="",
        help="只回補指定城市（逗號分隔）",
    )
    p.add_argument(
        "--retry-failed", action="store_true", dest="retry_failed",
        help="重跑所有 failed 城市",
    )
    p.add_argument(
        "--start-date", type=str, default="",
        dest="start_date",
        help="回補起始日 YYYY-MM-DD（傳給 05/06）",
    )
    p.add_argument(
        "--end-date", type=str, default="",
        dest="end_date",
        help="回補結束日 YYYY-MM-DD（傳給 05/06）",
    )
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    ok = run(
        cities_override=args.cities,
        retry_failed=args.retry_failed,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    sys.exit(0 if ok else 1)
