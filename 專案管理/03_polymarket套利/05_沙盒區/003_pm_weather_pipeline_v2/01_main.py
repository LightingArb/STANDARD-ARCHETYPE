"""
01_main.py — 唯一主程式入口（dispatcher）

重構狀態：STEP 8 完成（Collector / Signal 分離）
  - 03_market_catalog.py：market-level catalog
  - 04_market_master.py：market + station metadata 最終主檔
  - 05_D_forecast_fetch.py：D1 gfs_seamless forecast 抓取（previous_day 欄位）
  - 06_B_truth_fetch.py：WU daily high 真值抓取（experimental）
  - 07_daily_high_pipeline.py：forecast + truth → error table
  - 09_model_engine.py：誤差分布建模（Empirical / OU / QR）
  - 10_event_probability.py：事件機率計算（ECDF）
  - 11_ev_engine.py：EV 與交易信號計算
  - 12_city_scanner.py：Gamma API 城市掃描 → city_status.json
  - 13_city_status_manager.py：城市狀態機（含 bootstrap()）
  - 14_backfill_manager.py：歷史回補管理器
  - collector_main.py：長期/批次收集常駐程序
  - signal_main.py：即時信號常駐程序

⚠️ D1-only MVP baseline，模型基於少量樣本，僅供管線驗證，不可用於交易決策。

流程（live mode，預設）：
  02_init → 03_market_catalog → 04_market_master
          → 05_D_forecast_fetch → 06_B_truth_fetch → 07_daily_high_pipeline
          → 09_model_engine → 10_event_probability → [11_ev_engine]

  city_status.json 存在時：
    - live mode 只處理 ready 城市
    - --cities 是過濾器（從 ready 城市中篩選）

流程（historical mode）：
  02_init → 05_D_forecast_fetch --mode historical
          → 06_B_truth_fetch --mode historical → 07_daily_high_pipeline
          → 09_model_engine → 10_event_probability → [11_ev_engine]

  historical mode 的 --cities 不受 city_status 限制（用於回補任意城市）

用法：
  python 01_main.py
  python 01_main.py --cities "London,Paris" --start-date 2026-01-01 --end-date 2026-04-07
  python 01_main.py --mode historical --cities "London,Paris" --start-date 2026-03-28 --end-date 2026-04-06
  python 01_main.py --mode historical --cities "London,Paris" --skip-ev
"""

import argparse
import importlib.util
import json
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJ_DIR = Path(__file__).resolve().parent

CITY_STATUS_PATH = PROJ_DIR / "data" / "city_status.json"

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


# ============================================================
# 工具函數
# ============================================================

def run_script(script_name: str, args: list[str] = None, label: str = "") -> bool:
    """執行子腳本，回傳是否成功"""
    script_path = PROJ_DIR / script_name
    if not script_path.exists():
        log.error(f"Script not found: {script_path}")
        return False

    cmd = [sys.executable, str(script_path)] + (args or [])
    display_label = label or script_name

    log.info(f"{'='*50}")
    log.info(f"Running: {display_label}")
    log.info(f"  cmd: {' '.join(cmd)}")
    log.info(f"{'='*50}")

    result = None
    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJ_DIR),
            capture_output=False,
            text=True,
            timeout=1800,
        )
        if result.returncode != 0:
            log.error(f"{display_label} exited with code {result.returncode}")
            return False
        log.info(f"{display_label}: OK")
        return True
    except subprocess.TimeoutExpired:
        log.error(f"{display_label} timed out after 1800s")
        return False
    except Exception as e:
        log.error(f"{display_label} failed: {e}")
        return False


def parse_csv_arg(raw: str | None) -> list[str]:
    """把逗號分隔 CLI 參數轉成 list[str]。"""
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def date_to_month(date_str: str) -> str:
    """從日期字串取得月份：'2024-01-17' → '2024-01'"""
    return date_str[:7]


# ============================================================
# city_status.json helpers
# ============================================================

def _load_csm():
    """Lazy-load CityStatusManager from 13_city_status_manager.py."""
    spec = importlib.util.spec_from_file_location(
        "city_status_manager", PROJ_DIR / "13_city_status_manager.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.CityStatusManager()


def resolve_live_cities(cities_arg: str) -> str:
    """
    live mode 城市解析：
      - city_status.json 不存在 → 直接回傳 cities_arg（舊行為）
      - city_status.json 存在 → 從 ready 城市中篩選（--cities 是 filter）
    """
    if not CITY_STATUS_PATH.exists():
        log.info("city_status.json not found — using --cities directly (legacy mode)")
        return cities_arg

    try:
        csm = _load_csm()
        csm.bootstrap()
        ready = csm.get_ready_cities()
    except Exception as e:
        log.warning(f"city_status.json load failed: {e} — falling back to --cities")
        return cities_arg

    if not ready:
        log.error("No ready cities found. Live mode requires at least one ready city. Exiting.")
        sys.exit(1)

    if cities_arg:
        requested = {c.strip() for c in cities_arg.split(",") if c.strip()}
        selected = sorted(c for c in ready if c in requested)
        if not selected:
            log.warning(
                f"None of the requested cities {sorted(requested)} are ready. "
                f"Ready cities: {ready}"
            )
            return cities_arg  # fall back rather than running nothing
    else:
        selected = ready

    log.info(f"city_status.json: ready={ready}, selected={selected}")
    return ",".join(selected)


# ============================================================
# 主管線
# ============================================================

def run_pipeline(cities: str, start_date: str, end_date: str, mode: str, skip_ev: bool) -> dict:
    """
    Dispatcher（STEP 7）：
      live mode:       02_init → 03 → 04 → 05 → 06 → 07 → 09 → 10 → [11]
      historical mode: 02_init → 05 --mode historical → 06 --mode historical → 07 → 09 → 10 → [11]

    city_status.json（存在時）：
      live mode：只跑 ready 城市；--cities 是 filter
      historical mode：--cities 不受 city_status 限制
    """
    status = {
        "start_time": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "steps": {},
    }

    # ── Resolve effective cities ──
    if mode == "live":
        effective_cities = resolve_live_cities(cities)
        status["effective_cities"] = effective_cities
        log.info(f"Live mode effective cities: {effective_cities!r}")
    else:
        effective_cities = cities
        log.info(f"Historical mode cities (no city_status filter): {effective_cities!r}")

    # ── Step 1: 初始化（兩種 mode 都跑）──
    ok = run_script("02_init.py", label="02_init")
    status["steps"]["02_init"] = ok
    if not ok:
        status["error"] = "02_init failed"
        return status

    if mode == "live":
        # ── Step 2: Market catalog（03）──
        ok = run_script(
            "03_market_catalog.py",
            args=["--cities", effective_cities],
            label="03_market_catalog",
        )
        status["steps"]["03_market_catalog"] = ok
        if not ok:
            status["error"] = "03_market_catalog failed"
            return status

        # ── Step 3: Market master（04）──
        ok = run_script("04_market_master.py", label="04_market_master")
        status["steps"]["04_market_master"] = ok
        if not ok:
            status["error"] = "04_market_master failed"
            return status

        # ── 驗證主檔產出 ──
        master_csv = PROJ_DIR / "data" / "market_master.csv"
        if not master_csv.exists() or master_csv.stat().st_size == 0:
            log.error("data/market_master.csv not produced or empty!")
            status["error"] = "market_master.csv missing or empty"
            return status

    # ── Step 4: D1 Forecast Fetch ──
    forecast_args = ["--cities", effective_cities, "--mode", mode]
    if start_date:
        forecast_args += ["--start-date", start_date]
    if end_date:
        forecast_args += ["--end-date", end_date]
    ok = run_script("05_D_forecast_fetch.py", args=forecast_args, label="05_D_forecast_fetch")
    status["steps"]["05_D_forecast"] = ok
    if not ok:
        status["error"] = "05_D_forecast_fetch failed"
        return status

    # ── Step 5: B Truth Fetch (experimental: WU hidden API) ──
    truth_args = ["--cities", effective_cities, "--mode", mode]
    if start_date:
        truth_args += ["--start-date", start_date]
    if end_date:
        truth_args += ["--end-date", end_date]
    ok = run_script("06_B_truth_fetch.py", args=truth_args, label="06_B_truth_fetch")
    status["steps"]["06_B_truth"] = ok
    if not ok:
        status["error"] = "06_B_truth_fetch failed"
        return status

    # ── Step 6: Daily High Pipeline ──
    pipeline_args = ["--cities", effective_cities]
    if start_date:
        pipeline_args += ["--start-date", start_date]
    if end_date:
        pipeline_args += ["--end-date", end_date]
    ok = run_script("07_daily_high_pipeline.py", args=pipeline_args, label="07_daily_high_pipeline")
    status["steps"]["07_pipeline"] = ok
    if not ok:
        status["error"] = "07_daily_high_pipeline failed"
        return status

    # ── Step 7: Model Engine (09) ──
    model_args = ["--cities", effective_cities]
    ok = run_script("09_model_engine.py", args=model_args, label="09_model_engine")
    status["steps"]["09_model_engine"] = ok
    if not ok:
        status["error"] = "09_model_engine failed"
        return status

    # ── Step 8: Event Probability (10) ──
    prob_args = ["--cities", effective_cities]
    ok = run_script("10_event_probability.py", args=prob_args, label="10_event_probability")
    status["steps"]["10_event_probability"] = ok
    if not ok:
        status["error"] = "10_event_probability failed"
        return status

    # ── Step 8.5: Market Price Fetch (08) — non-blocking ──
    price_args = ["--cities", effective_cities]
    ok = run_script("08_market_price_fetch.py", args=price_args, label="08_market_price_fetch")
    status["steps"]["08_market_price_fetch"] = ok
    if not ok:
        log.warning("08_market_price_fetch failed (non-blocking — 11 will use naive_fair only)")

    # ── Step 9: EV Engine (11) — non-blocking, requires price input ──
    if not skip_ev:
        ev_args = ["--cities", effective_cities]
        ok = run_script("11_ev_engine.py", args=ev_args, label="11_ev_engine")
        status["steps"]["11_ev_engine"] = ok
        if not ok:
            log.warning("11_ev_engine failed (non-blocking — no price input?)")

    status["end_time"] = datetime.now(timezone.utc).isoformat()
    status["success"] = True
    return status


# ============================================================
# 主入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="PM Weather Pipeline（market-centered）")
    parser.add_argument(
        "--cities", type=str, default="London,Paris",
        help="只處理指定城市（逗號分隔，預設: London,Paris）",
    )
    parser.add_argument(
        "--start-date", type=str, default="",
        help="market_date 起始日 YYYY-MM-DD（傳給 05/06/07）",
    )
    parser.add_argument(
        "--end-date", type=str, default="",
        help="market_date 結束日 YYYY-MM-DD（傳給 05/06/07）",
    )
    parser.add_argument(
        "--mode", type=str, default="live", choices=["live", "historical"],
        help="live=從 market_master 讀日期；historical=直接用 start/end date + seed_cities（跳過 03/04）",
    )
    parser.add_argument(
        "--skip-ev", action="store_true", dest="skip_ev",
        help="跳過 11_ev_engine（沒有市場價格時使用）",
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("PM WEATHER PIPELINE")
    log.info(f"Time: {datetime.now(timezone.utc).isoformat()}")
    log.info(f"Mode: {args.mode}")
    log.info(f"Cities: {args.cities}")
    if args.start_date:
        log.info(f"Start date: {args.start_date}")
    if args.end_date:
        log.info(f"End date: {args.end_date}")
    log.info("=" * 60)

    status = run_pipeline(
        cities=args.cities,
        start_date=args.start_date,
        end_date=args.end_date,
        mode=args.mode,
        skip_ev=args.skip_ev,
    )

    # 寫 status JSON
    status_path = PROJ_DIR / "logs" / "01_main" / "run_status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    with open(status_path, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2, ensure_ascii=False)

    if status.get("success"):
        log.info(f"Pipeline completed successfully. Status: {status_path}")
    else:
        log.error(f"Pipeline failed: {status.get('error', 'unknown')}")
        log.error(f"Status: {status_path}")
        sys.exit(1)


if __name__ == "__main__":
    main()
