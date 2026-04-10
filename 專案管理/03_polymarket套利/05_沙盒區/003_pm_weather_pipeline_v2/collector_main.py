"""
collector_main.py — 長期/批次資料收集常駐程序

職責：
  - 每 24h：12_city_scanner（城市掃描）
  - 即時觸發：14_backfill_manager（discovered 城市回補）
  - 每 6h：05→07→09→10（forecast 更新 + 重算）
  - 每 24h：06→07→09→10（truth 更新 + 重算）

不碰即時價格（08）、不碰 alert（15）。
這些是 signal_main.py 的職責。

排程策略：用「下一個目標時間點」對齊，不用固定 sleep，避免漂移。

CLI：
  python collector_main.py            # 常駐
  python collector_main.py --once     # 跑一次（測試）
  python collector_main.py --verbose  # 詳細 log
"""

import argparse
import importlib.util
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


# ============================================================
# System health（共寫 data/_system_health.json）
# ============================================================

def _update_system_health(component: str, data: dict) -> None:
    """原子讀寫 data/_system_health.json。各進程只更新自己的 key。"""
    path = PROJ_DIR / "data" / "_system_health.json"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        existing: dict = {}
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                pass
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        existing[component] = {**data, "updated_at_utc": now}
        existing["updated_at_utc"] = now
        tmp = str(path) + f".{os.getpid()}.tmp"
        Path(tmp).write_text(json.dumps(existing, indent=2), encoding="utf-8")
        os.replace(tmp, str(path))
    except Exception as e:
        log.warning(f"_update_system_health({component}) failed: {e}")


# ============================================================
# ErrorReporter（連續失敗推 admin，帶冷卻）
# ============================================================

class ErrorReporter:
    """推送錯誤通知到 Telegram admin（10 分鐘冷卻）。"""
    COOLDOWN_MINUTES = 10

    def __init__(self, telegram_sender):
        self.telegram = telegram_sender
        self._cooldown: dict[str, datetime] = {}

    def report(self, source: str, error_msg: str, error_type: str = "generic") -> None:
        if not self.telegram:
            return
        if self._in_cooldown(error_type):
            return
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        text = (
            "⚠️ <b>系統錯誤</b>\n\n"
            f"來源：{source}\n"
            f"錯誤：{error_msg[:2000]}\n"
            f"時間：{now}"
        )
        ok, err = self.telegram.send_message(text)
        if not ok:
            log.warning(f"ErrorReporter: send failed: {err}")
        self._cooldown[error_type] = datetime.now(timezone.utc)

    def _in_cooldown(self, error_type: str) -> bool:
        last = self._cooldown.get(error_type)
        if last is None:
            return False
        return (datetime.now(timezone.utc) - last).total_seconds() < self.COOLDOWN_MINUTES * 60


def _load_alert_sender():
    """嘗試載入 15_alert_engine 的 TelegramSender（用於錯誤推送）。"""
    try:
        import importlib.util as ilu
        spec = ilu.spec_from_file_location("alert_engine_15", PROJ_DIR / "15_alert_engine.py")
        mod = ilu.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.load_telegram_config()
    except Exception as e:
        log.warning(f"_load_alert_sender: {e}")
        return None


# ============================================================
# 工具函數
# ============================================================

# 各步驟 timeout（秒）：truth/forecast fetch 最久
STEP_TIMEOUT: dict[str, int] = {
    "05_D_forecast_fetch.py": 1800,
    "06_B_truth_fetch.py":    3600,
    "07_daily_high_pipeline.py": 600,
    "09_model_engine.py":        600,
    "10_event_probability.py":   600,
    "12_city_scanner.py":        300,
    "14_backfill_manager.py":    7200,
}


def run_script(script_name: str, args: list[str] = None, label: str = "") -> bool:
    """執行子腳本，回傳是否成功。"""
    script_path = PROJ_DIR / script_name
    if not script_path.exists():
        log.error(f"Script not found: {script_path}")
        return False
    cmd = [sys.executable, str(script_path)] + (args or [])
    display_label = label or script_name
    timeout = STEP_TIMEOUT.get(script_name, 600)
    log.info(f"  Running: {display_label}")
    try:
        result = subprocess.run(cmd, cwd=str(PROJ_DIR), capture_output=False, text=True, timeout=timeout)
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


def _load_csm():
    """Lazy-load CityStatusManager from 13_city_status_manager.py."""
    spec = importlib.util.spec_from_file_location(
        "city_status_manager", PROJ_DIR / "13_city_status_manager.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.CityStatusManager()


# ============================================================
# 排程器（對齊 UTC 時鐘，避免漂移）
# ============================================================

def _secs_until_utc_hour(target_hour: int) -> float:
    """
    計算距離下一個 target_hour:00:00 UTC 的秒數。
    已過或距離 < 60s → 排到明天同時間。
    """
    from datetime import timedelta as _td
    now_utc = datetime.now(timezone.utc)
    today_target = now_utc.replace(
        hour=target_hour, minute=0, second=0, microsecond=0
    )
    if (now_utc - today_target).total_seconds() >= -60:
        today_target += _td(days=1)
    return max(1.0, (today_target - now_utc).total_seconds())


class CollectorScheduler:
    """
    排程對齊 UTC 時鐘：
      - 城市掃描：每天 06:00 UTC
      - Truth 更新：每天 00:00 UTC
      - Forecast 更新：每 6 小時（相對間隔）
    首次執行立刻觸發；後續依目標 UTC 時間對齊，不因任務拉長而漂移。
    """

    SCAN_HOUR_UTC = 6        # 城市掃描時間（UTC hour）
    TRUTH_HOUR_UTC = 0       # Truth 更新時間（UTC hour）
    FORECAST_INTERVAL_H = 6  # Forecast 每 6 小時（相對間隔）

    def __init__(self):
        now = time.monotonic()
        # 首次執行：立刻觸發所有任務
        self._next_scan = now
        self._next_forecast = now
        self._next_truth = now

    def should_scan(self) -> bool:
        return time.monotonic() >= self._next_scan

    def should_update_forecast(self) -> bool:
        return time.monotonic() >= self._next_forecast

    def should_update_truth(self) -> bool:
        return time.monotonic() >= self._next_truth

    def mark_scanned(self) -> None:
        secs = _secs_until_utc_hour(self.SCAN_HOUR_UTC)
        self._next_scan = time.monotonic() + secs
        log.info(f"Next city scan in {secs/3600:.1f}h (next {self.SCAN_HOUR_UTC:02d}:00 UTC)")

    def mark_forecast_updated(self) -> None:
        secs = self.FORECAST_INTERVAL_H * 3600
        self._next_forecast = time.monotonic() + secs
        log.info(f"Next forecast update in {self.FORECAST_INTERVAL_H}h")

    def mark_truth_updated(self) -> None:
        secs = _secs_until_utc_hour(self.TRUTH_HOUR_UTC)
        self._next_truth = time.monotonic() + secs
        log.info(f"Next truth update in {secs/3600:.1f}h (next {self.TRUTH_HOUR_UTC:02d}:00 UTC)")


# ============================================================
# 任務函數
# ============================================================

def task_city_scan() -> bool:
    """12_city_scanner：掃描 Polymarket 城市，更新 city_status.json。"""
    log.info("=== [TASK] City Scan (12) ===")
    return run_script("12_city_scanner.py", label="12_city_scanner")


def task_backfill(cities: list[str], csm) -> None:
    """14_backfill_manager：回補 discovered 城市，每個城市失敗不阻塞其他城市。"""
    if not cities:
        return
    log.info(f"=== [TASK] Backfill {cities} (14) ===")
    for city in cities:
        ok = run_script(
            "14_backfill_manager.py",
            args=["--cities", city],
            label=f"14_backfill({city})",
        )
        if ok:
            log.info(f"  Backfill OK: {city}")
        else:
            log.warning(f"  Backfill FAILED: {city} (non-blocking)")


def task_update_forecast(city: str, csm) -> bool:
    """05（forecast）→ 07（pipeline）→ 09（model）→ 10（probability）for one city."""
    log.info(f"  [forecast] {city}")
    ok = run_script("05_D_forecast_fetch.py", args=["--cities", city, "--mode", "live"], label=f"05({city})")
    if not ok:
        return False
    ok = run_script("07_daily_high_pipeline.py", args=["--cities", city], label=f"07({city})")
    if not ok:
        return False
    ok = run_script("09_model_engine.py", args=["--cities", city], label=f"09({city})")
    if not ok:
        return False
    ok = run_script("10_event_probability.py", args=["--cities", city], label=f"10({city})")
    if ok:
        # Build metadata 只在檔案成功落盤後才更新（10 以原子寫入完成後才 exit 0）
        try:
            csm.update_build_time(city, "probability")
            csm.update_build_time(city, "model")
        except Exception as e:
            log.warning(f"  update_build_time failed for {city}: {e}")
    return ok


def task_update_truth(city: str, csm) -> bool:
    """06（truth）→ 07（pipeline）→ 09（model）→ 10（probability）for one city."""
    log.info(f"  [truth] {city}")
    ok = run_script("06_B_truth_fetch.py", args=["--cities", city, "--mode", "live"], label=f"06({city})")
    if not ok:
        return False
    ok = run_script("07_daily_high_pipeline.py", args=["--cities", city], label=f"07({city})")
    if not ok:
        return False
    ok = run_script("09_model_engine.py", args=["--cities", city], label=f"09({city})")
    if not ok:
        return False
    ok = run_script("10_event_probability.py", args=["--cities", city], label=f"10({city})")
    if ok:
        # Build metadata 只在檔案成功落盤後才更新（10 以原子寫入完成後才 exit 0）
        try:
            csm.update_build_time(city, "probability")
            csm.update_build_time(city, "model")
        except Exception as e:
            log.warning(f"  update_build_time failed for {city}: {e}")
    return ok


# ============================================================
# 主循環
# ============================================================

def run_collector(once: bool = False, verbose: bool = False) -> None:
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=" * 60)
    log.info("COLLECTOR MAIN")
    log.info(f"Time: {datetime.now(timezone.utc).isoformat()}")
    log.info(f"Mode: {'once' if once else 'daemon'}")
    log.info("=" * 60)

    # 錯誤推送
    telegram_sender = _load_alert_sender()
    error_reporter = ErrorReporter(telegram_sender)

    # Bootstrap：首次使用時自動偵測既有城市（London/Paris 等）
    csm = _load_csm()
    auto_inited = csm.bootstrap()
    if auto_inited:
        log.info(f"bootstrap: auto-inited {auto_inited}")

    scheduler = CollectorScheduler()
    _update_system_health("collector_main", {
        "status": "running",
        "pid": os.getpid(),
        "last_error": None,
        "last_success_utc": None,
        "last_scan_utc": None,
        "last_forecast_utc": None,
        "last_truth_utc": None,
    })

    while True:
        cycle_start = time.monotonic()
        log.info(f"--- Collector cycle @ {datetime.now(timezone.utc).strftime('%H:%M:%S')} ---")

        # 1. 城市掃描（每 24h）
        if scheduler.should_scan():
            ok = task_city_scan()
            if ok:
                scheduler.mark_scanned()
                # Reload csm after scan（city_status.json 可能已更新）
                csm = _load_csm()
                # 自動重置 failed → discovered，讓 backfill 下一輪重試
                reset = csm.reset_failed_cities()
                if reset:
                    log.info(f"Auto-reset failed cities to discovered: {reset}")
                _update_system_health("collector_main", {
                    "status": "running",
                    "pid": os.getpid(),
                    "last_scan_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "last_error": None,
                })
            else:
                log.warning("City scan failed — will retry next cycle")
                error_reporter.report("collector_main", "12_city_scanner 失敗", "collector_scan_fail")

        # 2. 回補 discovered 城市（即時觸發）
        discovered = csm.get_cities_by_status("discovered")
        if discovered:
            log.info(f"Discovered cities to backfill: {discovered}")
            task_backfill(discovered, csm)
            csm = _load_csm()  # reload after backfill

        # 3. Forecast 更新（每 6h）
        if scheduler.should_update_forecast():
            ready = csm.get_ready_cities()
            forecast_failed_cities = []
            if ready:
                log.info(f"=== [TASK] Forecast update for {ready} ===")
                for city in ready:
                    ok = task_update_forecast(city, csm)
                    if not ok:
                        log.warning(f"  Forecast update failed: {city} (non-blocking)")
                        error_reporter.report(
                            "collector_main",
                            f"Forecast update 失敗：{city}",
                            f"collector_forecast_fail_{city}",
                        )
                        forecast_failed_cities.append(city)
            else:
                log.info("No ready cities for forecast update")
            scheduler.mark_forecast_updated()
            _update_system_health("collector_main", {
                "status": "running",
                "pid": os.getpid(),
                "last_forecast_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "last_error": f"forecast failed: {forecast_failed_cities}" if forecast_failed_cities else None,
            })

        # 4. Truth 更新（每 24h）
        if scheduler.should_update_truth():
            ready = csm.get_ready_cities()
            truth_failed_cities = []
            if ready:
                log.info(f"=== [TASK] Truth update for {ready} ===")
                for city in ready:
                    ok = task_update_truth(city, csm)
                    if not ok:
                        log.warning(f"  Truth update failed: {city} (non-blocking)")
                        error_reporter.report(
                            "collector_main",
                            f"Truth update 失敗：{city}",
                            f"collector_truth_fail_{city}",
                        )
                        truth_failed_cities.append(city)
            else:
                log.info("No ready cities for truth update")
            scheduler.mark_truth_updated()
            # Truth 更新完成後，重建峰值時段
            try:
                log.info("Rebuilding peak hours after truth update...")
                ph_result = subprocess.run(
                    [sys.executable, "tools/build_peak_hours.py"],
                    cwd=str(PROJ_DIR),
                    capture_output=True, text=True, timeout=120,
                )
                if ph_result.returncode == 0:
                    log.info("Peak hours rebuilt OK")
                else:
                    log.warning(f"Peak hours build failed: {ph_result.stderr[:200]}")
            except Exception as e:
                log.warning(f"Peak hours build error: {e}")
            _update_system_health("collector_main", {
                "status": "running",
                "pid": os.getpid(),
                "last_truth_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "last_error": f"truth failed: {truth_failed_cities}" if truth_failed_cities else None,
            })

        if once:
            log.info("Collector: --once mode, exiting after one cycle.")
            break

        # 每 60 秒檢查一次排程
        elapsed = time.monotonic() - cycle_start
        sleep_time = max(1.0, 60.0 - elapsed)
        log.debug(f"Collector sleeping {sleep_time:.0f}s")
        time.sleep(sleep_time)

    log.info("Collector done.")


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="長期/批次資料收集常駐程序"
    )
    p.add_argument("--once", action="store_true", help="跑一次後退出（測試用）")
    p.add_argument("--verbose", action="store_true", help="詳細 log")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    run_collector(once=args.once, verbose=args.verbose)
