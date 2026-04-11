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
import threading
import time
from datetime import datetime, timedelta, timezone
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
    "05_G_peak_fetch.py":      300,
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

def _next_aligned_utc_hour_ts(target_hour: int) -> float:
    """
    距離下一個 target_hour:00:00 UTC 的絕對 epoch 秒（P2-8：改用絕對 UTC，非 monotonic）。
    已過或距離 < 60s → 排到明天同時間。
    """
    now = datetime.now(timezone.utc)
    today_target = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
    if now >= today_target - timedelta(seconds=60):
        today_target += timedelta(days=1)
    return today_target.timestamp()


def _now_ts() -> float:
    """Current UTC epoch seconds."""
    return datetime.now(timezone.utc).timestamp()


class CollectorScheduler:
    """
    排程對齊 UTC 時鐘：
      - 城市掃描：每天 06:00 UTC
      - Truth 更新：每天 00:00 UTC
      - Forecast 更新：每 6 小時（相對間隔）
      - Obs：每 10 分鐘

    **P2-8**：`_next_*` 儲存為 UTC epoch 秒並持久化到 data/_collector_scheduler.json，
    重啟不會所有任務立即觸發（除非檔案不存在或已到期）。原本用 time.monotonic() 重啟後歸零。
    """

    STATE_PATH = PROJ_DIR / "data" / "_collector_scheduler.json"

    SCAN_HOUR_UTC = 6        # 城市掃描時間（UTC hour）
    TRUTH_HOUR_UTC = 0       # Truth 更新時間（UTC hour）
    FORECAST_INTERVAL_S = 6 * 3600   # Forecast 每 6 小時
    OBS_INTERVAL_S = 600             # 即時觀測每 10 分鐘

    def __init__(self):
        # 預設為 0 → 第一次會立刻觸發；_load_state 會覆寫為持久化值
        self._next_scan_ts: float = 0.0
        self._next_forecast_ts: float = 0.0
        self._next_truth_ts: float = 0.0
        self._next_obs_ts: float = 0.0
        self._load_state()

    # ── 排程判斷 ──────────────────────────────────────────────
    def should_scan(self) -> bool:
        return _now_ts() >= self._next_scan_ts

    def should_update_forecast(self) -> bool:
        return _now_ts() >= self._next_forecast_ts

    def should_update_truth(self) -> bool:
        return _now_ts() >= self._next_truth_ts

    def should_update_obs(self) -> bool:
        return _now_ts() >= self._next_obs_ts

    # ── 排程更新（每次都落盤）──────────────────────────────────
    def mark_scanned(self) -> None:
        self._next_scan_ts = _next_aligned_utc_hour_ts(self.SCAN_HOUR_UTC)
        secs = self._next_scan_ts - _now_ts()
        log.info(f"Next city scan in {secs/3600:.1f}h (next {self.SCAN_HOUR_UTC:02d}:00 UTC)")
        self._save_state()

    def mark_forecast_updated(self) -> None:
        self._next_forecast_ts = _now_ts() + self.FORECAST_INTERVAL_S
        log.info(f"Next forecast update in {self.FORECAST_INTERVAL_S / 3600:.0f}h")
        self._save_state()

    def mark_truth_updated(self) -> None:
        self._next_truth_ts = _next_aligned_utc_hour_ts(self.TRUTH_HOUR_UTC)
        secs = self._next_truth_ts - _now_ts()
        log.info(f"Next truth update in {secs/3600:.1f}h (next {self.TRUTH_HOUR_UTC:02d}:00 UTC)")
        self._save_state()

    def mark_obs_updated(self) -> None:
        self._next_obs_ts = _now_ts() + self.OBS_INTERVAL_S
        self._save_state()

    # ── 持久化（P2-8）──────────────────────────────────────────
    def _save_state(self) -> None:
        try:
            self.STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "next_scan_ts": self._next_scan_ts,
                "next_forecast_ts": self._next_forecast_ts,
                "next_truth_ts": self._next_truth_ts,
                "next_obs_ts": self._next_obs_ts,
                "saved_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            tmp = self.STATE_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            os.replace(str(tmp), str(self.STATE_PATH))
        except Exception as e:
            log.warning(f"CollectorScheduler._save_state: {e}")

    def _load_state(self) -> None:
        """Load persisted _next_* from disk. Missing/corrupt → fire immediately (0.0)."""
        if not self.STATE_PATH.exists():
            return
        try:
            data = json.loads(self.STATE_PATH.read_text(encoding="utf-8"))
            self._next_scan_ts = float(data.get("next_scan_ts", 0) or 0)
            self._next_forecast_ts = float(data.get("next_forecast_ts", 0) or 0)
            self._next_truth_ts = float(data.get("next_truth_ts", 0) or 0)
            self._next_obs_ts = float(data.get("next_obs_ts", 0) or 0)
            log.info(
                f"CollectorScheduler: resumed state "
                f"(scan in {(self._next_scan_ts - _now_ts())/3600:.1f}h, "
                f"forecast in {(self._next_forecast_ts - _now_ts())/3600:.1f}h, "
                f"obs in {(self._next_obs_ts - _now_ts()):.0f}s)"
            )
        except Exception as e:
            log.warning(f"CollectorScheduler._load_state: {e} — starting fresh")


# ============================================================
# 任務函數
# ============================================================

import csv as _csv_mod

_OBS_DIR = PROJ_DIR / "data" / "observations"
_LATEST_OBS_PATH = _OBS_DIR / "latest_obs.json"

# P1-5：長壽 CurrentObsFetcher（整個進程共用，cache TTL 才有意義）
_OBS_FETCHER = None  # type: ignore[assignment]


def _get_obs_fetcher():
    """Lazy singleton for CurrentObsFetcher (P1-5)."""
    global _OBS_FETCHER
    if _OBS_FETCHER is not None:
        return _OBS_FETCHER
    try:
        from _lib.current_obs_fetcher import CurrentObsFetcher, load_wu_api_key
    except Exception as e:
        log.warning(f"obs fetch: load fetcher module failed: {e}")
        return None
    api_key = load_wu_api_key()
    if not api_key:
        log.warning("obs fetch: no WU API key, skip")
        return None
    _OBS_FETCHER = CurrentObsFetcher(api_key)
    return _OBS_FETCHER


def _write_latest_json(
    new_results: dict,
    fetched_at_utc: str,
    failed_cities: Optional[list] = None,
) -> None:
    """
    merge 式寫入 latest_obs.json（P1-2 + P1-3 修正）：

    - 成功城市：覆蓋舊值，status=ok，清掉 stale_since_utc
    - 失敗城市：保留舊 high_c / current_temp_c，status=stale；
      若原本 status=ok 則記 stale_since_utc=fetched_at_utc
    - updated_at_utc 無條件更新（避免「全失敗就停留在舊時間」的盲點）
    - 原子寫入
    """
    old_cities: dict = {}
    if _LATEST_OBS_PATH.exists():
        try:
            raw = json.loads(_LATEST_OBS_PATH.read_text(encoding="utf-8"))
            old_cities = raw.get("cities", {}) if isinstance(raw, dict) else {}
            if not isinstance(old_cities, dict):
                old_cities = {}
        except Exception as e:
            log.warning(f"_write_latest_json: read existing failed ({e}), starting fresh")
            old_cities = {}

    # 成功城市：覆蓋
    for city, obs in new_results.items():
        old_cities[city] = obs

    # 失敗城市：保留舊值但標記 stale
    failed = failed_cities or []
    for city in failed:
        prev = old_cities.get(city)
        if not prev:
            continue  # 以前沒抓過就沒東西可保留
        prev = dict(prev)  # copy
        prev["last_attempt_utc"] = fetched_at_utc
        if prev.get("status") != "stale":
            # 首次從 ok → stale，記錄時間點
            prev["stale_since_utc"] = fetched_at_utc
        prev["status"] = "stale"
        old_cities[city] = prev

    output = {
        "schema_version": 2,
        "updated_at_utc": fetched_at_utc,
        "cities": old_cities,
    }
    _OBS_DIR.mkdir(parents=True, exist_ok=True)
    tmp = _LATEST_OBS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(str(tmp), str(_LATEST_OBS_PATH))


def _append_obs_csv(
    new_results: dict,
    failed_cities: Optional[list],
    fetched_at_utc: str,
) -> None:
    """按月分檔 append current_obs_YYYY-MM.csv（成功+失敗都記）。"""
    month_str = fetched_at_utc[:7]
    csv_path = _OBS_DIR / f"current_obs_{month_str}.csv"
    write_header = not csv_path.exists()
    fields = [
        "fetched_at_utc", "city", "station_code",
        "high_c", "current_temp_c", "obs_time_utc",
        "source", "status",
    ]
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = _csv_mod.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        for city, obs in sorted(new_results.items()):
            writer.writerow({
                "fetched_at_utc": fetched_at_utc,
                "city": city,
                "station_code": obs.get("station_code", ""),
                "high_c": obs.get("high_c", ""),
                "current_temp_c": obs.get("current_temp_c", ""),
                "obs_time_utc": obs.get("obs_time_utc", ""),
                "source": obs.get("source", ""),
                "status": obs.get("status", "ok"),
            })
        for city in sorted(failed_cities or []):
            writer.writerow({
                "fetched_at_utc": fetched_at_utc,
                "city": city,
                "station_code": "",
                "high_c": "",
                "current_temp_c": "",
                "obs_time_utc": "",
                "source": "",
                "status": "stale",
            })
        f.flush()


def _run_obs_fetch(ready_cities: list, seed_cities: dict) -> None:
    """
    每 10 分鐘：抓 WU 即時觀測，寫 latest_obs.json + 月 CSV。

    修正合集：
    - P1-2：無條件更新 updated_at_utc（即使全失敗）
    - P1-3：失敗城市保留舊值但 status=stale、帶 stale_since_utc
    - P1-4：current_temp_c 真的從 WU `temperature` 拿，不再複製 high_c；
            obs_time_utc 存成 int epoch（fetcher 已修）
    - P1-5：_get_obs_fetcher() 長壽 singleton，cache 跨輪生效
    - P2-1：並行抓取 + 300s 總預算
    """
    _OBS_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = _get_obs_fetcher()
    if fetcher is None:
        return

    now_utc = datetime.now(timezone.utc)
    fetched_at_utc = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    # 組合 cities_info（跳過沒 station_code 的）
    cities_info: list[dict] = []
    expected_cities: set = set()
    for city in ready_cities:
        station = seed_cities.get(city, {}).get("station_code", "")
        if not station:
            continue
        cities_info.append({"city": city, "station_code": station})
        expected_cities.add(city)

    if not cities_info:
        log.info("obs fetch: no ready cities with station_code — nothing to fetch")
        # 仍然更新 updated_at_utc（P1-2）
        _write_latest_json({}, fetched_at_utc, failed_cities=[])
        return

    # 並行抓取（P2-1）
    raw_obs = fetcher.get_all_parallel(
        cities_info,
        hours_to_settlement_map=None,  # 第一版不動態調 TTL
        max_workers=5,
        total_budget_s=300.0,
    )

    # 組裝 per-city schema（含 station_code + status=ok + schema_version）
    new_results: dict = {}
    for info in cities_info:
        city = info["city"]
        station = info["station_code"]
        obs = raw_obs.get(city)
        if not obs:
            continue
        new_results[city] = {
            "high_c": obs.get("high_c"),
            "current_temp_c": obs.get("current_temp_c"),  # WU `temperature`，可能為 None
            "obs_time_utc": obs.get("obs_time_utc"),      # ISO str or None
            "fetched_at_utc": fetched_at_utc,
            "last_success_utc": fetched_at_utc,
            "source": obs.get("source", "v3_current"),
            "station_code": station,
            "status": "ok",
            "stale_since_utc": None,
            "schema_version": 2,
        }

    failed_cities = sorted(expected_cities - set(new_results.keys()))

    # 無條件寫（P1-2）
    _write_latest_json(new_results, fetched_at_utc, failed_cities=failed_cities)
    _append_obs_csv(new_results, failed_cities, fetched_at_utc)

    log.info(
        f"obs fetch: {len(new_results)} ok, {len(failed_cities)} fail "
        f"(failed: {failed_cities if failed_cities else '-'})"
    )


def task_city_scan() -> bool:
    """12_city_scanner：掃描 Polymarket 城市，更新 city_status.json。"""
    log.info("=== [TASK] City Scan (12) ===")
    return run_script("12_city_scanner.py", label="12_city_scanner")


MAX_BACKFILL_PER_CYCLE = 1  # 每輪最多回補幾個城市（避免阻塞 obs/forecast/truth）


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
    """05（forecast）→ GFS峰值→ 07（pipeline）→ 09（model）→ 10（probability）for one city."""
    log.info(f"  [forecast] {city}")
    ok = run_script("05_D_forecast_fetch.py", args=["--cities", city, "--mode", "live"], label=f"05({city})")
    if not ok:
        return False

    # GFS 峰值更新（warning-only，失敗不阻塞後續步驟）
    try:
        import csv as _csv
        import json as _json
        from _lib.gfs_peak_hours import update_gfs_peak_hours as _upd_peak

        _seed_path = PROJ_DIR / "config" / "seed_cities.json"
        _city_tz = "UTC"
        if _seed_path.exists():
            _seed = _json.loads(_seed_path.read_text(encoding="utf-8"))
            _city_tz = _seed.get(city, {}).get("timezone", "UTC")

        _mm_path = PROJ_DIR / "data" / "market_master.csv"
        _market_dates: list[str] = []
        if _mm_path.exists():
            with open(_mm_path, "r", encoding="utf-8", newline="") as _f:
                for _row in _csv.DictReader(_f):
                    if (
                        _row.get("city") == city
                        and _row.get("market_enabled", "").lower() == "true"
                        and _row.get("parse_status", "") == "ok"
                        and _row.get("market_date_local")
                    ):
                        _market_dates.append(_row["market_date_local"])
        _market_dates = sorted(set(_market_dates))

        if _market_dates:
            _upd_peak(city, _city_tz, _market_dates)
        else:
            log.debug(f"  {city}: no active market_dates for GFS peak update")
    except Exception as _e:
        log.warning(f"  {city}: GFS peak hours update failed (non-blocking): {_e}")

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
# Obs 獨立線程
# ============================================================

def _obs_thread_loop(seed_cities: dict) -> None:
    """
    Daemon 線程：每 10 分鐘跑一次 obs fetch，與主迴圈完全平行。
    主迴圈跑 backfill（10-30 分鐘）時 obs 仍照常更新。
    """
    OBS_INTERVAL = 600  # 10 分鐘
    log.info("Obs thread: started (interval=10min)")
    while True:
        try:
            csm = _load_csm()
            ready = csm.get_ready_cities()
            _run_obs_fetch(ready, seed_cities)
        except Exception as e:
            log.warning(f"Obs thread error: {e}")
        time.sleep(OBS_INTERVAL)


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

    # 載入 seed_cities（obs fetch 需要 station_code）
    seed_cities: dict = {}
    seed_path = PROJ_DIR / "config" / "seed_cities.json"
    if seed_path.exists():
        try:
            seed_cities = json.loads(seed_path.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning(f"Failed to load seed_cities.json: {e}")

    # 確保 observations 目錄存在
    _OBS_DIR.mkdir(parents=True, exist_ok=True)

    # 啟動 obs daemon 線程（每 10 分鐘獨立更新，不受 backfill 阻塞）
    if not once:
        obs_thread = threading.Thread(
            target=_obs_thread_loop,
            args=(seed_cities,),
            daemon=True,
            name="obs-fetch",
        )
        obs_thread.start()
        log.info("Obs thread started (every 10 min, independent of main loop)")

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

        # 0. 重置卡住的 backfilling 城市（被 kill 打斷的，超過 4 小時自動 reset）
        stuck = csm.reset_stuck_backfilling(max_age_hours=4)
        if stuck:
            log.info(f"Auto-reset stuck backfilling cities: {stuck}")
            csm = _load_csm()

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
                # 掃描後也做 stuck backfilling reset（防止殘留）
                stuck = csm.reset_stuck_backfilling(max_age_hours=4)
                if stuck:
                    log.info(f"Auto-reset stuck backfilling (post-scan): {stuck}")
                _update_system_health("collector_main", {
                    "status": "running",
                    "pid": os.getpid(),
                    "last_scan_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "last_error": None,
                })
            else:
                log.warning("City scan failed — will retry next cycle")
                error_reporter.report("collector_main", "12_city_scanner 失敗", "collector_scan_fail")

        # 2. 回補 discovered 城市（每輪最多 MAX_BACKFILL_PER_CYCLE 個，避免阻塞排程）
        discovered = csm.get_cities_by_status("discovered")
        if discovered:
            batch = discovered[:MAX_BACKFILL_PER_CYCLE]
            remaining = len(discovered) - len(batch)
            if remaining:
                log.info(f"Backfill batch: {batch} (remaining queue: {remaining} cities)")
            else:
                log.info(f"Backfill batch: {batch}")
            task_backfill(batch, csm)
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
            # Truth 更新完成後，重建 remaining_gain 模型
            try:
                log.info("Rebuilding remaining gain models after truth update...")
                rg_result = subprocess.run(
                    [sys.executable, "tools/build_remaining_gain.py"],
                    cwd=str(PROJ_DIR),
                    capture_output=True, text=True, timeout=300,
                )
                if rg_result.returncode == 0:
                    log.info("Remaining gain models rebuilt OK")
                else:
                    log.warning(f"Remaining gain build failed: {rg_result.stderr[:200]}")
            except Exception as e:
                log.warning(f"Remaining gain build error: {e}")
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
    p.add_argument("--once-obs", action="store_true", help="只跑一次即時觀測後退出（測試用）")
    p.add_argument("--verbose", action="store_true", help="詳細 log")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    if args.once_obs:
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        seed_cities: dict = {}
        seed_path = PROJ_DIR / "config" / "seed_cities.json"
        if seed_path.exists():
            seed_cities = json.loads(seed_path.read_text(encoding="utf-8"))
        _OBS_DIR.mkdir(parents=True, exist_ok=True)
        csm = _load_csm()
        ready = csm.get_ready_cities()
        log.info(f"--once-obs: ready cities = {ready}")
        _run_obs_fetch(ready, seed_cities)
        sys.exit(0)
    run_collector(once=args.once, verbose=args.verbose)
