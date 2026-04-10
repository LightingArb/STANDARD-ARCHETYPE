"""
signal_main.py — 即時信號常駐程序

職責：只處理 ready 城市。
  1. 抓即時價格（08）
  2. 算 EV + 信號（11，in-process，回傳 in-memory 結果）
  3. 通報（15_alert_engine，STEP 10）

共享輸出契約（Signal Layer 只讀 finalized outputs）：
  ✅ 可讀：
    data/city_status.json                               （13 寫）
    data/market_master.csv                              （04 寫）
    data/results/probability/{city}/event_probability.csv（10 寫，原子寫入）
    data/raw/prices/book_state/{market_id}.json         （08 寫，原子寫入）
    data/raw/prices/market_prices.csv                   （08 寫，原子寫入）
    data/models/empirical/{city}/empirical_model.json   （09 寫，原子寫入）
    data/positions.json                                 （手動或 Bot）
  ❌ 不可直接碰：
    data/raw/D/、data/raw/B/（raw fetch，Collector 層職責）
    data/processed/（中間產物）
    暫存檔（*.tmp，原子寫入過程中的暫時檔案）
    12/13/14 內部邏輯（只透過 city_status.json 讀狀態）

城市 model 失敗降級規則：
  - 若某城市的 event_probability.csv 不存在或解析失敗 → 跳過該城市，log warning
  - 不整輪失敗（不影響其他城市）
  - signal_status → "model_stale"（由 11_ev_engine 內部處理）

不重疊：上一輪沒跑完，下一輪不啟動（interval - elapsed）。
退避：連續失敗 >= 3 次 → 最少 60 秒。

CLI：
  python signal_main.py                    # 常駐（預設 30 秒）
  python signal_main.py --once            # 跑一次
  python signal_main.py --interval 60     # 60 秒間隔
  python signal_main.py --verbose         # 詳細 log
"""

import argparse
import asyncio
import importlib.util
import json
import logging
import os
import signal as _signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

PROJ_DIR = Path(__file__).resolve().parent

# ── _lib（即時觀測 fetcher）──────────────────────────────────────
_lib_dir = PROJ_DIR / "_lib"
if str(_lib_dir) not in sys.path:
    sys.path.insert(0, str(_lib_dir))

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# 退避設定
BACKOFF_MIN_SLEEP = 60.0   # 連續失敗 >= BACKOFF_THRESHOLD 時最少等待秒數
BACKOFF_THRESHOLD = 3      # 觸發退避的連續失敗次數
WARN_DURATION_RATIO = 0.8  # cycle time / interval > 此比例時 log warning


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
# ErrorReporter（連續失敗/斷線推 admin，帶冷卻）
# ============================================================

class ErrorReporter:
    """推送錯誤通知到 Telegram admin（10 分鐘冷卻，同類錯誤不重複推）。"""
    COOLDOWN_MINUTES = 10

    def __init__(self, telegram_sender):
        self.telegram = telegram_sender
        self._cooldown: dict[str, datetime] = {}

    def report(self, source: str, error_msg: str, error_type: str = "generic") -> None:
        if not self.telegram:
            return
        if self._in_cooldown(error_type):
            log.debug(f"ErrorReporter: {error_type} in cooldown")
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


# ============================================================
# 模組快取（避免重複 exec_module）
# ============================================================

_csm_mod = None           # 13_city_status_manager
_ev_engine_mod = None     # 11_ev_engine
_alert_engine_mod = None  # 15_alert_engine
_bsm_mod = None           # 08c_book_state（WS 模式用）
_stream_mod = None        # 08b_price_stream（WS 模式用）
_pos_mgr_mod = None       # 16_position_manager（STEP 13）


# ============================================================
# 工具函數
# ============================================================

def run_script(script_name: str, args: list[str] = None, label: str = "") -> bool:
    """執行子腳本（subprocess），回傳是否成功。"""
    script_path = PROJ_DIR / script_name
    if not script_path.exists():
        log.error(f"Script not found: {script_path}")
        return False
    cmd = [sys.executable, str(script_path)] + (args or [])
    display_label = label or script_name
    log.info(f"  Running: {display_label}")
    try:
        result = subprocess.run(cmd, cwd=str(PROJ_DIR), capture_output=False, text=True, timeout=600)
        if result.returncode != 0:
            log.error(f"  {display_label} exited with code {result.returncode}")
            return False
        log.info(f"  {display_label}: OK")
        return True
    except subprocess.TimeoutExpired:
        log.error(f"  {display_label} timed out after 600s")
        return False
    except Exception as e:
        log.error(f"  {display_label} failed: {e}")
        return False


def _load_csm():
    """Lazy-load CityStatusManager from 13_city_status_manager.py（每輪重讀）。"""
    spec = importlib.util.spec_from_file_location(
        "city_status_manager", PROJ_DIR / "13_city_status_manager.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.CityStatusManager()


def _get_ev_engine_mod():
    """Lazy-load 11_ev_engine（module-level 只 exec 一次）。"""
    global _ev_engine_mod
    if _ev_engine_mod is None:
        spec = importlib.util.spec_from_file_location(
            "ev_engine_11", PROJ_DIR / "11_ev_engine.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _ev_engine_mod = mod
    return _ev_engine_mod


def _get_alert_engine_mod():
    """Lazy-load 15_alert_engine（module-level 只 exec 一次）。"""
    global _alert_engine_mod
    if _alert_engine_mod is None:
        spec = importlib.util.spec_from_file_location(
            "alert_engine_15", PROJ_DIR / "15_alert_engine.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _alert_engine_mod = mod
    return _alert_engine_mod


def _get_bsm_mod():
    """Lazy-load 08c_book_state（WS 模式用，module-level 只 exec 一次）。"""
    global _bsm_mod
    if _bsm_mod is None:
        spec = importlib.util.spec_from_file_location(
            "book_state_08c", PROJ_DIR / "08c_book_state.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _bsm_mod = mod
    return _bsm_mod


def _get_stream_mod():
    """Lazy-load 08b_price_stream（WS 模式用，module-level 只 exec 一次）。"""
    global _stream_mod
    if _stream_mod is None:
        spec = importlib.util.spec_from_file_location(
            "price_stream_08b", PROJ_DIR / "08b_price_stream.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _stream_mod = mod
    return _stream_mod


def _get_pos_mgr_mod():
    """Lazy-load 16_position_manager（STEP 13，module-level 只 exec 一次）。"""
    global _pos_mgr_mod
    if _pos_mgr_mod is None:
        spec = importlib.util.spec_from_file_location(
            "position_manager_16", PROJ_DIR / "16_position_manager.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _pos_mgr_mod = mod
    return _pos_mgr_mod


def _find_signal_for_position(pos: dict, ev_signals: list) -> Optional[dict]:
    """在 ev_signals 中找 market_id 匹配的 row。"""
    mid = pos.get("market_id", "")
    for sig in ev_signals:
        if sig.get("market_id", "") == mid:
            return sig
    return None


def _update_positions(
    position_mgr,
    alert_engine,
    ev_results: list,
) -> None:
    """
    更新 open positions 的 mark/edge/pnl（記憶體），並觸發 check_exits。
    由 run_signal 和 run_ws_mode 共用。
    """
    try:
        open_positions = position_mgr.get_open_positions()
        if not open_positions:
            return

        for pos in open_positions:
            current = _find_signal_for_position(pos, ev_results)
            if not current:
                continue

            side = pos.get("side", "")
            if side == "NO":
                mark = current.get("no_best_bid")
                edge = current.get("no_edge")
                ev = current.get("no_ev")
            else:
                mark = current.get("yes_best_bid")
                edge = current.get("yes_edge")
                ev = current.get("yes_ev")

            if mark is not None:
                try:
                    position_mgr.update_mark(pos["position_id"], float(mark), edge, ev)
                except (ValueError, TypeError):
                    log.warning(f"  update_mark: invalid mark value {mark!r} for {pos.get('position_id')}")

            # 追蹤 edge 正負交叉（供 EXIT cooldown 重置）
            if alert_engine is not None and edge is not None:
                try:
                    if float(edge) > 0:
                        alert_engine._edge_crossed_positive[pos["position_id"]] = True
                except (TypeError, ValueError):
                    pass

        position_mgr.flush_edges(min_interval_seconds=30)

        # check exits
        if alert_engine is not None:
            try:
                exit_alerts, warnings = alert_engine.check_exits(open_positions, ev_results)
                if exit_alerts:
                    log.info(f"  PositionManager: {len(exit_alerts)} EXIT alert(s)")
                    alert_engine.process_exits(exit_alerts, warnings)
                elif warnings:
                    alert_engine.process_exits([], warnings)
            except Exception as e:
                log.warning(f"  check_exits error: {e}")

        # check edge shrinks
        if alert_engine is not None:
            try:
                shrink_alerts = alert_engine.check_shrinks(open_positions, ev_results)
                if shrink_alerts:
                    log.info(f"  PositionManager: {len(shrink_alerts)} SHRINK alert(s)")
                    alert_engine.process_shrinks(shrink_alerts)
            except Exception as e:
                log.warning(f"  check_shrinks error: {e}")

    except Exception as e:
        log.warning(f"  _update_positions error: {e}")


def _run_ev_engine(
    cities: list[str],
    verbose: bool = False,
    book_source: str = "json",
    books_in_memory: Optional[dict] = None,
    current_obs: Optional[dict] = None,
) -> tuple[bool, list[dict]]:
    """
    Run 11_ev_engine in-process and return (success, ev_results).
    ev_results 是 in-memory dict list，直接傳給 AlertEngine.evaluate()。

    book_source="json"（預設，REST 模式）：11 從磁碟讀 book_state JSON。
    book_source="memory"（WS 模式）：11 從 books_in_memory dict 讀（不讀磁碟）。
    current_obs: {city: {high_c, ...}}（即時觀測，None 表示不啟用裁剪）。
    """
    try:
        mod = _get_ev_engine_mod()
        ok, rows = mod.run(
            cities=",".join(cities),
            model_name="empirical",
            yes_price_global=None,
            no_price_global=None,
            prices_csv_path=None,
            min_edge_override=None,
            verbose=verbose,
            book_source=book_source,
            books_in_memory=books_in_memory,
            current_obs=current_obs,
        )
        log.info(f"  11_ev_engine: {'OK' if ok else 'FAIL'} ({len(rows)} rows in-memory)")
        return (ok, rows)
    except Exception as e:
        log.error(f"  11_ev_engine in-process error: {e}")
        return (False, [])


def _load_params() -> dict:
    """Load trading_params.yaml as a flat dict（簡單 key: value，不支援巢狀）。"""
    params_path = PROJ_DIR / "config" / "trading_params.yaml"
    if not params_path.exists():
        return {}
    params: dict = {}
    for line in params_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        k, _, v = line.partition(":")
        v = v.split("#")[0].strip()
        params[k.strip()] = v
    return params


def _setup_alert_engine():
    """
    初始化 AlertEngine（只在 signal_main 啟動時呼叫一次）。
    回傳 (AlertEngine 實例, TelegramSender 實例)，失敗時回傳 (None, None)。
    """
    try:
        mod = _get_alert_engine_mod()
        params = _load_params()
        telegram = mod.load_telegram_config()
        engine = mod.AlertEngine(params, telegram_sender=telegram)
        engine.load_cooldown_from_history()
        log.info(
            f"AlertEngine ready ("
            f"min_edge={engine.min_edge}, "
            f"cooldown={engine.cooldown_minutes}m, "
            f"telegram={'yes' if telegram else 'no'})"
        )
        return engine, telegram
    except Exception as e:
        log.warning(f"AlertEngine setup failed: {e} — running without alerts")
        return None, None


# ============================================================
# Refresh flag + Signal state（供 telegram_bot.py 讀取）
# ============================================================

def _check_refresh_requested() -> Optional[dict]:
    """
    檢查 data/_refresh_requested flag（JSON 或空檔）。
    存在 → 讀取 metadata → 刪除 → 回傳 metadata dict（可能為 {}）。
    不存在 → 回傳 None。
    """
    flag = PROJ_DIR / "data" / "_refresh_requested"
    if not flag.exists():
        return None
    meta: dict = {}
    try:
        content = flag.read_text(encoding="utf-8").strip()
        if content:
            meta = json.loads(content)
    except Exception:
        pass
    try:
        flag.unlink()
        log.info(
            f"Refresh flag detected — starting cycle early "
            f"(requested_by={meta.get('requested_by_chat_id', '?')})"
        )
    except Exception:
        pass
    return meta


def _sleep_with_refresh_check(
    total_sleep: float,
    poll_interval: float = 5.0,
    loop_state: Optional["SignalLoopState"] = None,
) -> None:
    """
    Sleep for total_sleep seconds, polling every poll_interval seconds for refresh flag.
    Returns early if refresh is requested; stores metadata in loop_state if provided.
    """
    remaining = total_sleep
    while remaining > 0:
        time.sleep(min(poll_interval, remaining))
        remaining -= poll_interval
        meta = _check_refresh_requested()
        if meta is not None:
            if loop_state is not None and loop_state._pending_refresh_by is None:
                loop_state._pending_refresh_by = meta.get("requested_by_chat_id", "unknown")
            return


def _write_signal_state(
    loop_state: "SignalLoopState",
    ready_count: int,
    price_mode: str = "rest",
    ws_connected: bool = False,
    last_ws_event_utc: Optional[str] = None,
    ws_fallback_active: bool = False,
    ws_fallback_reason: Optional[str] = None,
) -> None:
    """
    原子寫入 data/_signal_state.json（供 telegram_bot.py 讀取）。
    失敗時只 log warning，不影響主循環。
    """
    state_path = PROJ_DIR / "data" / "_signal_state.json"
    try:
        state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_success_utc": loop_state.last_success_utc,
            "last_cycle_duration_ms": loop_state.last_cycle_duration_ms,
            "ready_city_count": ready_count,
            "total_cycles": loop_state.total_cycles,
            "total_failures": loop_state.total_failures,
            "consecutive_failures": loop_state.consecutive_failures,
            "last_refresh_completed_utc": loop_state.last_refresh_completed_utc,
            "price_mode": price_mode,
            "ws_connected": ws_connected,
            "last_ws_event_utc": last_ws_event_utc,
            "ws_fallback_active": ws_fallback_active,
            "ws_fallback_reason": ws_fallback_reason,
        }
        with tempfile.NamedTemporaryFile(
            "w", dir=str(state_path.parent), suffix=".tmp", delete=False, encoding="utf-8"
        ) as f:
            json.dump(payload, f)
            tmp = f.name
        os.replace(tmp, str(state_path))
    except Exception as e:
        log.warning(f"_write_signal_state failed: {e}")


def _setup_shutdown_hooks(position_mgr, flush_fn: Optional[Callable] = None) -> None:
    """設定 SIGTERM/SIGINT 優雅退出：flush positions + 更新 health。"""
    def _handler(signum, frame):
        log.info(f"Received signal {signum} — flushing before exit")
        if position_mgr is not None:
            try:
                position_mgr.flush_edges(min_interval_seconds=0)
                log.info("Positions flushed.")
            except Exception as e:
                log.warning(f"Flush positions error: {e}")
        if flush_fn is not None:
            try:
                flush_fn()
                log.info("BSM flushed.")
            except Exception as e:
                log.warning(f"Flush BSM error: {e}")
        _update_system_health("signal_main", {
            "status": "stopped",
            "pid": os.getpid(),
            "stop_reason": f"signal_{signum}",
        })
        sys.exit(0)

    for sig in (_signal.SIGTERM, _signal.SIGINT):
        try:
            _signal.signal(sig, _handler)
        except (OSError, ValueError):
            pass  # SIGTERM 在 Windows 某些環境不可用


# ============================================================
# Signal Loop State（運維健康狀態，非 alert 狀態）
# ============================================================

class SignalLoopState:
    """
    記錄信號循環的運維健康狀態。
    Alert memory（cooldown map 等）由 15_alert_engine.py 管理。
    """

    def __init__(self):
        self.last_success_utc: Optional[str] = None
        self.last_cycle_duration_ms: Optional[int] = None
        self.consecutive_failures: int = 0
        self.last_error: Optional[str] = None
        self.last_ready_city_count: int = 0
        self.total_cycles: int = 0
        self.total_failures: int = 0
        self.last_refresh_completed_utc: Optional[str] = None
        self._pending_refresh_by: Optional[str] = None  # set during sleep, cleared after cycle

    def record_success(self, duration_s: float) -> None:
        self.last_success_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.last_cycle_duration_ms = round(duration_s * 1000)
        self.consecutive_failures = 0
        self.last_error = None
        self.total_cycles += 1

    def record_failure(self, error: str) -> None:
        self.consecutive_failures += 1
        self.total_failures += 1
        self.total_cycles += 1
        self.last_error = error

    def record_skip(self) -> None:
        self.total_cycles += 1

    def summary(self) -> str:
        return (
            f"cycles={self.total_cycles} failures={self.total_failures} "
            f"consecutive_failures={self.consecutive_failures} "
            f"last_success={self.last_success_utc or 'never'} "
            f"last_duration_ms={self.last_cycle_duration_ms}"
        )


# ============================================================
# WS 模式輔助函式
# ============================================================

async def _async_sleep_with_refresh_check(
    total_sleep: float,
    poll_interval: float = 2.0,
    loop_state: Optional["SignalLoopState"] = None,
) -> None:
    """async 版 refresh flag 監聽。每 poll_interval 秒檢查一次。"""
    remaining = total_sleep
    while remaining > 0:
        await asyncio.sleep(min(poll_interval, remaining))
        remaining -= poll_interval
        meta = _check_refresh_requested()
        if meta is not None:
            if loop_state is not None and loop_state._pending_refresh_by is None:
                loop_state._pending_refresh_by = meta.get("requested_by_chat_id", "unknown")
            return


async def _wait_for_bootstrap(stream, timeout: float = 60.0) -> bool:
    """等 PriceStreamListener REST bootstrap 完成（最多 timeout 秒）。"""
    try:
        await asyncio.wait_for(stream._bootstrap_done.wait(), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        log.warning(f"Bootstrap wait timed out after {timeout}s — proceeding anyway")
        return False


def _get_ws_last_event_utc(bsm) -> Optional[str]:
    """從 BSM 取最新 last_event_utc（任一 book 最近的更新時間）。"""
    latest = None
    for book in bsm.books.values():
        if book.last_event_utc is not None:
            if latest is None or book.last_event_utc > latest:
                latest = book.last_event_utc
    if latest is None:
        return None
    return latest.strftime("%Y-%m-%dT%H:%M:%SZ")


async def run_ws_mode(
    interval: float = 5.0,
    once: bool = False,
    verbose: bool = False,
) -> None:
    """
    WS 驅動的 signal 主循環（--mode ws）。

    架構：
    1. 初始化 BookStateManager + PriceStreamListener
    2. 啟動 08b WS ingestion（背景 asyncio task）
    3. 等 REST bootstrap 完成（最多 60 秒）
    4. 主循環每 interval 秒：讀 in-memory book → 11 → 15
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=" * 60)
    log.info("SIGNAL MAIN  [WS mode]")
    log.info(f"Time    : {datetime.now(timezone.utc).isoformat()}")
    log.info(f"Mode    : {'once' if once else 'daemon'}")
    log.info(f"Interval: {interval}s")
    log.info("=" * 60)

    # 載入市場清單（需 token_id）
    stream_mod = _get_stream_mod()
    bsm_mod = _get_bsm_mod()

    csm = _load_csm()
    ready = csm.get_ready_cities()
    if not ready:
        log.warning("No ready cities — WS mode exiting")
        return

    cities_filter = set(ready)
    markets = stream_mod.load_markets(cities_filter)
    if not markets:
        log.error("No enabled markets with token_id — WS mode exiting")
        return

    # 初始化 BookStateManager + PriceStreamListener
    bsm = bsm_mod.BookStateManager()
    metadata_lookup = stream_mod.build_metadata_lookup(markets)
    stream = stream_mod.PriceStreamListener(bsm, markets, metadata_lookup)

    output_dir = PROJ_DIR / "data" / "raw" / "prices" / "book_state"
    output_dir.mkdir(parents=True, exist_ok=True)

    # AlertEngine 初始化（啟動時一次）
    alert_engine, telegram_sender = _setup_alert_engine()
    error_reporter = ErrorReporter(telegram_sender)

    # STEP 13：PositionManager 初始化（啟動時一次）
    try:
        pos_mgr_mod = _get_pos_mgr_mod()
        position_mgr = pos_mgr_mod.PositionManager()
        position_mgr.load()
        log.info(f"PositionManager ready ({len(position_mgr.get_open_positions())} open positions)")
    except Exception as e:
        log.warning(f"PositionManager setup failed: {e} — running without position tracking")
        position_mgr = None

    # 優雅退出 hook
    _setup_shutdown_hooks(
        position_mgr,
        flush_fn=lambda: None,  # BSM flush 在主循環結束後處理
    )

    loop_state = SignalLoopState()
    _update_system_health("signal_main", {
        "status": "running",
        "price_mode": "ws",
        "pid": os.getpid(),
        "consecutive_failures": 0,
        "last_error": None,
        "ws_connected": False,
        "ws_fallback_active": False,
        "ready_city_count": 0,
    })

    # 啟動 WS ingestion（背景 asyncio task）
    ws_task = asyncio.create_task(stream.run(output_dir, once=False))

    # 等 REST bootstrap 完成
    log.info("Waiting for REST bootstrap...")
    await _wait_for_bootstrap(stream, timeout=60.0)
    log.info(f"Bootstrap done — {len(bsm.books)} books in memory")

    if once:
        # once 模式：bootstrap 完後跑一次 signal cycle 就退出
        pass

    running = True
    while running:
        cycle_start = time.monotonic()
        log.info(f"--- WS signal cycle @ {datetime.now(timezone.utc).strftime('%H:%M:%S')} ---")

        ws_fallback_active = False
        ws_fallback_reason: Optional[str] = None

        try:
            # 每輪重讀 ready 城市（反映 collector 更新）
            csm = _load_csm()
            ready = csm.get_ready_cities()
            loop_state.last_ready_city_count = len(ready)

            if not ready:
                log.warning("No ready cities — skipping WS cycle")
                loop_state.record_skip()
            else:
                cities_arg = ",".join(ready)

                # 從 BSM 收集 in-memory book_state_dict
                books_in_memory: dict = {}
                ws_task_alive = not ws_task.done()
                if ws_task_alive:
                    for m in markets:
                        mid = m.get("market_id", "")
                        ob = bsm.get_book(mid)
                        if ob is not None:
                            meta = metadata_lookup(mid)
                            books_in_memory[mid] = ob.to_book_state_dict(meta)

                # 即時觀測（讀 collector_main 產生的 latest_obs.json）
                _ws_obs = _read_latest_obs()

                # ── 12B-2：WS fallback to REST ─────────────────
                if not books_in_memory:
                    ws_fallback_active = True
                    ws_fallback_reason = "ws_task_dead" if not ws_task_alive else "no_in_memory_books"
                    log.warning(
                        f"WS unavailable ({ws_fallback_reason}) — falling back to REST this cycle"
                    )
                    await asyncio.to_thread(
                        run_script,
                        "08_market_price_fetch.py",
                        ["--cities", cities_arg],
                        "08_market_price_fetch (REST fallback)",
                    )
                    ev_ok, ev_results = await asyncio.to_thread(
                        _run_ev_engine, ready, verbose, "json", None, _ws_obs
                    )
                else:
                    # 正常 WS 路徑
                    ev_ok, ev_results = await asyncio.to_thread(
                        _run_ev_engine,
                        ready,
                        verbose,
                        "memory",
                        books_in_memory,
                        _ws_obs,
                    )

                if not ev_ok:
                    raise RuntimeError("11_ev_engine failed (WS mode)")

                # 預計算 signal_summary.json（供 bot 快速讀取，不阻塞主流程）
                _write_signal_summary(ev_results)

                # ── WS 事件時效性檢查（> 5 分鐘無事件 → 推錯誤）──
                last_ws_utc = _get_ws_last_event_utc(bsm) if not ws_fallback_active else None
                if last_ws_utc and not ws_fallback_active:
                    try:
                        last_ws_dt = datetime.fromisoformat(last_ws_utc.replace("Z", "+00:00"))
                        stale_secs = (datetime.now(timezone.utc) - last_ws_dt).total_seconds()
                        if stale_secs > 300:
                            error_reporter.report(
                                "signal_main",
                                f"WS 超過 {stale_secs:.0f}s 無事件（last_event={last_ws_utc}）",
                                "ws_disconnect",
                            )
                    except Exception:
                        pass

                # 15：AlertEngine 通報
                if alert_engine is not None:
                    try:
                        alerts = alert_engine.evaluate(ev_results)
                        if alerts:
                            log.info(f"  AlertEngine: {len(alerts)} alert(s)")
                            alert_engine.process(alerts)
                        else:
                            log.debug("  AlertEngine: no alerts this cycle")
                    except Exception as e:
                        log.warning(f"  AlertEngine error: {e}")

                # 16：持倉更新 + EXIT 偵測（STEP 13）
                if position_mgr is not None:
                    _update_positions(position_mgr, alert_engine, ev_results)

                # flush dirty books（供 Bot 讀磁碟）
                if not ws_fallback_active:
                    try:
                        n = bsm.flush_dirty(output_dir, metadata_lookup)
                        if n > 0:
                            log.debug(f"  Flushed {n} dirty book(s)")
                    except Exception as e:
                        log.warning(f"  flush_dirty error: {e}")

                duration_s = time.monotonic() - cycle_start
                loop_state.record_success(duration_s)

                if loop_state._pending_refresh_by is not None:
                    loop_state.last_refresh_completed_utc = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    )
                    log.info(
                        f"Refresh cycle complete (requested_by={loop_state._pending_refresh_by})"
                    )
                    loop_state._pending_refresh_by = None

                if duration_s > interval * WARN_DURATION_RATIO:
                    log.warning(
                        f"WS cycle took {duration_s:.1f}s > "
                        f"{WARN_DURATION_RATIO*100:.0f}% of interval {interval}s"
                    )

        except Exception as e:
            loop_state.record_failure(str(e))
            log.error(f"WS signal cycle failed: {e}")
            if loop_state.consecutive_failures >= BACKOFF_THRESHOLD:
                error_reporter.report("signal_main", str(e), "signal_consecutive_fail")

        # WS 連線狀態（task 是否還活著）
        ws_alive = not ws_task.done()
        last_ws_utc = _get_ws_last_event_utc(bsm)
        _write_signal_state(
            loop_state, len(ready),
            price_mode="ws",
            ws_connected=ws_alive,
            last_ws_event_utc=last_ws_utc,
            ws_fallback_active=ws_fallback_active,
            ws_fallback_reason=ws_fallback_reason,
        )
        _update_system_health("signal_main", {
            "status": "running",
            "price_mode": "ws",
            "pid": os.getpid(),
            "last_success_utc": loop_state.last_success_utc,
            "last_cycle_duration_ms": loop_state.last_cycle_duration_ms,
            "consecutive_failures": loop_state.consecutive_failures,
            "last_error": loop_state.last_error,
            "ws_connected": ws_alive,
            "ws_fallback_active": ws_fallback_active,
            "ready_city_count": loop_state.last_ready_city_count,
        })

        log.debug(f"Loop state: {loop_state.summary()}")

        if once:
            running = False
            break

        # 不重疊：等到 interval 結束
        elapsed = time.monotonic() - cycle_start
        sleep_time = max(0.0, interval - elapsed)

        # 退避：連續失敗 >= BACKOFF_THRESHOLD
        if loop_state.consecutive_failures >= BACKOFF_THRESHOLD:
            sleep_time = max(sleep_time, BACKOFF_MIN_SLEEP)
            log.warning(
                f"Backing off to {sleep_time:.0f}s "
                f"(consecutive_failures={loop_state.consecutive_failures})"
            )

        await _async_sleep_with_refresh_check(sleep_time, loop_state=loop_state)

    # 優雅退出：停止 WS + 最後 flush
    stream.stop()
    ws_task.cancel()
    try:
        await ws_task
    except (asyncio.CancelledError, Exception):
        pass
    try:
        bsm.flush_all(output_dir, metadata_lookup)
    except Exception as e:
        log.warning(f"Final flush error: {e}")
    log.info("WS signal done.")


# ============================================================
# signal_summary.json 預計算（供 telegram_bot 快速讀取）
# ============================================================

def _write_signal_summary(ev_results: list) -> None:
    """ev_engine 跑完後，分四組寫 data/results/signal_summary.json（原子寫入）。

    四組：ranking（> 24h active BUY）、today（≤ 24h active BUY）、
          warning（last_forecast_warning BUY）、settling（< 6h，含全部合約）。
    """
    import math
    from collections import defaultdict

    def _sf(v, default=0.0):
        if v is None:
            return default
        try:
            f = float(v)
            return default if (math.isnan(f) or math.isinf(f)) else f
        except (TypeError, ValueError):
            return default

    def _lead_hrs(row):
        v = row.get("lead_hours_to_settlement", "")
        if v == "" or v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def _best_edge(row):
        return max(_sf(row.get("yes_edge"), -999), _sf(row.get("no_edge"), -999))

    def _sanitize(obj):
        """遞迴把 nan/inf 替換成 None，確保 JSON 可序列化。"""
        if isinstance(obj, dict):
            return {k: _sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_sanitize(v) for v in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj

    try:
        ranking, today, warning, settling_rows = [], [], [], []
        for row in ev_results:
            hours = _lead_hrs(row)
            action = row.get("signal_action", "")
            status = row.get("signal_status", "")
            is_buy = action in ("BUY_YES", "BUY_NO")
            is_active = status == "active"
            is_warn = status == "last_forecast_warning"

            if hours is not None and 0 < hours < 6:
                settling_rows.append(row)
            if is_active and is_buy:
                if hours is None or hours > 24:
                    ranking.append(row)
                elif hours <= 24:
                    today.append(row)
            if is_warn and is_buy:
                warning.append(row)

        ranking.sort(key=_best_edge, reverse=True)
        today.sort(key=_best_edge, reverse=True)
        warning.sort(key=_best_edge, reverse=True)

        # settling：按 (city, market_date_local) 分組
        grp: dict = defaultdict(list)
        for r in settling_rows:
            grp[(r.get("city", ""), r.get("market_date_local", ""))].append(r)

        settling = []
        for (city, date), rows in grp.items():
            hrs_list = [_lead_hrs(r) for r in rows if _lead_hrs(r) is not None]
            settling.append({
                "city": city,
                "market_date": date,
                "hours_to_settlement": min(hrs_list) if hrs_list else 0.0,
                "rows": rows,
            })
        settling.sort(key=lambda x: x["hours_to_settlement"])

        summary = _sanitize({
            "updated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "ranking": ranking,
            "today": today,
            "warning": warning,
            "settling": settling,
        })

        path = PROJ_DIR / "data" / "results" / "signal_summary.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
        log.info(
            f"signal_summary written: ranking={len(ranking)} today={len(today)} "
            f"warning={len(warning)} settling={len(settling)} groups"
        )
    except Exception as e:
        log.warning(f"_write_signal_summary failed: {e}")


# ============================================================
# 觀測 JSON 讀取（collector_main 已集中寫入）
# ============================================================

def _read_latest_obs() -> Optional[dict]:
    """讀 data/observations/latest_obs.json → 轉成 11 期待的 dict。"""
    obs_path = PROJ_DIR / "data" / "observations" / "latest_obs.json"
    if not obs_path.exists():
        return None
    try:
        raw = json.loads(obs_path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning(f"latest_obs.json parse error: {e}")
        return None
    cities_data = raw.get("cities", raw)
    current_obs: dict = {}
    for city, obs in cities_data.items():
        if not isinstance(obs, dict):
            continue
        if obs.get("status") != "ok":
            continue
        if obs.get("high_c") is None:
            continue
        current_obs[city] = {
            "high_c": obs["high_c"],
            "source": obs.get("source", ""),
            "obs_time": obs.get("obs_time_utc", ""),  # adapter: obs_time_utc → obs_time
        }
    return current_obs if current_obs else None


# ============================================================
# 主循環（REST 模式）
# ============================================================

def run_signal(
    once: bool = False,
    interval: float = 30.0,
    verbose: bool = False,
    alert_hook: Optional[Callable] = None,
) -> None:
    """
    信號主循環。

    alert_hook: 可選 backward-compat callback，簽名 alert_hook(ready_cities) → None。
                STEP 10 通報邏輯已由 AlertEngine 接管，alert_hook 保留以向後相容。
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=" * 60)
    log.info("SIGNAL MAIN")
    log.info(f"Time    : {datetime.now(timezone.utc).isoformat()}")
    log.info(f"Mode    : {'once' if once else 'daemon'}")
    log.info(f"Interval: {interval}s")
    log.info("=" * 60)

    # STEP 10：AlertEngine 初始化（啟動時一次，不是每輪）
    alert_engine, telegram_sender = _setup_alert_engine()
    error_reporter = ErrorReporter(telegram_sender)

    # STEP 13：PositionManager 初始化（啟動時一次）
    try:
        pos_mgr_mod = _get_pos_mgr_mod()
        position_mgr = pos_mgr_mod.PositionManager()
        position_mgr.load()
        log.info(f"PositionManager ready ({len(position_mgr.get_open_positions())} open positions)")
    except Exception as e:
        log.warning(f"PositionManager setup failed: {e} — running without position tracking")
        position_mgr = None

    # 即時觀測：由 collector_main 集中寫入 latest_obs.json，signal_main 每輪讀取

    # 優雅退出 hook
    _setup_shutdown_hooks(position_mgr)

    loop_state = SignalLoopState()
    _update_system_health("signal_main", {
        "status": "running",
        "price_mode": "rest",
        "pid": os.getpid(),
        "consecutive_failures": 0,
        "last_error": None,
        "ws_connected": False,
        "ws_fallback_active": False,
        "ready_city_count": 0,
    })

    while True:
        cycle_start = time.monotonic()
        log.info(f"--- Signal cycle @ {datetime.now(timezone.utc).strftime('%H:%M:%S')} ---")

        try:
            # 1. 讀取 ready 城市（每輪重讀，反映 collector 更新）
            csm = _load_csm()
            ready = csm.get_ready_cities()
            loop_state.last_ready_city_count = len(ready)

            if not ready:
                log.warning("No ready cities — skipping cycle")
                loop_state.record_skip()
            else:
                cities_arg = ",".join(ready)
                log.info(f"Ready cities: {ready}")

                # 2. 抓即時價格（08，subprocess + atomic write）
                ok_price = run_script(
                    "08_market_price_fetch.py",
                    args=["--cities", cities_arg],
                    label="08_market_price_fetch",
                )
                if not ok_price:
                    log.warning("08_market_price_fetch failed (non-blocking — 11 will use stale price)")

                # 3. 即時觀測（讀 collector_main 產生的 latest_obs.json）
                current_obs = _read_latest_obs()
                if current_obs:
                    _obs_summary = {c: f"{v['high_c']:.1f}C" for c, v in current_obs.items()}
                    log.info(f"Current obs: {_obs_summary}")

                # 4. 算 EV + 信號（11，in-process，回傳 in-memory 結果）
                ok_ev, ev_results = _run_ev_engine(
                    ready, verbose=verbose, current_obs=current_obs or None
                )
                if not ok_ev:
                    raise RuntimeError("11_ev_engine failed")

                # 4b. 預計算 signal_summary.json（供 bot 快速讀取，不阻塞主流程）
                _write_signal_summary(ev_results)

                # 5. STEP 10：AlertEngine 通報
                if alert_engine is not None:
                    try:
                        alerts = alert_engine.evaluate(ev_results)
                        if alerts:
                            log.info(f"  AlertEngine: {len(alerts)} alert(s) — processing")
                            alert_engine.process(alerts)
                        else:
                            log.debug("  AlertEngine: no alerts this cycle")
                    except Exception as e:
                        log.warning(f"  AlertEngine error: {e}")

                # 5. STEP 13：持倉更新 + EXIT 偵測
                if position_mgr is not None:
                    _update_positions(position_mgr, alert_engine, ev_results)

                # 6. Backward-compat alert_hook（STEP 10 前的預留接口）
                if alert_hook is not None:
                    try:
                        alert_hook(ready)
                    except Exception as e:
                        log.warning(f"alert_hook raised: {e}")

                duration_s = time.monotonic() - cycle_start
                loop_state.record_success(duration_s)
                # 若本輪是 refresh 觸發，標記完成時間
                if loop_state._pending_refresh_by is not None:
                    loop_state.last_refresh_completed_utc = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    )
                    log.info(
                        f"Refresh cycle complete (requested_by={loop_state._pending_refresh_by})"
                    )
                    loop_state._pending_refresh_by = None
                _write_signal_state(loop_state, len(ready))
                _update_system_health("signal_main", {
                    "status": "running",
                    "price_mode": "rest",
                    "pid": os.getpid(),
                    "last_success_utc": loop_state.last_success_utc,
                    "last_cycle_duration_ms": loop_state.last_cycle_duration_ms,
                    "consecutive_failures": 0,
                    "last_error": None,
                    "ws_connected": False,
                    "ws_fallback_active": False,
                    "ready_city_count": loop_state.last_ready_city_count,
                })

                # Duration warning
                if duration_s > interval * WARN_DURATION_RATIO:
                    log.warning(
                        f"Cycle took {duration_s:.1f}s which is >{WARN_DURATION_RATIO*100:.0f}% "
                        f"of interval {interval}s — consider increasing --interval"
                    )

        except Exception as e:
            loop_state.record_failure(str(e))
            log.error(f"Signal cycle failed: {e}")
            if loop_state.consecutive_failures >= BACKOFF_THRESHOLD:
                error_reporter.report("signal_main", str(e), "signal_consecutive_fail")

        log.debug(f"Loop state: {loop_state.summary()}")

        if once:
            log.info("Signal: --once mode, exiting after one cycle.")
            break

        # 不重疊：等到 interval 結束
        elapsed = time.monotonic() - cycle_start
        sleep_time = max(0.0, interval - elapsed)

        # 退避：連續失敗 >= BACKOFF_THRESHOLD
        if loop_state.consecutive_failures >= BACKOFF_THRESHOLD:
            sleep_time = max(sleep_time, BACKOFF_MIN_SLEEP)
            log.warning(
                f"Backing off to {sleep_time:.0f}s "
                f"(consecutive_failures={loop_state.consecutive_failures})"
            )

        log.debug(f"Signal sleeping {sleep_time:.1f}s")
        _sleep_with_refresh_check(sleep_time, loop_state=loop_state)

    log.info("Signal done.")


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="即時信號常駐程序（price → EV → signal → alert）"
    )
    p.add_argument(
        "--mode", choices=["rest", "ws"], default=None,
        help=(
            "價格來源模式：rest（08 REST 輪詢）或 ws（08b WS 常駐）。"
            "不帶此參數 → 嘗試 WS，啟動失敗自動退 REST。"
        ),
    )
    p.add_argument("--once", action="store_true", help="跑一次後退出（測試用）")
    p.add_argument(
        "--interval", type=float, default=None,
        help="兩輪間隔秒數（rest 預設 30，ws 預設 5，不計執行時間）",
    )
    p.add_argument("--verbose", action="store_true", help="詳細 log")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()

    if args.mode == "ws" or args.mode is None:
        ws_interval = args.interval if args.interval is not None else 5.0
        try:
            asyncio.run(run_ws_mode(interval=ws_interval, once=args.once, verbose=args.verbose))
        except KeyboardInterrupt:
            log.info("WS signal interrupted by user — exiting.")
        except Exception as e:
            if args.mode is None:
                # 不帶 --mode：WS 啟動失敗自動退 REST
                log.warning(f"WS mode startup failed ({e}) — falling back to REST")
                rest_interval = args.interval if args.interval is not None else 30.0
                try:
                    run_signal(once=args.once, interval=rest_interval, verbose=args.verbose)
                except KeyboardInterrupt:
                    log.info("REST signal interrupted by user — exiting.")
            else:
                # 明確指定 --mode ws：不做 fallback，直接報錯
                log.error(f"WS mode failed: {e}")
                sys.exit(1)
    else:
        # --mode rest
        rest_interval = args.interval if args.interval is not None else 30.0
        try:
            run_signal(once=args.once, interval=rest_interval, verbose=args.verbose)
        except KeyboardInterrupt:
            log.info("REST signal interrupted by user — exiting.")
