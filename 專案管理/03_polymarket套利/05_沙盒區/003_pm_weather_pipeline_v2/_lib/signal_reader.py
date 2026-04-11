"""
_lib/signal_reader.py — Bot 資料讀取層

設計原則：
  1. 只讀 finalized outputs（ev_signals.csv, city_status.json, alert_history, trading_params.yaml）
  2. 不碰 data/raw/, data/processed/, 任何 *.tmp
  3. 所有方法回傳 Python 原生型別（dict/list），不回傳 CSV row 字串
  4. 缺資料時回傳空 list/dict，不 raise（Bot 不應因缺資料 crash）
  5. 純函式：不寫任何 output，除了 request_refresh()（寫 flag）
"""

import csv
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

PROJ_DIR = Path(__file__).resolve().parent.parent


# ============================================================
# Float 欄位（ev_signals.csv 讀進來是字串，需轉型）
# ============================================================

_FLOAT_FIELDS = frozenset({
    "p_yes", "p_no",
    "precision_half",
    "yes_ask_price", "no_ask_price",
    "yes_edge", "no_edge",
    "yes_ev", "no_ev",
    "naive_fair_yes", "naive_fair_no",
    "fair_exit_yes", "fair_exit_no",
    "yes_fee", "no_fee",
    "yes_cost", "no_cost",
    "kelly_fraction_yes", "kelly_fraction_no",
    "kelly_amount", "fee_rate_used", "fee_exponent_used",
    "price_age_seconds",
    "yes_depth_usd", "no_depth_usd",
    "yes_sweet_shares", "yes_sweet_usd", "yes_sweet_avg_price",
    "yes_sweet_ev",
    "no_sweet_shares", "no_sweet_usd", "no_sweet_avg_price",
    "no_sweet_ev",
    "yes_fixed_shares", "yes_fixed_usd", "yes_fixed_avg_price",
    "yes_fixed_ev",
    "no_fixed_shares", "no_fixed_usd", "no_fixed_avg_price",
    "no_fixed_ev",
})

_BOOL_FIELDS = frozenset({
    "yes_sweet_exhausted", "yes_fixed_exhausted",
    "no_sweet_exhausted", "no_fixed_exhausted",
})


def _parse_row(raw: dict) -> dict:
    """Convert CSV string values to Python types for numeric/bool fields."""
    row = dict(raw)
    for field in _FLOAT_FIELDS:
        v = row.get(field, "")
        if v == "" or v is None:
            row[field] = None
        else:
            try:
                row[field] = float(v)
            except (ValueError, TypeError):
                row[field] = None
    for field in _BOOL_FIELDS:
        v = str(row.get(field, "")).lower()
        if v == "true":
            row[field] = True
        elif v == "false":
            row[field] = False
        # else leave as string
    return row


def _safe_float(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _best_edge(row: dict) -> float:
    return max(_safe_float(row.get("yes_edge"), -999), _safe_float(row.get("no_edge"), -999))


def _best_ev(row: dict) -> float:
    return max(_safe_float(row.get("yes_ev"), -999), _safe_float(row.get("no_ev"), -999))


def _best_depth(row: dict) -> float:
    return max(_safe_float(row.get("yes_sweet_usd"), 0), _safe_float(row.get("no_sweet_usd"), 0))


def _lead_hours(row: dict) -> Optional[float]:
    """Parse lead_hours_to_settlement from a row. Returns None if missing/invalid."""
    v = row.get("lead_hours_to_settlement", "")
    if v == "" or v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# ============================================================
# SignalDataReader
# ============================================================

class SignalDataReader:
    """
    Bot 的唯一資料來源。只讀 finalized outputs，不碰 raw/processed。
    所有方法回傳 Python 原生型別，缺資料時回傳空值，不 raise。
    """

    def __init__(self, data_dir: str = "data", config_dir: str = "config"):
        self.data_dir = PROJ_DIR / data_dir
        self.config_dir = PROJ_DIR / config_dir
        self.ev_signals_root = self.data_dir / "results" / "ev_signals"
        self.alert_history_dir = PROJ_DIR / "logs" / "15_alert"
        self._temp_unit_map: dict[str, str] = {}
        self._city_tz_map: dict[str, str] = {}
        self._token_map: dict[str, tuple[str, str]] = {}  # market_id → (yes_token_id, no_token_id)
        # mtime-based caches（v7.3.9）
        self._mm_mtime: float = 0.0                    # market_master.csv mtime
        self._cache_rows: dict[str, list[dict]] = {}   # city → parsed+joined rows
        self._cache_mtime: dict[str, float] = {}       # city → ev_signals.csv mtime
        self._load_market_master_maps()

    def _load_market_master_maps(self, force: bool = False) -> None:
        """從 market_master.csv 建立三個 lookup：
        - market_id → temp_unit
        - city → timezone
        - market_id → (yes_token_id, no_token_id)  # 供交易模組下單用

        **v7.3.9**：mtime-based 惰性重載。`force=True` 強制重讀。
        若 file 不存在 / mtime 沒變 / 讀檔失敗，則保留現有 maps（不清空）。
        backfill 新增城市時 ev_signals 路徑會先有資料，下一次 _read_city_ev_csv
        前的 _ensure_market_master_loaded() 會自動把新 token 拉進來。
        """
        path = self.data_dir / "market_master.csv"
        if not path.exists():
            return
        try:
            current_mtime = path.stat().st_mtime
        except OSError:
            return
        if not force and current_mtime == self._mm_mtime:
            return  # 沒變，沿用現有 maps

        new_temp_unit: dict[str, str] = {}
        new_city_tz: dict[str, str] = {}
        new_token: dict[str, tuple[str, str]] = {}
        try:
            with open(path, "r", encoding="utf-8", newline="") as f:
                for row in csv.DictReader(f):
                    mid = row.get("market_id", "")
                    unit = row.get("temp_unit", "")
                    if mid and unit:
                        new_temp_unit[mid] = unit
                    city = row.get("city", "")
                    tz = row.get("timezone", "")
                    if city and tz and city not in new_city_tz:
                        new_city_tz[city] = tz
                    yes_tok = row.get("yes_token_id", "")
                    no_tok = row.get("no_token_id", "")
                    if mid and (yes_tok or no_tok):
                        new_token[mid] = (yes_tok, no_tok)
        except Exception as e:
            log.warning(f"_load_market_master_maps: {e} (keeping previous maps)")
            return

        # 整體替換（成功才覆寫，避免半段失敗污染）
        self._temp_unit_map = new_temp_unit
        self._city_tz_map = new_city_tz
        self._token_map = new_token
        self._mm_mtime = current_mtime
        log.debug(
            f"_load_market_master_maps: {len(new_temp_unit)} temp_unit, "
            f"{len(new_city_tz)} tz, {len(new_token)} token entries "
            f"(mtime={current_mtime:.0f})"
        )

    def _ensure_market_master_loaded(self) -> None:
        """每次讀 ev_signals 之前呼叫，便宜（一次 stat() + mtime 比較）。"""
        self._load_market_master_maps(force=False)

    def get_token_ids(self, market_id: str) -> tuple[str, str]:
        """回傳 (yes_token_id, no_token_id)。找不到回傳 ("", "")。"""
        return self._token_map.get(market_id, ("", ""))

    # 保留舊名稱以防萬一有外部呼叫
    def _load_temp_unit_map(self) -> dict[str, str]:
        return self._temp_unit_map

    def get_city_timezone(self, city: str) -> str:
        """回傳城市的 timezone 字串（例如 'Europe/London'）。找不到回傳空字串。"""
        return self._city_tz_map.get(city, "")

    # ── City status ──────────────────────────────────────────

    def get_ready_cities(self) -> list[str]:
        """從 city_status.json 回傳 status=ready 的城市列表。"""
        status = self.get_city_status_all()
        return sorted(city for city, info in status.items() if info.get("status") == "ready")

    def get_city_status_all(self) -> dict:
        """city_status.json 完整內容，缺失時回傳 {}。"""
        path = self.data_dir / "city_status.json"
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning(f"get_city_status_all: {e}")
            return {}

    # ── EV Signals ───────────────────────────────────────────

    def _read_city_ev_csv(self, city: str) -> list[dict]:
        """Read and parse ev_signals.csv for a city. Returns [] on error.

        同時 join market_master：
        - temp_unit（若 ev_signals 沒有）
        - yes_token_id / no_token_id（交易模組下單用；ev_signals.csv 不含 token）

        **v7.3.9 cache**：mtime-based。signal_main 每 30s 寫一次 ev_signals.csv，
        只有該城市真的有新資料時才會 cache miss。其他時間 bot 按按鈕全部 hit cache，
        省掉每次重新 csv.DictReader + _parse_row（每 row ~30 個欄位 float 轉型）。
        讀檔失敗時保留上一輪快取，不清空。
        """
        # 確保 market_master maps 是最新的（便宜，stat + mtime 比較）
        self._ensure_market_master_loaded()

        path = self.ev_signals_root / city / "ev_signals.csv"
        if not path.exists():
            return []
        try:
            current_mtime = path.stat().st_mtime
        except OSError:
            return self._cache_rows.get(city, [])

        # Cache hit：mtime 沒變 → 直接回傳記憶體版本
        cached_mtime = self._cache_mtime.get(city)
        if cached_mtime == current_mtime and city in self._cache_rows:
            return self._cache_rows[city]

        # Cache miss：重讀、parse、join
        try:
            rows: list[dict] = []
            with open(path, "r", encoding="utf-8", newline="") as f:
                for raw in csv.DictReader(f):
                    row = _parse_row(raw)
                    mid = row.get("market_id", "")
                    if not row.get("temp_unit"):
                        row["temp_unit"] = self._temp_unit_map.get(mid, "C")
                    yes_tok, no_tok = self._token_map.get(mid, ("", ""))
                    row["yes_token_id"] = yes_tok
                    row["no_token_id"] = no_tok
                    rows.append(row)
        except Exception as e:
            log.warning(f"_read_city_ev_csv({city}): {e} (keeping previous cache)")
            return self._cache_rows.get(city, [])

        self._cache_rows[city] = rows
        self._cache_mtime[city] = current_mtime
        return rows

    def get_available_dates(self, city: str) -> list[str]:
        """該城市有哪些 market_date_local（排序，最舊→最新）。"""
        rows = self._read_city_ev_csv(city)
        dates = sorted({r.get("market_date_local", "") for r in rows if r.get("market_date_local")})
        return dates

    def get_city_signals(self, city: str, market_date: Optional[str] = None) -> list[dict]:
        """
        讀某城市某天的 ev_signals。
        market_date=None → 最近的日期。
        回傳所有合約（正負 edge 都有），按 best_edge 降序排列。
        """
        rows = self._read_city_ev_csv(city)
        if not rows:
            return []

        if market_date is None or market_date == "latest":
            dates = sorted({r.get("market_date_local", "") for r in rows if r.get("market_date_local")})
            market_date = dates[-1] if dates else None

        if market_date:
            rows = [r for r in rows if r.get("market_date_local") == market_date]

        rows.sort(key=_best_edge, reverse=True)
        return rows

    def get_all_signals_ranked(
        self,
        sort_by: str = "edge",
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[dict], int]:
        """
        跨城市排行。回傳 (results, total_count)。
        sort_by: "edge" / "ev" / "depth"
        只包含 signal_status="active" 且 signal_action in (BUY_YES, BUY_NO) 的行。
        """
        cities = self.get_ready_cities()
        all_rows: list[dict] = []
        for city in cities:
            rows = self._read_city_ev_csv(city)
            all_rows.extend(rows)

        # Filter: active + actionable + hours > 24 (None = include, treat as far future)
        actionable = [
            r for r in all_rows
            if r.get("signal_status") == "active"
            and r.get("signal_action") in ("BUY_YES", "BUY_NO")
            and (_lead_hours(r) is None or _lead_hours(r) > 24)
        ]

        sort_fn = {
            "edge": _best_edge,
            "ev": _best_ev,
            "depth": _best_depth,
        }.get(sort_by, _best_edge)

        actionable.sort(key=sort_fn, reverse=True)
        total = len(actionable)
        return (actionable[offset: offset + limit], total)

    def get_today_signals_ranked(
        self,
        sort_by: str = "edge",
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[dict], int]:
        """
        今日信號排行：signal_status=active + signal_action BUY + hours_to_settlement <= 24。
        """
        cities = self.get_ready_cities()
        all_rows: list[dict] = []
        for city in cities:
            rows = self._read_city_ev_csv(city)
            all_rows.extend(rows)

        actionable = [
            r for r in all_rows
            if r.get("signal_status") == "active"
            and r.get("signal_action") in ("BUY_YES", "BUY_NO")
            and _lead_hours(r) is not None
            and _lead_hours(r) <= 24
        ]

        sort_fn = {
            "edge": _best_edge,
            "ev": _best_ev,
            "depth": _best_depth,
        }.get(sort_by, _best_edge)

        actionable.sort(key=sort_fn, reverse=True)
        total = len(actionable)
        return (actionable[offset: offset + limit], total)

    def get_warning_signals_ranked(
        self,
        sort_by: str = "edge",
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[dict], int]:
        """
        預警排行：signal_status=last_forecast_warning + signal_action BUY（6-8h 最後預報）。
        """
        cities = self.get_ready_cities()
        all_rows: list[dict] = []
        for city in cities:
            rows = self._read_city_ev_csv(city)
            all_rows.extend(rows)

        actionable = [
            r for r in all_rows
            if r.get("signal_status") == "last_forecast_warning"
            and r.get("signal_action") in ("BUY_YES", "BUY_NO")
        ]

        sort_fn = {
            "edge": _best_edge,
            "ev": _best_ev,
            "depth": _best_depth,
        }.get(sort_by, _best_edge)

        actionable.sort(key=sort_fn, reverse=True)
        total = len(actionable)
        return (actionable[offset: offset + limit], total)

    def get_signal_detail(self, market_id: str) -> Optional[dict]:
        """
        按 market_id 前綴匹配，搜尋所有城市的 ev_signals。
        回傳第一個 match，找不到回傳 None。
        """
        cities = self.get_ready_cities()
        for city in cities:
            rows = self._read_city_ev_csv(city)
            for row in rows:
                if row.get("market_id", "").startswith(market_id):
                    return row
        return None

    # ── Alert History ─────────────────────────────────────────

    def get_alert_history(self, hours: int = 24) -> list[dict]:
        """
        最近 N 小時的 alert_history（從日切檔讀）。
        回傳按 generated_utc 降序排列的 list。
        """
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=hours)
        results: list[dict] = []

        for delta_days in (0, 1):
            dt = now - timedelta(days=delta_days)
            path = self.alert_history_dir / f"{dt.strftime('%Y-%m-%d')}_alert_history.csv"
            if not path.exists():
                continue
            try:
                with open(path, "r", encoding="utf-8", newline="") as f:
                    for row in csv.DictReader(f):
                        generated_str = row.get("generated_utc", "")
                        try:
                            generated = datetime.fromisoformat(
                                generated_str.replace("Z", "+00:00")
                            )
                        except ValueError:
                            continue
                        if generated >= cutoff:
                            results.append(dict(row))
            except Exception as e:
                log.warning(f"get_alert_history: {e}")

        results.sort(key=lambda r: r.get("generated_utc", ""), reverse=True)
        return results

    # ── Config & State ────────────────────────────────────────

    def get_trading_params(self) -> dict:
        """讀 trading_params.yaml（只讀，回傳 flat dict）。"""
        path = self.config_dir / "trading_params.yaml"
        if not path.exists():
            return {}
        params: dict = {}
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if ":" not in line:
                    continue
                k, _, v = line.partition(":")
                params[k.strip()] = v.split("#")[0].strip()
        except Exception as e:
            log.warning(f"get_trading_params: {e}")
        return params

    def get_last_refresh_time(self) -> Optional[str]:
        """signal_main 最近完成的時間（從 data/_signal_state.json 讀）。"""
        path = self.data_dir / "_signal_state.json"
        if not path.exists():
            return None
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
            return state.get("last_success_utc")
        except Exception as e:
            log.warning(f"get_last_refresh_time: parse failed ({e})")
            return None

    def get_signal_state(self) -> dict:
        """讀 data/_signal_state.json 完整內容。缺失時回傳 {}。"""
        path = self.data_dir / "_signal_state.json"
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning(f"get_signal_state: parse failed ({e})")
            return {}

    # ── Flag writers ──────────────────────────────────────────

    def request_refresh(self, chat_id: str = "") -> None:
        """寫 refresh flag（JSON 含 timestamp + requester），讓 signal_main 下一輪提前執行。"""
        flag = self.data_dir / "_refresh_requested"
        try:
            flag.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "requested_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "requested_by_chat_id": str(chat_id),
            }
            flag.write_text(json.dumps(payload), encoding="utf-8")
        except Exception as e:
            log.warning(f"request_refresh failed: {e}")

    def request_city_scan(self) -> None:
        """寫 scan flag，讓 collector_main 下一輪執行城市掃描。"""
        flag = self.data_dir / "_scan_requested"
        try:
            flag.parent.mkdir(parents=True, exist_ok=True)
            flag.touch()
        except Exception as e:
            log.warning(f"request_city_scan failed: {e}")

    # ── Positions（STEP 13）──────────────────────────────────

    def _read_positions(self) -> list:
        """讀 data/positions.json，回傳 positions list。缺失時回傳 []。"""
        path = self.data_dir / "positions.json"
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data.get("positions", [])
        except Exception as e:
            log.warning(f"_read_positions: {e}")
            return []

    def get_open_positions(self) -> list:
        """回傳所有 status=open 的持倉（最新進場在前）。"""
        positions = [p for p in self._read_positions() if p.get("status") == "open"]
        positions.sort(key=lambda p: p.get("entry_time_utc", ""), reverse=True)
        return positions

    def get_closed_positions(self) -> list:
        """回傳所有 status=closed 的持倉（最新平倉在前）。"""
        positions = [p for p in self._read_positions() if p.get("status") == "closed"]
        positions.sort(key=lambda p: p.get("exit_time_utc", ""), reverse=True)
        return positions

    def get_position(self, position_id: str) -> Optional[dict]:
        """按 position_id 查詢。找不到回傳 None。"""
        for p in self._read_positions():
            if p.get("position_id") == position_id:
                return p
        return None
