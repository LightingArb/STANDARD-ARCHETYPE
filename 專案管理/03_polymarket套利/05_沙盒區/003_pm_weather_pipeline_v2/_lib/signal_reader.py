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
    "yes_ask_price", "no_ask_price",
    "yes_edge", "no_edge",
    "yes_ev", "no_ev",
    "naive_fair_yes", "naive_fair_no",
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
        self._load_market_master_maps()

    def _load_market_master_maps(self) -> None:
        """從 market_master.csv 建立 market_id→temp_unit 和 city→timezone 兩個 lookup。"""
        path = self.data_dir / "market_master.csv"
        if not path.exists():
            return
        try:
            with open(path, "r", encoding="utf-8", newline="") as f:
                for row in csv.DictReader(f):
                    mid = row.get("market_id", "")
                    unit = row.get("temp_unit", "")
                    if mid and unit:
                        self._temp_unit_map[mid] = unit
                    city = row.get("city", "")
                    tz = row.get("timezone", "")
                    if city and tz and city not in self._city_tz_map:
                        self._city_tz_map[city] = tz
            log.debug(f"_load_market_master_maps: {len(self._temp_unit_map)} temp_unit, {len(self._city_tz_map)} tz entries")
        except Exception as e:
            log.warning(f"_load_market_master_maps: {e}")

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
        """Read and parse ev_signals.csv for a city. Returns [] on error."""
        path = self.ev_signals_root / city / "ev_signals.csv"
        if not path.exists():
            return []
        try:
            rows = []
            with open(path, "r", encoding="utf-8", newline="") as f:
                for raw in csv.DictReader(f):
                    row = _parse_row(raw)
                    # ev_signals.csv 沒有 temp_unit 欄位，從 market_master lookup 補入
                    if not row.get("temp_unit"):
                        mid = row.get("market_id", "")
                        row["temp_unit"] = self._temp_unit_map.get(mid, "C")
                    rows.append(row)
            return rows
        except Exception as e:
            log.warning(f"_read_city_ev_csv({city}): {e}")
            return []

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

        # Filter: active + actionable
        actionable = [
            r for r in all_rows
            if r.get("signal_status") == "active"
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
        except Exception:
            return None

    def get_signal_state(self) -> dict:
        """讀 data/_signal_state.json 完整內容。缺失時回傳 {}。"""
        path = self.data_dir / "_signal_state.json"
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
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
