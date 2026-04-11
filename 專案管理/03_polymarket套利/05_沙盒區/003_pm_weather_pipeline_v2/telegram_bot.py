"""
telegram_bot.py — Telegram Bot UI（STEP 11）

設計原則：
  1. Bot 是 UI，不是運算主體。只讀 finalized outputs（透過 SignalDataReader）。
  2. 分進程運行。Bot 掛了不影響 signal_main。
  3. 刷新走 refresh flag（data/_refresh_requested）。Bot 不直接跑 08/11。
  4. 第一版只支援私聊。不支援群組。
  5. 管理操作 admin-only（刷新、城市掃描）。查詢對 allowed 開放。

CLI：
  pip install python-telegram-bot  # v20+，async 版本
  python telegram_bot.py

Config 路徑（依優先順序）：
  1. 環境變數 PM_TELEGRAM_BOT_TOKEN, PM_TELEGRAM_CHAT_ID
  2. config/telegram.yaml（bot_token, chat_id, allowed_chat_ids, admin_chat_ids）
"""

import importlib.util
import json
import logging
import math
import os
import sys
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

PROJ_DIR = Path(__file__).resolve().parent

SEPARATOR = "────────────────"  # 16 個 ─，頁面分隔線
CITIES_PER_PAGE = 10  # 城市列表每頁數量

# ── 峰值時段靜態資料（啟動時讀，build_peak_hours.py 離線產生）─────────────
_PEAK_HOURS_PATH = PROJ_DIR / "config" / "city_peak_hours.json"
_peak_hours: dict = {}
try:
    if _PEAK_HOURS_PATH.exists():
        import json as _json_tmp
        _peak_hours = _json_tmp.loads(_PEAK_HOURS_PATH.read_text(encoding="utf-8"))
        del _json_tmp
except Exception as _e:
    pass  # 無峰值資料時優雅降級，_get_peak_info() 回空字串

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ── python-telegram-bot 可選 ──────────────────────────────────
try:
    from telegram import (
        Update,
        InlineKeyboardButton,
        InlineKeyboardMarkup,
        ReplyKeyboardMarkup,
        BotCommand,
    )
    from telegram.ext import (
        Application,
        CommandHandler,
        CallbackQueryHandler,
        ConversationHandler,
        MessageHandler,
        ContextTypes,
        filters,
    )
    _HAS_PTB = True
except ImportError:
    _HAS_PTB = False
    log.error("python-telegram-bot not installed. Run: pip install python-telegram-bot")

# ── signal_reader ─────────────────────────────────────────────
_lib_dir = PROJ_DIR / "_lib"
if str(_lib_dir) not in sys.path:
    sys.path.insert(0, str(_lib_dir))
try:
    from signal_reader import SignalDataReader  # type: ignore[import]
    _HAS_READER = True
except ImportError as e:
    _HAS_READER = False
    log.error(f"signal_reader not found: {e}")

# ── position_manager lazy loader ──────────────────────────────
_pos_mgr_mod_bot = None

def _get_pos_mgr_for_bot():
    """Lazy-load 16_position_manager（Bot 用，寫持倉）。"""
    global _pos_mgr_mod_bot
    if _pos_mgr_mod_bot is None:
        spec = importlib.util.spec_from_file_location(
            "position_manager_16_bot", PROJ_DIR / "16_position_manager.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _pos_mgr_mod_bot = mod
    return _pos_mgr_mod_bot

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
# UserManager（users.json — 獨立於 telegram.yaml 連線設定）
# ============================================================

class UserManager:
    """
    管理 data/users.json。
    讀寫 allowed_chat_ids / admin_chat_ids，不碰 telegram.yaml。
    """
    SCHEMA_VERSION = "users_v1"

    def __init__(self, path: str = "data/users.json"):
        self.path = PROJ_DIR / path

    def _empty(self) -> dict:
        return {
            "schema_version": self.SCHEMA_VERSION,
            "updated_at_utc": "",
            "allowed_chat_ids": [],
            "admin_chat_ids": [],
            "user_details": {},
        }

    def load(self) -> dict:
        if not self.path.exists():
            return self._empty()
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning(f"UserManager.load failed: {e}")
            return self._empty()

    def save(self, data: dict) -> None:
        """原子寫入。"""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with tempfile.NamedTemporaryFile(
                "w", dir=str(self.path.parent), suffix=".tmp",
                delete=False, encoding="utf-8"
            ) as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                tmp = f.name
            os.replace(tmp, str(self.path))
        except Exception as e:
            log.warning(f"UserManager.save failed: {e}")

    def bootstrap_from_chat_id(self, chat_id: str) -> None:
        """如果 users.json 不存在，用 chat_id 建立初始 admin。"""
        if self.path.exists():
            return
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        data = self._empty()
        data["updated_at_utc"] = now
        data["allowed_chat_ids"] = [str(chat_id)]
        data["admin_chat_ids"] = [str(chat_id)]
        data["user_details"][str(chat_id)] = {
            "display_name": "owner",
            "added_at_utc": now,
            "added_by": "bootstrap",
            "note": "auto-created from telegram.yaml chat_id",
        }
        self.save(data)
        log.info(f"UserManager: bootstrapped users.json (chat_id={chat_id})")

    def get_allowed(self) -> set[str]:
        return set(self.load().get("allowed_chat_ids", []))

    def get_admins(self) -> set[str]:
        return set(self.load().get("admin_chat_ids", []))

    def is_allowed(self, chat_id) -> bool:
        return str(chat_id) in self.get_allowed()

    def add_user(self, chat_id: str) -> bool:
        """Add to allowed_chat_ids. Returns False if already exists."""
        data = self.load()
        lst = data.setdefault("allowed_chat_ids", [])
        if str(chat_id) in lst:
            return False
        lst.append(str(chat_id))
        data["updated_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.save(data)
        return True

    def remove_user(self, chat_id: str) -> bool:
        """Remove from allowed_chat_ids and admin_chat_ids. Returns False if not found."""
        data = self.load()
        lst = data.get("allowed_chat_ids", [])
        if str(chat_id) not in lst:
            return False
        lst.remove(str(chat_id))
        data["allowed_chat_ids"] = lst
        admins = data.get("admin_chat_ids", [])
        if str(chat_id) in admins:
            admins.remove(str(chat_id))
            data["admin_chat_ids"] = admins
        data["updated_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.save(data)
        return True

    def list_users(self) -> list[dict]:
        """Return list of {chat_id, is_admin} sorted by chat_id."""
        data = self.load()
        allowed = set(data.get("allowed_chat_ids", []))
        admins = set(data.get("admin_chat_ids", []))
        return [
            {"chat_id": cid, "is_admin": cid in admins}
            for cid in sorted(allowed)
        ]


# ConversationHandler state constants
ENTRY_PRICE, ENTRY_SHARES, ENTRY_CONFIRM, ENTRY_DUPLICATE_CHECK = range(4)
EXIT_PRICE, EXIT_CONFIRM = range(4, 6)
ADMIN_ADD_WAIT_ID = 6
ADMIN_DEL_WAIT_ID = 7
ADMIN_DEL_CONFIRM = 8


# ============================================================
# 工具函數
# ============================================================

def _safe_float(v, default=0.0):
    """
    Safe float coercion. Returns `default` for None/empty/invalid input.

    **P2-4**：signature 改成接受任意 default（`Optional[float]` 實務），
    原本的型別註解 `default: float = 0.0` 與多處 `_safe_float(..., None)` 用法衝突。
    """
    if v is None or v == "":
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _fmt_pct(v) -> str:
    """Format float as '+12.3%' or '-5.1%'."""
    return f"{_safe_float(v):+.1%}"


def _fmt_usd(v) -> str:
    return f"${_safe_float(v):.0f}"


def _signal_emoji(row: dict) -> str:
    action = row.get("signal_action", "")
    if action in ("BUY_YES", "BUY_NO"):
        return "🟢"
    ye = _safe_float(row.get("yes_edge"), -999)
    ne = _safe_float(row.get("no_edge"), -999)
    if max(ye, ne) < -0.03:
        return "🔴"
    return "⚪"


def _best_edge(row: dict) -> float:
    return max(_safe_float(row.get("yes_edge"), -999), _safe_float(row.get("no_edge"), -999))


def _best_depth(row: dict) -> float:
    return max(_safe_float(row.get("yes_sweet_usd"), 0), _safe_float(row.get("no_sweet_usd"), 0))


def _model_prob_str(row: dict) -> str:
    """
    模型機率字串（YES/NO 擇高顯示）。p_yes 缺失時回傳 "模型—"。

    signal_reader._parse_row() 會把空字串轉成 None；若直接走 _safe_float() 預設 0.0，
    會造成「沒資料卻顯示模型NO 100%」的假訊號。這裡明確檢查 None/空字串。
    """
    raw = row.get("p_yes")
    if raw is None or raw == "":
        return "模型—"
    try:
        p_yes = float(raw)
    except (TypeError, ValueError):
        return "模型—"
    p_no = 1.0 - p_yes
    if p_yes >= p_no:
        return f"模型YES {p_yes*100:.0f}%"
    return f"模型NO {p_no*100:.0f}%"


def _truncate(text: str, limit: int = 4000) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 4] + "\n..."


def _market_id_short(market_id: str) -> str:
    """Truncate market_id for use in callback_data (leave room for prefix)."""
    return market_id[:46]


def _calc_settlement_hours(market_date_local: str, city_tz: str = "") -> Optional[float]:
    """
    Returns hours until settlement (midnight at end of market_date_local in city timezone).

    **P2-3**：拿掉 pytz fallback 路徑。pytz 需要 tz.localize(d) 才能正確 localize naive datetime，
    直接 `datetime(..., tzinfo=pytz_tz)` 會套用 LMT（本地平均時）偏移，結果錯。
    Python 3.9+ 都有 ZoneInfo，無需 pytz。
    """
    if not market_date_local:
        return None
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(city_tz) if city_tz else timezone.utc
        d = datetime.strptime(market_date_local, "%Y-%m-%d")
        # 結算時刻 = market_date 的隔天 00:00 local
        settlement_local = datetime(d.year, d.month, d.day, tzinfo=tz) + timedelta(days=1)
        settlement_utc = settlement_local.astimezone(timezone.utc)
        return (settlement_utc - datetime.now(timezone.utc)).total_seconds() / 3600
    except Exception as e:
        log.debug(f"_calc_settlement_hours({market_date_local}, {city_tz}): {e}")
        return None


def _fmt_date(date_str: str) -> str:
    """'2026-04-09' → '04/09'"""
    if not date_str or len(date_str) < 10:
        return date_str
    return date_str[5:].replace("-", "/")


def _fmt_contract_temp(row: dict) -> str:
    """格式化合約溫度顯示（考慮 temp_unit 和 market_type）"""
    temp_unit = row.get("temp_unit", "C")
    unit = "°F" if temp_unit == "F" else "°C"
    market_type = row.get("market_type", "")
    threshold = row.get("threshold")
    range_low = row.get("range_low")
    range_high = row.get("range_high")
    if market_type == "range" and range_low and range_high:
        return f"{range_low}–{range_high}{unit}"
    elif market_type in ("higher", "above") and threshold:
        return f"↑{threshold}{unit}"
    elif market_type == "below" and threshold:
        return f"↓{threshold}{unit}"
    elif threshold:
        return f"{threshold}{unit}"
    return f"?{unit}"


def _fmt_settlement(market_date_local: str, city_tz: str = "") -> str:
    """結算倒數格式化：X天Y小時 / Y小時 / Z分鐘"""
    h = _calc_settlement_hours(market_date_local, city_tz=city_tz)
    if h is None:
        return "?"
    if h < 0:
        return "已結算"
    total_hours = int(h)
    days = total_hours // 24
    hours = total_hours % 24
    if days > 0:
        return f"{days}天{hours}小時"
    if total_hours > 0:
        return f"{total_hours}小時"
    minutes = max(1, int(h * 60))
    return f"{minutes}分鐘"


def _fmt_settlement_full(market_date_local: str, city_tz: str = "") -> str:
    """結算：台北時間 MM/DD HH:MM（倒數X天X小時）"""
    taipei_str = _calc_settlement_taipei(market_date_local, city_tz)
    countdown = _fmt_settlement(market_date_local, city_tz=city_tz)
    if countdown == "已結算":
        return "結算：已結算"
    if taipei_str:
        return f"結算：台北時間 {taipei_str}（倒數{countdown}）"
    return f"結算：（倒數{countdown}）"


def _back_btn(label: str = "🔙 主選單") -> list:
    return [InlineKeyboardButton(label, callback_data="menu")]


# ============================================================
# 台北時間 helpers（UTC+8）
# ============================================================

TAIPEI_TZ = timezone(timedelta(hours=8))


def _to_taipei(utc_dt: datetime) -> datetime:
    """UTC datetime → 台北 datetime（UTC+8）。"""
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    return utc_dt.astimezone(TAIPEI_TZ)


def _fmt_taipei(utc_dt: datetime) -> str:
    """UTC datetime → '04/11 07:00'（台北時間）。"""
    return _to_taipei(utc_dt).strftime("%m/%d %H:%M")


def _fmt_taipei_from_timestamp(ts) -> str:
    """
    Unix timestamp（int/float/numeric str）或 ISO 字串 → '04/11 07:00'（台北時間）。
    失敗回傳空字串。

    **P1-4**：WU v3 的 validTimeUtc 是 epoch 秒；collector 存成 int，但舊 latest_obs.json
    可能殘留 stringified int（"1712846700"）。這裡同時接受 int/float/numeric string/ISO。
    """
    if ts is None or ts == "":
        return ""
    # int/float：直接用
    if isinstance(ts, (int, float)):
        try:
            dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            return _fmt_taipei(dt)
        except (ValueError, OSError):
            return ""
    # string：先試 numeric epoch，再試 ISO
    if isinstance(ts, str):
        s = ts.strip()
        if not s:
            return ""
        # 純數字 → epoch
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            try:
                dt = datetime.fromtimestamp(int(s), tz=timezone.utc)
                return _fmt_taipei(dt)
            except (ValueError, OSError):
                return ""
        # 帶小數點的數字字串
        try:
            f = float(s)
            # 粗略 sanity：epoch 秒 > 1e9（2001+）
            if f > 1e9:
                dt = datetime.fromtimestamp(f, tz=timezone.utc)
                return _fmt_taipei(dt)
        except ValueError:
            pass
        # ISO
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return _fmt_taipei(dt)
        except ValueError:
            return ""
    return ""


def _fmt_obs_age(fetched_at_utc: str) -> str:
    """Returns '（X分鐘前更新）' or empty string."""
    if not fetched_at_utc:
        return ""
    try:
        ft = datetime.fromisoformat(fetched_at_utc.replace("Z", "+00:00"))
        age_min = int((datetime.now(timezone.utc) - ft).total_seconds() / 60)
        return f"（{age_min}分鐘前更新）"
    except Exception:
        return ""


def _calc_settlement_taipei(market_date_str: str, city_tz_str: str) -> str:
    """計算結算時間（台北時間字串，格式 'MM/DD HH:MM'）。失敗回傳空字串。"""
    try:
        import zoneinfo
        from datetime import date as _date
        tz = zoneinfo.ZoneInfo(city_tz_str)
        md = _date.fromisoformat(market_date_str)
        next_day = md + timedelta(days=1)
        settlement_local = datetime(next_day.year, next_day.month, next_day.day, 0, 0, tzinfo=tz)
        return _fmt_taipei(settlement_local.astimezone(timezone.utc))
    except Exception:
        return ""


def _read_latest_obs() -> dict:
    """讀 data/observations/latest_obs.json → {city: obs_dict}。失敗回傳空 dict。

    **P2-5**：JSON 解析失敗時記 log.warning，避免檔案損壞時 Bot 無聲無息什麼都顯示不出來。
    """
    obs_path = PROJ_DIR / "data" / "observations" / "latest_obs.json"
    if not obs_path.exists():
        return {}
    try:
        raw = json.loads(obs_path.read_text(encoding="utf-8"))
        return raw.get("cities", raw) if isinstance(raw, dict) else {}
    except Exception as e:
        log.warning(f"_read_latest_obs: parse failed ({e}) — returning empty")
        return {}


# Note: 舊版規劃過 data/results/signal_summary.json 作為預計算 JSON 快取，但
# signal_main 從未實作此 writer。Bot 改透過 SignalDataReader 直讀 per-city
# ev_signals.csv，並由 reader 內部的 mtime cache 提供同等的「讀記憶體」效能（v7.3.9）。
# 此處移除 _read_signal_summary 與 4 個 handler 的 fallback 分支。


_MONTH_TO_SEASON = {
    1: "winter", 2: "winter", 3: "spring",
    4: "spring", 5: "spring", 6: "summer",
    7: "summer", 8: "summer", 9: "autumn",
    10: "autumn", 11: "autumn", 12: "winter",
}


def _get_peak_info(city: str, market_date_str: str, city_tz_str: str) -> str:
    """回傳峰值時段字串（schema_version=2，按季節）。無資料或解析失敗回傳空字串。

    當天：「峰值：MM/DD HH:MM-HH:MM ⏳/🔥/✅」
    其他日：「峰值：MM/DD HH:MM-HH:MM」
    """
    city_data = _peak_hours.get(city, {})
    if not city_data or not isinstance(city_data, dict):
        return ""
    seasons = city_data.get("seasons", {})
    if not seasons:
        return ""
    try:
        month = int(market_date_str[5:7])
    except Exception:
        return ""
    season = _MONTH_TO_SEASON.get(month)
    if not season:
        return ""
    peak = seasons.get(season)
    if not peak:
        return ""
    start_local = int(peak["start"])
    end_local = int(peak["end"])
    try:
        import zoneinfo as _zi
        from datetime import date as _date
        tz = _zi.ZoneInfo(city_tz_str)
        md = _date.fromisoformat(market_date_str)
        start_dt = datetime(md.year, md.month, md.day, start_local, 0, tzinfo=tz)
        end_dt = datetime(md.year, md.month, md.day, end_local, 0, tzinfo=tz)
        start_tp = _to_taipei(start_dt)
        end_tp = _to_taipei(end_dt)
        start_taipei = start_tp.strftime("%H:%M")
        # 跨台北午夜時，end 顯示含日期；同日則只顯示時間
        if start_tp.date() == end_tp.date():
            end_taipei = end_tp.strftime("%H:%M")
        else:
            end_taipei = end_tp.strftime("%m/%d %H:%M")
        date_prefix = start_tp.strftime("%m/%d")  # 從台北轉換後的 datetime 取日期
        now_local = datetime.now(tz)
        if md == now_local.date():
            current_hour = now_local.hour
            # P2-2：峰值窗口跨午夜處理（實務上天氣峰值多在下午，但仍應穩健）
            if start_local <= end_local:
                # 正常窗口（例如 13-17）
                before_peak = current_hour < start_local
                in_peak = start_local <= current_hour <= end_local
            else:
                # 跨午夜窗口（例如 22-02）
                before_peak = end_local < current_hour < start_local
                in_peak = current_hour >= start_local or current_hour <= end_local
            if before_peak:
                hours_to_peak = (start_dt - datetime.now(tz)).total_seconds() / 3600
                h_int = max(0, int(hours_to_peak))
                status = f"（倒數{h_int}小時）" if h_int > 0 else ""
            elif in_peak:
                status = "🔥峰值中"
            else:
                status = "✅已過峰值"
            return f"峰值：{date_prefix} {start_taipei}-{end_taipei} {status}"
        else:
            return f"峰值：{date_prefix} {start_taipei}-{end_taipei}"
    except Exception:
        return ""


# ============================================================
# 城市頁排序 helper
# ============================================================

_CITY_TYPE_PRIORITY = {"below": 0, "exact": 1, "range": 1, "above": 2, "higher": 2}


def _city_sort_key(row: dict):
    """城市合約列表排序 key：(溫度下界, 類型優先序)，由小到大。"""
    market_type = row.get("market_type", "")
    try:
        threshold = float(row.get("threshold") or 9999)
    except (ValueError, TypeError):
        threshold = 9999
    try:
        range_low = float(row.get("range_low") or threshold)
    except (ValueError, TypeError):
        range_low = threshold
    temp = range_low if market_type == "range" else threshold
    priority = _CITY_TYPE_PRIORITY.get(market_type, 1)
    return (temp, priority)


def _fmt_contract_block(row: dict) -> str:
    """
    Unified contract display block (no leading indent).
    Each info on its own line with ▲ prefix.

    已鎖定:   "temp  已鎖定（已超過）"             (1 line)
    有價格:   temp
              ▲ YES $X / NO $Y
              ▲ YES：P% / NO：Q%
              [▲ 買YES  市場$X→$P（+E%）D$D]      (if yes_edge > 1%)
              [▲ 買NO   市場$Y→$Q（+F%）D$D]      (if no_edge > 1%)
    無掛單:   temp
              ▲ YES：P% / NO：Q%
              ▲ 無掛單
    價格過時: temp
              ▲ YES：P% / NO：Q%
              ▲ 價格過時
    """
    temp_str = _fmt_contract_temp(row)
    clipped = str(row.get("observation_clipped", "")).lower() == "true"

    if clipped:
        return f"{temp_str}  已鎖定（已超過）"

    p_yes_raw = row.get("p_yes")
    if p_yes_raw is not None and p_yes_raw != "":
        try:
            p_yes = float(p_yes_raw)
            p_no = 1.0 - p_yes
        except (ValueError, TypeError):
            p_yes = p_no = None
    else:
        p_yes = p_no = None

    prob_str = (
        f"YES：{p_yes*100:.0f}% / NO：{p_no*100:.0f}%"
        if p_yes is not None else "YES：—% / NO：—%"
    )

    yes_ask = _safe_float(row.get("yes_ask_price"), None)
    no_ask = _safe_float(row.get("no_ask_price"), None)

    lines = [temp_str]

    # 無掛單 / 價格過時（skip price lines）
    if yes_ask is None and no_ask is None:
        lines.append(f"▲ {prob_str}")
        lines.append("▲ 無掛單")
        return "\n".join(lines)

    if row.get("signal_status", "") == "stale_price":
        lines.append(f"▲ {prob_str}")
        lines.append("▲ 價格過時")
        return "\n".join(lines)

    # 有價格
    y_str = f"YES ${yes_ask:.2f}" if yes_ask is not None else "YES $—"
    n_str = f"NO ${no_ask:.2f}" if no_ask is not None else "NO $—"
    lines.append(f"▲ {y_str} / {n_str}")
    lines.append(f"▲ {prob_str}")

    yes_edge = _safe_float(row.get("yes_edge"), 0.0)
    no_edge = _safe_float(row.get("no_edge"), 0.0)

    if yes_edge > 0.01 and p_yes is not None and yes_ask is not None:
        yes_depth = _safe_float(row.get("yes_sweet_usd"), 0) or _safe_float(row.get("yes_depth_usd"), 0)
        d_str = f"D${yes_depth:.0f}" if yes_depth > 0 else ""
        lines.append(f"▲ 買YES ${yes_ask:.2f}→${p_yes:.2f}（+{yes_edge*100:.1f}%）{d_str}".rstrip())

    if no_edge > 0.01 and p_no is not None and no_ask is not None:
        no_depth = _safe_float(row.get("no_sweet_usd"), 0) or _safe_float(row.get("no_depth_usd"), 0)
        d_str = f"D${no_depth:.0f}" if no_depth > 0 else ""
        lines.append(f"▲ 買NO ${no_ask:.2f}→${p_no:.2f}（+{no_edge*100:.1f}%）{d_str}".rstrip())

    return "\n".join(lines)


# ============================================================
# Config loader
# ============================================================

def load_telegram_config_full() -> Optional[dict]:
    """
    環境變數優先，yaml 次之。
    allowed_chat_ids / admin_chat_ids 來自 data/users.json（不再讀 telegram.yaml 的 list fields）。
    如果 users.json 不存在，自動用 chat_id bootstrap。

    回傳包含 bot_token, chat_id, allowed_chat_ids (set), admin_chat_ids (set) 的 dict，
    或 None（沒有 config）。
    """
    token = os.environ.get("PM_TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("PM_TELEGRAM_CHAT_ID")

    yaml_path = PROJ_DIR / "config" / "telegram.yaml"
    if yaml_path.exists():
        try:
            cfg: dict = {}
            for line in yaml_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if ":" not in line:
                    continue
                k, _, v = line.partition(":")
                cfg[k.strip()] = v.strip()

            token = token or cfg.get("bot_token", "").strip('"').strip("'")
            chat_id = chat_id or cfg.get("chat_id", "").strip('"').strip("'")
            enabled = cfg.get("enabled", "true").lower() not in ("false", "0", "no")
            if not enabled:
                log.info("Telegram disabled in config/telegram.yaml")
                return None
        except Exception as e:
            log.warning(f"Failed to parse telegram.yaml: {e}")

    if not token or not chat_id:
        return None

    # Bootstrap users.json from chat_id if not exists; then read allowed/admins from it
    um = UserManager()
    um.bootstrap_from_chat_id(str(chat_id))
    allowed = um.get_allowed()
    admins = um.get_admins()

    # Fallback: if users.json is empty (e.g. someone deleted entries), default to chat_id
    if not allowed:
        allowed.add(str(chat_id))
    if not admins:
        admins.add(str(chat_id))

    return {
        "bot_token": token,
        "chat_id": chat_id,
        "allowed_chat_ids": allowed,
        "admin_chat_ids": admins,
    }


# ============================================================
# WeatherSignalBot
# ============================================================

class WeatherSignalBot:
    def __init__(self, config: dict):
        self.reader = SignalDataReader()
        self.allowed: set[str] = config["allowed_chat_ids"]
        self.admins: set[str] = config["admin_chat_ids"]
        self.user_mgr = UserManager()
        self.page_size = 5
        self.city_page_size = 4

    # ── 權限檢查 ──────────────────────────────────────────────

    def _is_allowed(self, chat_id) -> bool:
        return str(chat_id) in self.allowed

    def _is_admin(self, chat_id) -> bool:
        return str(chat_id) in self.admins

    async def _check_access(self, update: "Update") -> bool:
        if not self._is_allowed(update.effective_chat.id):
            cid = update.effective_chat.id
            msg = f"請找管理員授權\n\n你的 ID：<code>{cid}</code>\n請將此 ID 傳給管理員"
            if update.message:
                await update.message.reply_text(msg, parse_mode="HTML")
            elif update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(msg, parse_mode="HTML")
            return False
        return True

    async def _check_admin(self, update: "Update") -> bool:
        if not self._is_admin(update.effective_chat.id):
            msg = "需要管理員權限"
            if update.callback_query:
                await update.callback_query.answer(msg, show_alert=True)
            elif update.message:
                await update.message.reply_text(msg)
            return False
        return True

    # ── 通用回覆 ──────────────────────────────────────────────

    async def _reply(
        self,
        update: "Update",
        text: str,
        keyboard: list = None,
    ) -> None:
        """統一回覆：callback query → edit；command → reply_text。"""
        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        text = _truncate(text)
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(
                text, reply_markup=reply_markup, parse_mode="HTML"
            )
        else:
            await update.message.reply_text(
                text, reply_markup=reply_markup, parse_mode="HTML"
            )

    # ── /start 和 /menu ───────────────────────────────────────

    async def cmd_start(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_access(update):
            return
        _update_system_health("telegram_bot", {
            "status": "running",
            "pid": os.getpid(),
            "last_callback_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        buttons = [["今日", "預警6-8h", "結算<6h"], ["排行", "城市", "管理"]]
        reply_keyboard = ReplyKeyboardMarkup(buttons, resize_keyboard=True)
        await update.message.reply_text("系統啟動", reply_markup=reply_keyboard)

    async def _show_main_menu_cb(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_access(update):
            return
        await self._show_main_menu(update, context)

    async def _show_main_menu(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        _update_system_health("telegram_bot", {
            "status": "running",
            "pid": os.getpid(),
            "last_callback_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        state = self.reader.get_signal_state()
        last_refresh = state.get("last_success_utc", "未知")
        ready_count = state.get("ready_city_count", "?")

        keyboard = [
            [InlineKeyboardButton("🔄 刷新資料", callback_data="refresh")],
            [InlineKeyboardButton("🏙️ 選城市", callback_data="cities")],
            [InlineKeyboardButton("📈 信號排行", callback_data="rank:edge:0")],
            [InlineKeyboardButton("💼 我的持倉", callback_data="positions")],
            [InlineKeyboardButton("📋 通報歷史", callback_data="history")],
            [InlineKeyboardButton("⚙️ 設定", callback_data="settings")],
            [InlineKeyboardButton("📡 城市管理", callback_data="city_mgmt")],
        ]
        text = (
            "📊 <b>Weather Signal Bot</b>\n\n"
            f"Ready 城市：{ready_count} 個\n"
            f"最近刷新：{last_refresh}\n\n"
            "選擇功能："
        )
        await self._reply(update, text, keyboard)

    # ── Reply keyboard 觸發的 command handlers ─────────────────

    async def cmd_ranking_msg(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        """排行 Reply keyboard button → > 24h active 信號"""
        if not await self._check_access(update):
            return
        await self._render_ranking(update, "edge", 0)

    async def cmd_today_msg(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        """今日 Reply keyboard button → <= 24h active 信號"""
        if not await self._check_access(update):
            return
        await self._render_today(update, "edge", 0)

    async def cmd_warning_msg(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        """預警 Reply keyboard button → 6-8h last_forecast_warning 信號"""
        if not await self._check_access(update):
            return
        await self._render_warning(update, "edge", 0)

    async def cmd_cities(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        await self.cb_cities(update, context)

    async def cmd_positions(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        await self.cb_positions(update, context)

    async def cmd_alert_history_msg(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        await self.cb_alert_history(update, context)

    async def cmd_settings(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        await self.cb_settings(update, context)

    async def cmd_admin(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        await self.cb_admin_panel(update, context)

    # ── 城市列表 ──────────────────────────────────────────────

    def _build_city_keyboard(self, cities: list, page: int = 1) -> list:
        """城市列表 inline keyboard，含分頁（每頁 CITIES_PER_PAGE 個）。"""
        sorted_cities = sorted(cities)
        total_pages = math.ceil(len(sorted_cities) / CITIES_PER_PAGE)
        page = max(1, min(page, total_pages))
        start = (page - 1) * CITIES_PER_PAGE
        end = start + CITIES_PER_PAGE
        page_cities = sorted_cities[start:end]

        keyboard = []
        for city in page_cities:
            dates = self.reader.get_available_dates(city)
            if len(dates) >= 2:
                date_hint = f"({_fmt_date(dates[0])} ~ {_fmt_date(dates[-1])})"
            elif len(dates) == 1:
                date_hint = f"({_fmt_date(dates[0])})"
            else:
                date_hint = ""
            keyboard.append([
                InlineKeyboardButton(
                    f"{city} {date_hint}", callback_data=f"city:{city}:latest"
                )
            ])

        if total_pages > 1:
            page_btns = []
            for p in range(1, total_pages + 1):
                label = f"✓{p}" if p == page else str(p)
                page_btns.append(InlineKeyboardButton(
                    label, callback_data=f"city_page:{p}"
                ))
            keyboard.append(page_btns)

        return keyboard

    async def cb_cities(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_access(update):
            return
        cities = self.reader.get_ready_cities()
        if not cities:
            await self._reply(update, "目前沒有 ready 城市", [])
            return
        keyboard = self._build_city_keyboard(cities, page=1)
        await self._reply(update, "選擇城市", keyboard)

    async def cb_city_page(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        """城市列表翻頁：city_page:{page}"""
        if not await self._check_access(update):
            return
        data = update.callback_query.data  # "city_page:{page}"
        try:
            page = int(data.split(":", 1)[1])
        except (ValueError, IndexError):
            page = 1
        cities = self.reader.get_ready_cities()
        if not cities:
            await update.callback_query.answer("目前沒有 ready 城市")
            return
        keyboard = self._build_city_keyboard(cities, page=page)
        await update.callback_query.answer()
        await update.callback_query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    # ── 城市信號頁（全合約梯形 + 日期按鈕 + 台北時間）──────────

    async def _render_city_signals(
        self,
        update: "Update",
        city: str,
        date: str,
        contract_offset: int = 0,
    ) -> None:
        """城市議題頁核心渲染邏輯，供 cb_city_signals 和 cb_rank_jump 共用。"""
        dates = self.reader.get_available_dates(city)
        city_tz = self.reader.get_city_timezone(city)

        # 預設 = 最早「未結算」日期；"latest" 同樣邏輯
        if not date or date == "latest":
            default_date = None
            for d in dates:
                h = _calc_settlement_hours(d, city_tz=city_tz)
                if h is not None and h > 0:
                    default_date = d
                    break
            date = default_date or (dates[-1] if dates else "")

        # 若 date 不在列表裡（e.g. 已結算），用最早未結算
        if date and dates and date not in dates:
            default_date = None
            for d in dates:
                h = _calc_settlement_hours(d, city_tz=city_tz)
                if h is not None and h > 0:
                    default_date = d
                    break
            date = default_date or (dates[-1] if dates else "")

        all_signals = self.reader.get_city_signals(city, date if date else None)

        # === 標題 ===
        settlement_hours = _calc_settlement_hours(date, city_tz=city_tz)
        _h = settlement_hours if settlement_hours is not None else 999

        header = f"<b>{city}</b> — {_fmt_date(date)}\n{_fmt_settlement_full(date, city_tz)}"

        # 預報最高溫（從 signal rows 取）
        _pred_high_c = None
        _obs_unit = "C"
        for _r in all_signals:
            _praw = _r.get("predicted_daily_high", "")
            if _praw not in ("", None):
                try:
                    _pred_high_c = float(_praw)
                    _obs_unit = _r.get("temp_unit", "C")
                    break
                except (ValueError, TypeError):
                    pass

        def _fmt_temp_val(c_val: float, unit: str) -> str:
            return f"{c_val * 9/5 + 32:.1f}°F" if unit == "F" else f"{c_val:.1f}°C"

        # 實況（優先 latest_obs.json，fallback signal rows）
        _obs_high_c = None
        _obs_age_str = ""
        _latest = _read_latest_obs().get(city, {})
        if _latest.get("status") == "ok" and _latest.get("high_c") is not None:
            _obs_high_c = float(_latest["high_c"])
            _obs_age_str = _fmt_obs_age(_latest.get("fetched_at_utc", ""))
        else:
            for _r in all_signals:
                _oraw = _r.get("observed_high_c", "")
                if _oraw not in ("", None):
                    try:
                        _obs_high_c = float(_oraw)
                        _obs_unit = _r.get("temp_unit", "C")
                        break
                    except (ValueError, TypeError):
                        pass

        # 條件式溫度顯示（依結算距離決定顯示哪些欄位）
        # > 24h: 只顯示預報  8-24h: 預報+實況  < 6h: 只顯示實況
        if _h > 24:
            if _pred_high_c is not None:
                header += f"\n預報：{_fmt_temp_val(_pred_high_c, _obs_unit)}"
        elif _h >= 6:
            if _pred_high_c is not None:
                header += f"\n預報：{_fmt_temp_val(_pred_high_c, _obs_unit)}"
            if _obs_high_c is not None:
                header += f"\n實況：{_fmt_temp_val(_obs_high_c, _obs_unit)}{_obs_age_str}"
        else:
            if _obs_high_c is not None:
                header += f"\n實況：{_fmt_temp_val(_obs_high_c, _obs_unit)}{_obs_age_str}"

        # 峰值時段
        _peak = _get_peak_info(city, date, city_tz)
        if _peak:
            header += f"\n{_peak}"

        # === 合約列表（全部合約，溫度由小到大） ===
        sorted_signals = sorted(all_signals, key=_city_sort_key)

        if not sorted_signals:
            body = "（無合約資料）"
        else:
            body = "\n\n".join(_fmt_contract_block(row) for row in sorted_signals)

        text = header + "\n\n" + body

        # 字數護欄
        if len(text) > 3500:
            text = text[:3496] + "\n..."

        # === 鍵盤：底部顯示所有日期按鈕 ===
        keyboard = []
        if dates:
            date_buttons = [
                InlineKeyboardButton(
                    f"✓{_fmt_date(d)}" if d == date else _fmt_date(d),
                    callback_data=f"city:{city}:{d}:0"
                )
                for d in dates
            ]
            # 超過 4 個日期時分兩排
            if len(date_buttons) <= 4:
                keyboard.append(date_buttons)
            else:
                keyboard.append(date_buttons[:4])
                keyboard.append(date_buttons[4:])

        await self._reply(update, text, keyboard)

    async def cb_city_signals(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_access(update):
            return

        # callback_data: "city:{city}:{date}" or "city:{city}:{date}:{contract_offset}"
        data = update.callback_query.data
        parts = data.split(":", 3)
        if len(parts) < 3:
            await update.callback_query.answer("格式錯誤")
            return
        _, city, date = parts[0], parts[1], parts[2]
        try:
            contract_offset = int(parts[3]) if len(parts) > 3 else 0
        except ValueError:
            contract_offset = 0

        await self._render_city_signals(update, city, date, contract_offset)

    # ── 信號排行（含翻頁 + 排序切換）─────────────────────────

    async def cb_ranking(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_access(update):
            return
        data = update.callback_query.data  # "rank:{sort_by}:{offset}"
        parts = data.split(":")
        sort_by = parts[1] if len(parts) > 1 else "edge"
        try:
            offset = int(parts[2]) if len(parts) > 2 else 0
        except ValueError:
            offset = 0
        await self._render_ranking(update, sort_by, offset)

    async def cb_today(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_access(update):
            return
        data = update.callback_query.data  # "today:{sort_by}:{offset}"
        parts = data.split(":")
        sort_by = parts[1] if len(parts) > 1 else "edge"
        try:
            offset = int(parts[2]) if len(parts) > 2 else 0
        except ValueError:
            offset = 0
        await self._render_today(update, sort_by, offset)

    async def cb_warning(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_access(update):
            return
        data = update.callback_query.data  # "warn:{sort_by}:{offset}"
        parts = data.split(":")
        sort_by = parts[1] if len(parts) > 1 else "edge"
        try:
            offset = int(parts[2]) if len(parts) > 2 else 0
        except ValueError:
            offset = 0
        await self._render_warning(update, sort_by, offset)

    async def _render_ranking(
        self,
        update: "Update",
        sort_by: str = "edge",
        offset: int = 0,
    ) -> None:
        # Reader 內部 mtime cache（v7.3.9）讓重複按按鈕直接從記憶體回
        results, total = self.reader.get_all_signals_ranked(
            sort_by=sort_by, limit=self.page_size, offset=offset
        )
        sort_label = {"edge": "價差排名", "depth": "深度排名"}.get(sort_by, sort_by)
        await self._render_ranked_page(
            update, results, total, sort_by, offset,
            title=sort_label,
            empty_msg="目前沒有符合條件的信號",
            nav_prefix="rank",
        )

    async def _render_today(
        self,
        update: "Update",
        sort_by: str = "edge",
        offset: int = 0,
    ) -> None:
        results, total = self.reader.get_today_signals_ranked(
            sort_by=sort_by, limit=self.page_size, offset=offset
        )
        sort_label = {"edge": "📋 今日信號（8-24小時）", "depth": "📋 今日信號（8-24小時·深度）"}.get(sort_by, "📋 今日信號（8-24小時）")
        await self._render_ranked_page(
            update, results, total, sort_by, offset,
            title=sort_label,
            empty_msg="目前沒有今日信號",
            nav_prefix="today",
            show_forecast_peak=True,
        )

    async def _render_warning(
        self,
        update: "Update",
        sort_by: str = "edge",
        offset: int = 0,
    ) -> None:
        results, total = self.reader.get_warning_signals_ranked(
            sort_by=sort_by, limit=self.page_size, offset=offset
        )
        sort_label = {"edge": "⚠️ 預警（6-8小時）", "depth": "⚠️ 預警（6-8小時·深度）"}.get(sort_by, "⚠️ 預警（6-8小時）")
        await self._render_ranked_page(
            update, results, total, sort_by, offset,
            title=sort_label,
            empty_msg="目前沒有預警信號",
            nav_prefix="warn",
            show_forecast_peak=True,
        )

    async def _render_ranked_page(
        self,
        update: "Update",
        results: list,
        total: int,
        sort_by: str,
        offset: int,
        title: str,
        empty_msg: str,
        nav_prefix: str,
        show_forecast_peak: bool = False,
    ) -> None:
        """排行/今日/預警三頁共用渲染邏輯。

        show_forecast_peak=True（今日/預警）時每筆多顯示：
          - 預報最高溫 + 實況最高溫（有才顯示）
          - 峰值時段 + 當前狀態
        """
        lines = [f"<b>{title}</b>", SEPARATOR]
        jump_btns = []

        if not results:
            lines.append(empty_msg)
        else:
            for i, row in enumerate(results, start=offset + 1):
                # 排行/今日/預警頁不顯示已鎖定合約
                if str(row.get("observation_clipped", "")).lower() == "true":
                    continue
                city = row.get("city", "?")
                date = row.get("market_date_local", "?")
                city_tz = self.reader.get_city_timezone(city)

                entry = f"{i})"
                entry += f"\n  {city} · {_fmt_date(date)}"
                entry += f"\n  {_fmt_settlement_full(date, city_tz)}"

                if show_forecast_peak:
                    temp_unit = row.get("temp_unit", "C")
                    # 預報
                    pred_raw = row.get("predicted_daily_high", "")
                    if pred_raw not in ("", None):
                        try:
                            pred_c = float(pred_raw)
                            pred_str = (
                                f"{pred_c * 9/5 + 32:.1f}°F" if temp_unit == "F"
                                else f"{pred_c:.1f}°C"
                            )
                            entry += f"\n  預報：{pred_str}"
                        except (ValueError, TypeError):
                            pass
                    # 實況（優先 latest_obs.json）
                    _latest = _read_latest_obs().get(city, {})
                    if _latest.get("status") == "ok" and _latest.get("high_c") is not None:
                        obs_c = float(_latest["high_c"])
                        obs_str = (
                            f"{obs_c * 9/5 + 32:.1f}°F" if temp_unit == "F"
                            else f"{obs_c:.1f}°C"
                        )
                        obs_age = _fmt_obs_age(_latest.get("fetched_at_utc", ""))
                        entry += f"\n  實況：{obs_str}{obs_age}"
                    else:
                        obs_raw = row.get("observed_high_c", "")
                        if obs_raw not in ("", None):
                            try:
                                obs_c = float(obs_raw)
                                obs_str = (
                                    f"{obs_c * 9/5 + 32:.1f}°F" if temp_unit == "F"
                                    else f"{obs_c:.1f}°C"
                                )
                                entry += f"\n  實況：{obs_str}"
                            except (ValueError, TypeError):
                                pass
                    # 峰值
                    peak_info = _get_peak_info(city, date, city_tz)
                    if peak_info:
                        entry += f"\n  {peak_info}"

                # 空行 + 合約區塊（所有行縮排 2 格）
                entry += "\n"
                block = _fmt_contract_block(row)
                for bl in block.split("\n"):
                    entry += f"\n  {bl}"

                lines.append(entry)
                lines.append(SEPARATOR)
                cb = f"rank_jump:{city}:{date}"
                if len(cb) <= 64:
                    jump_btns.append(InlineKeyboardButton(f"#{i}", callback_data=cb))
            total_pages = (total + self.page_size - 1) // self.page_size
            current_page = offset // self.page_size + 1
            lines.append(f"{current_page}/{total_pages}")

        nav = []
        if offset > 0:
            nav.append(InlineKeyboardButton(
                "←", callback_data=f"{nav_prefix}:{sort_by}:{max(0, offset - self.page_size)}"
            ))
        if offset + self.page_size < total:
            nav.append(InlineKeyboardButton(
                "→", callback_data=f"{nav_prefix}:{sort_by}:{offset + self.page_size}"
            ))

        sort_btns = []
        for s, label_cn in [("edge", "價差"), ("depth", "深度")]:
            label = f"{label_cn} ✓" if s == sort_by else label_cn
            sort_btns.append(InlineKeyboardButton(label, callback_data=f"{nav_prefix}:{s}:0"))

        keyboard = []
        if jump_btns:
            keyboard.append(jump_btns)
        if nav:
            keyboard.append(nav)
        keyboard.append(sort_btns)
        await self._reply(update, "\n".join(lines), keyboard)

    # ── 信號詳情（簡化版）────────────────────────────────────

    async def cb_signal_detail(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_access(update):
            return

        data = update.callback_query.data  # "detail:{market_id_prefix}"
        market_id_prefix = data[7:]  # strip "detail:"
        row = self.reader.get_signal_detail(market_id_prefix)
        if not row:
            await update.callback_query.answer("找不到此市場", show_alert=True)
            return

        city = row.get("city", "?")
        date = row.get("market_date_local", "?")
        threshold = row.get("threshold", "?")
        action = row.get("signal_action", "")
        status = row.get("signal_status", "?")
        mid_short = _market_id_short(row.get("market_id", ""))

        # 決定方向
        side = "NO" if "NO" in action else "YES"
        if side == "NO":
            entry_price = _safe_float(row.get("no_ask_price"))
            edge = _safe_float(row.get("no_edge"))
            depth = _safe_float(row.get("no_sweet_usd"))
        else:
            entry_price = _safe_float(row.get("yes_ask_price"))
            edge = _safe_float(row.get("yes_edge"))
            depth = _safe_float(row.get("yes_sweet_usd"))

        target_price = min(1.0, entry_price * (1 + abs(edge))) if entry_price > 0 else 0.0
        settlement_h = _calc_settlement_hours(date, city_tz=self.reader.get_city_timezone(city))

        # 第一層：決策資訊
        action_line = (
            f"🟢 建議：買 {side}" if action in ("BUY_YES", "BUY_NO")
            else f"⚪ 信號：{action or '無'}"
        )
        lines = [
            f"📊 <b>{city} — {date}</b>",
            f"Contract: {_fmt_contract_temp(row)} | Status: {status}",
            "",
            action_line,
            f"Edge: {_fmt_pct(edge)} | 深度: {_fmt_usd(depth)}",
            "",
            f"進場價: ${entry_price:.3f}",
            f"目標賣價（粗估）: ~${target_price:.3f}",
        ]
        if settlement_h is not None:
            lines.append(f"結算: {settlement_h:.0f}h")

        keyboard = []
        # Admin + active + BUY signal → 記錄進場按鈕
        if self._is_admin(update.effective_chat.id) and action in ("BUY_YES", "BUY_NO"):
            keyboard.append([InlineKeyboardButton(
                "📝 記錄進場",
                callback_data=f"entry:{mid_short}:{action}",
            )])
        keyboard.append([InlineKeyboardButton(
            "📋 完整數據", callback_data=f"fulldata:{mid_short}"
        )])
        # 返回城市信號頁，fallback 到排行
        back_city = row.get("city", "")
        back_date = row.get("market_date_local", "")
        if back_city and back_date:
            keyboard.append([InlineKeyboardButton(
                "🔙 返回", callback_data=f"city:{back_city}:{back_date}:0"
            )])
        else:
            keyboard.append(_back_btn("🔙 排行"))
        await self._reply(update, "\n".join(lines), keyboard)

    # ── 完整數據展開 ──────────────────────────────────────────

    async def cb_full_data(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_access(update):
            return

        data = update.callback_query.data  # "fulldata:{market_id_prefix}"
        market_id_prefix = data.split(":", 1)[1]
        row = self.reader.get_signal_detail(market_id_prefix)
        if not row:
            await update.callback_query.answer("找不到此市場", show_alert=True)
            return

        city = row.get("city", "?")
        date = row.get("market_date_local", "?")
        threshold = row.get("threshold", "?")
        action = row.get("signal_action", "?")
        status = row.get("signal_status", "?")
        mid_short = _market_id_short(row.get("market_id", ""))

        lines = [
            f"📋 <b>完整數據</b>",
            "",
            f"{city} — {date} — {_fmt_contract_temp(row)}",
            f"Signal: {action} | Status: {status}",
            "",
            f"p_YES={_safe_float(row.get('p_yes')):.3f} | p_NO={_safe_float(row.get('p_no')):.3f}",
        ]

        if row.get("yes_ask_price") is not None:
            lines += [
                f"Market: YES={_safe_float(row.get('yes_ask_price')):.3f}"
                f" / NO={_safe_float(row.get('no_ask_price')):.3f}",
                f"Edge: YES={_fmt_pct(row.get('yes_edge'))}"
                f" / NO={_fmt_pct(row.get('no_edge'))}",
                f"EV: YES=${_safe_float(row.get('yes_ev')):+.4f}"
                f" / NO=${_safe_float(row.get('no_ev')):+.4f}",
            ]

        if row.get("yes_sweet_usd") is not None:
            lines += [
                "",
                "<b>Depth (Sweet Spot):</b>",
                f"YES: {_fmt_usd(row.get('yes_sweet_usd'))} @ avg {_safe_float(row.get('yes_sweet_avg_price')):.4f}"
                f" | EV=${_safe_float(row.get('yes_sweet_ev')):+.4f}",
                f"NO:  {_fmt_usd(row.get('no_sweet_usd'))} @ avg {_safe_float(row.get('no_sweet_avg_price')):.4f}"
                f" | EV=${_safe_float(row.get('no_sweet_ev')):+.4f}",
            ]

        # Extra fields if available
        for field, label in [
            ("fee_rate", "Fee rate"), ("signal_generated_utc", "Generated"),
            ("price_updated_utc", "Price updated"),
        ]:
            val = row.get(field)
            if val is not None:
                lines.append(f"{label}: {val}")

        keyboard = [[InlineKeyboardButton("🔙 返回詳情", callback_data=f"detail:{mid_short}")]]
        await self._reply(update, "\n".join(lines), keyboard)

    # ── 刷新（admin only）────────────────────────────────────

    async def cb_refresh(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_admin(update):
            return
        self.reader.request_refresh(str(update.effective_chat.id))
        await update.callback_query.answer("✅ 已排入刷新（約 30 秒內生效）")

    # ── 通報歷史 ──────────────────────────────────────────────

    async def cb_alert_history(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_access(update):
            return
        history = self.reader.get_alert_history(hours=24)
        if not history:
            text = "📋 最近 24 小時無通報"
        else:
            lines = ["📋 <b>最近 24 小時通報</b>\n"]
            for h in history[:20]:  # 最多顯示 20 筆
                action = h.get("signal_action", "")
                city = h.get("city", "?")
                contract = h.get("contract", "?")
                edge = h.get("edge", "?")
                generated = h.get("generated_utc", "?")[:16]  # YYYY-MM-DDTHH:MM
                sent = h.get("sent_telegram", "false") == "true"
                ok_str = "✅" if sent else "📝"
                try:
                    edge_str = f"{float(edge):+.1%}"
                except (ValueError, TypeError):
                    edge_str = str(edge)
                lines.append(f"{ok_str} {generated} | {city} {action} {edge_str} | {contract}")
            sent_count = sum(1 for h in history if h.get("sent_telegram") == "true")
            lines.append(f"\n共 {len(history)} 則 | Telegram 成功 {sent_count}")
            text = "\n".join(lines)

        keyboard = [_back_btn()]
        await self._reply(update, text, keyboard)

    # ── 設定（只讀）──────────────────────────────────────────

    async def cb_settings(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_access(update):
            return
        params = self.reader.get_trading_params()
        show_keys = [
            "alert_min_edge", "alert_min_depth_usd", "alert_require_positive_ev",
            "alert_cooldown_minutes", "alert_min_settlement_hours",
            "min_edge", "fee_rate", "fee_exponent",
            "kelly_fraction", "bankroll",
        ]
        lines = ["⚙️ <b>目前設定</b>\n"]
        for key in show_keys:
            if key in params:
                lines.append(f"  {key}: {params[key]}")
        lines.append("\n<i>修改請編輯 config/trading_params.yaml</i>")
        keyboard = [_back_btn()]
        await self._reply(update, "\n".join(lines), keyboard)

    # ── 城市管理（admin only）────────────────────────────────

    async def cb_city_management(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_access(update):
            return
        status = self.reader.get_city_status_all()
        if not status:
            await self._reply(update, "❌ city_status.json 不存在", [_back_btn()])
            return

        buckets: dict[str, list] = {
            "ready": [], "discovered": [], "no_metadata": [], "failed": [], "other": []
        }
        for city, info in status.items():
            s = info.get("status", "other")
            buckets.setdefault(s, []).append((city, info))

        lines = ["📡 <b>城市管理</b>\n"]
        if buckets["ready"]:
            lines.append("✅ <b>Ready：</b>")
            for city, info in sorted(buckets["ready"]):
                lines.append(
                    f"  {city} — {info.get('error_row_count', '?')} samples"
                    f" — {info.get('market_count_active', '?')} markets"
                )
        if buckets["discovered"]:
            lines.append("\n⏳ <b>Discovered（待回補）：</b>")
            for city, _ in sorted(buckets["discovered"]):
                lines.append(f"  {city}")
        if buckets["no_metadata"]:
            lines.append("\n❌ <b>No metadata：</b>")
            for city, _ in sorted(buckets["no_metadata"]):
                lines.append(f"  {city} — 需補 seed_cities.json")
        if buckets["failed"]:
            lines.append("\n💥 <b>Failed：</b>")
            for city, info in sorted(buckets["failed"]):
                lines.append(f"  {city} — {info.get('last_error', '?')}")

        is_admin = self._is_admin(update.effective_chat.id)
        keyboard = []
        if is_admin:
            keyboard.append([InlineKeyboardButton("🔍 觸發城市掃描", callback_data="scan_cities")])
        keyboard.append(_back_btn())
        await self._reply(update, "\n".join(lines), keyboard)

    async def cb_scan_cities(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_admin(update):
            return
        self.reader.request_city_scan()
        await update.callback_query.answer("✅ 已排入城市掃描（collector_main 下一輪執行）")

    # ── 持倉頁（STEP 13）─────────────────────────────────────

    async def cb_positions(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_access(update):
            return

        open_pos = self.reader.get_open_positions()
        closed_pos = self.reader.get_closed_positions()

        lines = ["💼 <b>我的持倉</b>\n"]

        if open_pos:
            lines.append("🟢 <b>Open：</b>")
            for p in open_pos:
                pnl = p.get("unrealized_pnl_gross")
                pnl_str = f"${pnl:+.2f}" if pnl is not None else "?"
                edge = p.get("current_edge")
                entry_edge = p.get("entry_edge", 0)
                edge_str = f"{float(edge):+.1%}" if edge is not None else "?"
                lines.append(
                    f"  {p.get('city', '?')} {p.get('contract_label', '?')} "
                    f"— {p.get('side', '?')} × {_safe_float(p.get('shares')):.0f} "
                    f"@ ${_safe_float(p.get('entry_price')):.2f}\n"
                    f"  Edge: {_safe_float(entry_edge):+.1%} → {edge_str} | PnL: {pnl_str}"
                )
        else:
            lines.append("（無 open 持倉）")

        if closed_pos:
            lines.append(f"\n🔒 <b>Closed：</b>（最近 5 筆）")
            for p in closed_pos[:5]:
                pnl = p.get("pnl_gross")
                pnl_str = f"${pnl:+.2f}" if pnl is not None else "?"
                lines.append(
                    f"  {p.get('city', '?')} {p.get('contract_label', '?')} "
                    f"— {p.get('side', '?')} | PnL: {pnl_str}"
                )

        keyboard = []
        # Admin 可查看持倉詳情（含平倉按鈕）
        if self._is_admin(update.effective_chat.id) and open_pos:
            for p in open_pos[:3]:  # 最多顯示 3 個快速平倉按鈕
                pid = p.get("position_id", "")
                city = p.get("city", "?")
                side = p.get("side", "?")
                keyboard.append([InlineKeyboardButton(
                    f"📤 平倉：{city} {side}",
                    callback_data=f"close_pos:{pid}",
                )])
        keyboard.append(_back_btn())
        await self._reply(update, "\n".join(lines), keyboard)

    # ── 進場 ConversationHandler（STEP 13）───────────────────

    async def start_entry(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        """進場記錄起點：取得 market 資訊，顯示建議價格。"""
        if not await self._check_admin(update):
            return ConversationHandler.END

        data = update.callback_query.data  # "entry:{market_id_prefix}:{action}"
        parts = data.split(":", 2)
        if len(parts) < 3:
            await update.callback_query.answer("格式錯誤")
            return ConversationHandler.END

        _, mid_prefix, action = parts
        row = self.reader.get_signal_detail(mid_prefix)
        if not row:
            await update.callback_query.answer("找不到此市場信號")
            return ConversationHandler.END

        # 儲存 context
        context.user_data["entry_market_id"] = row.get("market_id", "")
        context.user_data["entry_market_id_short"] = mid_prefix
        context.user_data["entry_action"] = action
        context.user_data["entry_city"] = row.get("city", "")
        context.user_data["entry_market_date"] = row.get("market_date_local", "")
        context.user_data["entry_contract"] = _fmt_contract_temp(row)
        side = "NO" if action == "BUY_NO" else "YES"
        context.user_data["entry_side"] = side

        # 建議進場價（ask price）
        if side == "NO":
            suggested = row.get("no_ask_price")
            token_id = row.get("no_token_id", "") or ""
            edge = row.get("no_edge")
        else:
            suggested = row.get("yes_ask_price")
            token_id = row.get("yes_token_id", "") or ""
            edge = row.get("yes_edge")

        # 守門：market_master.csv 缺 token_id 時拒絕進場（避免持倉無法下單）
        if not token_id:
            await update.callback_query.answer(
                "❌ 此市場 token_id 缺失，無法記錄進場。請檢查 market_master.csv",
                show_alert=True,
            )
            return ConversationHandler.END

        context.user_data["entry_token_id"] = token_id
        context.user_data["entry_suggested_price"] = suggested
        context.user_data["entry_edge"] = edge
        context.user_data["entry_ev"] = row.get(
            "no_ev" if side == "NO" else "yes_ev"
        )

        # Check for existing open position on this market
        existing = next((
            p for p in self.reader.get_open_positions()
            if p.get("market_id") == row.get("market_id")
        ), None)
        if existing:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(
                f"⚠️ 此合約已有 open 持倉（{existing.get('side', '?')} × "
                f"{_safe_float(existing.get('shares')):.0f}）。確定要再新增？",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("繼續新增", callback_data="entry_dup_ok")],
                    [InlineKeyboardButton("❌ 取消", callback_data="entry_cancel")],
                ]),
                parse_mode="HTML",
            )
            return ENTRY_DUPLICATE_CHECK

        # Skip price step — use suggested price directly
        if suggested is not None:
            price = _safe_float(suggested)
            context.user_data["entry_price"] = price
            text_lines = [
                f"📝 <b>記錄進場：{action}</b>",
                "",
                f"{row.get('city', '?')} — {row.get('market_date_local', '?')} — {_fmt_contract_temp(row)}",
                f"Edge: {_fmt_pct(edge)}",
                f"進場價: ${price:.3f}",
                "",
                "選擇股數：",
            ]
            keyboard = [
                [
                    InlineKeyboardButton(f"50 股 ≈${price*50:.1f}", callback_data="entry_shares:50"),
                    InlineKeyboardButton(f"100 股 ≈${price*100:.1f}", callback_data="entry_shares:100"),
                    InlineKeyboardButton(f"200 股 ≈${price*200:.1f}", callback_data="entry_shares:200"),
                ],
                [InlineKeyboardButton("自訂（輸入數字）", callback_data="entry_shares:custom")],
                [InlineKeyboardButton("❌ 取消", callback_data="entry_cancel")],
            ]
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(
                "\n".join(text_lines),
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML",
            )
            return ENTRY_SHARES

        # Fallback: no suggested price — ask manually
        text_lines = [
            f"📝 <b>記錄進場：{action}</b>",
            "",
            f"{row.get('city', '?')} — {row.get('market_date_local', '?')}",
            f"Contract: {_fmt_contract_temp(row)}",
            f"Edge: {_fmt_pct(edge)}",
            "",
            "請輸入進場價格：",
        ]
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            "\n".join(text_lines),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ 取消", callback_data="entry_cancel")]
            ]),
            parse_mode="HTML",
        )
        return ENTRY_PRICE

    async def use_suggested_price(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        data = update.callback_query.data  # "entry_suggested:{price}"
        try:
            price = float(data.split(":", 1)[1])
        except (ValueError, IndexError):
            await update.callback_query.answer("價格解析失敗")
            return ENTRY_PRICE
        context.user_data["entry_price"] = price
        return await self._ask_entry_shares(update, context)

    async def receive_entry_price(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        try:
            price = float(update.message.text.strip())
            if not (0.0 < price < 1.0):
                raise ValueError
        except ValueError:
            await update.message.reply_text("⚠️ 請輸入 0~1 之間的價格（例如 0.78）")
            return ENTRY_PRICE
        context.user_data["entry_price"] = price
        return await self._ask_entry_shares(update, context, via_message=True)

    async def _ask_entry_shares(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE", via_message: bool = False
    ) -> int:
        price = context.user_data.get("entry_price", 0)
        text = f"進場價格：${price:.3f}\n\n請選擇股數（shares）："
        keyboard = [
            [
                InlineKeyboardButton("50", callback_data="entry_shares:50"),
                InlineKeyboardButton("100", callback_data="entry_shares:100"),
                InlineKeyboardButton("200", callback_data="entry_shares:200"),
            ],
            [InlineKeyboardButton("自訂（輸入數字）", callback_data="entry_shares:custom")],
            [InlineKeyboardButton("❌ 取消", callback_data="entry_cancel")],
        ]
        markup = InlineKeyboardMarkup(keyboard)
        if via_message:
            await update.message.reply_text(text, reply_markup=markup, parse_mode="HTML")
        else:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(text, reply_markup=markup, parse_mode="HTML")
        return ENTRY_SHARES

    async def select_shares(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        data = update.callback_query.data  # "entry_shares:{shares|custom}"
        val = data.split(":", 1)[1]
        if val == "custom":
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(
                "請輸入股數（正整數，例如 150）：",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ 取消", callback_data="entry_cancel")
                ]]),
            )
            return ENTRY_SHARES
        try:
            shares = float(val)
        except ValueError:
            await update.callback_query.answer("解析失敗")
            return ENTRY_SHARES
        context.user_data["entry_shares"] = shares
        return await self._show_entry_confirm(update, context)

    async def receive_custom_shares(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        try:
            shares = float(update.message.text.strip())
            if shares <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("⚠️ 請輸入正整數（例如 150）")
            return ENTRY_SHARES
        context.user_data["entry_shares"] = shares
        return await self._show_entry_confirm(update, context, via_message=True)

    async def _show_entry_confirm(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE", via_message: bool = False
    ) -> int:
        d = context.user_data
        price = d.get("entry_price", 0)
        shares = d.get("entry_shares", 0)
        action = d.get("entry_action", "")
        city = d.get("entry_city", "?")
        contract = d.get("entry_contract", "?")
        market_date = d.get("entry_market_date", "?")
        edge = d.get("entry_edge")
        cost = price * shares
        lines = [
            "✅ <b>確認進場？</b>",
            "",
            f"  {city} — {market_date} — {contract}",
            f"  Action: {action}",
            f"  Price:  ${price:.3f} × {shares:.0f} shares",
            f"  Cost:   ≈${cost:.2f}",
            f"  Edge:   {_fmt_pct(edge)}",
        ]
        keyboard = [
            [InlineKeyboardButton("✅ 確認進場", callback_data="entry_confirm")],
            [InlineKeyboardButton("❌ 取消", callback_data="entry_cancel")],
        ]
        markup = InlineKeyboardMarkup(keyboard)
        if via_message:
            await update.message.reply_text(
                "\n".join(lines), reply_markup=markup, parse_mode="HTML"
            )
        else:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(
                "\n".join(lines), reply_markup=markup, parse_mode="HTML"
            )
        return ENTRY_CONFIRM

    async def confirm_entry(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        d = context.user_data
        try:
            mod = _get_pos_mgr_for_bot()
            pm = mod.PositionManager()
            pm.load()
            entry_price = _safe_float(d.get("entry_price"))
            shares = _safe_float(d.get("entry_shares"))
            edge = d.get("entry_edge")
            ev = d.get("entry_ev")
            # 簡單估算 fee per share
            fee_rate, fee_exp = 0.025, 0.5
            inner = max(0.0, entry_price * (1 - entry_price))
            fee_per_share = entry_price * fee_rate * (inner ** fee_exp)

            position_id = pm.add_position(
                market_id=d.get("entry_market_id", ""),
                token_id=d.get("entry_token_id", ""),
                city=d.get("entry_city", ""),
                market_date=d.get("entry_market_date", ""),
                contract_label=d.get("entry_contract", ""),
                side=d.get("entry_side", ""),
                entry_price=entry_price,
                shares=shares,
                entry_fee_per_share=fee_per_share,
                entry_edge=_safe_float(edge) if edge is not None else 0.0,
                entry_ev=_safe_float(ev) if ev is not None else 0.0,
                signal_action=d.get("entry_action", ""),
            )
            await update.callback_query.answer("✅ 已記錄進場")
            await update.callback_query.edit_message_text(
                f"✅ <b>進場已記錄</b>\n\nID: <code>{position_id}</code>\n"
                f"{d.get('entry_city', '?')} {d.get('entry_contract', '?')} "
                f"{d.get('entry_side', '?')} × {shares:.0f} @ ${entry_price:.3f}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("💼 我的持倉", callback_data="positions"),
                    InlineKeyboardButton("🏠", callback_data="menu"),
                ]]),
            )
        except ValueError as e:
            # 例如 market_id / token_id 缺失（position_manager 的硬守門）
            log.warning(f"confirm_entry ValueError: {e}")
            await update.callback_query.answer(f"❌ {e}", show_alert=True)
        except Exception as e:
            log.error(f"confirm_entry error: {e}")
            await update.callback_query.answer("❌ 記錄失敗，請重試")
        context.user_data.clear()
        return ConversationHandler.END

    async def cancel_entry(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        context.user_data.clear()
        if update.callback_query:
            await update.callback_query.answer("已取消")
            await update.callback_query.edit_message_text(
                "已取消進場記錄",
                reply_markup=InlineKeyboardMarkup([_back_btn()]),
            )
        return ConversationHandler.END

    async def confirm_duplicate_entry(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        """用戶確認在已有持倉的合約上繼續新增進場。"""
        suggested = context.user_data.get("entry_suggested_price")
        action = context.user_data.get("entry_action", "")
        edge = context.user_data.get("entry_edge")
        if suggested is not None:
            price = _safe_float(suggested)
            context.user_data["entry_price"] = price
            text_lines = [
                f"📝 <b>記錄進場：{action}</b>",
                "",
                f"{context.user_data.get('entry_city', '?')} — "
                f"{context.user_data.get('entry_market_date', '?')} — "
                f"{context.user_data.get('entry_contract', '?')}",
                f"Edge: {_fmt_pct(edge)}",
                f"進場價: ${price:.3f}",
                "",
                "選擇股數：",
            ]
            keyboard = [
                [
                    InlineKeyboardButton(f"50 股 ≈${price*50:.1f}", callback_data="entry_shares:50"),
                    InlineKeyboardButton(f"100 股 ≈${price*100:.1f}", callback_data="entry_shares:100"),
                    InlineKeyboardButton(f"200 股 ≈${price*200:.1f}", callback_data="entry_shares:200"),
                ],
                [InlineKeyboardButton("自訂（輸入數字）", callback_data="entry_shares:custom")],
                [InlineKeyboardButton("❌ 取消", callback_data="entry_cancel")],
            ]
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(
                "\n".join(text_lines),
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML",
            )
            return ENTRY_SHARES
        return await self._ask_entry_shares(update, context)

    # ── Dismiss（冪等移除按鈕）───────────────────────────────

    async def cb_dismiss(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        """忽略推送訊息 — 只移除 inline keyboard，保留訊息文字。冪等。"""
        query = update.callback_query
        await query.answer("已忽略")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass  # 訊息已被編輯過或過期，忽略

    # ── 排行跳城市頁 ─────────────────────────────────────────

    async def cb_rank_jump(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        """rank_jump:{city}:{date} → 城市議題頁"""
        if not await self._check_access(update):
            return
        data = update.callback_query.data  # "rank_jump:{city}:{date}"
        parts = data.split(":", 2)
        if len(parts) < 3:
            await update.callback_query.answer("格式錯誤")
            return
        _, city, date = parts
        await self._render_city_signals(update, city, date, contract_offset=0)

    # ── 結算中頁面（< 6h）────────────────────────────────────

    async def cmd_settling_msg(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        """Reply keyboard「結算中」按鈕 → 掃描並顯示 < 6h 的城市。"""
        if not await self._check_access(update):
            return
        await self._handle_settling(update, context)

    async def _handle_settling(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        """掃描 ev_signals 找 < 6h 的城市並顯示結算中頁面。

        Reader 內部 mtime cache（v7.3.9）讓重複按按鈕的 N 個城市掃描接近免費。
        """
        settling: list[tuple[str, str, float]] = []
        for city in self.reader.get_ready_cities():
            city_tz = self.reader.get_city_timezone(city)
            for d in self.reader.get_available_dates(city):
                h = _calc_settlement_hours(d, city_tz=city_tz)
                if h is not None and 0 < h < 6:
                    settling.append((city, d, h))
        if not settling:
            await update.message.reply_text(
                "<b>結算&lt;6h</b>\n目前沒有即將結算的合約",
                parse_mode="HTML",
            )
            return
        settling.sort(key=lambda x: x[2])
        for city, market_date, hours in settling:
            msg = self._render_settling_page(city, market_date, hours)
            await update.message.reply_text(msg, parse_mode="HTML")

    def _render_settling_page(
        self,
        city: str,
        market_date: str,
        hours: float,
    ) -> str:
        """渲染單一城市的結算中頁面。Reader cache 讓 get_city_signals 通常是 hit。"""
        city_tz = self.reader.get_city_timezone(city)
        rows = self.reader.get_city_signals(city, market_date)

        # === 標題（拆兩行：結算<6h / 城市·日期）===
        header = f"結算&lt;6h\n<b>{city}</b> · {_fmt_date(market_date)}\n"
        header += _fmt_settlement_full(market_date, city_tz) + "\n"

        # 實況（優先 latest_obs.json，fallback ev_signals）
        _obs_high_c = None
        _obs_unit = "C"
        _obs_age_str = ""
        _sl = _read_latest_obs().get(city, {})
        if _sl.get("status") == "ok" and _sl.get("high_c") is not None:
            _obs_high_c = float(_sl["high_c"])
            for r in rows:
                _obs_unit = r.get("temp_unit", "C")
                break
            _obs_age_str = _fmt_obs_age(_sl.get("fetched_at_utc", ""))
        else:
            for r in rows:
                _raw = r.get("observed_high_c", "")
                if _raw not in ("", None):
                    try:
                        _obs_high_c = float(_raw)
                        _obs_unit = r.get("temp_unit", "C")
                        break
                    except (ValueError, TypeError):
                        pass

        if _obs_high_c is not None:
            obs_display = (
                f"{_obs_high_c * 9 / 5 + 32:.1f}°F" if _obs_unit == "F"
                else f"{_obs_high_c:.1f}°C"
            )
            header += f"實況：{obs_display}{_obs_age_str}\n"

        # 峰值時段
        _peak_settling = _get_peak_info(city, market_date, city_tz)
        if _peak_settling:
            header += _peak_settling + "\n"

        # 分類：已鎖定 / 未鎖定（讀 observation_clipped 欄位）
        locked: list[dict] = []
        unlocked: list[dict] = []
        for r in rows:
            if str(r.get("observation_clipped", "")).lower() == "true":
                locked.append(r)
            else:
                unlocked.append(r)

        locked.sort(key=_city_sort_key)
        unlocked.sort(key=_city_sort_key)

        def _settling_contract_lines(r: dict) -> str:
            """所有行縮排 2 格。"""
            block = _fmt_contract_block(r)
            return "\n".join(f"  {line}" for line in block.split("\n"))

        body = ""
        if unlocked:
            body += "\n未鎖定：\n"
            body += "\n\n".join(_settling_contract_lines(r) for r in unlocked)
            body += "\n"

        if locked:
            body += "\n已鎖定：\n"
            body += "\n\n".join(_settling_contract_lines(r) for r in locked)
            body += "\n"

        result = header + body
        if len(result) > 4000:
            result = result[:3996] + "\n..."
        return result

    # ── Admin 管理面板 ────────────────────────────────────────

    async def cb_admin_panel(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_access(update):
            return
        is_admin = self._is_admin(update.effective_chat.id)
        if is_admin:
            keyboard = [
                [InlineKeyboardButton("顯示我的 ID", callback_data="admin_my_id")],
                [
                    InlineKeyboardButton("新增用戶", callback_data="admin_add"),
                    InlineKeyboardButton("刪除用戶", callback_data="admin_del"),
                ],
                [InlineKeyboardButton("所有用戶", callback_data="admin_list")],
                [InlineKeyboardButton("系統狀態", callback_data="admin_status")],
            ]
            await self._reply(update, "管理員面板", keyboard)
        else:
            keyboard = [
                [InlineKeyboardButton("顯示我的 ID", callback_data="admin_my_id")],
            ]
            await self._reply(update, "管理面板", keyboard)

    async def cb_admin_my_id(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_access(update):
            return
        cid = update.effective_chat.id
        text = f"ID：<code>{cid}</code>"
        keyboard = [[InlineKeyboardButton("返回管理", callback_data="admin_panel")]]
        await self._reply(update, text, keyboard)

    async def cb_admin_list(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_admin(update):
            return
        users = self.user_mgr.list_users()
        lines = [f"授權用戶（{len(users)} 人）\n"]
        for i, u in enumerate(users, 1):
            role = "Admin" if u["is_admin"] else "User"
            lines.append(f"{i}. {u['chat_id']} — {role}")
        keyboard = [[InlineKeyboardButton("返回管理", callback_data="admin_panel")]]
        await self._reply(update, "\n".join(lines), keyboard)

    async def cb_admin_system_status(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> None:
        if not await self._check_admin(update):
            return

        state = self.reader.get_signal_state()
        ready_cities = self.reader.get_ready_cities()

        health_path = PROJ_DIR / "data" / "_system_health.json"
        health: dict = {}
        if health_path.exists():
            try:
                health = json.loads(health_path.read_text(encoding="utf-8"))
            except Exception:
                pass

        sm_info = health.get("signal_main", {})
        collector_info = health.get("collector_main", {})
        bot_info = health.get("telegram_bot", {})
        forecast_info = health.get("forecast", {})
        truth_info = health.get("truth", {})

        def _since(ts: str) -> str:
            if not ts:
                return "未知"
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                secs = int((datetime.now(timezone.utc) - dt).total_seconds())
                if secs < 60:
                    return f"{secs} 秒前"
                if secs < 3600:
                    return f"{secs // 60} 分鐘前"
                return f"{secs // 3600} 小時前"
            except Exception:
                return ts[:16]

        signal_ts = sm_info.get("updated_at_utc") or state.get("last_success_utc", "")
        collector_ts = collector_info.get("updated_at_utc", "")
        forecast_ts = forecast_info.get("updated_at_utc", "")
        truth_ts = truth_info.get("updated_at_utc", "")

        lines = [
            "系統狀態\n",
            f"訊號更新(signal)：{_since(signal_ts)}",
            f"資料收集(collector)：{_since(collector_ts)}",
            f"機器人(bot)：運行中",
            "",
            f"就緒城市(ready)：{len(ready_cities)}",
            f"最近預報(forecast)：{_since(forecast_ts)}",
            f"最近真值(truth)：{_since(truth_ts)}",
        ]
        keyboard = [[InlineKeyboardButton("返回管理", callback_data="admin_panel")]]
        await self._reply(update, "\n".join(lines), keyboard)

    # Admin 新增用戶 ConversationHandler
    async def cb_admin_add_start(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        if not await self._check_admin(update):
            return ConversationHandler.END
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("請輸入要新增的 Chat ID：")
        return ADMIN_ADD_WAIT_ID

    async def receive_add_user_id(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        raw = update.message.text.strip()
        if not raw.lstrip("-").isdigit():
            await update.message.reply_text("請輸入純數字 ID（例如 123456789）")
            return ADMIN_ADD_WAIT_ID
        ok = self.user_mgr.add_user(raw)
        self.allowed = self.user_mgr.get_allowed()
        total = len(self.user_mgr.list_users())
        if ok:
            text = f"用戶已新增\n\nID：{raw}\n目前授權用戶：{total} 人"
        else:
            text = f"此 ID 已存在（{raw}）"
        await update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("返回管理", callback_data="admin_panel")
            ]]),
        )
        return ConversationHandler.END

    # Admin 刪除用戶 ConversationHandler
    async def cb_admin_del_start(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        if not await self._check_admin(update):
            return ConversationHandler.END
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("請輸入要刪除的 Chat ID：")
        return ADMIN_DEL_WAIT_ID

    async def receive_del_user_id(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        raw = update.message.text.strip()
        if not raw.lstrip("-").isdigit():
            await update.message.reply_text("請輸入純數字 ID（例如 123456789）")
            return ADMIN_DEL_WAIT_ID
        # Cannot delete yourself
        if str(raw) == str(update.effective_chat.id):
            await update.message.reply_text(
                "不能刪除自己",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("返回管理", callback_data="admin_panel")
                ]]),
            )
            return ConversationHandler.END
        context.user_data["del_user_id"] = raw
        await update.message.reply_text(
            f"確定刪除用戶 {raw}？",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("確定", callback_data="admin_del_confirm"),
                 InlineKeyboardButton("取消", callback_data="admin_del_cancel")],
            ]),
        )
        return ADMIN_DEL_CONFIRM

    async def confirm_del_user(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        raw = context.user_data.pop("del_user_id", "")
        ok = self.user_mgr.remove_user(raw)
        self.allowed = self.user_mgr.get_allowed()
        if ok:
            total = len(self.user_mgr.list_users())
            msg = f"用戶已刪除\n\nID：{raw}\n目前授權用戶：{total} 人"
        else:
            msg = f"找不到此 ID（{raw}）"
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            msg,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("返回管理", callback_data="admin_panel")
            ]]),
        )
        return ConversationHandler.END

    async def cancel_del_user(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        context.user_data.pop("del_user_id", None)
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            "已取消",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("返回管理", callback_data="admin_panel")
            ]]),
        )
        return ConversationHandler.END

    # ── 平倉 ConversationHandler（STEP 13）───────────────────

    async def start_exit(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        if not await self._check_admin(update):
            return ConversationHandler.END

        data = update.callback_query.data  # "close_pos:{position_id}"
        position_id = data.split(":", 1)[1]
        pos = self.reader.get_position(position_id)
        if not pos:
            await update.callback_query.answer("找不到此持倉", show_alert=True)
            return ConversationHandler.END

        context.user_data["exit_position_id"] = position_id
        context.user_data["exit_pos"] = pos

        mark = pos.get("mark_price")
        side = pos.get("side", "?")
        entry_price = pos.get("entry_price", 0)

        lines = [
            f"📤 <b>記錄平倉</b>",
            "",
            f"  {pos.get('city', '?')} — {pos.get('contract_label', '?')}",
            f"  {side} × {_safe_float(pos.get('shares')):.0f} @ ${_safe_float(entry_price):.3f}",
            "",
            "請輸入平倉價格：",
        ]
        keyboard = []
        if mark is not None:
            keyboard.append([InlineKeyboardButton(
                f"✅ 用現價 ${_safe_float(mark):.3f}（best bid）",
                callback_data=f"exit_bid:{mark}",
            )])
        keyboard.append([InlineKeyboardButton("❌ 取消", callback_data="exit_cancel")])

        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML",
        )
        return EXIT_PRICE

    async def use_current_bid(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        data = update.callback_query.data  # "exit_bid:{price}"
        try:
            price = float(data.split(":", 1)[1])
        except (ValueError, IndexError):
            await update.callback_query.answer("價格解析失敗")
            return EXIT_PRICE
        context.user_data["exit_price"] = price
        return await self._show_exit_confirm(update, context)

    async def receive_exit_price(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        try:
            price = float(update.message.text.strip())
            if not (0.0 < price <= 1.0):
                raise ValueError
        except ValueError:
            await update.message.reply_text("⚠️ 請輸入 0~1 之間的平倉價格（例如 0.82）")
            return EXIT_PRICE
        context.user_data["exit_price"] = price
        return await self._show_exit_confirm(update, context, via_message=True)

    async def _show_exit_confirm(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE", via_message: bool = False
    ) -> int:
        pos = context.user_data.get("exit_pos", {})
        exit_price = context.user_data.get("exit_price", 0)
        shares = _safe_float(pos.get("shares"))
        entry_cost = _safe_float(pos.get("entry_cost_total"))
        pnl_est = shares * exit_price - entry_cost
        lines = [
            "✅ <b>確認平倉？</b>",
            "",
            f"  {pos.get('city', '?')} {pos.get('contract_label', '?')}",
            f"  {pos.get('side', '?')} × {shares:.0f}",
            f"  平倉價：${exit_price:.3f}",
            f"  預估 PnL（gross）：${pnl_est:+.2f}",
        ]
        keyboard = [
            [InlineKeyboardButton("✅ 確認平倉", callback_data="exit_confirm")],
            [InlineKeyboardButton("❌ 取消", callback_data="exit_cancel")],
        ]
        markup = InlineKeyboardMarkup(keyboard)
        if via_message:
            await update.message.reply_text(
                "\n".join(lines), reply_markup=markup, parse_mode="HTML"
            )
        else:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(
                "\n".join(lines), reply_markup=markup, parse_mode="HTML"
            )
        return EXIT_CONFIRM

    async def confirm_exit(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        d = context.user_data
        position_id = d.get("exit_position_id", "")
        exit_price = _safe_float(d.get("exit_price"))
        try:
            mod = _get_pos_mgr_for_bot()
            pm = mod.PositionManager()
            pm.load()
            ok = pm.close_position(
                position_id=position_id,
                exit_price=exit_price,
                exit_reason="manual_bot",
            )
            if ok:
                pos = d.get("exit_pos", {})
                shares = _safe_float(pos.get("shares"))
                entry_cost = _safe_float(pos.get("entry_cost_total"))
                pnl = shares * exit_price - entry_cost
                await update.callback_query.answer("✅ 已記錄平倉")
                await update.callback_query.edit_message_text(
                    f"✅ <b>平倉已記錄</b>\n\n"
                    f"ID: <code>{position_id}</code>\n"
                    f"PnL（gross）：${pnl:+.2f}",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("💼 我的持倉", callback_data="positions"),
                        InlineKeyboardButton("🏠", callback_data="menu"),
                    ]]),
                )
            else:
                await update.callback_query.answer("❌ 平倉失敗（已關閉或找不到）", show_alert=True)
        except Exception as e:
            log.error(f"confirm_exit error: {e}")
            await update.callback_query.answer("❌ 記錄失敗，請重試")
        context.user_data.clear()
        return ConversationHandler.END

    async def cancel_exit(
        self, update: "Update", context: "ContextTypes.DEFAULT_TYPE"
    ) -> int:
        context.user_data.clear()
        if update.callback_query:
            await update.callback_query.answer("已取消")
            await update.callback_query.edit_message_text(
                "已取消平倉記錄",
                reply_markup=InlineKeyboardMarkup([_back_btn()]),
            )
        return ConversationHandler.END


# ============================================================
# Bot 啟動
# ============================================================

def main() -> None:
    if not _HAS_PTB:
        print("ERROR: python-telegram-bot not installed.")
        print("Run: pip install python-telegram-bot")
        sys.exit(1)

    if not _HAS_READER:
        print("ERROR: signal_reader not found in _lib/.")
        sys.exit(1)

    config = load_telegram_config_full()
    if not config:
        print("ERROR: No Telegram config found.")
        print("Create config/telegram.yaml or set PM_TELEGRAM_BOT_TOKEN + PM_TELEGRAM_CHAT_ID")
        sys.exit(1)

    log.info(f"Starting bot (allowed={config['allowed_chat_ids']}, admins={config['admin_chat_ids']})")

    bot = WeatherSignalBot(config)
    app = Application.builder().token(config["bot_token"]).build()

    # 設定 Bot 指令列表
    async def post_init(application):
        await application.bot.set_my_commands([
            BotCommand("start", "主選單（設定 Reply keyboard）"),
        ])

    app.post_init = post_init

    # ── 進場 ConversationHandler（隱藏，保留功能）────────────
    entry_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(bot.start_entry, pattern="^entry:")],
        states={
            ENTRY_DUPLICATE_CHECK: [
                CallbackQueryHandler(bot.confirm_duplicate_entry, pattern="^entry_dup_ok$"),
                CallbackQueryHandler(bot.cancel_entry, pattern="^entry_cancel$"),
            ],
            ENTRY_PRICE: [
                CallbackQueryHandler(bot.use_suggested_price, pattern="^entry_suggested:"),
                CallbackQueryHandler(bot.cancel_entry, pattern="^entry_cancel$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, bot.receive_entry_price),
            ],
            ENTRY_SHARES: [
                CallbackQueryHandler(bot.select_shares, pattern="^entry_shares:"),
                CallbackQueryHandler(bot.cancel_entry, pattern="^entry_cancel$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, bot.receive_custom_shares),
            ],
            ENTRY_CONFIRM: [
                CallbackQueryHandler(bot.confirm_entry, pattern="^entry_confirm$"),
                CallbackQueryHandler(bot.cancel_entry, pattern="^entry_cancel$"),
            ],
        },
        fallbacks=[CallbackQueryHandler(bot.cancel_entry, pattern="^entry_cancel$")],
        per_message=False,
    )

    # ── 平倉 ConversationHandler（隱藏，保留功能）────────────
    exit_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(bot.start_exit, pattern="^close_pos:")],
        states={
            EXIT_PRICE: [
                CallbackQueryHandler(bot.use_current_bid, pattern="^exit_bid:"),
                CallbackQueryHandler(bot.cancel_exit, pattern="^exit_cancel$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, bot.receive_exit_price),
            ],
            EXIT_CONFIRM: [
                CallbackQueryHandler(bot.confirm_exit, pattern="^exit_confirm$"),
                CallbackQueryHandler(bot.cancel_exit, pattern="^exit_cancel$"),
            ],
        },
        fallbacks=[CallbackQueryHandler(bot.cancel_exit, pattern="^exit_cancel$")],
        per_message=False,
    )

    # ── Admin 新增用戶 ConversationHandler ───────────────────
    admin_add_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(bot.cb_admin_add_start, pattern="^admin_add$")],
        states={
            ADMIN_ADD_WAIT_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bot.receive_add_user_id),
            ],
        },
        fallbacks=[CallbackQueryHandler(bot.cb_admin_panel, pattern="^admin_panel$")],
        per_message=False,
    )

    # ── Admin 刪除用戶 ConversationHandler ───────────────────
    admin_del_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(bot.cb_admin_del_start, pattern="^admin_del$")],
        states={
            ADMIN_DEL_WAIT_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bot.receive_del_user_id),
            ],
            ADMIN_DEL_CONFIRM: [
                CallbackQueryHandler(bot.confirm_del_user, pattern="^admin_del_confirm$"),
                CallbackQueryHandler(bot.cancel_del_user, pattern="^admin_del_cancel$"),
            ],
        },
        fallbacks=[CallbackQueryHandler(bot.cancel_del_user, pattern="^admin_del_cancel$")],
        per_message=False,
    )

    # 註冊 handlers（ConversationHandler 必須在一般 CallbackQueryHandler 之前）
    app.add_handler(entry_conv)
    app.add_handler(exit_conv)
    app.add_handler(admin_add_conv)
    app.add_handler(admin_del_conv)
    app.add_handler(CommandHandler("start", bot.cmd_start))
    # Reply keyboard text handlers（6 個按鈕，2 排）
    app.add_handler(MessageHandler(filters.Regex("^排行$"), bot.cmd_ranking_msg))
    app.add_handler(MessageHandler(filters.Regex("^今日$"), bot.cmd_today_msg))
    app.add_handler(MessageHandler(filters.Regex("^預警6-8h$"), bot.cmd_warning_msg))
    app.add_handler(MessageHandler(filters.Regex("^結算<6h$"), bot.cmd_settling_msg))
    app.add_handler(MessageHandler(filters.Regex("^城市$"), bot.cmd_cities))
    app.add_handler(MessageHandler(filters.Regex("^管理$"), bot.cmd_admin))
    # Fallback：任何未匹配文字 → 重設 Reply keyboard
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.cmd_start))
    # Callback handlers
    app.add_handler(CallbackQueryHandler(bot.cb_cities, pattern="^cities$"))
    app.add_handler(CallbackQueryHandler(bot.cb_city_page, pattern="^city_page:"))
    app.add_handler(CallbackQueryHandler(bot.cb_city_signals, pattern="^city:"))
    app.add_handler(CallbackQueryHandler(bot.cb_ranking, pattern="^rank:"))
    app.add_handler(CallbackQueryHandler(bot.cb_today, pattern="^today:"))
    app.add_handler(CallbackQueryHandler(bot.cb_warning, pattern="^warn:"))
    app.add_handler(CallbackQueryHandler(bot.cb_rank_jump, pattern="^rank_jump:"))
    app.add_handler(CallbackQueryHandler(bot.cb_signal_detail, pattern="^detail:"))
    app.add_handler(CallbackQueryHandler(bot.cb_full_data, pattern="^fulldata:"))
    app.add_handler(CallbackQueryHandler(bot.cb_dismiss, pattern="^dismiss:"))
    app.add_handler(CallbackQueryHandler(bot.cb_refresh, pattern="^refresh$"))
    app.add_handler(CallbackQueryHandler(bot.cb_positions, pattern="^positions$"))
    app.add_handler(CallbackQueryHandler(bot.cb_alert_history, pattern="^history$"))
    app.add_handler(CallbackQueryHandler(bot.cb_settings, pattern="^settings$"))
    app.add_handler(CallbackQueryHandler(bot.cb_city_management, pattern="^city_mgmt$"))
    app.add_handler(CallbackQueryHandler(bot.cb_scan_cities, pattern="^scan_cities$"))
    # Admin panel callbacks
    app.add_handler(CallbackQueryHandler(bot.cb_admin_panel, pattern="^admin_panel$"))
    app.add_handler(CallbackQueryHandler(bot.cb_admin_my_id, pattern="^admin_my_id$"))
    app.add_handler(CallbackQueryHandler(bot.cb_admin_list, pattern="^admin_list$"))
    app.add_handler(CallbackQueryHandler(bot.cb_admin_system_status, pattern="^admin_status$"))
    app.add_handler(CallbackQueryHandler(bot._show_main_menu_cb, pattern="^menu$"))

    # 啟動時寫 health
    _update_system_health("telegram_bot", {
        "status": "running",
        "started_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pid": os.getpid(),
        "last_callback_utc": None,
    })

    print("Bot started. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
