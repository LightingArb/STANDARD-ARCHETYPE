"""
15_alert_engine.py — 進場通報引擎 (STEP 10) + 出場通報（STEP 13）

職責：
  1. 評估 in-memory EV 信號（不重讀 CSV）
  2. 生成符合條件的 alert list
  3. 推送 Telegram（可選）
  4. 寫 alert_history 日切檔（logs/15_alert/YYYY-MM-DD_alert_history.csv）
  5. 持倉出場偵測（check_exits）+ 出場通報（process_exits）

設計原則：
  - 寧可漏推不可錯推：所有非 active 狀態都 suppress（包括 model_stale）
  - Telegram 是可選的：沒有 config 就只寫 log
  - AlertEngine 吃 in-memory 結果，不重讀 CSV
  - EXIT 只在 signal_status == active 時觸發，非 active 發 warning 不發 EXIT

Cooldown 設計（進場）：
  key = (market_id, signal_action)
  - 不只用 market_id：BUY_NO → BUY_YES 方向翻轉時不應被舊 cooldown 壓掉
  - 跨重啟恢復：啟動時從 alert_history 讀取最近 cooldown_minutes 內的紀錄

EXIT Cooldown 設計：
  key = (position_id, "EXIT")，30 分鐘
  - 若中間 edge 曾轉正再轉負，視為新事件，重置 cooldown

Alert History 欄位（日切檔）：
  generated_utc, market_id, city, market_date, contract,
  signal_action, edge, ev, sweet_spot_usd, sweet_spot_avg_price,
  depth_basis, edge_basis, settlement_hours,
  sent_telegram, send_error, cooldown_applied
"""

import csv
import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

EXIT_COOLDOWN_MINUTES = 30

try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

PROJ_DIR = Path(__file__).resolve().parent
log = logging.getLogger(__name__)


# ============================================================
# 工具函數
# ============================================================

def _parse_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).lower() in ("true", "1", "yes")


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_utc_str() -> str:
    return _now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")


def _market_id_short(market_id: str) -> str:
    """Truncate market_id for Telegram callback_data length limit (leave room for prefix)."""
    return market_id[:46]


def _calc_settlement_hours(market_date_local: str, city_tz: str = "") -> Optional[float]:
    """
    market_date_local = 'YYYY-MM-DD'.
    Settlement = midnight of next day in city's local timezone (UTC if unknown).
    Returns None if unparseable.
    """
    if not market_date_local:
        return None
    try:
        tz = timezone.utc
        if city_tz:
            try:
                import zoneinfo
                tz = zoneinfo.ZoneInfo(city_tz)
            except Exception:
                try:
                    import pytz
                    tz = pytz.timezone(city_tz)
                except Exception:
                    tz = timezone.utc
        d = datetime.strptime(market_date_local, "%Y-%m-%d")
        settlement = datetime(d.year, d.month, d.day, tzinfo=tz) + timedelta(days=1)
        return (settlement - _now_utc()).total_seconds() / 3600
    except (ValueError, TypeError):
        return None


def _fmt_alert_contract(row: dict) -> str:
    """格式化合約溫度顯示供 alert 訊息使用"""
    temp_unit = row.get("temp_unit", "C")
    unit = "°F" if temp_unit == "F" else "°C"
    market_type = row.get("market_type", "")
    threshold = row.get("threshold", "")
    range_low = row.get("range_low", "")
    range_high = row.get("range_high", "")
    if market_type == "range" and range_low and range_high:
        return f"{range_low}–{range_high}{unit}"
    elif threshold:
        return f"{threshold}{unit}"
    return f"?{unit}"


def _get_directional_fields(row: dict, action: str) -> dict:
    """
    Extract the correct side's edge/ev/depth fields based on signal_action.
    Returns a dict with: edge, ev, sweet_spot_usd, sweet_spot_ev,
                         sweet_spot_avg_price, has_depth.
    """
    if action == "BUY_YES":
        prefix = "yes"
    else:
        prefix = "no"

    sweet_usd_raw = row.get(f"{prefix}_sweet_usd")
    has_depth = sweet_usd_raw is not None and sweet_usd_raw != ""

    return {
        "edge": _safe_float(row.get(f"{prefix}_edge")),
        "ev": _safe_float(row.get(f"{prefix}_ev")),
        "sweet_spot_usd": _safe_float(sweet_usd_raw),
        "sweet_spot_ev": _safe_float(row.get(f"{prefix}_sweet_ev")),
        "sweet_spot_avg_price": _safe_float(row.get(f"{prefix}_sweet_avg_price")),
        "has_depth": has_depth,
    }


# ============================================================
# TelegramSender
# ============================================================

class TelegramSender:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{token}"

    def send_message(
        self,
        text: str,
        parse_mode: str = "HTML",
        reply_markup: Optional[dict] = None,
    ) -> tuple[bool, Optional[str]]:
        """
        推送訊息到 Telegram。可選帶 inline keyboard（reply_markup 為 raw dict）。
        timeout=10 秒，不卡住整輪 signal。
        失敗時回傳 (False, error_msg)，不 raise。
        """
        if not _HAS_REQUESTS:
            return (False, "requests not installed")
        if len(text) > 4000:
            text = text[:3990] + "\n..."
        try:
            payload: dict = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": parse_mode,
            }
            if reply_markup:
                payload["reply_markup"] = reply_markup
            resp = _requests.post(
                f"{self.api_url}/sendMessage",
                json=payload,
                timeout=10,
            )
            if resp.status_code == 200:
                return (True, None)
            else:
                return (False, f"HTTP {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            return (False, str(e))


# ============================================================
# Alert History（日切檔）
# ============================================================

ALERT_HISTORY_FIELDS = [
    "generated_utc",
    "alert_key",          # "{market_id}|{signal_action}"，方便 cooldown 重建和 debug
    "market_id",
    "city",
    "market_date",
    "contract",
    "signal_action",
    "edge",
    "ev",
    "sweet_spot_usd",
    "sweet_spot_avg_price",
    "depth_basis",
    "edge_basis",
    "settlement_hours",
    "sent_telegram",
    "send_error",
    "cooldown_applied",
]


def _history_path(history_dir: Path, dt: Optional[datetime] = None) -> Path:
    dt = dt or _now_utc()
    return history_dir / f"{dt.strftime('%Y-%m-%d')}_alert_history.csv"


def _append_alert_history(history_dir: Path, row: dict) -> None:
    """Append one alert row to today's history CSV (create with header if new)."""
    history_dir.mkdir(parents=True, exist_ok=True)
    path = _history_path(history_dir)
    write_header = not path.exists()
    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ALERT_HISTORY_FIELDS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(row)


# ============================================================
# 訊息格式
# ============================================================

def format_entry_alert(alert: dict) -> str:
    action = alert.get("signal_action", "")
    action_emoji = "🟢" if action in ("BUY_YES", "BUY_NO") else "⚪"
    action_label = action.replace("_", " ")

    edge = _safe_float(alert.get("edge"))
    ev = _safe_float(alert.get("ev"))
    sweet_spot_usd = _safe_float(alert.get("sweet_spot_usd"))
    sweet_spot_avg_price = _safe_float(alert.get("sweet_spot_avg_price"))
    entry_price = _safe_float(alert.get("entry_price"))
    target_price = _safe_float(alert.get("target_price"))
    settlement_h = alert.get("settlement_hours")

    lines = [
        f"{action_emoji} <b>{action_label} Signal</b>",
        "",
        f"{alert.get('city', '')} — {alert.get('market_date', '')} — {alert.get('contract', '')}",
        f"Edge: {edge:+.1%} | EV: ${ev:+.3f}",
        f"Depth: ${sweet_spot_usd:.0f} @ avg ${sweet_spot_avg_price:.3f}",
    ]

    if entry_price > 0:
        lines.append(f"進場價: ${entry_price:.3f} | 目標賣價（粗估）: ~${target_price:.3f}")

    if settlement_h is not None and settlement_h != "":
        lines.append(f"Settlement: {float(settlement_h):.0f}h")

    if alert.get("polymarket_url"):
        lines.append(f'\n<a href="{alert["polymarket_url"]}">View on Polymarket →</a>')

    return "\n".join(lines)


def format_shrink_alert(alert: dict) -> str:
    """格式化 Edge 縮水通知訊息（HTML）。"""
    entry_edge = _safe_float(alert.get("entry_edge"))
    current_edge = _safe_float(alert.get("current_edge"))
    entry_price = _safe_float(alert.get("entry_price"))
    current_price = _safe_float(alert.get("current_price"))
    return "\n".join([
        "🟡 <b>Edge 縮水</b>",
        "",
        f"{alert.get('city', '')} — {alert.get('contract', '')}",
        f"進場 Edge: {entry_edge:+.1%} → 現在: {current_edge:+.1%}",
        f"進場價: ${entry_price:.3f} | 現價: ${current_price:.3f}",
    ])


def format_exit_alert(alert: dict) -> str:
    """格式化 EXIT 出場通知訊息（HTML）。"""
    entry_edge = _safe_float(alert.get("entry_edge"))
    current_edge = _safe_float(alert.get("current_edge"))
    entry_price = _safe_float(alert.get("entry_price"))
    shares = _safe_float(alert.get("shares"))
    return "\n".join([
        "🔴 <b>EXIT Signal</b>",
        "",
        f"{alert.get('city', '')} — {alert.get('market_date', '')} — {alert.get('contract', '')}",
        f"Side: {alert.get('side', '')} | Entry: ${entry_price:.2f} × {shares:.0f} shares",
        f"Entry Edge: {entry_edge:+.1%} → Now: {current_edge:+.1%}",
        "",
        "建議：考慮平倉",
    ])


# ============================================================
# AlertEngine
# ============================================================

class AlertEngine:
    def __init__(self, alert_config: dict, telegram_sender: Optional[TelegramSender] = None):
        """
        alert_config 是 trading_params.yaml 的 flat dict（alert_* 前綴）。
        telegram_sender 可選；沒有則只寫 log + history。
        """
        self.min_edge = _safe_float(alert_config.get("alert_min_edge"), 0.30)
        self.min_depth_usd = _safe_float(alert_config.get("alert_min_depth_usd"), 100.0)
        self.require_positive_ev = _parse_bool(
            alert_config.get("alert_require_positive_ev", True)
        )
        self.cooldown_minutes = _safe_float(alert_config.get("alert_cooldown_minutes"), 10.0)
        self.min_settlement_hours = _safe_float(
            alert_config.get("alert_min_settlement_hours"), 2.0
        )
        self.max_per_city_per_cycle = int(
            _safe_float(alert_config.get("alert_max_per_city_per_cycle"), 3.0)
        )
        self.max_total_per_cycle = int(
            _safe_float(alert_config.get("alert_max_total_per_cycle"), 10.0)
        )

        self.shrink_threshold = _safe_float(
            alert_config.get("position_edge_shrink_threshold"), 0.10
        )

        # (market_id, signal_action) → last_alert_utc（進場 cooldown）
        self.cooldown_map: dict[tuple, datetime] = {}
        # (position_id, "EXIT") → last_exit_alert_utc（出場 cooldown）
        self.exit_cooldown_map: dict[tuple, datetime] = {}
        # position_id → last_shrink_alert_utc（縮水通知 cooldown，獨立）
        self.shrink_cooldown_map: dict[str, datetime] = {}
        # 追蹤 edge 是否曾轉正（用於重置 EXIT cooldown）
        # position_id → True 表示上次 update_mark 時 edge > 0
        self._edge_crossed_positive: dict[str, bool] = {}
        self.telegram = telegram_sender
        self.history_dir = PROJ_DIR / "logs" / "15_alert"
        self.exit_history_dir = PROJ_DIR / "logs" / "15_exit"

        # Load city → timezone mapping from market_master.csv
        self._city_tz: dict[str, str] = {}
        master_path = PROJ_DIR / "data" / "market_master.csv"
        if master_path.exists():
            try:
                with open(master_path, "r", encoding="utf-8", newline="") as _f:
                    for _row in csv.DictReader(_f):
                        _city = _row.get("city", "")
                        _tz = _row.get("timezone", "")
                        if _city and _tz and _city not in self._city_tz:
                            self._city_tz[_city] = _tz
                log.info(f"AlertEngine: loaded timezone map for {len(self._city_tz)} cities")
            except Exception as e:
                log.warning(f"AlertEngine: could not load timezone map: {e}")

    # ── Cooldown helpers ─────────────────────────────────────

    def _is_in_cooldown(self, market_id: str, signal_action: str) -> bool:
        key = (market_id, signal_action)
        if key not in self.cooldown_map:
            return False
        elapsed = (_now_utc() - self.cooldown_map[key]).total_seconds()
        return elapsed < self.cooldown_minutes * 60

    def _apply_cooldown(self, market_id: str, signal_action: str) -> None:
        """不管 Telegram 是否成功，生成 alert 就吃 cooldown。"""
        self.cooldown_map[(market_id, signal_action)] = _now_utc()

    # ── 跨重啟恢復 ──────────────────────────────────────────

    def load_cooldown_from_history(self) -> int:
        """
        啟動時從最近的 alert_history CSV 重建 cooldown_map。
        讀最近 1 天的檔案（今天 + 昨天），找 cooldown_minutes 內的紀錄。
        回傳重建的 cooldown entry 數量。
        """
        restored = 0
        now = _now_utc()
        cutoff_secs = self.cooldown_minutes * 60

        for delta_days in (0, 1):
            dt = now - timedelta(days=delta_days)
            path = _history_path(self.history_dir, dt)
            if not path.exists():
                continue
            try:
                with open(path, "r", encoding="utf-8", newline="") as f:
                    for row in csv.DictReader(f):
                        generated_str = row.get("generated_utc", "")
                        market_id = row.get("market_id", "")
                        action = row.get("signal_action", "")
                        if not (generated_str and market_id and action):
                            continue
                        try:
                            generated = datetime.fromisoformat(
                                generated_str.replace("Z", "+00:00")
                            )
                        except ValueError:
                            continue
                        elapsed = (now - generated).total_seconds()
                        if elapsed < cutoff_secs:
                            key = (market_id, action)
                            # Keep the most recent entry if multiple
                            if key not in self.cooldown_map or generated > self.cooldown_map[key]:
                                self.cooldown_map[key] = generated
                                restored += 1
            except Exception as e:
                log.warning(f"load_cooldown_from_history: error reading {path}: {e}")

        if restored:
            log.info(f"AlertEngine: restored {restored} cooldown entries from history")
        return restored

    # ── 評估 ─────────────────────────────────────────────────

    def evaluate(self, ev_signals: list[dict]) -> list[dict]:
        """
        評估所有信號，回傳應推送的 alert 列表（已按城市限流 + 全局限流）。

        ev_signals 是 in-memory 的 dict list（不是從 CSV 讀的字串）。

        檢查順序：
        1. signal_status == "active"？（model_stale 也 suppress）
        2. signal_action in ("BUY_YES", "BUY_NO")？
        3. has_depth？（無深度資料直接跳過，不評估 orderbook 質量）
        4. edge >= min_edge？
        5. sweet_spot_usd >= min_depth_usd？
        6. sweet_spot_ev > 0？（require_positive_ev）
        7. settlement >= min_settlement_hours？
        8. 不在 cooldown 內？

        城市級限流 → 全局限流。
        排序依據：sweet_spot_ev > edge > sweet_spot_usd（降序）。
        """
        candidates: list[dict] = []

        for row in ev_signals:
            signal_status = row.get("signal_status", "")
            signal_action = row.get("signal_action", "")
            market_id = row.get("market_id", "")

            # 1. signal_status == "active"
            if signal_status != "active":
                continue

            # 2. signal_action in ("BUY_YES", "BUY_NO")
            if signal_action not in ("BUY_YES", "BUY_NO"):
                continue

            # 3. 方向性欄位
            df = _get_directional_fields(row, signal_action)
            edge = df["edge"]
            sweet_spot_usd = df["sweet_spot_usd"]
            sweet_spot_ev = df["sweet_spot_ev"]
            has_depth = df["has_depth"]

            # 4. has_depth=False → 不推送（無深度資料，不評估 orderbook 質量）
            if not has_depth:
                log.debug(f"Skipping {market_id} {signal_action}: no depth data")
                continue

            # 5. edge >= min_edge
            if edge < self.min_edge:
                continue

            # 6. depth >= min_depth_usd
            if sweet_spot_usd < self.min_depth_usd:
                continue

            # 7. sweet_spot_ev > 0（require_positive_ev）
            if self.require_positive_ev and sweet_spot_ev <= 0:
                continue

            # 8. settlement >= min_settlement_hours
            city = row.get("city", "")
            market_date = row.get("market_date_local", "")
            city_tz = self._city_tz.get(city, "")
            settlement_h = _calc_settlement_hours(market_date, city_tz)
            if settlement_h is not None and settlement_h < self.min_settlement_hours:
                continue

            # 9. cooldown
            if self._is_in_cooldown(market_id, signal_action):
                continue

            # Entry price for alert message
            if signal_action == "BUY_YES":
                ep = _safe_float(row.get("yes_ask_price"))
            else:
                ep = _safe_float(row.get("no_ask_price"))
            target_p = round(min(1.0, ep * (1 + edge)), 4) if ep > 0 else 0.0

            candidates.append({
                "market_id": market_id,
                "city": city,
                "market_date": market_date,
                "contract": _fmt_alert_contract(row),
                "signal_action": signal_action,
                "edge": edge,
                "ev": df["ev"],
                "sweet_spot_usd": sweet_spot_usd,
                "sweet_spot_ev": sweet_spot_ev,
                "sweet_spot_avg_price": df["sweet_spot_avg_price"],
                "settlement_hours": settlement_h,
                "depth_basis": "sweet_spot",
                "edge_basis": "best_only",
                "polymarket_url": row.get("polymarket_url", ""),
                "entry_price": ep,
                "target_price": target_p,
            })

        if not candidates:
            return []

        def _sort_key(c: dict):
            return (
                _safe_float(c.get("sweet_spot_ev")),
                _safe_float(c.get("edge")),
                _safe_float(c.get("sweet_spot_usd")),
            )

        # 城市級限流
        city_groups: dict[str, list] = {}
        for c in candidates:
            city_groups.setdefault(c["city"], []).append(c)

        city_limited: list[dict] = []
        for group in city_groups.values():
            group.sort(key=_sort_key, reverse=True)
            city_limited.extend(group[: self.max_per_city_per_cycle])

        # 全局限流
        city_limited.sort(key=_sort_key, reverse=True)
        return city_limited[: self.max_total_per_cycle]

    # ── 推送 + 記錄 ──────────────────────────────────────────

    def process(self, alerts: list[dict]) -> None:
        """
        對每個 alert：
        1. 吃 cooldown（不管 Telegram 是否成功）
        2. 嘗試推 Telegram（如果有 sender）
        3. 寫 alert_history（日切檔）
        """
        for alert in alerts:
            market_id = alert["market_id"]
            signal_action = alert["signal_action"]
            generated_utc = _now_utc_str()

            # 1. 吃 cooldown（先於 Telegram，不管送達與否）
            self._apply_cooldown(market_id, signal_action)

            # 2. 嘗試推 Telegram
            sent_telegram = False
            send_error = ""
            if self.telegram is not None:
                text = format_entry_alert(alert)
                ok, err = self.telegram.send_message(text)
                sent_telegram = ok
                send_error = err or ""
                if ok:
                    log.info(
                        f"  Telegram OK: {alert.get('city')} {signal_action} "
                        f"edge={alert['edge']:+.1%}"
                    )
                else:
                    log.warning(
                        f"  Telegram FAIL: {market_id} {signal_action} — {err}"
                    )
            else:
                settlement_h = alert.get("settlement_hours")
                sh_str = f"{float(settlement_h):.0f}h" if settlement_h is not None else "?"
                log.info(
                    f"  Alert (no Telegram): {alert.get('city')} {signal_action} "
                    f"edge={alert['edge']:+.1%} "
                    f"depth=${alert.get('sweet_spot_usd', 0):.0f} "
                    f"settle={sh_str}"
                )

            # 3. 寫 alert_history
            settlement_h = alert.get("settlement_hours")
            settlement_str = (
                round(float(settlement_h), 1)
                if settlement_h is not None
                else ""
            )
            history_row = {
                "generated_utc": generated_utc,
                "alert_key": f"{market_id}|{signal_action}",
                "market_id": market_id,
                "city": alert.get("city", ""),
                "market_date": alert.get("market_date", ""),
                "contract": alert.get("contract", ""),
                "signal_action": signal_action,
                "edge": round(alert.get("edge", 0), 6),
                "ev": round(alert.get("ev", 0), 6),
                "sweet_spot_usd": round(alert.get("sweet_spot_usd", 0), 2),
                "sweet_spot_avg_price": round(alert.get("sweet_spot_avg_price", 0), 6),
                "depth_basis": alert.get("depth_basis", "sweet_spot"),
                "edge_basis": alert.get("edge_basis", "best_only"),
                "settlement_hours": settlement_str,
                "sent_telegram": str(sent_telegram).lower(),
                "send_error": send_error,
                "cooldown_applied": "true",
            }
            try:
                _append_alert_history(self.history_dir, history_row)
            except Exception as e:
                log.warning(f"  Failed to write alert history: {e}")

    # ── EXIT Cooldown helpers ────────────────────────────────

    def _is_exit_in_cooldown(self, position_id: str) -> bool:
        """
        檢查 EXIT cooldown（30 分鐘）。
        若 edge 中間曾轉正（_edge_crossed_positive），視為新事件，重置 cooldown。
        """
        key = (position_id, "EXIT")
        if key not in self.exit_cooldown_map:
            return False
        # edge 曾轉正 → 重置 cooldown（本次不在 cooldown 內）
        if self._edge_crossed_positive.get(position_id, False):
            self._edge_crossed_positive[position_id] = False
            del self.exit_cooldown_map[key]
            return False
        elapsed = (_now_utc() - self.exit_cooldown_map[key]).total_seconds()
        return elapsed < EXIT_COOLDOWN_MINUTES * 60

    def _apply_exit_cooldown(self, position_id: str) -> None:
        self.exit_cooldown_map[(position_id, "EXIT")] = _now_utc()
        # 清除 edge_crossed_positive，避免立即重置
        self._edge_crossed_positive[position_id] = False

    # ── 出場偵測（STEP 13）──────────────────────────────────

    def check_exits(
        self,
        positions: list,
        ev_signals: list,
    ) -> tuple[list, list]:
        """
        檢查 open positions，回傳 (exit_alerts, position_warnings)。

        exit_alerts：signal_status == active 且對應方向 edge < 0
        position_warnings：非 active 狀態，或找不到對應 market（不推 EXIT）
        """
        exit_alerts: list[dict] = []
        warnings: list[dict] = []

        for pos in positions:
            if pos.get("status") != "open":
                continue

            position_id = pos.get("position_id", "")
            market_id = pos.get("market_id", "")
            side = pos.get("side", "")

            # 找對應的最新 signal
            current = self._find_signal(market_id, ev_signals)

            if not current:
                warnings.append({
                    "position_id": position_id,
                    "type": "market_missing",
                    "message": f"Market {market_id} not found in current signals",
                })
                continue

            signal_status = current.get("signal_status", "")

            if signal_status == "active":
                # 取對應方向的 edge
                if side == "NO":
                    edge = current.get("no_edge")
                else:
                    edge = current.get("yes_edge")

                if edge is not None and float(edge) < 0:
                    if not self._is_exit_in_cooldown(position_id):
                        exit_alerts.append({
                            "alert_type": "exit",
                            "position_id": position_id,
                            "market_id": market_id,
                            "city": pos.get("city", ""),
                            "market_date": pos.get("market_date", ""),
                            "contract": pos.get("contract_label", ""),
                            "side": side,
                            "entry_price": pos.get("entry_price"),
                            "entry_edge": pos.get("entry_edge"),
                            "current_edge": float(edge),
                            "shares": pos.get("shares"),
                        })
            else:
                warnings.append({
                    "position_id": position_id,
                    "type": f"signal_{signal_status}",
                    "message": f"Signal status is {signal_status!r}, EXIT suppressed",
                })

        return exit_alerts, warnings

    def _find_signal(self, market_id: str, ev_signals: list) -> Optional[dict]:
        """在 ev_signals 中找 market_id 匹配的 row。"""
        for sig in ev_signals:
            if sig.get("market_id", "") == market_id:
                return sig
        return None

    # ── 出場通報 + 記錄（STEP 13）───────────────────────────

    def process_exits(self, exit_alerts: list, warnings: list) -> None:
        """
        處理出場 alerts：
        1. 吃 EXIT cooldown
        2. 推 Telegram
        3. 寫 exit_history（日切檔）
        warnings 只 log，不推送。
        """
        for alert in exit_alerts:
            position_id = alert.get("position_id", "")

            # 1. 吃 cooldown
            self._apply_exit_cooldown(position_id)

            # 2. 推 Telegram
            sent_telegram = False
            send_error = ""
            if self.telegram is not None:
                text = format_exit_alert(alert)
                ok, err = self.telegram.send_message(text)
                sent_telegram = ok
                send_error = err or ""
                if ok:
                    log.info(
                        f"  EXIT Telegram OK: {alert.get('city')} {alert.get('side')} "
                        f"edge={_safe_float(alert.get('current_edge')):+.1%}"
                    )
                else:
                    log.warning(
                        f"  EXIT Telegram FAIL: {position_id} — {err}"
                    )
            else:
                log.info(
                    f"  EXIT alert (no Telegram): {alert.get('city')} "
                    f"{alert.get('contract')} {alert.get('side')} "
                    f"edge={_safe_float(alert.get('current_edge')):+.1%}"
                )

            # 3. 寫 exit_history
            try:
                self._write_exit_history(alert, sent_telegram, send_error)
            except Exception as e:
                log.warning(f"  Failed to write exit history: {e}")

        for w in warnings:
            log.info(
                f"Position warning: {w.get('position_id')} — {w.get('message', '')}"
            )

    def _write_exit_history(
        self,
        alert: dict,
        sent_telegram: bool,
        send_error: str,
    ) -> None:
        """寫出場歷史到 logs/15_exit/YYYY-MM-DD_exit_history.csv。"""
        fields = [
            "generated_utc", "position_id", "market_id", "city",
            "market_date", "contract", "side",
            "entry_price", "entry_edge", "current_edge", "shares",
            "sent_telegram", "send_error",
        ]
        self.exit_history_dir.mkdir(parents=True, exist_ok=True)
        now = _now_utc()
        path = self.exit_history_dir / f"{now.strftime('%Y-%m-%d')}_exit_history.csv"
        write_header = not path.exists()
        with open(path, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            writer.writerow({
                "generated_utc": _now_utc_str(),
                "position_id": alert.get("position_id", ""),
                "market_id": alert.get("market_id", ""),
                "city": alert.get("city", ""),
                "market_date": alert.get("market_date", ""),
                "contract": alert.get("contract", ""),
                "side": alert.get("side", ""),
                "entry_price": alert.get("entry_price", ""),
                "entry_edge": alert.get("entry_edge", ""),
                "current_edge": alert.get("current_edge", ""),
                "shares": alert.get("shares", ""),
                "sent_telegram": str(sent_telegram).lower(),
                "send_error": send_error,
            })


    # ── Edge 縮水偵測（STEP 13 附加）───────────────────────────

    def check_shrinks(
        self,
        positions: list,
        ev_signals: list,
    ) -> list[dict]:
        """
        檢查 open positions 的 edge 縮水。
        回傳 shrink_alerts（0 < edge < shrink_threshold，且不在獨立 cooldown 內）。
        edge < 0 由 check_exits 處理，此處不重複觸發。
        """
        shrink_alerts: list[dict] = []

        for pos in positions:
            if pos.get("status") != "open":
                continue

            position_id = pos.get("position_id", "")
            market_id = pos.get("market_id", "")
            side = pos.get("side", "")

            current = self._find_signal(market_id, ev_signals)
            if not current:
                continue

            if current.get("signal_status", "") != "active":
                continue

            if side == "NO":
                edge_raw = current.get("no_edge")
                current_price = _safe_float(current.get("no_best_bid"))
            else:
                edge_raw = current.get("yes_edge")
                current_price = _safe_float(current.get("yes_best_bid"))

            if edge_raw is None:
                continue

            try:
                edge_f = float(edge_raw)
            except (TypeError, ValueError):
                continue

            # 只在 0 < edge < shrink_threshold 時觸發（edge < 0 由 EXIT 處理）
            if not (0 < edge_f < self.shrink_threshold):
                continue

            # 獨立 cooldown（EXIT_COOLDOWN_MINUTES 分鐘）
            now = _now_utc()
            last = self.shrink_cooldown_map.get(position_id)
            if last and (now - last).total_seconds() < EXIT_COOLDOWN_MINUTES * 60:
                continue

            self.shrink_cooldown_map[position_id] = now
            shrink_alerts.append({
                "alert_type": "shrink",
                "position_id": position_id,
                "market_id": market_id,
                "city": pos.get("city", ""),
                "contract": pos.get("contract_label", ""),
                "side": side,
                "entry_edge": _safe_float(pos.get("entry_edge")),
                "current_edge": edge_f,
                "entry_price": _safe_float(pos.get("entry_price")),
                "current_price": current_price,
            })

        return shrink_alerts

    def process_shrinks(self, shrink_alerts: list) -> None:
        """推送 edge 縮水通知（帶 [記錄平倉] [知道了] 按鈕）。"""
        for alert in shrink_alerts:
            position_id = alert.get("position_id", "")

            if self.telegram is not None:
                text = format_shrink_alert(alert)
                ok, err = self.telegram.send_message(text)
                if ok:
                    log.info(
                        f"  SHRINK Telegram OK: {alert.get('city')} {alert.get('side')} "
                        f"edge={alert.get('current_edge', 0):+.1%}"
                    )
                else:
                    log.warning(f"  SHRINK Telegram FAIL: {position_id} — {err}")
            else:
                log.info(
                    f"  SHRINK alert (no Telegram): {alert.get('city')} "
                    f"{alert.get('contract')} {alert.get('side')} "
                    f"edge={alert.get('current_edge', 0):+.1%}"
                )


# ============================================================
# Telegram config loader
# ============================================================

def load_telegram_config() -> Optional[TelegramSender]:
    """
    環境變數優先，yaml 次之。沒有 config → 回傳 None（只寫 log）。

    環境變數：PM_TELEGRAM_BOT_TOKEN, PM_TELEGRAM_CHAT_ID
    YAML 路徑：config/telegram.yaml（不進 repo）
    """
    token = os.environ.get("PM_TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("PM_TELEGRAM_CHAT_ID")

    if not token or not chat_id:
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
                    cfg[k.strip()] = v.strip().strip('"').strip("'")

                enabled = cfg.get("enabled", "true").lower() in ("true", "1", "yes")
                if not enabled:
                    log.info("Telegram disabled in config/telegram.yaml")
                    return None

                token = token or cfg.get("bot_token")
                chat_id = chat_id or cfg.get("chat_id")
            except Exception as e:
                log.warning(f"Failed to load config/telegram.yaml: {e}")

    if token and chat_id:
        log.info("Telegram sender configured")
        return TelegramSender(token, chat_id)

    log.info("No Telegram config found — alerts will be logged only")
    return None
