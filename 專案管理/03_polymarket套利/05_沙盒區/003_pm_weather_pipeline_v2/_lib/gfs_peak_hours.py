"""
_lib/gfs_peak_hours.py — GFS hourly forecast 峰值時段計算

從 GFS hourly CSV 找出每個 market_date 的峰值溫度小時，
寫入 data/gfs_peak_hours.json（供 telegram_bot 使用）。

輸出格式（merge 式，不覆蓋已鎖定條目）：
  {
    "schema_version": 1,
    "generated_at_utc": "...",
    "cities": {
      "London": {
        "2026-04-12": {
          "peak_start": 14,
          "peak_end": 16,
          "peak_hour": 15,
          "snapshot_time_utc": "...",
          "locked": false,
          "updated_at_utc": "..."
        }
      }
    }
  }

鎖定邏輯：
  - 未來日期 → 正常計算
  - 今天 + now_local <= peak_end_datetime → 正常更新（覆蓋）
  - 今天 + now_local > peak_end_datetime → 鎖定，不覆蓋
  - 過去日期 → 不更新
"""

from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger(__name__)

PROJ_DIR = Path(__file__).resolve().parent.parent
OUTPUT_PATH = PROJ_DIR / "data" / "gfs_peak_hours.json"
PEAK_WINDOW_HALF = 1  # ±1h


# ============================================================
# 讀取 GFS hourly CSV
# ============================================================

def _load_gfs_hourly_csv(city: str, market_date: str) -> list[dict]:
    """
    讀取 data/raw/D/{city}/gfs_seamless/forecast_hourly_{market_date}.csv。
    回傳所有 rows（dict list）。
    """
    path = (
        PROJ_DIR / "data" / "raw" / "D" / city / "gfs_seamless"
        / f"forecast_hourly_{market_date}.csv"
    )
    if not path.exists():
        log.debug(f"GFS hourly CSV not found: {path}")
        return []
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))
    return rows


# ============================================================
# 峰值偵測
# ============================================================

def _detect_peak(rows: list[dict]) -> dict | None:
    """
    從 GFS hourly rows 找峰值時段。

    步驟：
    1. 按 snapshot_time_utc 降序，找最新 snapshot
    2. 篩出該 snapshot 的所有 rows
    3. 對 forecast_temp 取最大值 → peak_hour（local hour from target_time_local）
    4. 連續同溫 → 取最早 peak_hour（與 05_G_peak_fetch 行為一致）
    5. peak_start = max(0, peak_hour - PEAK_WINDOW_HALF)
    6. peak_end   = min(23, peak_hour + PEAK_WINDOW_HALF)

    回傳 dict 或 None（資料不足）。
    """
    if not rows:
        return None

    # 選最新 snapshot
    latest_snap = max(r["snapshot_time_utc"] for r in rows)
    snap_rows = [r for r in rows if r["snapshot_time_utc"] == latest_snap]

    # 解析 (local_hour, temp)
    points: list[tuple[int, float]] = []
    for r in snap_rows:
        try:
            # target_time_local 格式：2026-04-12T15:00 或 2026-04-12T15:00:00
            tl = r.get("target_time_local", "")
            if "T" in tl:
                hour = int(tl.split("T")[1].split(":")[0])
            else:
                continue
            temp = float(r["forecast_temp"])
            points.append((hour, temp))
        except (ValueError, KeyError, IndexError):
            continue

    if not points:
        return None

    max_temp = max(t for _, t in points)
    peak_hours = sorted(h for h, t in points if t == max_temp)
    peak_hour = peak_hours[0]  # 並列取最早

    peak_start = max(0, peak_hour - PEAK_WINDOW_HALF)
    peak_end = min(23, peak_hour + PEAK_WINDOW_HALF)

    return {
        "peak_start": peak_start,
        "peak_end": peak_end,
        "peak_hour": peak_hour,
        "snapshot_time_utc": latest_snap,
    }


# ============================================================
# 鎖定判斷
# ============================================================

def _is_peak_passed(market_date: str, peak_end: int, city_tz: str) -> bool:
    """
    peak_end 小時（local）已過 → 回傳 True（應鎖定，不覆蓋）。
    """
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(city_tz)
        now_local = datetime.now(tz)
        # peak_end 所在的 datetime（當天）
        peak_end_dt = datetime(
            int(market_date[:4]),
            int(market_date[5:7]),
            int(market_date[8:10]),
            peak_end, 59, 59,
            tzinfo=tz,
        )
        return now_local > peak_end_dt
    except Exception:
        return False


def _is_past_date(market_date: str, city_tz: str) -> bool:
    """market_date 是否已是昨天或更早（本地時間）。"""
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(city_tz)
        today = datetime.now(tz).date()
        mdate = datetime.strptime(market_date, "%Y-%m-%d").date()
        return mdate < today
    except Exception:
        return False


# ============================================================
# JSON I/O
# ============================================================

def _load_existing() -> dict:
    if not OUTPUT_PATH.exists():
        return {}
    try:
        return json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save(data: dict) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(OUTPUT_PATH) + f".{os.getpid()}.tmp"
    Path(tmp).write_text(
        json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    os.replace(tmp, str(OUTPUT_PATH))


# ============================================================
# 公開 API
# ============================================================

def get_gfs_peak_window(city: str, market_date: str) -> dict | None:
    """
    讀取 gfs_peak_hours.json，回傳指定城市 + 日期的峰值資料，或 None。

    回傳格式：{"peak_start": int, "peak_end": int, "peak_hour": int, ...}
    """
    existing = _load_existing()
    return existing.get("cities", {}).get(city, {}).get(market_date)


def update_gfs_peak_hours(city: str, city_tz: str, market_dates: list[str]) -> None:
    """
    對指定城市的 market_dates 計算峰值，merge 寫入 gfs_peak_hours.json。

    鎖定邏輯：
    - 過去日期 → 跳過（不計算）
    - 今天 + peak 已過 → 鎖定（不覆蓋）
    - 其他 → 計算並覆蓋

    呼叫者通常是 collector_main.task_update_forecast()，在 05_D_forecast_fetch 之後。
    """
    if not market_dates:
        return

    existing = _load_existing()
    old_cities: dict = existing.get("cities", {})
    city_data: dict = old_cities.get(city, {})

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    updated = 0

    for market_date in market_dates:
        # 過去日期 → 跳過
        if _is_past_date(market_date, city_tz):
            log.debug(f"  {city} {market_date}: past date, skip")
            continue

        # 已鎖定 → 跳過
        existing_entry = city_data.get(market_date, {})
        if existing_entry.get("locked", False):
            log.debug(f"  {city} {market_date}: locked, skip")
            continue

        # 讀取 GFS CSV 並偵測峰值
        rows = _load_gfs_hourly_csv(city, market_date)
        peak = _detect_peak(rows)
        if peak is None:
            log.warning(f"  {city} {market_date}: no GFS data for peak detection")
            continue

        # 峰值已過 → 鎖定
        peak_end = peak["peak_end"]
        should_lock = _is_peak_passed(market_date, peak_end, city_tz)

        city_data[market_date] = {
            **peak,
            "locked": should_lock,
            "updated_at_utc": now_utc,
        }
        status = "LOCKED" if should_lock else "updated"
        log.info(
            f"  {city} {market_date}: peak={peak['peak_hour']:02d}h "
            f"[{peak['peak_start']:02d}-{peak['peak_end']:02d}] {status}"
        )
        updated += 1

    if updated == 0:
        log.debug(f"  {city}: no peak hours updated")
        return

    old_cities[city] = city_data
    output = {
        "schema_version": 1,
        "generated_at_utc": now_utc,
        "cities": old_cities,
    }
    _save(output)
    log.info(f"gfs_peak_hours.json updated ({city}: {updated} dates)")
