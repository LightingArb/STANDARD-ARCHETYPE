#!/usr/bin/env python3
"""
build_peak_hours.py — 離線建立各城市峰值時段（季節 p25-p75，schema_version=2）

每個 CSV（forecast_hourly_YYYY-MM-DD.csv）包含多個 GFS snapshot × 24 小時。
只取 lead_day 最小的那組（最新 snapshot）找峰值小時。
按季節（spring/summer/autumn/winter）分組，算 p25-p75。
門檻：總天數 >= 365 才輸出。

用法：
  cd 003_pm_weather_pipeline_v2
  python tools/build_peak_hours.py
"""

import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

PROJ_DIR = Path(__file__).resolve().parent.parent
SEED_PATH = PROJ_DIR / "config" / "seed_cities.json"
RAW_DIR = PROJ_DIR / "data" / "raw" / "D"
OUTPUT = PROJ_DIR / "config" / "city_peak_hours.json"

MIN_DAYS = 365

SEASONS = {
    "spring": [3, 4, 5],
    "summer": [6, 7, 8],
    "autumn": [9, 10, 11],
    "winter": [12, 1, 2],
}

MONTH_TO_SEASON = {}
for _season, _months in SEASONS.items():
    for _m in _months:
        MONTH_TO_SEASON[_m] = _season


def get_season(month: int) -> str | None:
    return MONTH_TO_SEASON.get(month)


def build():
    if not SEED_PATH.exists():
        print(f"ERROR: seed_cities.json not found at {SEED_PATH}", file=sys.stderr)
        sys.exit(1)

    seed = json.loads(SEED_PATH.read_text(encoding="utf-8"))

    result: dict = {
        "schema_version": 2,
        "method": "seasonal_p25_p75",
        "min_days": MIN_DAYS,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    for city, info in sorted(seed.items()):
        if city.startswith("_"):
            continue
        if not info.get("city_enabled", True):
            continue

        tz_str = info.get("timezone", "")
        gfs_dir = RAW_DIR / city / "gfs_seamless"
        if not gfs_dir.exists():
            continue

        csv_files = sorted(gfs_dir.glob("forecast_hourly_*.csv"))
        if not csv_files:
            print(f"  SKIP {city}: no forecast_hourly_*.csv files")
            continue

        seasonal_peaks: dict[str, list[float]] = defaultdict(list)
        total_days = 0

        for csv_file in csv_files:
            try:
                with open(csv_file, encoding="utf-8") as f:
                    rows = list(csv.DictReader(f))
            except Exception:
                continue

            if not rows:
                continue

            # 只保留 value_status == "ok"（或欄位不存在）
            ok_rows = [r for r in rows if r.get("value_status", "ok") == "ok"]
            if not ok_rows:
                continue

            # 找最小 lead_day（= 最新 snapshot）
            lead_days: list[int] = []
            for r in ok_rows:
                try:
                    lead_days.append(int(float(r.get("lead_day", 999))))
                except Exception:
                    lead_days.append(999)

            min_ld = min(lead_days)
            latest = [r for r, ld in zip(ok_rows, lead_days) if ld == min_ld]

            if not latest:
                continue

            # 找最高溫（forecast_temp）
            temps: list[float] = []
            for r in latest:
                try:
                    temps.append(float(r.get("forecast_temp", float("-inf")) or float("-inf")))
                except Exception:
                    temps.append(float("-inf"))

            max_temp = max(temps)
            if max_temp <= -90:
                continue

            # tie-breaking：plateau 中點
            peak_hours_list: list[int] = []
            for r, t in zip(latest, temps):
                if abs(t - max_temp) < 0.01:
                    time_str = r.get("target_time_local", "")
                    if "T" in time_str:
                        try:
                            h = int(time_str.split("T")[1].split(":")[0])
                            peak_hours_list.append(h)
                        except Exception:
                            pass

            if not peak_hours_list:
                continue

            peak_hour = sum(peak_hours_list) / len(peak_hours_list)

            # 取月份（從檔名：forecast_hourly_YYYY-MM-DD.csv）
            date_part = csv_file.stem.replace("forecast_hourly_", "")
            try:
                month = int(date_part.split("-")[1])
            except Exception:
                continue

            season = get_season(month)
            if season:
                seasonal_peaks[season].append(peak_hour)
                total_days += 1

        # 門檻
        if total_days < MIN_DAYS:
            print(f"  SKIP {city}: {total_days} days < {MIN_DAYS}")
            continue

        # 算 p25-p75
        city_result: dict = {
            "total_days": total_days,
            "timezone": tz_str,
            "seasons": {},
        }

        for season in ["spring", "summer", "autumn", "winter"]:
            hours = seasonal_peaks.get(season, [])
            if len(hours) < 10:
                continue
            s = sorted(hours)
            p25 = int(s[len(s) // 4])
            p75 = int(s[3 * len(s) // 4])
            if p75 < p25:
                p25, p75 = p75, p25
            city_result["seasons"][season] = {
                "start": p25,
                "end": p75,
                "n": len(hours),
            }

        if city_result["seasons"]:
            result[city] = city_result
            seasons_str = ", ".join(
                f"{s}:{v['start']}-{v['end']}"
                for s, v in city_result["seasons"].items()
            )
            print(f"  OK {city}: {total_days} days | {seasons_str}")
        else:
            print(f"  SKIP {city}: no valid seasons after p25-p75")

    OUTPUT.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    cities_out = [k for k in result if k not in ("schema_version", "method", "min_days", "generated_at")]
    print(f"\nWritten: {OUTPUT}")
    print(f"Cities with peak data: {len(cities_out)}")


if __name__ == "__main__":
    build()
