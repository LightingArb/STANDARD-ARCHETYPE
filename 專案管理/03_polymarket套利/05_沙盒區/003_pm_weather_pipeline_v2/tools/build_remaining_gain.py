#!/usr/bin/env python3
"""
build_remaining_gain.py — 離線建立 remaining_gain ECDF 模型

邏輯：
  1. 對每個城市讀 data/raw/D/{city}/gfs_seamless/forecast_hourly_*.csv
  2. 每天取 lead_day = min(lead_day) 的那組（最新 snapshot）
  3. 按 target_time_utc 去重（處理 duplicate rows，8.2% London days 有 dup）
  4. 去重後 < MIN_HOURS_PER_DAY → 跳過這天
  5. 按 target_time_local 排序
  6. 計算 running_max 和 remaining_gain（= final_max - running_max，單調遞減）
  7. 按 local_hour 分桶（24 個桶，每桶 ~800 樣本）
  8. 每桶存 sorted_gains 供 ECDF 查詢

輸出：data/models/remaining_gain/{city}/remaining_gain_model.json

用法：
  cd 001_polymarket
  python tools/build_remaining_gain.py
"""

import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

PROJ_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = PROJ_DIR / "data" / "raw" / "D"
MODEL_DIR = PROJ_DIR / "data" / "models" / "remaining_gain"
SEED_PATH = PROJ_DIR / "config" / "seed_cities.json"

MIN_DAYS = 365          # 城市最低天數（< 1 年不建模）
MIN_HOURS_PER_DAY = 20  # 每天去重後最低有效小時數
MIN_BUCKET_SAMPLES = 30 # bucket 最低樣本數


def build():
    if not SEED_PATH.exists():
        print(f"ERROR: seed_cities.json not found: {SEED_PATH}", file=sys.stderr)
        sys.exit(1)

    seed = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    total_cities = 0

    for city in sorted(seed.keys()):
        if city.startswith("_"):
            continue
        if not seed[city].get("city_enabled", True):
            continue

        gfs_dir = RAW_DIR / city / "gfs_seamless"
        if not gfs_dir.exists():
            continue

        csv_files = sorted(gfs_dir.glob("forecast_hourly_*.csv"))
        if not csv_files:
            print(f"  SKIP {city}: no forecast_hourly_*.csv")
            continue

        # 按 local_hour 收集 remaining_gain 樣本
        gains_by_hour: dict[int, list[float]] = defaultdict(list)
        n_days = 0
        n_skipped = 0

        for csv_file in csv_files:
            try:
                with open(csv_file, encoding="utf-8") as f:
                    rows = list(csv.DictReader(f))

                ok_rows = [r for r in rows if r.get("value_status", "ok") == "ok"]
                if not ok_rows:
                    continue

                # 取 lead_day 最小的那組（最新 snapshot）
                lead_days: list[int] = []
                for r in ok_rows:
                    try:
                        lead_days.append(int(float(r.get("lead_day", 999))))
                    except (ValueError, TypeError):
                        lead_days.append(999)

                min_ld = min(lead_days)
                latest = [r for r, ld in zip(ok_rows, lead_days) if ld == min_ld]

                # 去重：按 target_time_utc
                seen: set[str] = set()
                deduped: list[dict] = []
                for r in latest:
                    key = r.get("target_time_utc", "")
                    if key and key not in seen:
                        seen.add(key)
                        deduped.append(r)

                if len(deduped) < MIN_HOURS_PER_DAY:
                    n_skipped += 1
                    continue

                # 按 target_time_local 排序
                deduped.sort(key=lambda r: r.get("target_time_local", ""))

                # 解析氣溫（全部 Celsius，GFS 保證）
                temps: list[float] = []
                for r in deduped:
                    try:
                        temps.append(float(r.get("forecast_temp", 0) or 0))
                    except (ValueError, TypeError):
                        temps.append(0.0)

                final_max = max(temps)
                if final_max < -50:  # 無效資料
                    continue

                # 計算每小時的 remaining_gain
                running_max = float("-inf")
                for r, temp in zip(deduped, temps):
                    running_max = max(running_max, temp)
                    gain = round(final_max - running_max, 2)  # >= 0，單調遞減

                    time_str = r.get("target_time_local", "")
                    if "T" not in time_str:
                        continue
                    try:
                        hour = int(time_str.split("T")[1].split(":")[0])
                    except (ValueError, IndexError):
                        continue

                    gains_by_hour[hour].append(gain)

                n_days += 1

            except Exception as e:
                print(f"  WARNING {city} {csv_file.name}: {e}")
                continue

        # 城市天數門檻
        if n_days < MIN_DAYS:
            print(f"  SKIP {city}: {n_days} days < {MIN_DAYS} (skipped_days={n_skipped})")
            continue

        # 建模型
        city_model_dir = MODEL_DIR / city
        city_model_dir.mkdir(parents=True, exist_ok=True)

        buckets: dict = {}
        for hour in range(24):
            gains = gains_by_hour.get(hour, [])
            if len(gains) < MIN_BUCKET_SAMPLES:
                continue

            sorted_gains = sorted(gains)
            n = len(sorted_gains)
            avg = sum(sorted_gains) / n
            p_zero = sum(1 for g in sorted_gains if g < 0.5) / n  # P(gain < 0.5°C)

            buckets[str(hour)] = {
                "hour": hour,
                "n_samples": n,
                "mean_gain": round(avg, 3),
                "p_zero_gain": round(p_zero, 3),
                "q50_gain": round(sorted_gains[n // 2], 2),
                "q90_gain": round(sorted_gains[int(n * 0.9)], 2),
                "max_gain": round(sorted_gains[-1], 2),
                "sorted_gains": sorted_gains,
            }

        if not buckets:
            print(f"  SKIP {city}: no valid buckets after MIN_BUCKET_SAMPLES={MIN_BUCKET_SAMPLES}")
            continue

        model = {
            "schema_version": 1,
            "model_type": "remaining_gain_ecdf",
            "city": city,
            "unit": "C",
            "bucket_type": "local_hour",
            "n_total_days": n_days,
            "n_valid_buckets": len(buckets),
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "buckets": buckets,
        }

        model_path = city_model_dir / "remaining_gain_model.json"
        model_path.write_text(
            json.dumps(model, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        # 摘要（顯示 h14 和 h16 以驗算下午升溫空間）
        h14 = buckets.get("14", {})
        h16 = buckets.get("16", {})
        print(
            f"  OK {city}: {n_days} days, {len(buckets)} buckets | "
            f"h14 mean={h14.get('mean_gain','?')} q50={h14.get('q50_gain','?')} | "
            f"h16 mean={h16.get('mean_gain','?')} q50={h16.get('q50_gain','?')}"
        )
        total_cities += 1

    print(f"\nTotal cities with remaining_gain model: {total_cities}")
    print(f"Models written to: {MODEL_DIR}")


if __name__ == "__main__":
    build()
