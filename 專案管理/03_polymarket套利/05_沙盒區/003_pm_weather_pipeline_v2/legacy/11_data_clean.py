"""
11_data_clean.py — 資料清洗合併

把 D（預報）、A（氣象站觀測）、C（ERA5 再分析）三個來源的 snapshot CSV
對齊到同一個時間軸，算出誤差，輸出乾淨的 raw_forecast_table.csv。

核心邏輯：
  1. 讀所有 D CSV → forecast_temp
  2. 讀所有 A CSV → observed_A
  3. 讀所有 C CSV → observed_C
  4. 以 city + target_time_utc 配對
  5. 算 error_A = observed_A - forecast_temp
  6. 算 error_C = observed_C - forecast_temp
  7. 加 lead_bucket（每 24h 一桶，1-7）

用法：
  python 11_data_clean.py
  python 11_data_clean.py --d-root 08_snapshot/D --a-root 08_snapshot/A --c-root 08_snapshot/C
  python 11_data_clean.py --cities London,Tokyo --force
"""

import argparse
import csv
import logging
import math
import sys
from pathlib import Path
from datetime import datetime

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ============================================================
# 預設路徑
# ============================================================
DEFAULT_D_ROOT = PROJ_DIR / "08_snapshot" / "D"
DEFAULT_A_ROOT = PROJ_DIR / "08_snapshot" / "A"
DEFAULT_C_ROOT = PROJ_DIR / "08_snapshot" / "C"
DEFAULT_OUTPUT_DIR = PROJ_DIR / "data" / "cleaned"

DEFAULT_CITIES = ["London", "Shanghai", "New York City", "Paris", "Toronto", "Tokyo"]

# ============================================================
# 輸出欄位定義
# ============================================================
OUTPUT_FIELDS = [
    "city",
    "model",
    "target_time_utc",
    "target_time_local",
    "snapshot_date_local",
    "lead_hours",
    "lead_bucket",
    "forecast_temp",
    "observed_A",
    "observed_C",
    "error_A",
    "error_C",
    "timezone",
    "observed_A_status",
    "observed_C_status",
]


# ============================================================
# 工具函式
# ============================================================

def try_float(s) -> float | None:
    """安全轉 float，失敗回 None。"""
    if s is None or str(s).strip() == "":
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def round_or_empty(v: float | None, digits: int = 4) -> str:
    """float 轉字串，None 轉空字串。"""
    if v is None:
        return ""
    return str(round(v, digits))


def normalize_utc(ts_str: str) -> str:
    """
    把各種 UTC 時間格式統一成 'YYYY-MM-DDTHH:MM:SSZ'。
    
    處理情境：
      - '2026-01-01T01:00:00Z'  → 不動
      - '2026-01-01 01:00:00Z'  → 補 T
      - '2026-01-01T01:00Z'     → 補 :00
      - '2026-01-01 01:00'      → 補 T + Z
    """
    if not ts_str or not ts_str.strip():
        return ""
    s = ts_str.strip()

    # 把空格換成 T
    if "T" not in s and " " in s:
        s = s.replace(" ", "T", 1)

    # 去掉尾部 Z 做格式化，最後再加回來
    if s.endswith("Z"):
        s = s[:-1]

    # 補齊秒數：如果只有 HH:MM 沒有 :SS
    # 判斷 T 後面的部分
    if "T" in s:
        date_part, time_part = s.split("T", 1)
        colons = time_part.count(":")
        if colons == 1:
            time_part += ":00"
        s = date_part + "T" + time_part

    return s + "Z"


def round_to_hour_utc(ts_str: str) -> str:
    """
    把 UTC 時間 round 到最近整點。
    
    IEM 觀測可能是 13:52，要 round 到 14:00 才能跟 D 的整點預測配對。
    """
    norm = normalize_utc(ts_str)
    if not norm:
        return ""
    try:
        # 去掉 Z 解析
        dt = datetime.strptime(norm, "%Y-%m-%dT%H:%M:%SZ")
        # round 到最近整點
        if dt.minute >= 30:
            dt = dt.replace(minute=0, second=0)
            from datetime import timedelta
            dt += timedelta(hours=1)
        else:
            dt = dt.replace(minute=0, second=0)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return norm


def compute_lead_bucket(lead_hours: int) -> int:
    """
    lead_hours → lead_bucket（每 24 小時一桶）
    
    1-24   → 1
    25-48  → 2
    49-72  → 3
    73-96  → 4
    97-120 → 5
    121-144 → 6
    145-168 → 7
    """
    if lead_hours <= 0:
        return 0
    return min(((lead_hours - 1) // 24) + 1, 7)


# ============================================================
# CSV 讀取
# ============================================================

def find_snapshot_csvs(root: Path, city: str, model: str = None) -> list[Path]:
    """
    在指定目錄下找所有 snapshot_batch__*.csv。
    
    D 結構：{root}/{city}/{model}/snapshot_batch__*.csv
    A/C 結構：{root}/{city}/snapshot_batch__*.csv
    """
    if model:
        search_dir = root / city / model
    else:
        search_dir = root / city

    if not search_dir.exists():
        return []

    return sorted(search_dir.glob("snapshot_batch__*.csv"))


def load_d_csvs(d_root: Path, city: str, model: str) -> dict[str, dict]:
    """
    讀取某個 city × model 的所有 D snapshot CSV。
    回傳 dict，key = target_time_utc（整點），value = row dict。
    """
    index = {}
    csv_files = find_snapshot_csvs(d_root, city, model)

    for csv_path in csv_files:
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                # 跳過非 ok 的資料
                if row.get("value_status", "").strip() != "ok":
                    continue

                target_utc = normalize_utc(row.get("target_time_utc", ""))
                if not target_utc:
                    continue

                index[target_utc] = row

    return index


def load_observation_csvs(root: Path, city: str) -> dict[str, dict]:
    """
    讀取某個 city 的所有 A 或 C snapshot CSV。
    回傳 dict，key = target_time_utc（round 到整點），value = row dict。
    
    A 和 C 的 CSV 結構相同，只是目錄不同。
    """
    index = {}
    csv_files = find_snapshot_csvs(root, city, model=None)

    for csv_path in csv_files:
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                if row.get("value_status", "").strip() != "ok":
                    continue

                target_utc = row.get("target_time_utc", "")
                if not target_utc:
                    continue

                # A 可能不是整點（13:52），round 到最近整點
                rounded_utc = round_to_hour_utc(target_utc)
                if not rounded_utc:
                    continue

                # 若同一個整點有多筆，取最後一筆（最新的）
                index[rounded_utc] = row

    return index


# ============================================================
# 發現可用的 model 清單
# ============================================================

def discover_models(d_root: Path, city: str) -> list[str]:
    """掃描 D root 下某個城市有哪些 model 子目錄。"""
    city_dir = d_root / city
    if not city_dir.exists():
        return []
    return sorted([
        d.name for d in city_dir.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    ])


# ============================================================
# 主流程：Join D + A + C
# ============================================================

def join_city_model(
    d_index: dict[str, dict],
    a_index: dict[str, dict],
    c_index: dict[str, dict],
    city: str,
    model: str,
) -> list[dict]:
    """
    以 D 為主表，left join A 和 C。
    
    配對邏輯：
      D.target_time_utc == A.target_time_utc（round 到整點）
      D.target_time_utc == C.target_time_utc
    """
    rows = []

    for target_utc, d_row in sorted(d_index.items()):
        forecast_temp = try_float(d_row.get("temperature_2m"))
        if forecast_temp is None:
            continue

        lead_hours_raw = try_float(d_row.get("horizon_hour"))
        if lead_hours_raw is None:
            continue
        lead_hours = int(lead_hours_raw)

        # ---- 配對 A ----
        a_row = a_index.get(target_utc)
        observed_A = None
        observed_A_status = "missing"
        if a_row:
            observed_A = try_float(a_row.get("temperature_2m"))
            if observed_A is not None:
                observed_A_status = "ok"

        # ---- 配對 C ----
        c_row = c_index.get(target_utc)
        observed_C = None
        observed_C_status = "missing"
        if c_row:
            observed_C = try_float(c_row.get("temperature_2m"))
            if observed_C is not None:
                observed_C_status = "ok"

        # ---- 算誤差 ----
        error_A = (observed_A - forecast_temp) if observed_A is not None else None
        error_C = (observed_C - forecast_temp) if observed_C is not None else None

        # ---- 組 row ----
        out = {
            "city": city,
            "model": model,
            "target_time_utc": target_utc,
            "target_time_local": d_row.get("target_time_local", ""),
            "snapshot_date_local": d_row.get("snapshot_date_local", ""),
            "lead_hours": str(lead_hours),
            "lead_bucket": str(compute_lead_bucket(lead_hours)),
            "forecast_temp": round_or_empty(forecast_temp, 2),
            "observed_A": round_or_empty(observed_A, 2),
            "observed_C": round_or_empty(observed_C, 2),
            "error_A": round_or_empty(error_A, 4),
            "error_C": round_or_empty(error_C, 4),
            "timezone": d_row.get("timezone", ""),
            "observed_A_status": observed_A_status,
            "observed_C_status": observed_C_status,
        }
        rows.append(out)

    return rows


# ============================================================
# CSV 寫入
# ============================================================

def write_csv(rows: list[dict], fields: list[str], path: Path):
    """寫 CSV，自動建目錄。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    log.info(f"  wrote {len(rows)} rows → {path}")


# ============================================================
# 統計摘要
# ============================================================

def print_summary(all_rows: list[dict]):
    """印出簡短統計摘要。"""
    total = len(all_rows)
    a_ok = sum(1 for r in all_rows if r.get("observed_A_status") == "ok")
    c_ok = sum(1 for r in all_rows if r.get("observed_C_status") == "ok")
    both_ok = sum(
        1 for r in all_rows
        if r.get("observed_A_status") == "ok" and r.get("observed_C_status") == "ok"
    )

    log.info("")
    log.info("=" * 70)
    log.info("DATA CLEANING SUMMARY")
    log.info("=" * 70)
    log.info(f"  Total D forecast rows:    {total:>8}")
    log.info(f"  Matched with A (ok):      {a_ok:>8}  ({a_ok/total*100:.1f}%)" if total else "")
    log.info(f"  Matched with C (ok):      {c_ok:>8}  ({c_ok/total*100:.1f}%)" if total else "")
    log.info(f"  Matched both A+C (ok):    {both_ok:>8}  ({both_ok/total*100:.1f}%)" if total else "")
    log.info(f"  A missing:                {total - a_ok:>8}")
    log.info(f"  C missing:                {total - c_ok:>8}")

    # 按城市 × 模型 breakdown
    combos = {}
    for r in all_rows:
        key = (r["city"], r["model"])
        if key not in combos:
            combos[key] = {"total": 0, "a_ok": 0, "c_ok": 0}
        combos[key]["total"] += 1
        if r.get("observed_A_status") == "ok":
            combos[key]["a_ok"] += 1
        if r.get("observed_C_status") == "ok":
            combos[key]["c_ok"] += 1

    log.info("")
    log.info(f"  {'city':<18} {'model':<22} {'total':>6} {'A_ok':>6} {'C_ok':>6}")
    log.info(f"  {'-'*64}")
    for (city, model), stats in sorted(combos.items()):
        log.info(
            f"  {city:<18} {model:<22} "
            f"{stats['total']:>6} {stats['a_ok']:>6} {stats['c_ok']:>6}"
        )
    log.info("=" * 70)


# ============================================================
# CLI
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="11_data_clean: join D forecasts with A/C observations"
    )
    parser.add_argument(
        "--d-root", type=Path, default=DEFAULT_D_ROOT,
        help="D snapshot root directory",
    )
    parser.add_argument(
        "--a-root", type=Path, default=DEFAULT_A_ROOT,
        help="A snapshot root directory",
    )
    parser.add_argument(
        "--c-root", type=Path, default=DEFAULT_C_ROOT,
        help="C snapshot root directory",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
        help="Output directory for cleaned CSV",
    )
    parser.add_argument(
        "--cities", type=str, default=None,
        help="Comma-separated city list (default: all 6 cities)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Force regeneration even if output exists",
    )
    return parser.parse_args()


# ============================================================
# Main
# ============================================================

def main():
    args = parse_args()

    # 城市清單
    if args.cities:
        cities = [c.strip() for c in args.cities.split(",")]
    else:
        cities = DEFAULT_CITIES

    output_path = args.output_dir / "raw_forecast_table.csv"

    # Skip 邏輯
    if output_path.exists() and not args.force:
        log.info(f"Output already exists: {output_path}")
        log.info("Use --force to regenerate.")
        return

    log.info("=" * 70)
    log.info("11_data_clean: START")
    log.info(f"  D root: {args.d_root}")
    log.info(f"  A root: {args.a_root}")
    log.info(f"  C root: {args.c_root}")
    log.info(f"  Cities: {cities}")
    log.info(f"  Output: {output_path}")
    log.info("=" * 70)

    all_rows = []

    for city in cities:
        log.info(f"\n--- {city} ---")

        # 讀 A 和 C（整個城市共用一份，不分 model）
        a_index = load_observation_csvs(args.a_root, city)
        c_index = load_observation_csvs(args.c_root, city)
        log.info(f"  A observations loaded: {len(a_index)} hours")
        log.info(f"  C observations loaded: {len(c_index)} hours")

        # 找這個城市有哪些 D model
        models = discover_models(args.d_root, city)
        if not models:
            log.warning(f"  No D models found for {city}, skip.")
            continue
        log.info(f"  D models found: {models}")

        # 對每個 model，讀 D 然後 join
        for model in models:
            d_index = load_d_csvs(args.d_root, city, model)
            log.info(f"  D loaded: {city} × {model} = {len(d_index)} rows")

            rows = join_city_model(d_index, a_index, c_index, city, model)
            log.info(f"  Joined: {len(rows)} rows")

            all_rows.extend(rows)

    # 寫出
    if all_rows:
        write_csv(all_rows, OUTPUT_FIELDS, output_path)
        print_summary(all_rows)
    else:
        log.warning("No data produced. Check that A/C/D snapshots exist.")

    log.info("\n11_data_clean: DONE")


if __name__ == "__main__":
    main()
