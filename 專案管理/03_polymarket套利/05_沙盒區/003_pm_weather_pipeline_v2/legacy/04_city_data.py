"""
04_city_data.py — 城市資料整理，產出 01_city.csv

讀取：
  1. data/03_city_market_mapping.json（03 的語義分析輸出）
  2. config/seed_cities.json（半靜態 metadata seed）
  3. config/city_override.json（可選，人工覆蓋）

產出：
  - data/01_city.csv（唯一 live 城市主檔）
  - logs/04_city_data/city_data_report.log（處理報告）

不做：
  - 不呼叫 Polymarket API
  - 不做市場解析
  - 不使用 rglob 搜尋外部 JSON
  - 不依賴外部 repo 路徑
"""

import argparse
import csv
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ============================================================
# 01_city.csv 欄位定義（最終版）
# ============================================================

CITY_CSV_FIELDS = [
    "city",
    "country",
    "unit",
    "precision",
    "settlement_source_type",
    "truth_mode",
    "lat",
    "lon",
    "station_code",
    "timezone",
    "settlement_url",
    "city_enabled",
    "market_count",
    "date_range",
    "temp_units_seen",
    "market_types_seen",
    "manual_note",
]

# 必要 metadata 欄位（缺任何一項 → city_enabled=false）
REQUIRED_METADATA = ["lat", "lon", "station_code", "timezone"]


# ============================================================
# 載入
# ============================================================

def load_json(path: Path) -> dict:
    if not path.exists():
        log.warning(f"File not found: {path}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # 過濾 _meta key
    return {k: v for k, v in data.items() if not k.startswith("_")}


def load_mapping(path: Path) -> dict:
    """載入 03_city_market_mapping.json"""
    mapping = load_json(path)
    if not mapping:
        log.error(f"City market mapping is empty or missing: {path}")
    else:
        log.info(f"Loaded mapping: {len(mapping)} cities from {path.name}")
    return mapping


def load_seed(path: Path) -> dict:
    """載入 config/seed_cities.json"""
    seed = load_json(path)
    if not seed:
        log.warning(f"Seed cities is empty or missing: {path}")
    else:
        log.info(f"Loaded seed: {len(seed)} cities from {path.name}")
    return seed


def load_override(path: Path) -> dict:
    """載入 config/city_override.json（可選）"""
    override = load_json(path)
    if override:
        log.info(f"Loaded override: {len(override)} entries from {path.name}")
    return override


# ============================================================
# 合併邏輯
# ============================================================

def merge_city_data(
    mapping: dict,
    seed: dict,
    override: dict,
) -> list[dict]:
    """
    合併城市資料：
    1. mapping 提供：city, country, unit, precision, settlement_source_type,
                    market_count, date_range, temp_units_seen, market_types_seen
    2. seed 提供：lat, lon, station_code, timezone, settlement_url, truth_mode, city_enabled
    3. override 覆蓋 seed 的任何欄位
    """
    # 收集所有城市（mapping + seed 的聯集）
    all_cities = sorted(set(list(mapping.keys()) + list(seed.keys())))

    rows = []
    incomplete_cities = []
    new_cities = []

    for city in all_cities:
        map_data = mapping.get(city, {})
        seed_data = seed.get(city, {})
        over_data = override.get(city, {})

        # 合併：override > seed > mapping
        merged = {}
        merged["city"] = city

        # 從 mapping 取語義分析結果
        merged["country"] = over_data.get("country") or seed_data.get("country") or map_data.get("country", "")
        merged["unit"] = over_data.get("unit") or map_data.get("unit") or seed_data.get("unit", "")
        merged["precision"] = over_data.get("precision") or map_data.get("precision") or seed_data.get("precision", "")
        merged["settlement_source_type"] = (
            over_data.get("settlement_source_type")
            or map_data.get("settlement_source_type")
            or seed_data.get("settlement_source_type", "")
        )
        merged["market_count"] = str(map_data.get("market_count", ""))
        merged["date_range"] = map_data.get("date_range", "")
        merged["temp_units_seen"] = map_data.get("temp_units_seen", "")
        merged["market_types_seen"] = map_data.get("market_types_seen", "")

        # 從 seed/override 取 metadata
        merged["lat"] = str(over_data.get("lat") if over_data.get("lat") is not None else (seed_data.get("lat") if seed_data.get("lat") is not None else ""))
        merged["lon"] = str(over_data.get("lon") if over_data.get("lon") is not None else (seed_data.get("lon") if seed_data.get("lon") is not None else ""))
        merged["station_code"] = over_data.get("station_code") or seed_data.get("station_code", "")
        merged["timezone"] = over_data.get("timezone") or seed_data.get("timezone", "")
        merged["settlement_url"] = over_data.get("settlement_url") or seed_data.get("settlement_url", "")
        merged["truth_mode"] = over_data.get("truth_mode") or seed_data.get("truth_mode", "official_daily_summary")

        # city_enabled 判定
        explicit_enabled = over_data.get("city_enabled")
        if explicit_enabled is None:
            explicit_enabled = seed_data.get("city_enabled")

        # 檢查 metadata 完整度
        missing_fields = []
        for field in REQUIRED_METADATA:
            val = merged.get(field, "")
            if val is None or str(val).strip() == "" or str(val).strip() == "None":
                missing_fields.append(field)

        if missing_fields:
            merged["city_enabled"] = "false"
            note_parts = [f"incomplete: missing {','.join(missing_fields)}"]
            incomplete_cities.append((city, missing_fields))
        elif explicit_enabled is False or str(explicit_enabled).lower() == "false":
            merged["city_enabled"] = "false"
            deferred_reason = seed_data.get("deferred_reason") or over_data.get("deferred_reason", "")
            note_parts = [f"deferred: {deferred_reason}"] if deferred_reason else ["deferred"]
        else:
            merged["city_enabled"] = "true"
            note_parts = []

        # 標記 mapping 中沒有但 seed 中有的城市
        if city not in mapping:
            note_parts.append("not in current Polymarket markets")
            new_cities.append(city)

        # 標記 seed 中沒有的城市（新發現）
        if city not in seed:
            note_parts.append("new city: not in seed_cities.json")
            new_cities.append(city)

        merged["manual_note"] = "; ".join(note_parts) if note_parts else ""

        rows.append(merged)

    # Log summary
    enabled_count = sum(1 for r in rows if r["city_enabled"] == "true")
    disabled_count = len(rows) - enabled_count
    log.info(f"Total cities: {len(rows)} (enabled={enabled_count}, disabled={disabled_count})")

    if incomplete_cities:
        log.warning(f"Incomplete metadata ({len(incomplete_cities)} cities):")
        for city, fields in incomplete_cities:
            log.warning(f"  {city}: missing {', '.join(fields)}")

    if new_cities:
        log.info(f"New/orphan cities: {', '.join(sorted(set(new_cities)))}")

    return rows


# ============================================================
# 寫檔
# ============================================================

def write_city_csv(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CITY_CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    log.info(f"Written: {output_path} ({len(rows)} rows)")


def write_report(rows: list[dict], report_path: Path) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        f"04_city_data report @ {now_str}",
        f"Total cities: {len(rows)}",
        f"Enabled: {sum(1 for r in rows if r['city_enabled'] == 'true')}",
        f"Disabled: {sum(1 for r in rows if r['city_enabled'] != 'true')}",
        "",
        "City details:",
    ]
    for row in rows:
        status = "ON " if row["city_enabled"] == "true" else "OFF"
        note = f"  ({row['manual_note']})" if row["manual_note"] else ""
        lines.append(
            f"  [{status}] {row['city']:<25} {row['country']:<3} "
            f"{row['station_code']:<6} {row['timezone']:<30}{note}"
        )

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    log.info(f"Written: {report_path}")


# ============================================================
# 主流程
# ============================================================

def run(dry_run: bool = False, verbose: bool = False) -> list[dict]:
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    mapping_path = PROJ_DIR / "data" / "03_city_market_mapping.json"
    seed_path = PROJ_DIR / "config" / "seed_cities.json"
    override_path = PROJ_DIR / "config" / "city_override.json"
    city_csv_path = PROJ_DIR / "data" / "01_city.csv"
    report_path = PROJ_DIR / "logs" / "04_city_data" / "city_data_report.log"

    log.info("=" * 50)
    log.info("04_city_data: City metadata assembly")
    log.info("=" * 50)

    # Step 1: 載入
    log.info("Step 1: Load sources...")
    mapping = load_mapping(mapping_path)
    seed = load_seed(seed_path)
    override = load_override(override_path)

    if not mapping and not seed:
        log.error("Both mapping and seed are empty. Cannot produce 01_city.csv.")
        return []

    # Step 2: 合併
    log.info("Step 2: Merge city data...")
    rows = merge_city_data(mapping, seed, override)

    # Step 3: 寫檔
    if dry_run:
        log.info("[DRY RUN] Skip write.")
        for row in rows:
            status = "ON " if row["city_enabled"] == "true" else "OFF"
            log.info(f"  [{status}] {row['city']:<25} {row['country']:<3} "
                     f"{row['station_code']:<6} {row['timezone']}")
    else:
        write_city_csv(rows, city_csv_path)
        write_report(rows, report_path)

    log.info("04_city_data done.")
    return rows


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="City metadata assembly → data/01_city.csv",
    )
    p.add_argument("--dry-run", action="store_true", help="Only print, no write")
    p.add_argument("--verbose", action="store_true", help="Verbose log")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    run(dry_run=args.dry_run, verbose=args.verbose)
