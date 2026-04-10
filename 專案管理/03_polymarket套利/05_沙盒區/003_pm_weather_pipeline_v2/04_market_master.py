"""
04_market_master.py — market master 組裝，產出 market_master.csv

讀取：
  1. data/03_market_catalog.csv（03 的 market-level catalog）
  2. config/seed_cities.json（站點 metadata seed）
  3. config/city_override.json（可選，人工覆蓋，優先序：override > seed）

產出：
  - data/market_master.csv（每列 = 一個市場 + station metadata + 啟停狀態）
  - logs/04_market_master/master_report.log（處理報告）

不做：
  - 不呼叫 Polymarket API
  - 不做市場解析
  - 不抓任何氣象資料
"""

import argparse
import csv
import json
import logging
import sys
from datetime import datetime, timezone, date as _date
try:
    from zoneinfo import ZoneInfo as _ZoneInfo
except ImportError:
    _ZoneInfo = None  # type: ignore[assignment]
from pathlib import Path

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ============================================================
# market_master.csv 欄位定義
# ============================================================

MASTER_FIELDS = [
    # --- 來自 03_market_catalog ---
    "market_id",
    "market_slug",
    "question",           # 保留供審計
    "condition_id",
    "yes_token_id",
    "no_token_id",
    "city",
    "city_raw",           # 保留供審計
    "market_date_local",
    "market_date_raw",    # 保留供審計
    "market_type",
    "threshold",
    "range_low",
    "range_high",
    "temp_unit",
    "precision",
    "metric_type",
    "parse_status",       # 保留供審計
    # --- 來自 seed_cities.json（override > seed）---
    "country",
    "station_id",
    "lat",
    "lon",
    "timezone",
    "settlement_source_type",
    "settlement_url",
    # --- 計算欄位 ---
    "market_enabled",
    "disabled_reason",
]


# ============================================================
# 工具函數
# ============================================================

def _is_empty(val) -> bool:
    """判斷值是否為空（None、空字串、"None" 字串）"""
    return val is None or str(val).strip() == "" or str(val).strip() == "None"


# ============================================================
# 載入
# ============================================================

def load_catalog_csv(path: Path) -> list[dict]:
    """載入 data/03_market_catalog.csv"""
    if not path.exists():
        log.error(f"Catalog CSV not found: {path}")
        return []
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    log.info(f"Loaded catalog: {len(rows)} rows from {path.name}")
    return rows


def load_json_config(path: Path, label: str) -> dict:
    """載入 JSON 設定檔，過濾 _meta key"""
    if not path.exists():
        log.warning(f"{label} not found: {path}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    filtered = {k: v for k, v in data.items() if not k.startswith("_")}
    if filtered:
        log.info(f"Loaded {label}: {len(filtered)} entries from {path.name}")
    return filtered


# ============================================================
# market_enabled 判斷邏輯
# ============================================================

def compute_market_enabled(row: dict, station_data: dict) -> tuple[bool, str]:
    """
    判斷一個市場是否 enabled。

    回傳 (enabled: bool, disabled_reason: str)

    需同時滿足：
      A. metadata 完整性：parse_status=ok、market_date_local 非空、
         station_id/timezone/lat/lon 非空、city_enabled=true
      B. 合約欄位完整性：range 需有 range_low/range_high/temp_unit；
         higher/below/exact 需有 threshold/temp_unit
    """
    # A0. 過期市場（market_date_local < city local date）
    market_date_str = row.get("market_date_local", "")
    if market_date_str:
        try:
            tz_str = station_data.get("timezone", "")
            local_today = _date.today()  # fallback
            if tz_str and _ZoneInfo is not None:
                try:
                    local_today = datetime.now(_ZoneInfo(tz_str)).date()
                except Exception:
                    pass
            if _date.fromisoformat(market_date_str) < local_today:
                return False, f"expired ({market_date_str})"
        except ValueError:
            pass

    # A1. parse_status == "ok"
    if row.get("parse_status") != "ok":
        return False, f"parse_status={row.get('parse_status', 'unknown')}"

    # A2. market_date_local 非空
    if _is_empty(row.get("market_date_local")):
        return False, "market_date_local empty"

    # A3. station_id 非空
    if _is_empty(station_data.get("station_code")):
        return False, "station_id missing in seed"

    # A4. timezone 非空
    if _is_empty(station_data.get("timezone")):
        return False, "timezone missing in seed"

    # A5. lat / lon 非空
    if station_data.get("lat") is None or station_data.get("lon") is None:
        return False, "lat/lon missing in seed"

    # A6. city_enabled == true in seed
    city_enabled = station_data.get("city_enabled")
    if city_enabled is False or str(city_enabled).lower() == "false":
        reason = station_data.get("deferred_reason", "")
        return False, f"city_enabled=false{': ' + reason if reason else ''}"

    # B. 合約欄位完整性
    market_type = row.get("market_type", "")
    temp_unit = row.get("temp_unit", "")

    if market_type == "range":
        if _is_empty(row.get("range_low")) or _is_empty(row.get("range_high")):
            return False, "range market: range_low or range_high missing"
        if _is_empty(temp_unit):
            return False, "range market: temp_unit missing"
    elif market_type in ("higher", "below", "exact"):
        if _is_empty(row.get("threshold")):
            return False, f"{market_type} market: threshold missing"
        if _is_empty(temp_unit):
            return False, f"{market_type} market: temp_unit missing"

    return True, ""


# ============================================================
# 主要合併邏輯
# ============================================================

def build_master_rows(
    catalog_rows: list[dict],
    seed: dict,
    override: dict,
) -> list[dict]:
    """
    把 catalog 每一列補上 station metadata，計算 market_enabled。

    優先序：override > seed
    override 只覆蓋 metadata 欄位（station_code、timezone 等），
    不覆蓋 market 本身的 question/market_type/threshold/range/* /temp_unit。
    """
    master_rows = []
    no_seed_cities: list[str] = []
    enabled_count = 0
    disabled_count = 0

    for row in catalog_rows:
        city = row.get("city", "")
        seed_data = seed.get(city, {})
        over_data = override.get(city, {})

        if not seed_data:
            no_seed_cities.append(city)

        # 合併 station metadata（override > seed）
        def _get(field, default=""):
            val = over_data.get(field)
            if val is None:
                val = seed_data.get(field)
            if val is None:
                return default
            return val

        # 合併後的 station_data（用於 enabled 判斷）
        merged_station: dict = {
            "station_code": _get("station_code"),
            "timezone": _get("timezone"),
            "lat": over_data.get("lat") if over_data.get("lat") is not None else seed_data.get("lat"),
            "lon": over_data.get("lon") if over_data.get("lon") is not None else seed_data.get("lon"),
            "city_enabled": _get("city_enabled", True),
            "deferred_reason": _get("deferred_reason", ""),
        }

        enabled, disabled_reason = compute_market_enabled(row, merged_station)

        master_row = {
            # from catalog
            "market_id": row.get("market_id", ""),
            "market_slug": row.get("market_slug", ""),
            "question": row.get("question", ""),
            "condition_id": row.get("condition_id", ""),
            "yes_token_id": row.get("yes_token_id", ""),
            "no_token_id": row.get("no_token_id", ""),
            "city": city,
            "city_raw": row.get("city_raw", ""),
            "market_date_local": row.get("market_date_local", ""),
            "market_date_raw": row.get("market_date_raw", ""),
            "market_type": row.get("market_type", ""),
            "threshold": row.get("threshold", ""),
            "range_low": row.get("range_low", ""),
            "range_high": row.get("range_high", ""),
            "temp_unit": row.get("temp_unit", ""),
            "precision": row.get("precision", ""),
            "metric_type": row.get("metric_type", "daily_high"),
            "parse_status": row.get("parse_status", ""),
            # from seed/override
            "country": _get("country"),
            "station_id": _get("station_code"),   # seed uses station_code → master uses station_id
            "lat": str(merged_station["lat"]) if merged_station["lat"] is not None else "",
            "lon": str(merged_station["lon"]) if merged_station["lon"] is not None else "",
            "timezone": merged_station["timezone"],
            "settlement_source_type": _get("settlement_source_type"),
            "settlement_url": _get("settlement_url"),
            # computed
            "market_enabled": "true" if enabled else "false",
            "disabled_reason": disabled_reason,
        }

        master_rows.append(master_row)

        if enabled:
            enabled_count += 1
        else:
            disabled_count += 1

    log.info(f"Total master rows: {len(master_rows)} (enabled={enabled_count}, disabled={disabled_count})")

    if no_seed_cities:
        unique_missing = sorted(set(no_seed_cities))
        log.warning(f"Cities not in seed_cities.json ({len(unique_missing)}): {', '.join(unique_missing)}")

    return master_rows


# ============================================================
# 寫檔
# ============================================================

def write_master_csv(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MASTER_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    log.info(f"Written: {output_path} ({len(rows)} rows)")


def write_report(rows: list[dict], report_path: Path) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        f"04_market_master report @ {now_str}",
        f"Total market rows: {len(rows)}",
        f"Enabled: {sum(1 for r in rows if r['market_enabled'] == 'true')}",
        f"Disabled: {sum(1 for r in rows if r['market_enabled'] != 'true')}",
        "",
        "Market details:",
    ]
    for row in rows:
        status = "ON " if row["market_enabled"] == "true" else "OFF"
        reason = f"  ({row['disabled_reason']})" if row["disabled_reason"] else ""
        lines.append(
            f"  [{status}] {row['city']:<15} {row['market_date_local']:<12} "
            f"{row['market_type']:<7} {row['station_id']:<6}{reason}"
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

    catalog_path = PROJ_DIR / "data" / "03_market_catalog.csv"
    seed_path = PROJ_DIR / "config" / "seed_cities.json"
    override_path = PROJ_DIR / "config" / "city_override.json"
    master_path = PROJ_DIR / "data" / "market_master.csv"
    report_path = PROJ_DIR / "logs" / "04_market_master" / "master_report.log"

    log.info("=" * 50)
    log.info("04_market_master: Market master assembly")
    log.info("=" * 50)

    # Step 1: 載入
    log.info("Step 1: Load sources...")
    catalog_rows = load_catalog_csv(catalog_path)
    seed = load_json_config(seed_path, "seed_cities")
    override = load_json_config(override_path, "city_override")

    if not catalog_rows:
        log.error("Catalog CSV is empty or missing. Cannot produce market_master.csv.")
        return []

    # Step 2: 組裝 master
    log.info("Step 2: Build market master...")
    master_rows = build_master_rows(catalog_rows, seed, override)

    # Step 3: 寫檔
    if dry_run:
        log.info("[DRY RUN] Skip write.")
        for row in master_rows[:10]:
            status = "ON " if row["market_enabled"] == "true" else "OFF"
            reason = f"  ({row['disabled_reason']})" if row["disabled_reason"] else ""
            log.info(
                f"  [{status}] {row['city']:<15} {row['market_date_local']:<12} "
                f"{row['market_type']:<7} {row['station_id']:<6}{reason}"
            )
    else:
        write_master_csv(master_rows, master_path)
        write_report(master_rows, report_path)

    log.info("04_market_master done.")
    return master_rows


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Market master assembly → data/market_master.csv",
    )
    p.add_argument("--dry-run", action="store_true", help="Only print, no write")
    p.add_argument("--verbose", action="store_true", help="Verbose log")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    run(dry_run=args.dry_run, verbose=args.verbose)
