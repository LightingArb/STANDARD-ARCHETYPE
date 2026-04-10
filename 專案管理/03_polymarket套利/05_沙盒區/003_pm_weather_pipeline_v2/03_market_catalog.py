"""
03_market_catalog.py — Polymarket 天氣市場解析 → market-level catalog CSV

從 Polymarket Gamma API 掃描所有天氣市場，做語意解析，
產出 market-level catalog CSV（每列 = 一個市場）。

職責：
  1. 呼叫 Polymarket Gamma API
  2. regex 解析市場問題（5 種 body pattern）
  3. 城市正規化
  4. 日期解析（API 欄位優先，heuristic fallback）
  5. 精度推導（從合約數值格式，不從 seed 讀取）
  6. 城市 filter（由 --cities 控制，預設: London,Paris）
  7. 輸出 market-level catalog CSV + detail CSV（含全部市場）

不做：
  - 不查詢 lat/lon/station_code/timezone（那是 04 的事）
  - 不做城市層聚合
  - 不輸出 JSON

輸出：
  - data/03_market_catalog.csv（只含通過 filter 的市場）
  - logs/03_market_catalog/market_parse_detail.csv（全部市場，含 filtered_out_reason）
"""

import argparse
import csv
import logging
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

PROJ_DIR = Path(__file__).resolve().parent

# ============================================================
# 常數
# ============================================================

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
TAG_IDS = [84, 103040]
PAGE_SIZE = 100
REQUEST_TIMEOUT = 20
REQUEST_DELAY = 0.1

FILTER_KEYWORD = "highest temperature in"

# ============================================================
# 城市 Alias
# ============================================================

CITY_ALIASES: dict[str, str] = {
    "NYC": "New York City",
}

# ============================================================
# Regex
# ============================================================

_RE_QUESTION = re.compile(
    r"^Will the highest temperature in (.+?) be (.+?) on ([A-Za-z]+ \d{1,2})\?$",
    re.IGNORECASE,
)

_BODY_PATTERNS = [
    ("range", re.compile(
        r"^(-?\d+(?:\.\d+)?)[-\u2013](-?\d+(?:\.\d+)?)\s*°\s*([CF])$",
        re.IGNORECASE)),
    ("range", re.compile(
        r"^between\s+(-?\d+(?:\.\d+)?)(?:[-\u2013]|\s+and\s+)(-?\d+(?:\.\d+)?)\s*°\s*([CF])$",
        re.IGNORECASE)),
    ("higher", re.compile(
        r"^(-?\d+(?:\.\d+)?)\s*°\s*([CF])\s+or\s+higher$",
        re.IGNORECASE)),
    ("below", re.compile(
        r"^(-?\d+(?:\.\d+)?)\s*°\s*([CF])\s+or\s+(?:below|lower)$",
        re.IGNORECASE)),
    ("exact", re.compile(
        r"^(-?\d+(?:\.\d+)?)\s*°\s*([CF])$",
        re.IGNORECASE)),
]

# ============================================================
# 月份對照表
# ============================================================

_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# ============================================================
# CSV 欄位定義
# ============================================================

CATALOG_FIELDS = [
    "market_id", "market_slug", "question", "condition_id",
    "yes_token_id", "no_token_id",
    "city_raw", "city",
    "market_date_raw", "market_date_local",
    "market_type", "threshold", "range_low", "range_high",
    "temp_unit", "precision", "metric_type",
    "parse_status", "parse_note",
]

DETAIL_FIELDS = CATALOG_FIELDS + ["filtered_out_reason"]

# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ============================================================
# Gamma API 抓取
# ============================================================

def fetch_all_weather_markets(verbose: bool = False) -> list[dict]:
    """從 Gamma API 抓所有天氣市場（分頁 + 去重 + 關鍵字過濾）。"""
    import requests

    id_to_market: dict[str, dict] = {}
    id_to_tags: dict[str, set[int]] = {}

    for tag_id in TAG_IDS:
        offset = 0
        tag_count = 0
        while True:
            try:
                resp = requests.get(
                    f"{GAMMA_BASE_URL}/markets",
                    params={
                        "tag_id": tag_id,
                        "active": "true",
                        "closed": "false",
                        "limit": PAGE_SIZE,
                        "offset": offset,
                    },
                    timeout=REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                page = resp.json()

                if not page:
                    break

                for m in page:
                    mid = str(m.get("id", ""))
                    if not mid:
                        continue
                    if mid not in id_to_market:
                        id_to_market[mid] = m
                        id_to_tags[mid] = set()
                    id_to_tags[mid].add(tag_id)

                tag_count += len(page)
                offset += PAGE_SIZE

                if verbose:
                    log.info(f"  tag_id={tag_id} offset={offset} page={len(page)}")

                if len(page) < PAGE_SIZE:
                    break

                time.sleep(REQUEST_DELAY)

            except Exception as e:
                log.warning(f"Gamma API error (tag_id={tag_id}, offset={offset}): {e}")
                break

        log.info(f"tag_id={tag_id}: {tag_count} markets")

    unique = []
    for mid, m in id_to_market.items():
        tags_sorted = sorted(id_to_tags[mid])
        m["_source_tag_ids"] = "|".join(str(t) for t in tags_sorted)
        unique.append(m)

    log.info(f"Deduplicated: {len(unique)}")

    keyword = FILTER_KEYWORD.lower()
    filtered = [m for m in unique if keyword in m.get("question", "").lower()]
    log.info(f"Keyword filtered: {len(filtered)}")

    return filtered


# ============================================================
# 城市正規化
# ============================================================

def normalize_city(city_raw: str) -> str:
    stripped = city_raw.strip()
    return CITY_ALIASES.get(stripped, stripped)


# ============================================================
# Body 解析
# ============================================================

def parse_body(body_raw: str) -> tuple:
    """
    解析 body：回傳 (market_type, threshold, range_low, range_high, temp_unit, error)
    """
    body = body_raw.strip()
    for mtype, pattern in _BODY_PATTERNS:
        m = pattern.match(body)
        if not m:
            continue
        groups = m.groups()
        if mtype == "range":
            return mtype, None, float(groups[0]), float(groups[1]), groups[2].upper(), None
        else:
            return mtype, float(groups[0]), None, None, groups[1].upper(), None

    return "unknown_body", None, None, None, None, f"unmatched body: {body}"


# ============================================================
# 兩層語意解析
# ============================================================

def parse_market(market: dict) -> dict:
    """解析單一市場，回傳解析結果 dict。"""
    question = market.get("question", "").strip()
    market_id = str(market.get("id", ""))
    market_slug = market.get("slug", "")
    condition_id = (
        market.get("conditionId") or market.get("condition_id", "")
    )

    # Extract YES/NO token IDs from clobTokenIds (JSON-encoded string "[yes_id, no_id]")
    yes_token_id = ""
    no_token_id = ""
    clob_raw = market.get("clobTokenIds", "")
    if clob_raw:
        try:
            import json as _json
            token_ids = _json.loads(clob_raw)
            if isinstance(token_ids, list) and len(token_ids) >= 2:
                yes_token_id = str(token_ids[0])
                no_token_id = str(token_ids[1])
        except Exception as e:
            log.warning(f"Failed to parse clobTokenIds for market {market_id}: {e}")

    result = {
        "market_id": market_id,
        "market_slug": market_slug,
        "question": question,
        "condition_id": condition_id or "",
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id,
        "city_raw": "",
        "city": "",
        "market_date_raw": "",
        "market_date_local": "",
        "market_type": "",
        "threshold": "",
        "range_low": "",
        "range_high": "",
        "temp_unit": "",
        "precision": "",
        "metric_type": "daily_high",
        "parse_status": "",
        "parse_note": "",
    }

    qm = _RE_QUESTION.match(question)
    if not qm:
        result["parse_status"] = "fail"
        result["parse_note"] = f"main regex mismatch: {question[:120]}"
        return result

    city_raw = qm.group(1).strip()
    body_raw = qm.group(2).strip()
    date_raw = qm.group(3).strip()

    result["city_raw"] = city_raw
    result["city"] = normalize_city(city_raw)
    result["market_date_raw"] = date_raw

    mtype, threshold, rlow, rhigh, unit, berror = parse_body(body_raw)
    result["market_type"] = mtype
    result["temp_unit"] = unit or ""
    result["threshold"] = threshold if threshold is not None else ""
    result["range_low"] = rlow if rlow is not None else ""
    result["range_high"] = rhigh if rhigh is not None else ""

    if berror:
        result["parse_status"] = "partial"
        result["parse_note"] = berror
    else:
        result["parse_status"] = "ok"

    # 日期解析
    result["market_date_local"] = resolve_market_date(market, date_raw) or ""

    # 精度推導
    result["precision"] = infer_precision(mtype, threshold, rlow, rhigh, unit or "")

    return result


# ============================================================
# 日期解析
# ============================================================

def resolve_market_date(market: dict, date_raw: str) -> Optional[str]:
    """
    把日期文字轉成完整日期 YYYY-MM-DD。

    優先序：
    1. 優先嘗試 Gamma API 回傳的時間欄位（endDateIso / end_date_iso / endDate / end_date /
       closeTime / close_time 等），配合 YYYY-MM-DD 格式直接取日期部分
    2. 若 API 無可用時間欄位，fallback 到 date_raw regex 解析：
       - 用當前年份
       - 如果解析出的日期已過去超過 30 天，加一年
    3. 解析失敗回傳 None
    """
    # Priority 1: API time fields
    api_date_fields = [
        "endDateIso", "end_date_iso",
        "endDate", "end_date",
        "closeTime", "close_time",
        "gameStartTime", "game_start_time",
    ]
    for field in api_date_fields:
        val = market.get(field)
        if not val:
            continue
        try:
            # 取前 10 字元 "YYYY-MM-DD"
            date_part = str(val)[:10]
            datetime.strptime(date_part, "%Y-%m-%d")
            return date_part
        except (ValueError, TypeError):
            continue

    # Priority 2: date_raw heuristic
    if not date_raw:
        return None

    parts = date_raw.strip().split()
    if len(parts) != 2:
        return None

    month_str, day_str = parts
    month_num = _MONTH_MAP.get(month_str.lower())
    if month_num is None:
        return None
    try:
        day_num = int(day_str)
    except ValueError:
        return None

    today = datetime.now(timezone.utc).date()
    year = today.year
    try:
        candidate = date(year, month_num, day_num)
    except ValueError:
        return None

    # 若已過去超過 30 天，加一年
    if (today - candidate).days > 30:
        try:
            candidate = date(year + 1, month_num, day_num)
        except ValueError:
            return None

    return candidate.strftime("%Y-%m-%d")


# ============================================================
# 精度推導（從合約數值格式）
# ============================================================

def infer_precision(
    market_type: str,
    threshold,
    range_low,
    range_high,
    temp_unit: str,
) -> str:
    """
    從合約門檻值格式推導精度。

    規則：
    - 若門檻值有小數（如 21.5）→ 0.1C 或 0.1F
    - 若門檻值為整數 + unit=C → 1C
    - 若門檻值為整數 + unit=F → 1F
    - 無法判斷 → unknown
    """
    if not temp_unit:
        return "unknown"

    values = [v for v in [threshold, range_low, range_high] if v is not None]
    if not values:
        return "unknown"

    try:
        has_decimal = any(float(v) != float(int(float(v))) for v in values)
    except (ValueError, TypeError):
        return "unknown"

    if has_decimal:
        return f"0.1{temp_unit.upper()}"

    unit = temp_unit.upper()
    if unit == "F":
        return "1F"
    if unit == "C":
        return "1C"

    return "unknown"


# ============================================================
# Filter 邏輯
# ============================================================

def compute_filtered_out_reason(row: dict, active_cities: set[str]) -> str:
    """
    回傳 filtered_out_reason 字串。空字串表示通過所有 filter。

    優先序（第一個命中就回傳）：
    1. inactive_city
    2. parse_fail
    3. date_resolve_fail
    4. missing_contract_fields
    """
    # 1. inactive_city
    if row["city"] not in active_cities:
        return "inactive_city"

    # 2. parse_fail
    if row["parse_status"] == "fail":
        return "parse_fail"

    # 3. date_resolve_fail
    if not row["market_date_local"]:
        return "date_resolve_fail"

    # 4. missing_contract_fields
    market_type = row["market_type"]
    temp_unit = row["temp_unit"]

    def _empty(val) -> bool:
        return val is None or str(val).strip() == "" or str(val).strip() == "None"

    if market_type == "range":
        if _empty(row["range_low"]) or _empty(row["range_high"]) or _empty(temp_unit):
            return "missing_contract_fields"
    elif market_type in ("higher", "below", "exact"):
        if _empty(row["threshold"]) or _empty(temp_unit):
            return "missing_contract_fields"

    return ""


# ============================================================
# 寫檔
# ============================================================

def write_catalog_csv(
    rows: list[dict],
    output_path: Path,
    active_cities: set | None = None,
) -> None:
    """
    Merge 模式：保留舊檔裡不在 active_cities 裡的城市行，再合併新行。
    如果 active_cities 為 None，退化成全量覆寫（舊行為）。
    """
    merged_rows: list[dict] = list(rows)
    if active_cities and output_path.exists():
        try:
            with open(output_path, "r", encoding="utf-8", newline="") as f:
                for existing_row in csv.DictReader(f):
                    if existing_row.get("city") not in active_cities:
                        merged_rows.append(existing_row)
        except Exception as e:
            log.warning(f"write_catalog_csv: could not read existing file for merge: {e}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CATALOG_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in merged_rows:
            writer.writerow(row)
    kept = len(merged_rows) - len(rows)
    log.info(
        f"Written catalog: {output_path} "
        f"({len(merged_rows)} rows = {len(rows)} new + {kept} preserved)"
    )


def write_detail_csv(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DETAIL_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    log.info(f"Written detail: {output_path} ({len(rows)} rows)")


# ============================================================
# 主流程
# ============================================================

def run(
    cities: list[str],
    dry_run: bool = False,
    verbose: bool = False,
) -> list[dict]:
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    active_cities = set(cities)
    catalog_path = PROJ_DIR / "data" / "03_market_catalog.csv"
    detail_path = PROJ_DIR / "logs" / "03_market_catalog" / "market_parse_detail.csv"

    log.info("=" * 50)
    log.info("03_market_catalog: Polymarket 天氣市場解析")
    log.info(f"  active cities: {sorted(active_cities)}")
    log.info("=" * 50)

    # Step 1: 抓取市場
    log.info("Step 1: Gamma API market scan...")
    raw_markets = fetch_all_weather_markets(verbose=verbose)
    log.info(f"  {len(raw_markets)} highest_temperature markets fetched")

    # Step 2: 解析每個市場
    log.info("Step 2: Parse markets...")
    parsed = []
    ok_count = fail_count = partial_count = 0
    for market in raw_markets:
        row = parse_market(market)
        parsed.append(row)
        if row["parse_status"] == "ok":
            ok_count += 1
        elif row["parse_status"] == "fail":
            fail_count += 1
        elif row["parse_status"] == "partial":
            partial_count += 1

    log.info(f"  parse: ok={ok_count}  partial={partial_count}  fail={fail_count}")

    # Step 3: 計算 filter reason（detail 用）+ 篩出 catalog
    log.info("Step 3: Filter to active cities and apply quality checks...")
    detail_rows = []
    catalog_rows = []

    inactive_city_count = 0
    parse_fail_count = 0
    date_fail_count = 0
    contract_fail_count = 0

    for row in parsed:
        reason = compute_filtered_out_reason(row, active_cities)
        detail_row = dict(row)
        detail_row["filtered_out_reason"] = reason
        detail_rows.append(detail_row)

        if reason == "":
            catalog_rows.append(row)
        elif reason == "inactive_city":
            inactive_city_count += 1
        elif reason == "parse_fail":
            parse_fail_count += 1
        elif reason == "date_resolve_fail":
            date_fail_count += 1
        elif reason == "missing_contract_fields":
            contract_fail_count += 1

    log.info(f"  catalog rows: {len(catalog_rows)}")
    log.info(f"  filtered: inactive_city={inactive_city_count}  parse_fail={parse_fail_count}  "
             f"date_resolve_fail={date_fail_count}  missing_contract_fields={contract_fail_count}")

    # 顯示 catalog 城市分佈
    city_dist: dict[str, int] = {}
    for row in catalog_rows:
        city_dist[row["city"]] = city_dist.get(row["city"], 0) + 1
    for city, count in sorted(city_dist.items()):
        log.info(f"  {city}: {count} markets in catalog")

    # Step 4: 寫檔
    if dry_run:
        log.info("[DRY RUN] Skip write.")
        for row in catalog_rows[:10]:
            log.info(f"  {row['market_id'][:8]}  {row['city']:<15}  {row['market_date_local']}  "
                     f"{row['market_type']:<7}  {row['parse_status']}")
    else:
        write_catalog_csv(catalog_rows, catalog_path, active_cities=active_cities)
        write_detail_csv(detail_rows, detail_path)

    log.info("03_market_catalog done.")
    return catalog_rows


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Polymarket 天氣市場解析 → data/03_market_catalog.csv",
    )
    p.add_argument(
        "--cities", type=str, default="London,Paris",
        help="只處理指定城市（逗號分隔，預設: London,Paris）",
    )
    p.add_argument("--dry-run", action="store_true", help="只印出結果，不寫檔")
    p.add_argument("--verbose", action="store_true", help="詳細 log")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    cities = [c.strip() for c in args.cities.split(",") if c.strip()]
    run(cities=cities, dry_run=args.dry_run, verbose=args.verbose)
