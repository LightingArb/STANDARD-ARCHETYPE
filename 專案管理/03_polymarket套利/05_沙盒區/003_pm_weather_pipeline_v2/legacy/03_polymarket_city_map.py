"""
03_polymarket_city_map.py — Polymarket 天氣市場語義分析前置層

從 Polymarket Gamma API 掃描所有天氣市場，做兩層語意解析，
產出城市→市場映射 JSON 與市場解析審計 CSV。

職責：
  1. 呼叫 Polymarket Gamma API
  2. regex 解析市場問題（5 種 body pattern）
  3. 城市正規化與聚合
  4. 判定 country / unit / precision / settlement_source_type

不做：
  - 不查詢 lat/lon/station_code/timezone
  - 不讀任何外部 JSON
  - 不產生 01_city.csv（那是 04_city_data.py 的事）

輸出：
  - data/03_city_market_mapping.json
  - logs/03_city_map/market_parse_detail.csv
"""

import argparse
import csv
import json
import logging
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
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
# 城市 Alias（保守版：只放已知明確的別名）
# ============================================================

CITY_ALIASES: dict[str, str] = {
    "NYC": "New York City",
}

# ============================================================
# 內建城市→國家映射（用於推斷 country）
# ============================================================

CITY_COUNTRY_MAP: dict[str, str] = {
    "Amsterdam": "NL", "Ankara": "TR", "Atlanta": "US", "Austin": "US",
    "Beijing": "CN", "Buenos Aires": "AR", "Busan": "KR",
    "Chengdu": "CN", "Chicago": "US", "Chongqing": "CN",
    "Dallas": "US", "Denver": "US",
    "Helsinki": "FI", "Hong Kong": "HK", "Houston": "US",
    "Istanbul": "TR", "Jakarta": "ID",
    "Kuala Lumpur": "MY", "London": "GB", "Los Angeles": "US",
    "Lucknow": "IN", "Madrid": "ES", "Mexico City": "MX",
    "Miami": "US", "Milan": "IT", "Moscow": "RU", "Munich": "DE",
    "New York City": "US", "Panama City": "PA", "Paris": "FR",
    "San Francisco": "US", "Sao Paulo": "BR", "Seattle": "US",
    "Seoul": "KR", "Shanghai": "CN", "Shenzhen": "CN",
    "Singapore": "SG", "Taipei": "TW", "Tel Aviv": "IL",
    "Tokyo": "JP", "Toronto": "CA", "Warsaw": "PL",
    "Wellington": "NZ", "Wuhan": "CN",
}

# ============================================================
# 內建結算來源類型映射（預設 WU，例外列出）
# ============================================================

CITY_SETTLEMENT_TYPE: dict[str, str] = {
    "Hong Kong": "HKO",
    "Istanbul": "NOAA",
    "Moscow": "NOAA",
    "Taipei": "NOAA",
    "Tel Aviv": "NOAA",
}

# ============================================================
# Regex
# ============================================================

_RE_QUESTION = re.compile(
    r"^Will the highest temperature in (.+?) be (.+?) on ([A-Za-z]+ \d{1,2})\?$",
    re.IGNORECASE,
)

# BODY 5 種 regex（按優先序：range > higher > below > exact）
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

# 市場解析明細 CSV 欄位
DETAIL_FIELDS = [
    "market_id", "question", "slug", "market_family",
    "parse_status", "city_raw", "city_normalized",
    "date_raw", "body_raw", "market_type", "temp_unit",
    "threshold", "range_low", "range_high", "parse_note",
]

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
    question = market.get("question", "").strip()
    slug = market.get("slug", "")
    market_id = str(market.get("id", ""))

    result = {
        "market_id": market_id,
        "question": question,
        "slug": slug,
        "market_family": "",
        "parse_status": "",
        "city_raw": "",
        "city_normalized": "",
        "date_raw": "",
        "body_raw": "",
        "market_type": "",
        "temp_unit": "",
        "threshold": "",
        "range_low": "",
        "range_high": "",
        "parse_note": "",
    }

    qm = _RE_QUESTION.match(question)
    if not qm:
        result["market_family"] = "highest_temperature"
        result["parse_status"] = "fail"
        result["parse_note"] = f"main regex mismatch: {question[:120]}"
        return result

    city_raw = qm.group(1).strip()
    body_raw = qm.group(2).strip()
    date_raw = qm.group(3).strip()

    result["market_family"] = "highest_temperature"
    result["city_raw"] = city_raw
    result["city_normalized"] = normalize_city(city_raw)
    result["body_raw"] = body_raw
    result["date_raw"] = date_raw

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

    return result


# ============================================================
# 日期排序 helper
# ============================================================

_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def parse_date_raw_for_sort(date_raw: str) -> Optional[tuple[int, int]]:
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
    return (month_num, day_num)


# ============================================================
# 精度推斷
# ============================================================

def infer_precision(city: str, unit: str) -> str:
    """根據城市和溫度單位推斷結算精度"""
    if city == "Hong Kong":
        return "0.1C"
    if unit == "F":
        return "1F"
    return "1C"


# ============================================================
# 城市層彙總 → mapping JSON
# ============================================================

def aggregate_to_mapping(parsed_rows: list[dict]) -> dict:
    """
    把逐市場解析結果聚合為城市映射 dict。
    回傳 {city_name: {normalized_city, country, unit, precision, ...}, ...}
    """
    city_data: dict[str, dict] = {}

    for row in parsed_rows:
        city = row["city_normalized"]
        if not city:
            continue

        if city not in city_data:
            city_data[city] = {
                "temp_units": set(),
                "market_types": set(),
                "dates": [],
                "count": 0,
                "ok_count": 0,
                "fail_count": 0,
            }

        cd = city_data[city]
        cd["count"] += 1

        if row["temp_unit"]:
            cd["temp_units"].add(row["temp_unit"])
        if row["market_type"] and row["market_type"] != "unknown_body":
            cd["market_types"].add(row["market_type"])

        if row["date_raw"]:
            sort_key = parse_date_raw_for_sort(row["date_raw"])
            if sort_key is not None:
                cd["dates"].append((sort_key, row["date_raw"]))

        if row["parse_status"] == "ok":
            cd["ok_count"] += 1
        elif row["parse_status"] in ("fail", "partial"):
            cd["fail_count"] += 1

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    mapping: dict[str, dict] = {}

    for city in sorted(city_data.keys()):
        cd = city_data[city]

        # 日期範圍
        dates = cd["dates"]
        if dates:
            dates_sorted = sorted(dates, key=lambda x: x[0])
            earliest = dates_sorted[0][1]
            latest = dates_sorted[-1][1]
            date_range = f"{earliest} ~ {latest}" if earliest != latest else earliest
        else:
            date_range = ""

        # 主要溫度單位（取最多的）
        primary_unit = sorted(cd["temp_units"])[0] if cd["temp_units"] else ""

        mapping[city] = {
            "normalized_city": city,
            "country": CITY_COUNTRY_MAP.get(city, ""),
            "unit": primary_unit,
            "precision": infer_precision(city, primary_unit),
            "settlement_source_type": CITY_SETTLEMENT_TYPE.get(city, "WU"),
            "market_count": cd["count"],
            "date_range": date_range,
            "temp_units_seen": "|".join(sorted(cd["temp_units"])),
            "market_types_seen": "|".join(sorted(cd["market_types"])),
            "last_seen_at": now_str,
        }

    return mapping


# ============================================================
# 寫檔
# ============================================================

def write_mapping_json(mapping: dict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2, ensure_ascii=False)
    log.info(f"Written: {output_path} ({len(mapping)} cities)")


def write_detail_csv(parsed_rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DETAIL_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in parsed_rows:
            writer.writerow(row)
    log.info(f"Written: {output_path} ({len(parsed_rows)} rows)")


# ============================================================
# 主流程
# ============================================================

def run(dry_run: bool = False, verbose: bool = False) -> dict:
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    mapping_path = PROJ_DIR / "data" / "03_city_market_mapping.json"
    detail_path = PROJ_DIR / "logs" / "03_city_map" / "market_parse_detail.csv"

    log.info("=" * 50)
    log.info("03_polymarket_city_map: Polymarket 天氣市場語義分析")
    log.info("=" * 50)

    # Step 1: 抓取市場
    log.info("Step 1: Gamma API market scan...")
    raw_markets = fetch_all_weather_markets(verbose=verbose)
    log.info(f"  {len(raw_markets)} highest_temperature markets")

    # Step 2: 兩層語意解析
    log.info("Step 2: Two-layer semantic parse...")
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

    log.info(f"  ok={ok_count}  partial={partial_count}  fail={fail_count}")

    type_counter = Counter(r["market_type"] for r in parsed if r["market_type"])
    unit_counter = Counter(r["temp_unit"] for r in parsed if r["temp_unit"])
    log.info(f"  market_type: {dict(type_counter.most_common())}")
    log.info(f"  temp_unit: {dict(unit_counter.most_common())}")

    # Step 3: 城市層彙總 → mapping
    log.info("Step 3: City aggregation...")
    mapping = aggregate_to_mapping(parsed)
    log.info(f"  {len(mapping)} unique cities")

    # 列出 country 未知的城市
    unknown_country = [c for c, v in mapping.items() if not v.get("country")]
    if unknown_country:
        log.warning(f"  Unknown country for: {', '.join(unknown_country)}")
        log.warning("  -> Please add to CITY_COUNTRY_MAP in 03_polymarket_city_map.py")

    # Step 4: 寫檔
    if dry_run:
        log.info("[DRY RUN] Skip write.")
        for city, info in mapping.items():
            log.info(f"  {city:<25} unit={info['unit']:<2} count={info['market_count']:<4} "
                     f"country={info['country']:<3} settlement={info['settlement_source_type']}")
    else:
        write_mapping_json(mapping, mapping_path)
        write_detail_csv(parsed, detail_path)

    log.info("03_polymarket_city_map done.")
    return mapping


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Polymarket 天氣市場語義分析 → city_market_mapping.json",
    )
    p.add_argument("--dry-run", action="store_true", help="只印出結果，不寫檔")
    p.add_argument("--verbose", action="store_true", help="詳細 log")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    run(dry_run=args.dry_run, verbose=args.verbose)
