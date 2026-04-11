"""
12_city_scanner.py — 城市掃描（Polymarket Gamma API）

從 Polymarket Gamma API 掃描所有活躍天氣市場，抽取城市清單，
對照 seed_cities.json，更新 data/city_status.json。

邏輯：
  1. 呼叫 Gamma API（與 03_market_catalog.py 相同 API）
  2. 解析每個市場的城市名（複用相同 regex）
  3. 去重，得到城市清單
  4. 對每個城市：
     a. 有完整 metadata（station_code, timezone 等）→ 城市 seed 已知
     b. 沒有 → no_metadata
  5. 更新規則（不重置既有狀態）：
     - 不存在 + 有 metadata → 新建 discovered
     - 不存在 + 缺 metadata → 新建 no_metadata
     - 已存在（任何狀態）→ 只更新 market_count_active + last_scan_at_utc
     - 例外：no_metadata → discovered（metadata 補齊後升級）

CLI：
  python 12_city_scanner.py
  python 12_city_scanner.py --verbose
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ============================================================
# Gamma API 常數（與 03_market_catalog.py 相同）
# ============================================================

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
TAG_IDS = [84, 103040]
PAGE_SIZE = 100
REQUEST_TIMEOUT = 20
REQUEST_DELAY = 0.1

FILTER_KEYWORD = "highest temperature in"

CITY_ALIASES: dict[str, str] = {
    "NYC": "New York City",
}

_RE_QUESTION = re.compile(
    r"^Will the highest temperature in (.+?) be (.+?) on ([A-Za-z]+ \d{1,2})\?$",
    re.IGNORECASE,
)

_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


# ============================================================
# Gamma API
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
        log.info(f"tag_id={tag_id}: {tag_count} markets fetched")

    keyword = FILTER_KEYWORD.lower()
    filtered = [m for m in id_to_market.values() if keyword in m.get("question", "").lower()]
    log.info(f"Total unique markets after keyword filter: {len(filtered)}")
    return filtered


# ============================================================
# 日期解析（與 03 相同邏輯）
# ============================================================

def _resolve_market_date(market: dict, date_raw: str) -> Optional[str]:
    api_date_fields = [
        "endDateIso", "end_date_iso", "endDate", "end_date",
        "closeTime", "close_time", "gameStartTime", "game_start_time",
    ]
    for field in api_date_fields:
        val = market.get(field)
        if not val:
            continue
        try:
            date_part = str(val)[:10]
            datetime.strptime(date_part, "%Y-%m-%d")
            return date_part
        except (ValueError, TypeError):
            continue
    return None


# ============================================================
# 城市抽取
# ============================================================

def normalize_city(city_raw: str) -> str:
    stripped = city_raw.strip()
    return CITY_ALIASES.get(stripped, stripped.title())


def extract_city_market_info(market: dict) -> Optional[dict]:
    """從單一市場解析城市名、日期、parse_status。None = 無法解析。"""
    question = market.get("question", "").strip()
    m = _RE_QUESTION.match(question)
    if not m:
        return None
    city_raw = m.group(1).strip()
    date_raw = m.group(3).strip()

    city = normalize_city(city_raw)
    date_str = _resolve_market_date(market, date_raw)

    # 判斷市場是否 active（API 回傳的欄位）
    is_active = market.get("active", False) and not market.get("closed", True)

    return {
        "city": city,
        "market_date_local": date_str or "",
        "is_active": is_active,
    }


def build_city_market_counts(markets: list[dict]) -> dict[str, int]:
    """
    city → parse_success 且 is_active 的 daily_high 市場數量（= market_count_active）。
    """
    counts: dict[str, int] = {}
    for m in markets:
        info = extract_city_market_info(m)
        if info and info["city"] and info["is_active"] and info["market_date_local"]:
            city = info["city"]
            counts[city] = counts.get(city, 0) + 1
    return counts


# ============================================================
# seed_cities.json loader
# ============================================================

def load_seed_cities(path: Path) -> dict[str, dict]:
    """Returns {city_name: seed_info_dict}"""
    if not path.exists():
        log.warning(f"seed_cities.json not found: {path}")
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        log.warning(f"Could not load seed_cities.json: {e}")
        return {}
    return {k: v for k, v in raw.items() if not k.startswith("_")}


def has_complete_metadata(seed_info: dict) -> bool:
    """seed entry 必須有 station_code + timezone 才算完整。"""
    return bool(seed_info.get("station_code")) and bool(seed_info.get("timezone"))


# ============================================================
# Main scan logic
# ============================================================

def run(verbose: bool = False) -> bool:
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=" * 55)
    log.info("12_city_scanner: Polymarket 天氣城市掃描")
    log.info("=" * 55)

    # Lazy import CityStatusManager here to avoid circular deps
    import importlib.util
    _spec = importlib.util.spec_from_file_location(
        "city_status_manager", PROJ_DIR / "13_city_status_manager.py"
    )
    _mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    CityStatusManager = _mod.CityStatusManager

    # Load seed metadata
    seed_path = PROJ_DIR / "config" / "seed_cities.json"
    seed_cities = load_seed_cities(seed_path)
    log.info(f"Loaded {len(seed_cities)} cities from seed_cities.json")

    # Fetch Gamma API
    log.info("Fetching Gamma API markets...")
    markets = fetch_all_weather_markets(verbose=verbose)

    # Build city → market_count_active mapping
    city_counts = build_city_market_counts(markets)
    all_scanned_cities = set(city_counts.keys())
    log.info(f"Found {len(all_scanned_cities)} unique cities on Polymarket: {sorted(all_scanned_cities)}")

    # Load status manager
    csm = CityStatusManager()

    # Update stats
    scan_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    new_discovered = []
    new_no_metadata = []
    upgraded = []
    updated_existing = []

    for city in sorted(all_scanned_cities):
        market_count = city_counts.get(city, 0)
        current_status = csm.get_status(city)

        if current_status is not None:
            # City already exists — only update scan metadata
            # Exception: no_metadata → discovered if seed now has it
            if current_status == "no_metadata" and city in seed_cities and has_complete_metadata(seed_cities[city]):
                seed = seed_cities[city]
                # city_enabled 過濾：seed 明確禁用的城市不升級
                city_enabled = seed.get("city_enabled", True)
                if city_enabled is False or str(city_enabled).lower() == "false":
                    log.info(f"  {city}: SKIP no_metadata→discovered — city_enabled=false in seed")
                    csm.update_scan_info(city, market_count, scan_time)
                    updated_existing.append(city)
                else:
                    csm.set_discovered(city, metadata={
                        "timezone": seed.get("timezone", ""),
                        "station_code": seed.get("station_code", ""),
                        "country": seed.get("country", ""),
                        "supported_metrics": ["daily_high"],
                        "market_count_active": market_count,
                    }, force=False)
                    csm.update_scan_info(city, market_count, scan_time)
                    upgraded.append(city)
                    log.info(f"  {city}: no_metadata → discovered (metadata now available)")
            else:
                csm.update_scan_info(city, market_count, scan_time)
                updated_existing.append(city)
                if verbose:
                    log.info(f"  {city}: [{current_status}] updated market_count={market_count}")
        else:
            # New city
            if city in seed_cities and has_complete_metadata(seed_cities[city]):
                seed = seed_cities[city]
                # city_enabled 過濾：seed 明確禁用的城市不 discovered
                city_enabled = seed.get("city_enabled", True)
                if city_enabled is False or str(city_enabled).lower() == "false":
                    log.info(f"  {city}: SKIP NEW city — city_enabled=false in seed")
                    continue
                csm.set_discovered(city, metadata={
                    "timezone": seed.get("timezone", ""),
                    "station_code": seed.get("station_code", ""),
                    "country": seed.get("country", ""),
                    "supported_metrics": ["daily_high"],
                    "market_count_active": market_count,
                })
                new_discovered.append(city)
                log.info(f"  {city}: NEW → discovered (markets={market_count})")
            else:
                csm.set_no_metadata(city)
                new_no_metadata.append(city)
                log.info(f"  {city}: NEW → no_metadata (not in seed or incomplete)")

    log.info("=" * 55)
    log.info("=== 12 City Scanner Summary ===")
    log.info(f"Total cities on Polymarket : {len(all_scanned_cities)}")
    log.info(f"New discovered             : {len(new_discovered)}  {new_discovered}")
    log.info(f"New no_metadata            : {len(new_no_metadata)}  {new_no_metadata}")
    log.info(f"Upgraded no_meta→discovered: {len(upgraded)}  {upgraded}")
    log.info(f"Existing (updated only)    : {len(updated_existing)}")
    log.info(f"Ready cities               : {csm.get_ready_cities()}")
    log.info("=" * 55)

    return True


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Polymarket 天氣城市掃描 → city_status.json"
    )
    p.add_argument("--verbose", action="store_true")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    ok = run(verbose=args.verbose)
    sys.exit(0 if ok else 1)
