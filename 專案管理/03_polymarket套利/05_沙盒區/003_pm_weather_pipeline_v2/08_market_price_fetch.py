"""
08_market_price_fetch.py — Polymarket CLOB 即時價格抓取（並行版）

從 Polymarket CLOB API 並行抓取所有 enabled 市場的 YES/NO orderbook，
計算 best_bid/ask，同時輸出 market_prices.csv 和完整 book_state JSON。

讀取：
  data/market_master.csv（market_enabled == true 的市場）

輸出：
  data/raw/prices/market_prices.csv          ← 保留（向後相容）
  data/raw/prices/book_state/{market_id}.json ← 新增（完整 orderbook）

行為：
  - 10 線程並行（MAX_WORKERS=10）
  - 每個線程：YES + NO 之間 sleep 0.1 秒
  - 429 → 單線程 sleep 5 秒後重試一次；仍失敗則記錄，不停其他線程
  - 無需 auth
"""

import argparse
import csv
import json
import logging
import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

PROJ_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

CLOB_BOOK_URL = "https://clob.polymarket.com/book"
MAX_WORKERS = 10
INTRA_MARKET_SLEEP = 0.1   # YES → NO 之間的 sleep（秒），單線程內
RETRY_SLEEP_429 = 5.0       # 429 後重試前的等待時間（秒）

PRICE_FIELDS = [
    "market_id",
    "city",
    "market_date_local",
    "market_type",
    "threshold",
    "range_low",
    "range_high",
    "yes_token_id",
    "no_token_id",
    "yes_best_bid",
    "yes_best_ask",
    "no_best_bid",
    "no_best_ask",
    "yes_mid_price",
    "no_mid_price",
    "spread",
    "fetch_time_utc",
]


# ============================================================
# CLOB API
# ============================================================

def fetch_book_with_retry(token_id: str) -> Optional[dict]:
    """
    GET /book?token_id=<id>，含 429 單次重試。
    回傳 raw JSON dict，或 None（失敗）。
    """
    if not token_id:
        return None
    for attempt in range(2):
        try:
            resp = requests.get(
                CLOB_BOOK_URL,
                params={"token_id": token_id},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=20,
            )
            if resp.status_code == 429:
                if attempt == 0:
                    log.warning(f"  429 token {token_id[:16]}... retry in {RETRY_SLEEP_429}s")
                    time.sleep(RETRY_SLEEP_429)
                    continue
                else:
                    log.warning(f"  429 again token {token_id[:16]}..., giving up")
                    return None
            if resp.status_code != 200:
                log.warning(f"  /book HTTP {resp.status_code} token {token_id[:16]}...")
                return None
            return resp.json()
        except Exception as e:
            log.warning(f"  /book exception token {token_id[:16]}...: {e}")
            return None
    log.error(f"  429 rate limit: gave up after 2 retries for token {token_id[:20]}...")
    return None


def extract_best_bid_ask(book: Optional[dict]) -> tuple[str, str]:
    """
    best_bid = max(bids[*].price)
    best_ask = min(asks[*].price)
    Returns ("", "") if book is None or lists are empty.
    """
    if book is None:
        return "", ""
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    best_bid = ""
    best_ask = ""
    if bids:
        try:
            best_bid = str(max(float(b["price"]) for b in bids))
        except (KeyError, ValueError, TypeError):
            best_bid = ""
    if asks:
        try:
            best_ask = str(min(float(a["price"]) for a in asks))
        except (KeyError, ValueError, TypeError):
            best_ask = ""
    return best_bid, best_ask


def compute_mid(bid: str, ask: str) -> str:
    try:
        return str(round((float(bid) + float(ask)) / 2, 6))
    except (ValueError, TypeError):
        return ""


def compute_spread(yes_bid: str, yes_ask: str) -> str:
    try:
        return str(round(float(yes_ask) - float(yes_bid), 6))
    except (ValueError, TypeError):
        return ""


def safe_float(v) -> Optional[float]:
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# ============================================================
# Book state helpers
# ============================================================

def build_contract_label(mkt: dict) -> str:
    mt = mkt.get("market_type", "")
    th = mkt.get("threshold", "")
    rl = mkt.get("range_low", "")
    rh = mkt.get("range_high", "")
    if mt == "below":
        return f"Below {th}\u00b0C"
    elif mt == "exact":
        return f"Exactly {th}\u00b0C"
    elif mt == "higher":
        return f"Above {th}\u00b0C"
    elif mt == "range":
        return f"{rl}\u2013{rh}\u00b0C"
    return f"{mt} {th}"


def build_book_state(
    mkt: dict,
    yes_book: Optional[dict],
    no_book: Optional[dict],
    fetch_time_utc: str,
    fetch_duration_ms: int = 0,
) -> dict:
    """組裝完整 book_state JSON dict。"""
    yes_bids = yes_book.get("bids", []) if yes_book else []
    yes_asks = yes_book.get("asks", []) if yes_book else []
    no_bids = no_book.get("bids", []) if no_book else []
    no_asks = no_book.get("asks", []) if no_book else []

    yes_bid_str, yes_ask_str = extract_best_bid_ask(yes_book)
    no_bid_str, no_ask_str = extract_best_bid_ask(no_book)

    ybb = safe_float(yes_bid_str)
    yba = safe_float(yes_ask_str)
    nbb = safe_float(no_bid_str)
    nba = safe_float(no_ask_str)

    # book_complete: all four sides must have at least one level
    book_complete = all([
        len(yes_bids) > 0,
        len(yes_asks) > 0,
        len(no_bids) > 0,
        len(no_asks) > 0,
    ])

    # fetch_status
    if yes_book is None and no_book is None:
        fetch_status = "failed"
    elif not yes_bids and not yes_asks and not no_bids and not no_asks:
        fetch_status = "empty_book"
    else:
        fetch_status = "ok"

    return {
        "market_id": mkt.get("market_id", ""),
        "market_slug": mkt.get("market_slug", ""),
        "city": mkt.get("city", ""),
        "market_date_local": mkt.get("market_date_local", ""),
        "contract_label": build_contract_label(mkt),
        "market_type": mkt.get("market_type", ""),
        "threshold": mkt.get("threshold", ""),
        "metric_type": mkt.get("metric_type", "daily_high"),
        "yes_token_id": mkt.get("yes_token_id", "").strip(),
        "no_token_id": mkt.get("no_token_id", "").strip(),
        "yes_bids": yes_bids,
        "yes_asks": yes_asks,
        "no_bids": no_bids,
        "no_asks": no_asks,
        "yes_best_bid": ybb,
        "yes_best_ask": yba,
        "no_best_bid": nbb,
        "no_best_ask": nba,
        "yes_mid_price": round((ybb + yba) / 2, 6) if ybb is not None and yba is not None else None,
        "yes_spread": round(yba - ybb, 6) if ybb is not None and yba is not None else None,
        "no_mid_price": round((nbb + nba) / 2, 6) if nbb is not None and nba is not None else None,
        "no_spread": round(nba - nbb, 6) if nbb is not None and nba is not None else None,
        "yes_depth_levels": len(yes_asks),
        "no_depth_levels": len(no_asks),
        "book_complete": book_complete,
        "fetch_duration_ms": fetch_duration_ms,
        "snapshot_fetch_time_utc": fetch_time_utc,
        "source": "rest_snapshot",
        "is_stale": False,
        "fetch_status": fetch_status,
    }


# ============================================================
# Worker（執行於線程）
# ============================================================

def fetch_single_market(mkt: dict, fetch_time_utc: str) -> dict:
    """
    抓取單個市場的 YES + NO orderbook。
    回傳 {"market_id", "csv_row", "book_state", "fetch_status"}。
    在 ThreadPoolExecutor 中執行，不可使用 global state 寫入。
    """
    market_id = mkt.get("market_id", "")
    yes_token = mkt.get("yes_token_id", "").strip()
    no_token = mkt.get("no_token_id", "").strip()

    if not yes_token or not no_token:
        return {"market_id": market_id, "csv_row": None, "book_state": None, "fetch_status": "no_token"}

    _t0 = time.time()
    yes_book = fetch_book_with_retry(yes_token)
    time.sleep(INTRA_MARKET_SLEEP)
    no_book = fetch_book_with_retry(no_token)
    fetch_duration_ms = round((time.time() - _t0) * 1000)

    book_state = build_book_state(mkt, yes_book, no_book, fetch_time_utc, fetch_duration_ms)
    fetch_status = book_state["fetch_status"]

    yes_bid = str(book_state["yes_best_bid"]) if book_state["yes_best_bid"] is not None else ""
    yes_ask = str(book_state["yes_best_ask"]) if book_state["yes_best_ask"] is not None else ""
    no_bid = str(book_state["no_best_bid"]) if book_state["no_best_bid"] is not None else ""
    no_ask = str(book_state["no_best_ask"]) if book_state["no_best_ask"] is not None else ""

    csv_row = {
        "market_id": market_id,
        "city": mkt.get("city", ""),
        "market_date_local": mkt.get("market_date_local", ""),
        "market_type": mkt.get("market_type", ""),
        "threshold": mkt.get("threshold", ""),
        "range_low": mkt.get("range_low", ""),
        "range_high": mkt.get("range_high", ""),
        "yes_token_id": yes_token,
        "no_token_id": no_token,
        "yes_best_bid": yes_bid,
        "yes_best_ask": yes_ask,
        "no_best_bid": no_bid,
        "no_best_ask": no_ask,
        "yes_mid_price": compute_mid(yes_bid, yes_ask),
        "no_mid_price": compute_mid(no_bid, no_ask),
        "spread": compute_spread(yes_bid, yes_ask),
        "fetch_time_utc": fetch_time_utc,
    }

    log.info(
        f"  [{fetch_status}] {mkt.get('city')} {mkt.get('market_date_local')} "
        f"{build_contract_label(mkt)}"
        f"  YES ask={yes_ask or '—'}  NO ask={no_ask or '—'}"
    )

    return {
        "market_id": market_id,
        "csv_row": csv_row,
        "book_state": book_state,
        "fetch_status": fetch_status,
    }


# ============================================================
# I/O
# ============================================================

def load_market_master(path: Path, cities_filter: set) -> list[dict]:
    if not path.exists():
        log.error(f"market_master.csv not found: {path}")
        return []
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("market_enabled", "").lower() != "true":
                continue
            if cities_filter and row.get("city", "") not in cities_filter:
                continue
            rows.append(dict(row))
    log.info(f"Loaded {len(rows)} enabled markets from market_master.csv")
    return rows


def _atomic_write_json(path: Path, data: dict) -> None:
    """原子寫入 JSON：先寫 .tmp → os.replace() 覆蓋，防止讀到半寫狀態。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", dir=str(path.parent), suffix=".tmp", delete=False, encoding="utf-8"
    ) as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        tmp = f.name
    os.replace(tmp, str(path))


def _atomic_write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    """原子寫入 CSV：先寫 .tmp → os.replace() 覆蓋。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", dir=str(path.parent), suffix=".tmp", delete=False, encoding="utf-8", newline=""
    ) as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        tmp = f.name
    os.replace(tmp, str(path))


def write_prices_csv(rows: list[dict], path: Path) -> None:
    _atomic_write_csv(path, rows, PRICE_FIELDS)
    log.info(f"Written: {path.relative_to(PROJ_DIR)} ({len(rows)} rows)")


def write_book_state(book_state: dict, book_state_dir: Path) -> None:
    market_id = book_state.get("market_id", "unknown")
    json_path = book_state_dir / f"{market_id}.json"
    _atomic_write_json(json_path, book_state)


# ============================================================
# 一致性驗證
# ============================================================

def verify_book_csv_consistency(book_states: list[dict], csv_rows: list[dict]) -> int:
    """
    全量比對 book_state 與 market_prices.csv 中對應欄位。
    不阻塞主流程，僅 log warning。回傳不一致的市場數量。
    """
    csv_index = {r["market_id"]: r for r in csv_rows if r.get("market_id")}
    mismatches = []
    TOLERANCE = 0.001

    for book in book_states:
        mid = book.get("market_id", "")
        csv_row = csv_index.get(mid)
        if not csv_row:
            continue

        checks = [
            ("yes_best_ask", book.get("yes_best_ask"), csv_row.get("yes_best_ask")),
            ("yes_best_bid", book.get("yes_best_bid"), csv_row.get("yes_best_bid")),
            ("no_best_ask", book.get("no_best_ask"), csv_row.get("no_best_ask")),
            ("no_best_bid", book.get("no_best_bid"), csv_row.get("no_best_bid")),
        ]
        for field, book_val, csv_val in checks:
            if book_val is None or csv_val is None or csv_val == "":
                continue
            try:
                if abs(float(book_val) - float(csv_val)) > TOLERANCE:
                    mismatches.append(f"{mid}/{field}: book={book_val} csv={csv_val}")
                    break  # one mismatch per market is enough
            except (ValueError, TypeError):
                pass

    if mismatches:
        log.warning(f"book_state vs CSV mismatch: {len(mismatches)} markets")
        for m in mismatches[:5]:  # log first 5
            log.warning(f"  mismatch: {m}")
    return len(mismatches)


# ============================================================
# Main logic
# ============================================================

def run(cities: str, verbose: bool) -> bool:
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=" * 55)
    log.info("08_market_price_fetch: Polymarket CLOB price fetch (parallel)")
    log.info(f"MAX_WORKERS={MAX_WORKERS}")
    log.info("=" * 55)

    cities_filter = {c.strip() for c in cities.split(",") if c.strip()} if cities else set()

    master_path = PROJ_DIR / "data" / "market_master.csv"
    markets = load_market_master(master_path, cities_filter)

    if not markets:
        log.info("No enabled markets found — writing empty prices CSV")
        write_prices_csv([], PROJ_DIR / "data" / "raw" / "prices" / "market_prices.csv")
        return True

    fetch_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    book_state_dir = PROJ_DIR / "data" / "raw" / "prices" / "book_state"
    book_state_dir.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    results: list[dict] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_single_market, mkt, fetch_time_utc): mkt
            for mkt in markets
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                mkt = futures[future]
                log.warning(f"  Worker exception for {mkt.get('market_id', '?')}: {e}")
                results.append({
                    "market_id": mkt.get("market_id", "?"),
                    "csv_row": None,
                    "book_state": None,
                    "fetch_status": "worker_exception",
                })

    elapsed = time.time() - start_time

    # Collect outputs
    csv_rows = [r["csv_row"] for r in results if r.get("csv_row")]
    book_states = [r["book_state"] for r in results if r.get("book_state")]

    # Write book_state JSONs
    for bs in book_states:
        write_book_state(bs, book_state_dir)

    # Write market_prices.csv（向後相容）
    out_path = PROJ_DIR / "data" / "raw" / "prices" / "market_prices.csv"
    write_prices_csv(csv_rows, out_path)

    # Summary
    ok_count = sum(1 for r in results if r.get("fetch_status") == "ok")
    failed_count = sum(1 for r in results if r.get("fetch_status") == "failed")
    empty_count = sum(1 for r in results if r.get("fetch_status") == "empty_book")
    no_token_count = sum(1 for r in results if r.get("fetch_status") == "no_token")
    exception_count = sum(1 for r in results if r.get("fetch_status") == "worker_exception")

    log.info("=" * 55)
    log.info("=== 08 Price Fetch Summary ===")
    log.info(f"Total markets : {len(markets)}")
    log.info(f"Success (ok)  : {ok_count}")
    log.info(f"Empty book    : {empty_count}")
    log.info(f"Failed        : {failed_count + exception_count}")
    log.info(f"No token      : {no_token_count}")
    log.info(f"Time          : {elapsed:.1f}s")
    log.info(f"Output        : market_prices.csv ({len(csv_rows)} rows) + {len(book_states)} book_state JSONs")
    log.info("=" * 55)

    # Consistency check (non-blocking)
    verify_book_csv_consistency(book_states, csv_rows)

    return True


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Polymarket CLOB price fetch (parallel) → market_prices.csv + book_state/"
    )
    p.add_argument("--cities", type=str, default="", help="城市 filter（逗號分隔）")
    p.add_argument("--verbose", action="store_true")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    ok = run(cities=args.cities, verbose=args.verbose)
    sys.exit(0 if ok else 1)
