"""
08b_price_stream.py — Polymarket WebSocket Price Stream

訂閱 Polymarket WS Market 頻道，維護 in-memory orderbook（透過 08c_book_state），
定期 flush 到 book_state JSON（與 08 REST 版輸出格式完全相容）。

流程：
  1. REST bootstrap（用 08 的 fetch_book_with_retry）→ 建立所有 market 的初始 snapshot
  2. WS connect + subscribe（asset_id = token_id）
  3. 並行：listen（book/price_change/last_trade_price）+ flush_loop（每 5 秒）
  4. 定期一致性抽查（每 5 分鐘抽 3 個 market 比對 WS vs REST）
  5. 斷線 → 指數退避重連（5s, 10s, 20s, ... max 120s），重連後 REST bootstrap 重建 state
  6. Ctrl+C → 優雅退出（最後 flush 一次）

輸出：
  data/raw/prices/book_state/{market_id}.json  （與 08 REST 版相容）

CLI：
  python 08b_price_stream.py --cities "London,Paris"   # 常駐
  python 08b_price_stream.py --cities "London" --once  # REST bootstrap 快照一次後退出
  python 08b_price_stream.py --verbose                 # 詳細 log

依賴：
  pip install websockets
"""

import argparse
import asyncio
import csv
import importlib.util
import json
import logging
import random
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

# ── websockets 可選 ───────────────────────────────────────────
try:
    import websockets
    import websockets.exceptions
    _HAS_WS = True
except ImportError:
    _HAS_WS = False
    log.error("websockets not installed. Run: pip install websockets")

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
FLUSH_INTERVAL = 5.0            # 保底 flush 秒數
CONSISTENCY_INTERVAL = 300.0    # 一致性抽查間隔（秒）
CONSISTENCY_SAMPLE = 3          # 每次抽查 market 數量
MAX_RECONNECT_ATTEMPTS = 10

# price_change.side 映射：WS 推的是 BUY/SELL（order side），不是 bid/ask（book side）
# BUY order = 有人想買 = 掛在 bid side
# SELL order = 有人想賣 = 掛在 ask side
SIDE_MAP = {
    "BUY": "bids",
    "SELL": "asks",
}


def _now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ============================================================
# 模組載入（importlib，和 signal_main 同一模式）
# ============================================================

def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_bsm_mod = None    # 08c_book_state
_rest_mod = None   # 08_market_price_fetch（REST bootstrap 用）


def _get_bsm_mod():
    global _bsm_mod
    if _bsm_mod is None:
        _bsm_mod = _load_module("book_state_08c", PROJ_DIR / "08c_book_state.py")
    return _bsm_mod


def _get_rest_mod():
    global _rest_mod
    if _rest_mod is None:
        _rest_mod = _load_module("market_price_fetch_08", PROJ_DIR / "08_market_price_fetch.py")
    return _rest_mod


# ============================================================
# 市場資料載入
# ============================================================

def load_markets(cities_filter: set) -> list[dict]:
    """從 market_master.csv 載入 enabled 市場（含 token_id）。"""
    path = PROJ_DIR / "data" / "market_master.csv"
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
            yes_token = row.get("yes_token_id", "").strip()
            no_token = row.get("no_token_id", "").strip()
            if not yes_token or not no_token:
                log.warning(f"Skipping {row.get('market_id')} — missing token_id")
                continue
            rows.append(dict(row))
    log.info(f"Loaded {len(rows)} enabled markets from market_master.csv")
    return rows


def build_metadata_lookup(markets: list[dict]):
    """回傳 market_id → metadata dict 的查找函式（用於 08c.to_book_state_dict）。"""
    rest_mod = _get_rest_mod()
    index: dict[str, dict] = {}
    for m in markets:
        mid = m.get("market_id", "")
        index[mid] = {
            "market_slug": m.get("market_slug", ""),
            "city": m.get("city", ""),
            "market_date_local": m.get("market_date_local", ""),
            "contract_label": rest_mod.build_contract_label(m),
            "market_type": m.get("market_type", ""),
            "threshold": m.get("threshold", ""),
            "metric_type": m.get("metric_type", "daily_high"),
            "yes_token_id": m.get("yes_token_id", "").strip(),
            "no_token_id": m.get("no_token_id", "").strip(),
        }

    def lookup(market_id: str) -> dict:
        return index.get(market_id, {"market_id": market_id})

    return lookup


# ============================================================
# PriceStreamListener
# ============================================================

class PriceStreamListener:
    """
    WS 訂閱 + book state 維護。
    全部 I/O 在 asyncio event loop 中執行。
    """

    def __init__(self, bsm, markets: list[dict], metadata_lookup):
        """
        bsm: BookStateManager（來自 08c_book_state）
        markets: market dicts（含 yes_token_id / no_token_id）
        metadata_lookup: market_id → dict
        """
        self.bsm = bsm
        self.markets = markets
        self.metadata_lookup = metadata_lookup

        # token_id → (market_id, "yes"/"no")
        self.token_to_market: dict[str, tuple[str, str]] = {}
        for m in markets:
            mid = m.get("market_id", "")
            yes_token = m.get("yes_token_id", "").strip()
            no_token = m.get("no_token_id", "").strip()
            if yes_token:
                self.token_to_market[yes_token] = (mid, "yes")
            if no_token:
                self.token_to_market[no_token] = (mid, "no")

        self.ws = None
        self.reconnect_attempts: int = 0
        self._running: bool = True
        self._last_consistency_check: float = 0.0
        self._bootstrap_done: asyncio.Event = asyncio.Event()  # signal_main 用來等 bootstrap

    # ── WS 連線 ──────────────────────────────────────────────

    async def connect(self) -> None:
        self.ws = await websockets.connect(
            WS_URL,
            ping_interval=30,
            ping_timeout=10,
            open_timeout=20,
        )
        self.reconnect_attempts = 0
        log.info("WS connected")

    async def subscribe(self) -> None:
        asset_ids = list(self.token_to_market.keys())
        msg = {
            "assets_ids": asset_ids,
            "type": "subscribe",
        }
        await self.ws.send(json.dumps(msg))
        log.info(f"Subscribed to {len(asset_ids)} assets ({len(self.markets)} markets)")

    # ── REST bootstrap ────────────────────────────────────────

    async def rest_bootstrap(self) -> None:
        """用 REST /book 載入所有 market 的初始 snapshot。非同步包同步呼叫。"""
        rest_mod = _get_rest_mod()
        ok_count = 0
        fail_count = 0

        for m in self.markets:
            mid = m.get("market_id", "")
            yes_token = m.get("yes_token_id", "").strip()
            no_token = m.get("no_token_id", "").strip()
            try:
                # 用 REST 的 fetch（同步），在事件循環線程中直接呼叫
                yes_book = await asyncio.get_event_loop().run_in_executor(
                    None, rest_mod.fetch_book_with_retry, yes_token
                )
                no_book = await asyncio.get_event_loop().run_in_executor(
                    None, rest_mod.fetch_book_with_retry, no_token
                )
                yes_bids = yes_book.get("bids", []) if yes_book else []
                yes_asks = yes_book.get("asks", []) if yes_book else []
                no_bids = no_book.get("bids", []) if no_book else []
                no_asks = no_book.get("asks", []) if no_book else []

                book = self.bsm.get_or_create(mid)
                book.apply_snapshot(yes_bids, yes_asks, no_bids, no_asks)
                ok_count += 1
            except Exception as e:
                log.warning(f"REST bootstrap failed for {mid}: {e}")
                fail_count += 1

        log.info(f"REST bootstrap done: {ok_count} ok, {fail_count} failed")

    # ── 消息處理 ─────────────────────────────────────────────

    def on_message_sync(self, raw: str) -> None:
        """
        解析 WS event → 更新 book state。
        支援單個 dict 和 list of events（初始推送有時是列表）。
        """
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as e:
            log.warning(f"JSON decode error: {e} — raw: {raw[:200]}")
            return

        # 有時 WS 會推 list of events
        if isinstance(payload, list):
            for item in payload:
                self._dispatch(item)
        elif isinstance(payload, dict):
            self._dispatch(payload)

    def _dispatch(self, msg: dict) -> None:
        event_type = msg.get("event_type") or msg.get("type", "")
        if event_type == "book":
            self._handle_book(msg)
        elif event_type == "price_change":
            self._handle_price_change(msg)
        elif event_type == "last_trade_price":
            self._handle_last_trade_price(msg)
        elif event_type == "best_bid_ask":
            # 忽略：我們從 bids/asks dict 自己算 best，不依賴此輔助事件
            # 需要 custom_feature_enabled: true 才會收到，未來可做交叉驗證
            log.debug(f"best_bid_ask event received (ignored): {msg}")
        # 其他 event type（如 "tick_size_change"）靜默忽略

    def _handle_book(self, msg: dict) -> None:
        """WS book event（單 token 完整 snapshot）→ apply_side_snapshot。"""
        asset_id = msg.get("asset_id", "")
        if asset_id not in self.token_to_market:
            return
        market_id, side = self.token_to_market[asset_id]
        bids = msg.get("bids", [])
        asks = msg.get("asks", [])
        book = self.bsm.get_or_create(market_id)
        book.apply_side_snapshot(side, bids, asks)
        log.debug(f"book: {market_id} {side} bids={len(bids)} asks={len(asks)}")

    def _handle_price_change(self, msg: dict) -> None:
        """WS price_change event（增量）→ apply_price_change。"""
        asset_id = msg.get("asset_id", "")
        if asset_id not in self.token_to_market:
            return
        market_id, side = self.token_to_market[asset_id]
        book = self.bsm.get_or_create(market_id)

        changes = msg.get("changes", [])
        for change in changes:
            price = str(change.get("price", ""))
            size = str(change.get("size", ""))
            ws_side = str(change.get("side", "")).upper()  # "BUY" (bid) or "SELL" (ask)
            mapped = SIDE_MAP.get(ws_side)
            if not mapped:
                log.debug(f"Unknown side in price_change: {ws_side!r} for {market_id}")
                continue
            book_side = f"{side}_{mapped}"  # e.g. "yes_bids" / "no_asks"
            book.apply_price_change(book_side, price, size)

        if changes:
            log.debug(f"price_change: {market_id} {side} {len(changes)} changes")

    def _handle_last_trade_price(self, msg: dict) -> None:
        """WS last_trade_price → 更新 last_trade_price（不影響 orderbook）。"""
        asset_id = msg.get("asset_id", "")
        if asset_id not in self.token_to_market:
            return
        market_id, _ = self.token_to_market[asset_id]
        book = self.bsm.get_or_create(market_id)
        book.last_trade_price = str(msg.get("price", ""))

    # ── 一致性驗證 ───────────────────────────────────────────

    async def verify_consistency(self, output_dir: Path) -> int:
        """
        抽樣 CONSISTENCY_SAMPLE 個 market，比對 WS state 的 yes_best_ask 與新 REST snapshot。
        回傳 mismatch 數量。非 blocking：只 log warning。
        """
        rest_mod = _get_rest_mod()
        market_ids = list(self.bsm.books.keys())
        if not market_ids:
            return 0

        sample_ids = random.sample(market_ids, min(CONSISTENCY_SAMPLE, len(market_ids)))
        mismatches = 0
        TOLERANCE = 0.001

        # Build token index for quick lookup
        token_index: dict[str, dict] = {m["market_id"]: m for m in self.markets}

        for market_id in sample_ids:
            m = token_index.get(market_id)
            if not m:
                continue
            yes_token = m.get("yes_token_id", "").strip()
            if not yes_token:
                continue
            try:
                yes_book = await asyncio.get_event_loop().run_in_executor(
                    None, rest_mod.fetch_book_with_retry, yes_token
                )
                if not yes_book:
                    continue
                _, rest_yes_ask_str = rest_mod.extract_best_bid_ask(yes_book)
                rest_yes_ask = float(rest_yes_ask_str) if rest_yes_ask_str else None

                ws_book = self.bsm.books.get(market_id)
                ws_yes_ask = ws_book.yes_best_ask if ws_book else None

                if rest_yes_ask is not None and ws_yes_ask is not None:
                    diff = abs(rest_yes_ask - ws_yes_ask)
                    if diff > TOLERANCE:
                        log.warning(
                            f"Consistency mismatch: {market_id} "
                            f"WS={ws_yes_ask:.4f} REST={rest_yes_ask:.4f} diff={diff:.4f}"
                        )
                        mismatches += 1
                    else:
                        log.debug(f"Consistency OK: {market_id} diff={diff:.6f}")
            except Exception as e:
                log.warning(f"Consistency check error for {market_id}: {e}")

        log.info(f"Consistency check: {mismatches}/{len(sample_ids)} mismatch(es)")
        return mismatches

    # ── flush loop（保底）────────────────────────────────────

    async def flush_loop(self, output_dir: Path) -> None:
        """每 FLUSH_INTERVAL 秒 flush dirty books。定期觸發一致性抽查。"""
        while self._running:
            await asyncio.sleep(FLUSH_INTERVAL)
            try:
                n = self.bsm.flush_dirty(output_dir, self.metadata_lookup)
                if n > 0:
                    log.debug(f"flush_loop: flushed {n} book(s)")
            except Exception as e:
                log.warning(f"flush_loop error: {e}")

            # 定期一致性抽查
            now = time.monotonic()
            if now - self._last_consistency_check >= CONSISTENCY_INTERVAL:
                self._last_consistency_check = now
                try:
                    await self.verify_consistency(output_dir)
                except Exception as e:
                    log.warning(f"Periodic consistency check error: {e}")

    # ── 重連 ─────────────────────────────────────────────────

    async def reconnect(self, output_dir: Path) -> bool:
        """指數退避重連。成功回傳 True。重連後 REST bootstrap 重建 state。"""
        self.reconnect_attempts = 0
        while self.reconnect_attempts < MAX_RECONNECT_ATTEMPTS and self._running:
            wait = min(5 * (2 ** self.reconnect_attempts), 120)  # 5→10→20→...→120s
            log.warning(
                f"WS reconnecting in {wait}s "
                f"(attempt {self.reconnect_attempts + 1}/{MAX_RECONNECT_ATTEMPTS})"
            )
            await asyncio.sleep(wait)
            try:
                await self.connect()
                await self.subscribe()
                await self.rest_bootstrap()         # 重連後用 REST 重建 state（保險）
                n = self.bsm.flush_all(output_dir, self.metadata_lookup)
                log.info(f"WS reconnected — REST bootstrap done, flushed {n} books")
                return True
            except Exception as e:
                self.reconnect_attempts += 1
                log.error(f"Reconnect attempt {self.reconnect_attempts} failed: {e}")

        log.error("Max reconnect attempts reached — WS permanently offline")
        return False

    # ── 主入口 ───────────────────────────────────────────────

    async def run(self, output_dir: Path, once: bool = False) -> None:
        """
        主流程：
          1. REST bootstrap（所有 market 初始 snapshot）
          2. [once 模式] flush 後退出
          3. WS connect + subscribe
          4. 啟動時一致性比對（等 2 秒讓 WS book events 進來）
          5. 並行：listen + flush_loop
          6. 斷線 → 重連；永久離線 → flush stale state 退出
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        # 1. REST bootstrap
        log.info("=" * 55)
        log.info("08b REST bootstrap starting...")
        await self.rest_bootstrap()
        n = self.bsm.flush_all(output_dir, self.metadata_lookup)
        log.info(f"REST bootstrap complete — flushed {n} books to {output_dir}")
        self._bootstrap_done.set()  # 通知 signal_main WS 模式可以開始讀 in-memory book

        if once:
            log.info("--once mode: REST bootstrap done, exiting.")
            return

        # 2. WS connect + subscribe
        await self.connect()
        await self.subscribe()

        # 3. 啟動時一致性比對（等 WS book events 先進來）
        log.info("Waiting 2s for initial WS book events...")
        await asyncio.sleep(2.0)
        self._last_consistency_check = time.monotonic()
        await self.verify_consistency(output_dir)

        # 4. 並行 listen + flush
        flush_task = asyncio.create_task(self.flush_loop(output_dir))

        try:
            while self._running:
                try:
                    async for raw in self.ws:
                        if not self._running:
                            break
                        try:
                            self.on_message_sync(raw)
                        except Exception as e:
                            log.error(f"Message handling error: {e}")

                except websockets.exceptions.ConnectionClosed as e:
                    log.warning(f"WS connection closed: {e}")
                    if not self._running:
                        break
                    # flush 當前狀態再重連
                    self.bsm.flush_all(output_dir, self.metadata_lookup)
                    success = await self.reconnect(output_dir)
                    if not success:
                        self.bsm.mark_all_stale()
                        self.bsm.flush_all(output_dir, self.metadata_lookup)
                        break

                except Exception as e:
                    log.error(f"WS receive error: {e}")
                    if not self._running:
                        break
                    self.bsm.flush_all(output_dir, self.metadata_lookup)
                    success = await self.reconnect(output_dir)
                    if not success:
                        self.bsm.mark_all_stale()
                        self.bsm.flush_all(output_dir, self.metadata_lookup)
                        break

        finally:
            flush_task.cancel()
            try:
                await flush_task
            except asyncio.CancelledError:
                pass
            # 退出前最後 flush
            n = self.bsm.flush_all(output_dir, self.metadata_lookup)
            log.info(f"WS listener stopped. Final flush: {n} books.")

    def stop(self) -> None:
        """優雅停止（Ctrl+C 呼叫）。"""
        self._running = False


# ============================================================
# CLI 入口
# ============================================================

async def async_main(args) -> None:
    if not _HAS_WS:
        log.error("websockets not installed. Run: pip install websockets")
        sys.exit(1)

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    cities_filter = (
        {c.strip() for c in args.cities.split(",") if c.strip()}
        if args.cities
        else set()
    )

    markets = load_markets(cities_filter)
    if not markets:
        log.error("No enabled markets found. Check market_master.csv and --cities filter.")
        sys.exit(1)

    bsm_mod = _get_bsm_mod()
    bsm = bsm_mod.BookStateManager()
    metadata_lookup = build_metadata_lookup(markets)

    listener = PriceStreamListener(bsm, markets, metadata_lookup)

    output_dir = PROJ_DIR / "data" / "raw" / "prices" / "book_state"

    log.info("=" * 55)
    log.info("08b_price_stream: Polymarket WS ingestion")
    log.info(f"Markets  : {len(markets)}")
    log.info(f"Assets   : {len(listener.token_to_market)}")
    log.info(f"Mode     : {'once (REST bootstrap only)' if args.once else 'daemon'}")
    log.info(f"Output   : {output_dir}")
    log.info("=" * 55)

    await listener.run(output_dir, once=args.once)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Polymarket WS price stream → book_state JSON（與 08 REST 版相容）"
    )
    p.add_argument(
        "--cities", type=str, default="",
        help="城市 filter（逗號分隔），空白 = 所有 enabled 城市",
    )
    p.add_argument("--once", action="store_true", help="REST bootstrap 快照一次後退出")
    p.add_argument("--verbose", action="store_true", help="詳細 log（DEBUG level）")
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        log.info("Interrupted by user — exiting.")
