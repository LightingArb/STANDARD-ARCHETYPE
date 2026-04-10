"""
08c_book_state.py — In-memory OrderBook State Manager

記憶體 orderbook 管理。只提供 data structure，不包含 WS 或 REST 邏輯。
供 08b_price_stream.py 使用。

設計原則：
  - 內部用 dict（price→size）做 O(1) 查找更新
  - flush 時才轉 sorted list
  - dirty flag 避免每個 event 都寫磁碟
  - apply_snapshot（REST 全量）vs apply_side_snapshot（WS 單 token）vs apply_price_change（增量）
  - is_stale() per-market，不是全局
  - to_book_state_dict() 輸出與 08_market_price_fetch.build_book_state() 完全相容
"""

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


# ============================================================
# 工具函數
# ============================================================

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_utc_str() -> str:
    return _now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")


def _atomic_write_json(path: Path, data: dict) -> None:
    """原子寫入 JSON：先寫 .tmp → os.replace() 覆蓋。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", dir=str(path.parent), suffix=".tmp", delete=False, encoding="utf-8"
    ) as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        tmp = f.name
    os.replace(tmp, str(path))


# ============================================================
# OrderBook
# ============================================================

class OrderBook:
    """單一 market 的 orderbook（記憶體版）。"""

    STALE_THRESHOLD_SECONDS = 300  # 5 分鐘無更新 → stale

    def __init__(self, market_id: str):
        self.market_id = market_id
        # 內部用 dict，key=price_str，value=size_str（快速查找）
        self.yes_bids_by_price: dict[str, str] = {}
        self.yes_asks_by_price: dict[str, str] = {}
        self.no_bids_by_price: dict[str, str] = {}
        self.no_asks_by_price: dict[str, str] = {}

        self.last_trade_price: Optional[str] = None
        self.last_event_utc: Optional[datetime] = None
        self.last_snapshot_utc: Optional[str] = None
        self.event_count_since_snapshot: int = 0
        self.dirty: bool = False
        self._last_flush_time: Optional[datetime] = None

    # ── 私有工具 ──────────────────────────────────────────────

    def _parse_book_side(self, entries: list) -> dict:
        """Convert [{price, size}, ...] → {price_str: size_str}。"""
        result: dict[str, str] = {}
        for e in entries:
            p = str(e.get("price", ""))
            s = str(e.get("size", ""))
            if p and s:
                result[p] = s
        return result

    def _best_price(self, book: dict, highest: bool) -> Optional[float]:
        """best bid = highest price; best ask = lowest price。"""
        if not book:
            return None
        try:
            prices = [float(p) for p in book.keys()]
            return max(prices) if highest else min(prices)
        except (ValueError, TypeError):
            return None

    def _sorted_side(self, book: dict, descending: bool) -> list:
        """dict → sorted [{price, size}, ...] list。"""
        try:
            return sorted(
                [{"price": p, "size": s} for p, s in book.items()],
                key=lambda x: float(x["price"]),
                reverse=descending,
            )
        except (ValueError, TypeError):
            return [{"price": p, "size": s} for p, s in book.items()]

    # ── 計算屬性（供外部讀取）─────────────────────────────────

    @property
    def yes_best_bid(self) -> Optional[float]:
        return self._best_price(self.yes_bids_by_price, highest=True)

    @property
    def yes_best_ask(self) -> Optional[float]:
        return self._best_price(self.yes_asks_by_price, highest=False)

    @property
    def no_best_bid(self) -> Optional[float]:
        return self._best_price(self.no_bids_by_price, highest=True)

    @property
    def no_best_ask(self) -> Optional[float]:
        return self._best_price(self.no_asks_by_price, highest=False)

    # ── 更新方法 ──────────────────────────────────────────────

    def apply_snapshot(
        self,
        yes_bids: list,
        yes_asks: list,
        no_bids: list,
        no_asks: list,
    ) -> None:
        """載入完整 book（REST 全量 bootstrap）。四個 side 一起替換。"""
        self.yes_bids_by_price = self._parse_book_side(yes_bids)
        self.yes_asks_by_price = self._parse_book_side(yes_asks)
        self.no_bids_by_price = self._parse_book_side(no_bids)
        self.no_asks_by_price = self._parse_book_side(no_asks)
        self.last_snapshot_utc = _now_utc_str()
        self.event_count_since_snapshot = 0
        self.last_event_utc = _now_utc()
        self.dirty = True

    def apply_side_snapshot(self, side: str, bids: list, asks: list) -> None:
        """
        WS book event：更新單個 token 的 side，不影響另一 side。
        side: "yes" 或 "no"
        """
        bids_dict = self._parse_book_side(bids)
        asks_dict = self._parse_book_side(asks)
        if side == "yes":
            self.yes_bids_by_price = bids_dict
            self.yes_asks_by_price = asks_dict
        elif side == "no":
            self.no_bids_by_price = bids_dict
            self.no_asks_by_price = asks_dict
        else:
            log.warning(f"apply_side_snapshot: unknown side={side!r} for {self.market_id}")
            return
        self.last_event_utc = _now_utc()
        self.event_count_since_snapshot = 0
        self.last_snapshot_utc = _now_utc_str()
        self.dirty = True

    def apply_price_change(self, side: str, price: str, size: str) -> None:
        """
        套用 WS price_change 增量更新。
        side: "yes_bids" / "yes_asks" / "no_bids" / "no_asks"
        size == "0" → 移除該 price level；size > "0" → 新增或更新。
        """
        book = getattr(self, f"{side}_by_price", None)
        if book is None:
            log.warning(f"apply_price_change: unknown side={side!r} for {self.market_id}")
            return
        price_str = str(price)
        size_str = str(size)
        if size_str == "0":
            book.pop(price_str, None)
        else:
            book[price_str] = size_str
        self.last_event_utc = _now_utc()
        self.event_count_since_snapshot += 1
        self.dirty = True

    # ── 狀態查詢 ──────────────────────────────────────────────

    def is_stale(self) -> bool:
        """上次事件超過 STALE_THRESHOLD_SECONDS 秒 → stale。"""
        if not self.last_event_utc:
            return True
        elapsed = (_now_utc() - self.last_event_utc).total_seconds()
        return elapsed > self.STALE_THRESHOLD_SECONDS

    def to_book_state_dict(self, metadata: dict) -> dict:
        """
        輸出成與 08_market_price_fetch.build_book_state() 相容的 book_state JSON schema。
        新增欄位：last_event_utc, event_count_since_snapshot。
        """
        yes_bids = self._sorted_side(self.yes_bids_by_price, descending=True)
        yes_asks = self._sorted_side(self.yes_asks_by_price, descending=False)
        no_bids = self._sorted_side(self.no_bids_by_price, descending=True)
        no_asks = self._sorted_side(self.no_asks_by_price, descending=False)

        ybb = self.yes_best_bid
        yba = self.yes_best_ask
        nbb = self.no_best_bid
        nba = self.no_best_ask

        def _mid(bid, ask):
            if bid is not None and ask is not None:
                return round((bid + ask) / 2, 6)
            return None

        def _spread(bid, ask):
            if bid is not None and ask is not None:
                return round(ask - bid, 6)
            return None

        book_complete = all([yes_bids, yes_asks, no_bids, no_asks])

        if not yes_bids and not yes_asks and not no_bids and not no_asks:
            fetch_status = "empty_book"
        else:
            fetch_status = "ok"

        last_event_str = (
            self.last_event_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            if self.last_event_utc
            else None
        )
        last_event_str = last_event_str or ""

        return {
            "market_id": self.market_id,
            "market_slug": metadata.get("market_slug", ""),
            "city": metadata.get("city", ""),
            "market_date_local": metadata.get("market_date_local", ""),
            "contract_label": metadata.get("contract_label", ""),
            "market_type": metadata.get("market_type", ""),
            "threshold": metadata.get("threshold", ""),
            "metric_type": metadata.get("metric_type", "daily_high"),
            "yes_token_id": metadata.get("yes_token_id", ""),
            "no_token_id": metadata.get("no_token_id", ""),
            "yes_bids": yes_bids,
            "yes_asks": yes_asks,
            "no_bids": no_bids,
            "no_asks": no_asks,
            "yes_best_bid": ybb,
            "yes_best_ask": yba,
            "no_best_bid": nbb,
            "no_best_ask": nba,
            "yes_mid_price": _mid(ybb, yba),
            "yes_spread": _spread(ybb, yba),
            "no_mid_price": _mid(nbb, nba),
            "no_spread": _spread(nbb, nba),
            "yes_depth_levels": len(yes_asks),
            "no_depth_levels": len(no_asks),
            "book_complete": book_complete,
            "fetch_duration_ms": 0,           # WS 不適用，填 0
            "snapshot_fetch_time_utc": self.last_snapshot_utc,
            "source": "ws_stream",
            "is_stale": self.is_stale(),
            "fetch_status": fetch_status,
            "last_event_utc": last_event_str,
            "event_count_since_snapshot": self.event_count_since_snapshot,
        }


# ============================================================
# BookStateManager
# ============================================================

class BookStateManager:
    """
    管理所有 market 的 orderbook。

    Flush 規則：
    1. dirty = True：有事件更新但還沒寫磁碟
    2. debounce = 1 秒：距上次 flush < 1 秒不寫（避免高頻打磁碟）
    3. 保底 flush = 5 秒：由 08b flush_loop 驅動，就算沒有新事件也定期 flush
    4. 強制 flush：重連後、shutdown 前（呼叫 flush_all，bypass debounce）

    優先序：dirty + debounce 主導，5 秒全量是保底。
    """

    FLUSH_DEBOUNCE_SECONDS = 1.0  # 同一 book 距上次 flush < 此秒數 → 跳過

    def __init__(self):
        self.books: dict[str, OrderBook] = {}

    def get_or_create(self, market_id: str) -> OrderBook:
        if market_id not in self.books:
            self.books[market_id] = OrderBook(market_id)
        return self.books[market_id]

    def get_book(self, market_id: str) -> Optional[OrderBook]:
        """回傳已存在的 OrderBook；不存在回傳 None（不自動創建）。"""
        return self.books.get(market_id)

    def flush_dirty(self, output_dir: Path, metadata_lookup) -> int:
        """
        只 flush dirty 的 book（含 debounce）。
        metadata_lookup: market_id → dict（city, contract_label 等）。
        回傳 flush 了幾個 book。
        """
        flushed = 0
        now = _now_utc()
        for market_id, book in self.books.items():
            if not book.dirty:
                continue
            # debounce：距上次 flush < 1 秒 → 跳過（避免高頻寫磁碟）
            if (
                book._last_flush_time is not None
                and (now - book._last_flush_time).total_seconds() < self.FLUSH_DEBOUNCE_SECONDS
            ):
                continue
            try:
                metadata = metadata_lookup(market_id)
                state = book.to_book_state_dict(metadata)
                path = output_dir / f"{market_id}.json"
                _atomic_write_json(path, state)
                book.dirty = False
                book._last_flush_time = now
                flushed += 1
            except Exception as e:
                log.warning(f"flush_dirty({market_id}): {e}")
        return flushed

    def flush_all(self, output_dir: Path, metadata_lookup) -> int:
        """強制 flush 所有 book（bypass debounce）。"""
        for book in self.books.values():
            book.dirty = True
            book._last_flush_time = None
        return self.flush_dirty(output_dir, metadata_lookup)

    def mark_all_stale(self) -> None:
        """標記所有 book 為 stale（WS 永久離線時）。"""
        for book in self.books.values():
            book.last_event_utc = None  # forces is_stale() → True
            book.dirty = True
