"""
binance_spot.py — Binance BTC/USDT price feed (REST + WebSocket).

Provides:
  - BinanceSpotService  : REST polling for spot price and K-line backtrack
  - BinanceCache        : in-memory last-trade cache (updated by WS consumer)
  - RingBuffer          : time-bounded FIFO of (ts_seconds, price) samples
  - BinanceFeed         : async WS consumer — keeps BinanceCache and RingBuffer live

The REST service (BinanceSpotService) answers "what is BTC right now" and
"Can I backtrack BTC at timestamp X?".  The WS consumer (BinanceFeed) provides
a continuous stream for drift checks and round-baseline computation.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

import httpx
import websockets

from src.config import binance as cfg, baseline_cfg
from src.infra.logging import get_logger
from src.market.models import MarketInfo

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Shared data structures (used by both REST and WS consumers)
# ---------------------------------------------------------------------------

@dataclass
class BinanceTrade:
    """A single Binance trade event."""
    id: int
    price: float
    quantity: float
    ts: int        # Unix ms


@dataclass
class BinanceCache:
    """In-memory price cache updated by the WS consumer."""
    last_trade: BinanceTrade | None = field(default=None)
    last_price: float = 0.0
    ts_ms: int = 0

    def update(self, trade: BinanceTrade) -> None:
        self.last_trade = trade
        self.last_price = trade.price
        self.ts_ms = trade.ts


class RingBuffer:
    """Time-bounded FIFO of (ts_seconds, price) samples."""

    __slots__ = ("retention_s", "_deque")

    def __init__(self, retention_s: int) -> None:
        self.retention_s = retention_s
        self._deque: deque[tuple[float, float]] = deque()

    def append(self, ts_seconds: float, price: float) -> None:
        self._deque.append((ts_seconds, price))
        self._evict()

    def _evict(self) -> None:
        cutoff = time.time() - self.retention_s
        while self._deque and self._deque[0][0] < cutoff:
            self._deque.popleft()

    def samples_in(self, lo: float, hi: float) -> list[float]:
        """All prices where lo <= ts < hi."""
        return [p for _, p in self._deque if lo <= _ < hi]

    def __len__(self) -> int:
        return len(self._deque)


def _median(values: list[float]) -> float:
    if not values:
        raise ValueError("Cannot compute median of empty list")
    s = sorted(values)
    n = len(s)
    if n % 2 == 0:
        return (s[n // 2 - 1] + s[n // 2]) / 2.0
    return s[n // 2]


# ---------------------------------------------------------------------------
# REST service (original BinanceSpotService)
# ---------------------------------------------------------------------------

class BinanceSpotService:
    """BTC/USDT spot price service via REST API."""

    BINANCE_TICKER_URL = "https://api.binance.com/api/v3/ticker/price"
    BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
    SYMBOL = "BTCUSDT"
    HTTP_TIMEOUT = 5.0

    def __init__(self) -> None:
        self._http: Optional[httpx.AsyncClient] = None
        self._cached_price: Optional[float] = None
        self._cached_at: Optional[datetime] = None
        self._last_log_at: Optional[datetime] = None
        self._kline_cache: dict[int, float] = {}

    async def _get_client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=httpx.Timeout(self.HTTP_TIMEOUT, connect=3.0),
                limits=httpx.Limits(max_connections=5, max_keepalive_connections=2),
            )
        return self._http

    async def close(self) -> None:
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def fetch_price(self) -> Optional[float]:
        """Fetch BTC/USDT current spot price (2-second cache)."""
        now = datetime.now(timezone.utc)
        if (
            self._cached_price is not None
            and self._cached_at is not None
            and (now - self._cached_at).total_seconds() < 2.0
        ):
            return self._cached_price

        client = await self._get_client()
        try:
            resp = await client.get(self.BINANCE_TICKER_URL, params={"symbol": self.SYMBOL})
            resp.raise_for_status()
            data = resp.json()
            price = float(data.get("price", 0.0))
            if price <= 0:
                raise ValueError(f"Invalid price: {price}")
            self._cached_price = price
            self._cached_at = now
            elapsed = (now - self._last_log_at).total_seconds() if self._last_log_at else 999.0
            if elapsed >= 10.0:
                log.debug("binance_spot BTC/USDT = {}", price)
                self._last_log_at = now
            return price
        except Exception as exc:
            log.warning("binance_spot fetch failed: {}", exc)
            return self._cached_price

    async def fetch_price_at(self, ts_utc: datetime) -> Optional[float]:
        """Backtrack BTC spot near `ts_utc` (close of the nearest 1m K-line)."""
        if ts_utc.tzinfo is None:
            ts_utc = ts_utc.replace(tzinfo=timezone.utc)
        end_ms = int(ts_utc.timestamp() * 1000) + 60_000
        start_ms = end_ms - 3 * 60_000

        cache_key = start_ms
        if cache_key in self._kline_cache:
            return self._kline_cache[cache_key]

        client = await self._get_client()
        try:
            resp = await client.get(
                self.BINANCE_KLINES_URL,
                params={
                    "symbol": self.SYMBOL,
                    "interval": "1m",
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "limit": 3,
                },
            )
            resp.raise_for_status()
            arr = resp.json()
            if not arr:
                log.warning("binance_spot klines empty for ts={}", ts_utc.isoformat())
                return None
            target_ts_ms = int(ts_utc.timestamp() * 1000)
            best_kline, best_delta = None, None
            for kline in arr:
                k_open = int(kline[0])
                k_close = int(kline[6])
                if k_open <= target_ts_ms < k_close:
                    best_kline = kline
                    break
                delta = min(abs(target_ts_ms - k_open), abs(target_ts_ms - k_close))
                if best_delta is None or delta < best_delta:
                    best_delta = delta
                    best_kline = kline
            if best_kline is None:
                return None
            close_price = float(best_kline[4])
            if close_price <= 0:
                return None
            self._kline_cache[cache_key] = close_price
            log.debug(
                "binance_spot kline @{} = {} (requested ts={})",
                datetime.fromtimestamp(int(best_kline[0]) / 1000, tz=timezone.utc).isoformat(),
                close_price, ts_utc.isoformat(),
            )
            return close_price
        except Exception as exc:
            log.warning("binance_spot klines fetch failed (ts={}): {}", ts_utc.isoformat(), exc)
            return None

    async def fetch_at_window_open(
        self,
        window_start: Optional[datetime] = None,
        now: Optional[datetime] = None,
    ) -> Optional[float]:
        """Return BTC spot price at window open (K-line backtrack, with 90s live-fallback)."""
        if window_start is not None:
            if window_start.tzinfo is None:
                window_start = window_start.replace(tzinfo=timezone.utc)
            _now = now or datetime.now(timezone.utc)
            elapsed_since_open = (_now - window_start).total_seconds()
            if elapsed_since_open <= 90.0:
                fresh = await self.fetch_price()
                if fresh is not None and fresh > 0:
                    return fresh
            kline_px = await self.fetch_price_at(window_start)
            if kline_px is not None and kline_px > 0:
                return kline_px
            log.warning(
                "binance_spot: kline fetch failed for window_start={} — falling back to spot price "
                "(may be inaccurate; entry_open_offset check will be stale until next tick)",
                window_start.isoformat(),
            )
            return await self.fetch_price()
        log.debug("fetch_at_window_open called without window_start — using current price only")
        return await self.fetch_price()

    async def update_market_spot(self, mkt: MarketInfo) -> None:
        """Update MarketInfo with current BTC spot price."""
        price = await self.fetch_price()
        if price is not None and price > 0:
            mkt.btc_spot_now = price


# ---------------------------------------------------------------------------
# WebSocket consumer (migrated from binance_trades.py)
# ---------------------------------------------------------------------------

class BinanceFeed:
    """Async WS consumer — keeps BinanceCache and RingBuffer up to date."""

    def __init__(
        self,
        cache: BinanceCache,
        ring: RingBuffer,
        on_connect: Optional[Callable[[], None]] = None,
    ) -> None:
        self._cache = cache
        self._ring = ring
        self._on_connect = on_connect
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._url = cfg().WS_URL
        self._base_delay = cfg().RECONNECT_BASE_S
        self._max_delay = cfg().RECONNECT_MAX_S

    @property
    def last_price(self) -> float:
        return self._cache.last_price

    @property
    def last_ts_ms(self) -> int:
        return self._cache.ts_ms

    def drift(self, baseline: float) -> Optional[float]:
        """abs(binance_now - baseline) / baseline, or None if no data yet."""
        if self._cache.last_price <= 0 or baseline <= 0:
            return None
        return abs(self._cache.last_price - baseline) / baseline

    def compute_baseline(self, round_start_ts: int) -> tuple[float, int]:
        """Return (median_price, sample_count) for [start - WINDOW_S, start)."""
        bcfg = baseline_cfg()
        lo = round_start_ts - bcfg.WINDOW_S
        hi = round_start_ts
        prices = self._ring.samples_in(lo, hi)
        if len(prices) < bcfg.MIN_SAMPLES:
            raise ValueError(
                f"Baseline samples insufficient: {len(prices)} < {bcfg.MIN_SAMPLES}"
            )
        return _median(prices), len(prices)

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run())
        log.info("binance_feed.started", url=self._url)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("binance_feed.stopped")

    async def _run(self) -> None:
        delay = self._base_delay
        while self._running:
            try:
                async with websockets.connect(self._url, ping_interval=None) as ws:
                    delay = self._base_delay
                    log.debug("binance_feed.connected")
                    if self._on_connect:
                        asyncio.create_task(self._on_connect())
                    async for raw in ws:
                        if not self._running:
                            break
                        await self._process(raw)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning("binance_feed.error", error=str(exc), retry_in_s=delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._max_delay)

    async def _process(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            log.debug("binance_feed.bad_json", raw=raw[:80])
            return
        if msg.get("e") != "trade":
            return
        trade = BinanceTrade(
            id=int(msg["t"]),
            price=float(msg["p"]),
            quantity=float(msg["q"]),
            ts=int(msg["T"]),
        )
        self._cache.update(trade)
        self._ring.append(trade.ts / 1000.0, trade.price)
