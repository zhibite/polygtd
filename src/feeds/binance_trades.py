"""Binance BTC/USDT public trade WebSocket feed.

Subscribes to:  wss://stream.binance.com:9443/ws/btcusdt@trade

This is the source of the BTC spot price used for drift checks.
Only public data — no auth required.

Key behaviour:
  - Maintains a time-bounded ring buffer of (timestamp, price).
  - Provides a `drift()` method that returns |now - baseline| / baseline.
  - Auto-reconnects with exponential back-off on disconnect.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable

import websockets

from src.config import binance as cfg, baseline_cfg
from src.infra.logging import get_logger

log = get_logger(__name__)


@dataclass
class BinanceTrade:
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


@dataclass
class RingBuffer:
    """Time-bounded FIFO of (ts_seconds, price) samples."""

    retention_s: int
    _deque: deque = field(default_factory=deque)

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


class BinanceFeed:
    """Async WS consumer that keeps BinanceCache and RingBuffer up to date."""

    def __init__(
        self,
        cache: BinanceCache,
        ring: RingBuffer,
        on_connect: Callable[[], None] | None = None,
    ) -> None:
        self._cache = cache
        self._ring = ring
        self._on_connect = on_connect
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._url = cfg().WS_URL
        self._base_delay = cfg().RECONNECT_BASE_S
        self._max_delay = cfg().RECONNECT_MAX_S

    # ------------------------------------------------------------------
    # Public API (read cache / ring from outside)
    # ------------------------------------------------------------------

    @property
    def last_price(self) -> float:
        return self._cache.last_price

    @property
    def last_ts_ms(self) -> int:
        return self._cache.ts_ms

    def drift(self, baseline: float) -> float | None:
        """Return abs(binance_now - baseline) / baseline, or None if no data yet."""
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

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        delay = self._base_delay

        while self._running:
            try:
                async with websockets.connect(self._url, ping_interval=None) as ws:
                    delay = self._base_delay  # reset back-off on successful connect
                    log.debug("binance_feed.connected")
                    if self._on_connect:
                        self._on_connect()

                    async for raw in ws:
                        if not self._running:
                            break
                        await self._process(raw)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning("binance_feed.error",
                            error=str(exc), retry_in_s=delay)
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
        # ring buffer uses seconds
        self._ring.append(trade.ts / 1000.0, trade.price)
