"""Polymarket CLOB Market WebSocket feed.

Subscribes to:  wss://ws-subscriptions-clob.polymarket.com/ws/market
(Requires custom_feature_enabled: true to receive best_bid_ask and last_trade_price.)

Provides:
  - PriceCache: last_trade_price / best_bid / best_ask per token_id
  - Tick size change notifications
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Callable

import websockets

from src.infra.logging import get_logger

log = get_logger(__name__)

MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass
class PriceCache:
    """Per-token price snapshot, updated by MarketWS."""

    last_trade_price: dict[str, float] = field(default_factory=dict)
    best_bid: dict[str, float] = field(default_factory=dict)
    best_ask: dict[str, float] = field(default_factory=dict)
    tick_size: dict[str, float] = field(default_factory=dict)

    def last(self, token: str) -> float:
        return self.last_trade_price.get(token, 0.0)

    def bid(self, token: str) -> float:
        return self.best_bid.get(token, 0.0)

    def ask(self, token: str) -> float:
        return self.best_ask.get(token, 0.0)

    def mid(self, token: str) -> float:
        b = self.best_bid.get(token)
        a = self.best_ask.get(token)
        if b and a:
            return (b + a) / 2.0
        return 0.0


MarketWSConnected = Callable[[], None]
MarketWSDisconnected = Callable[[], None]


class MarketWS:
    """Async WebSocket consumer for CLOB market data."""

    def __init__(
        self,
        cache: PriceCache,
        on_connect: MarketWSConnected | None = None,
        on_disconnect: MarketWSDisconnected | None = None,
    ) -> None:
        self._cache = cache
        self._on_connect = on_connect
        self._on_disconnect = on_disconnect
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._url = MARKET_WS_URL
        self._pending_tokens: set[str] = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def subscribe_tokens(self, token_ids: list[str]) -> None:
        """Queue token subscriptions and send immediately if WS is connected."""
        for tid in token_ids:
            self._pending_tokens.add(tid)

    @property
    def subscribed_tokens(self) -> frozenset[str]:
        return frozenset(self._pending_tokens)

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run())
        log.info("market_ws.started", url=self._url)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("market_ws.stopped")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        delay = 1.0
        max_delay = 30.0

        while self._running:
            try:
                async with websockets.connect(self._url, ping_interval=10) as ws:
                    delay = 1.0
                    log.debug("market_ws.connected")

                    if self._pending_tokens:
                        await self._subscribe(ws, list(self._pending_tokens))

                    if self._on_connect:
                        asyncio.create_task(self._on_connect())

                    async for raw in ws:
                        if not self._running:
                            break
                        await self._process(raw)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning("market_ws.error", error=str(exc), retry_in_s=delay)
                if self._on_disconnect:
                    self._on_disconnect()
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)

    async def _subscribe(self, ws: websockets.WebSocketClientProtocol, tokens: list[str]) -> None:
        payload = {
            "type": "market",
            "assets_ids": tokens,
            "custom_feature_enabled": True,
        }
        await ws.send(json.dumps(payload))
        log.info("market_ws.subscribed", tokens=[t[:16] for t in tokens])

    async def _process(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        # best_bid_ask
        bba = msg.get("best_bid_ask")
        if bba and isinstance(bba, dict):
            token = bba.get("asset_id", "")
            bids = bba.get("bids", [])
            asks = bba.get("asks", [])
            if bids:
                self._cache.best_bid[token] = float(bids[0])
            if asks:
                self._cache.best_ask[token] = float(asks[0])

        # last_trade_price (list of [price, size])
        ltp = msg.get("last_trade_price")
        if ltp and isinstance(ltp, list) and len(ltp) >= 1:
            token = msg.get("asset_id", "")
            self._cache.last_trade_price[token] = float(ltp[0])

        # tick_size_change
        tsc = msg.get("tick_size_change")
        if tsc and isinstance(tsc, dict):
            token = tsc.get("asset_id", "")
            tick = float(tsc.get("tick_size", "0.01"))
            old_tick = self._cache.tick_size.get(token, tick)
            self._cache.tick_size[token] = tick
            log.warning("market_ws.tick_size_change",
                        token=token[:16], old=old_tick, new=tick)

        # price_change (incremental)
        pc = msg.get("price_change")
        if pc and isinstance(pc, dict):
            token = pc.get("asset_id", "")
            for side, vals in [("bid", pc.get("bids", [])),
                               ("ask", pc.get("asks", []))]:
                if vals:
                    attr = "best_bid" if side == "bid" else "best_ask"
                    getattr(self._cache, attr)[token] = float(vals[0])
