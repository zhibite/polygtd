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
        self._ws: websockets.WebSocketClientProtocol | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def subscribe_tokens(self, token_ids: list[str]) -> None:
        """Queue token subscriptions and send immediately if WS is connected."""
        new_tokens = [tid for tid in token_ids if tid not in self._pending_tokens]
        for tid in new_tokens:
            self._pending_tokens.add(tid)
        if new_tokens and self._ws is not None and self._ws.state == websockets.State.OPEN:
            # WS is already connected — send subscription right away
            asyncio.create_task(self._subscribe(self._ws, new_tokens))

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
                    self._ws = ws
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
            finally:
                self._ws = None

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

        # Skip non-dict messages (some WS messages may be arrays)
        if not isinstance(msg, dict):
            return

        msg_type = msg.get("type", "")

        # Extract token - may be in 'market' field or in price_changes entries
        token = msg.get("token") or msg.get("asset_id") or ""
        market_field = msg.get("market", {})
        if not token and isinstance(market_field, dict):
            token = market_field.get("asset_id") or market_field.get("assetID") or ""

        # Debug: log first few unique message types (info level)
        if not hasattr(self, "_logged_types"):
            self._logged_types = set()
        if msg_type not in self._logged_types and len(self._logged_types) < 3:
            self._logged_types.add(msg_type)
            pcs = msg.get("price_changes", [])
            # Log first price_change entry structure
            pc_sample = pcs[0] if pcs else "EMPTY"
            log.info("market_ws.msg_type", type=msg_type, market=msg.get("market"), pc_sample=str(pc_sample)[:200])

        # best_bid_ask - can be nested dict, flat dict, or list of {price, size}
        bba = msg.get("best_bid_ask")
        if isinstance(bba, dict):
            bba_token = bba.get("asset_id") or bba.get("assetID") or token
            bids = bba.get("bids", [])
            asks = bba.get("asks", [])
            if bids and isinstance(bids, list) and len(bids) > 0:
                self._cache.best_bid[bba_token] = float(bids[0])
            if asks and isinstance(asks, list) and len(asks) > 0:
                self._cache.best_ask[bba_token] = float(asks[0])
        elif isinstance(bba, list) and len(bba) > 0:
            # Flat list format: [{"price": "0.55", "size": "100"}, ...]
            for x in bba:
                if isinstance(x, dict) and "price" in x:
                    self._cache.best_bid[token] = float(x["price"])
                    break

        # last_trade_price
        ltp = msg.get("last_trade_price")
        if isinstance(ltp, list) and len(ltp) >= 1:
            # [price, size] format
            self._cache.last_trade_price[token] = float(ltp[0])
        elif isinstance(ltp, dict):
            price = ltp.get("price") or ltp.get("last_trade_price")
            if price is not None:
                self._cache.last_trade_price[token] = float(price)

        # price_changes (list of incremental updates) - Plural!
        pcs = msg.get("price_changes", [])
        if isinstance(pcs, list):
            for pc in pcs:
                if isinstance(pc, dict):
                    # Get token from price_changes entry or fall back to message level
                    pc_token = pc.get("asset_id") or pc.get("assetID") or token
                    bids = pc.get("bids", [])
                    asks = pc.get("asks", [])
                    if bids and isinstance(bids, list) and len(bids) > 0:
                        v = bids[0]
                        self._cache.best_bid[pc_token] = float(v) if isinstance(v, (int, float)) else float(v["price"]) if isinstance(v, dict) else 0.0
                    if asks and isinstance(asks, list) and len(asks) > 0:
                        v = asks[0]
                        self._cache.best_ask[pc_token] = float(v) if isinstance(v, (int, float)) else float(v["price"]) if isinstance(v, dict) else 0.0
