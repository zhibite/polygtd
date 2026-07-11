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
    """Per-token price snapshot, updated by MarketWS and REST polling."""

    last_trade_price: dict[str, float] = field(default_factory=dict)
    best_bid: dict[str, float] = field(default_factory=dict)
    best_ask: dict[str, float] = field(default_factory=dict)
    tick_size: dict[str, float] = field(default_factory=dict)
    midpoint: dict[str, float] = field(default_factory=dict)  # Updated via REST /midpoint
    # H-V6-12: provenance map. Values are "ws", "rest", or absent.
    # set_token_source() is called whenever best_bid / best_ask is mutated
    # so STATUS can show `bid_src=ws` vs `bid_src=rest` to the operator.
    bid_source: dict[str, str] = field(default_factory=dict)
    ask_source: dict[str, str] = field(default_factory=dict)

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

    def get_midpoint(self, token: str) -> float:
        """REST midpoint price — more stable than last_trade_price for strategy decisions."""
        return self.midpoint.get(token, 0.0)

    def bid_src(self, token: str) -> str:
        """'ws' if MarketWS seeded the bid, 'rest' if REST /book poller did,
        '' if cache is empty. Read-only — call set_token_source() to write."""
        return self.bid_source.get(token, "")

    def ask_src(self, token: str) -> str:
        return self.ask_source.get(token, "")

    def clear_token(self, token: str) -> None:
        """Clear cached data for a specific token (e.g., on window change)."""
        self.last_trade_price.pop(token, None)
        self.best_bid.pop(token, None)
        self.best_ask.pop(token, None)
        self.tick_size.pop(token, None)
        self.midpoint.pop(token, None)
        self.bid_source.pop(token, None)
        self.ask_source.pop(token, None)


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
        # H-V6-10: dump the first N raw WS messages so we can see what
        # Polymarket is actually sending. Once we know the real shape, the
        # diagnostic can be turned off. The 10x log-flood cap is to keep
        # the firehose manageable in production.
        self._raw_dump_remaining: int = 5
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

        # H-V6-10 diagnostic: dump the first few raw messages so we can
        # confirm the real Polymarket wire shape. Production symptom
        # 2026-07-11 01:30 round 01:30 — STATUS shows bids=0.0000 for
        # every token every tick, no `best_bid_ask` event was ever
        # parsed; we need the raw JSON to verify what to feed the parser.
        if self._raw_dump_remaining > 0:
            self._raw_dump_remaining -= 1
            log.info(
                "market_ws.raw_sample",
                keys=sorted(msg.keys()),
                asset_id=msg.get("asset_id"),
                event_type=msg.get("event_type"),
                payload=str(msg)[:400],
            )

        # ── Polymarket Market Channel payload schema ─────────────────────────
        # Real messages (event_type-driven):
        #   best_bid_ask:  { event_type, market, asset_id, best_bid, best_ask,
        #                    spread, timestamp }
        #   price_change:  { event_type, market, price_changes: [{ asset_id,
        #                    price, size, side, hash, best_bid, best_ask }],
        #                    timestamp }
        #   book:          { event_type, asset_id, market, bids:[{price,size}],
        #                    asks:[{price,size}], timestamp, hash }
        #   last_trade_price: { event_type, asset_id, price, side, size,
        #                       fee_rate_bps, timestamp }
        # Legacy / defensive reads kept below for older shapes (price-only or
        # nested best_bid_ask dict).
        event_type = msg.get("event_type") or msg.get("type") or ""

        # Extract token id — best_bid_ask / book / last_trade_price use top-
        # level `asset_id`; price_change puts it inside each price_changes[].
        token = (
            msg.get("asset_id")
            or msg.get("token")
            or ""
        )
        market_field = msg.get("market")
        if not token and isinstance(market_field, dict):
            token = market_field.get("asset_id") or market_field.get("assetID") or ""

        # ── best_bid / best_ask ──────────────────────────────────────────────
        # Polymarket sends these as TOP-LEVEL scalars in best_bid_ask events
        # and inside each price_changes[] item. We seed the cache from
        # whichever shape is present.
        if event_type == "best_bid_ask":
            bb_raw = msg.get("best_bid")
            ba_raw = msg.get("best_ask")
            try:
                if bb_raw is not None and bb_raw != "":
                    self._cache.best_bid[token] = float(bb_raw)
                    self._cache.bid_source[token] = "ws"
            except (TypeError, ValueError):
                pass
            try:
                if ba_raw is not None and ba_raw != "":
                    self._cache.best_ask[token] = float(ba_raw)
                    self._cache.ask_source[token] = "ws"
            except (TypeError, ValueError):
                pass

        # price_change → per-asset updates (each carries its own best_bid/ask)
        pcs = msg.get("price_changes", [])
        if isinstance(pcs, list):
            for pc in pcs:
                if not isinstance(pc, dict):
                    continue
                pc_token = pc.get("asset_id") or token
                if not pc_token:
                    continue
                # Bump last_trade_price from price_changes too (these are
                # trade executions, the price is the actual fill).
                pc_price = pc.get("price")
                if pc_price is not None and pc_price != "":
                    try:
                        self._cache.last_trade_price[pc_token] = float(pc_price)
                    except (TypeError, ValueError):
                        pass
                # Many price_changes carry best_bid / best_ask (older docs).
                # When present, prefer them over a stale cache value.
                for key, cache_attr, src_attr in (
                    ("best_bid", "best_bid", "bid_source"),
                    ("best_ask", "best_ask", "ask_source"),
                ):
                    raw = pc.get(key)
                    if raw is None or raw == "":
                        continue
                    try:
                        self._cache.__dict__[cache_attr][pc_token] = float(raw)
                        self._cache.__dict__[src_attr][pc_token] = "ws"
                    except (TypeError, ValueError):
                        pass

        # book event — top-level bids/asks arrays (full snapshot)
        if event_type == "book":
            bids = msg.get("bids") or []
            asks = msg.get("asks") or []
            if token and isinstance(bids, list) and bids:
                try:
                    self._cache.best_bid[token] = float(bids[0].get("price"))
                    self._cache.bid_source[token] = "ws"
                except (AttributeError, TypeError, ValueError):
                    pass
            if token and isinstance(asks, list) and asks:
                try:
                    self._cache.best_ask[token] = float(asks[0].get("price"))
                    self._cache.ask_source[token] = "ws"
                except (AttributeError, TypeError, ValueError):
                    pass

        # ── Legacy / defensive paths (kept for forward-compat) ──────────────
        # Some messages (older servers, debug echoes) carry a nested
        # `best_bid_ask` dict. Read it as a last resort; do NOT clobber a
        # better value we just wrote above.
        bba = msg.get("best_bid_ask")
        if isinstance(bba, dict):
            bba_token = bba.get("asset_id") or bba.get("assetID") or token
            bids = bba.get("bids", [])
            asks = bba.get("asks", [])
            if bids and isinstance(bids, list) and len(bids) > 0:
                try:
                    self._cache.best_bid.setdefault(bba_token, float(bids[0]))
                    self._cache.bid_source.setdefault(bba_token, "ws")
                except (TypeError, ValueError):
                    pass
            if asks and isinstance(asks, list) and len(asks) > 0:
                try:
                    self._cache.best_ask.setdefault(bba_token, float(asks[0]))
                    self._cache.ask_source.setdefault(bba_token, "ws")
                except (TypeError, ValueError):
                    pass

        # last_trade_price (top-level field on dedicated event_type)
        ltp = msg.get("last_trade_price")
        if isinstance(ltp, list) and len(ltp) >= 1:
            try:
                self._cache.last_trade_price[token] = float(ltp[0])
            except (TypeError, ValueError):
                pass
        elif isinstance(ltp, dict):
            price = ltp.get("price") or ltp.get("last_trade_price")
            if price not in (None, ""):
                try:
                    self._cache.last_trade_price[token] = float(price)
                except (TypeError, ValueError):
                    pass
        elif isinstance(ltp, str) and ltp and token:
            # top-level last_trade_price event: { event_type, asset_id,
            # price, side, size, fee_rate_bps, timestamp }
            try:
                self._cache.last_trade_price[token] = float(ltp)
            except (TypeError, ValueError):
                pass
