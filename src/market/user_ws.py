"""User WebSocket — receives own order/trade events.

Subscribes to:  wss://ws-subscriptions-clob.polymarket.com/ws/user
Auth: L2 API key (apiKey / secret / passphrase) or private-key derived.

Events of interest:
  - order PLACEMENT   -> record open order
  - order UPDATE      -> update size_matched (partial fills)
  - order CANCELLATION -> remove from open orders
  - trade MATCHED     -> multi-maker fill (non-final until CONFIRMED)
  - trade MINED       -> intermediate (rare)
  - trade CONFIRMED   -> final settlement

CRITICAL — maker_orders[] processing:
  A single trade event contains a `maker_orders[]` array where EACH entry
  is an independent filled order with its own:
    - order_id  (complete hash, not a prefix)
    - asset_id  (the specific token this maker order was on — UP or DOWN)

  We MUST fire the trade callback once per maker_orders[] entry, passing
  the full order_id AND the maker's asset_id.  We must NOT use [:16] prefixes
  as identifiers — collisions are guaranteed over many rounds.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Callable

import websockets

from src.infra.logging import get_logger

log = get_logger(__name__)

USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"


# ---------------------------------------------------------------------------
# Event types
# ---------------------------------------------------------------------------


@dataclass
class OrderEvent:
    """Fired when an order PLACEMENT / UPDATE / CANCELLATION arrives."""
    order_id: str           # complete server order_id hash
    event_type: str         # "PLACEMENT" | "UPDATE" | "CANCELLATION"
    side: str               # "BUY" | "SELL"
    token_id: str           # asset_id of this order
    price: float
    size: float
    size_matched: float
    expires_at: int
    ts_ms: int


@dataclass
class MakerFill:
    """
    One maker-order fill inside a trade event.

    This is the authoritative unit — each maker_orders[] entry is one MakerFill.
    The parent TradeEvent is only a container.
    """
    maker_order_id: str     # complete server order_id of the maker order
    asset_id: str           # token this maker order was on (UP or DOWN token address)
    price: float
    size: float             # shares filled on this maker order
    trade_id: str
    status: str             # "MATCHED" | "MINED" | "CONFIRMED"
    ts_ms: int


@dataclass
class TradeEvent:
    """Legacy wrapper — kept for OrderManager compatibility."""
    order_id: str
    trade_id: str
    status: str
    price: float
    size: float
    ts_ms: int


# ---------------------------------------------------------------------------
# Typed callbacks
# ---------------------------------------------------------------------------

# (MakerFill) -> None
UserMakerFillCallback = Callable[[MakerFill], None]
# (OrderEvent) -> None
UserOrderCallback = Callable[[OrderEvent], None]


# ---------------------------------------------------------------------------
# Order cache (optional, for reference)
# ---------------------------------------------------------------------------

@dataclass
class UserOrderCache:
    """Tracks open orders locally keyed by full server order_id."""

    orders: dict[str, dict] = field(default_factory=dict)

    def upsert(self, ev: OrderEvent) -> None:
        self.orders[ev.order_id] = {
            "side": ev.side,
            "token_id": ev.token_id,
            "price": ev.price,
            "size": ev.size,
            "size_matched": ev.size_matched,
            "expires_at": ev.expires_at,
            "ts_ms": ev.ts_ms,
        }

    def remove(self, order_id: str) -> bool:
        return self.orders.pop(order_id, None) is not None

    def size_matched(self, order_id: str) -> float:
        return self.orders.get(order_id, {}).get("size_matched", 0.0)


# ---------------------------------------------------------------------------
# UserWS
# ---------------------------------------------------------------------------

class UserWS:
    """Async WebSocket consumer for authenticated user order/trade events."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        on_maker_fill: UserMakerFillCallback | None = None,
        on_order: UserOrderCallback | None = None,
        order_cache: UserOrderCache | None = None,
        clob: object | None = None,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase
        self._on_maker_fill = on_maker_fill
        self._on_order = on_order
        self._cache = order_cache or UserOrderCache()
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._url = USER_WS_URL
        self._pending_markets: set[str] = set()
        # H-V6-12 fix: optional reference to CLOBClient so that when the WS
        # delivers a trade event with size=0 (Polymarket WS quirk — see
        # market_ws raw sample in production 2026-07-11), we can fall
        # back to `clob.get_order_filled_size(order_id)` to read the
        # authoritative `size_matched` from REST. This is what
        # D:\05.project2\1.bquant\polymart3 does in its polling loop
        # to recover from MATCHED-no-fills events.
        self._clob = clob

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def subscribe_markets(self, condition_ids: list[str]) -> None:
        for cid in condition_ids:
            self._pending_markets.add(cid)

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run())
        log.info("user_ws.started", url=self._url)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("user_ws.stopped")

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        delay = 1.0
        max_delay = 30.0

        while self._running:
            try:
                async with websockets.connect(self._url, ping_interval=10) as ws:
                    delay = 1.0
                    log.debug("user_ws.connected")

                    await self._authenticate(ws)
                    if self._pending_markets:
                        await self._subscribe(ws, list(self._pending_markets))

                    async for raw in ws:
                        if not self._running:
                            break
                        await self._process(raw)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning("user_ws.error", error=str(exc), retry_in_s=delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)

    async def _authenticate(self, ws: websockets.WebSocketClientProtocol) -> None:
        await ws.send(json.dumps({
            "type": "user",
            "auth": {
                "apiKey": self._api_key,
                "secret": self._api_secret,
                "passphrase": self._api_passphrase,
            },
        }))
        log.info("user_ws.auth_sent")

    async def _subscribe(
        self, ws: websockets.WebSocketClientProtocol, condition_ids: list[str]
    ) -> None:
        await ws.send(json.dumps({
            "type": "user",
            "markets": condition_ids,
        }))
        log.info("user_ws.subscribed", markets=[c[:16] for c in condition_ids])

    async def _process(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        event_type = msg.get("event_type", "")
        type_ = msg.get("type", "")
        content = msg.get("content", msg)

        # H-V6-12 debug: dump every trade message at info level so we can see
        # which fields Polymarket actually sends (size, maker_orders, etc.).
        # Throttled to first 5 per connection to avoid log spam in steady state.
        if event_type == "trade" or type_ == "trade":
            try:
                self._trade_dump_count += 1
            except AttributeError:
                self._trade_dump_count = 0
            if self._trade_dump_count < 5:
                log.info(
                    "user_ws.trade_raw",
                    keys=list((content or {}).keys()),
                    content=str(content)[:600],
                )

        if event_type == "order" or type_ == "order":
            await self._handle_order(content)
        elif event_type == "trade" or type_ == "trade":
            await self._handle_trade(content)

    # ------------------------------------------------------------------
    # Order events
    # ------------------------------------------------------------------

    async def _handle_order(self, content: dict) -> None:
        try:
            order_id = str(content.get("orderID", "") or content.get("order_id", ""))
            side = str(content.get("side", "BUY")).upper()
            token_id = str(content.get("token", "") or content.get("asset_id", ""))
            price = float(content.get("price", 0))
            size = float(content.get("size", 0))
            size_matched = float(content.get("size_matched", 0))
            expires_at = int(content.get("expiration", 0))
            ts_ms = int(content.get("ts", 0)) or int(time.time() * 1000)
            evt_type = str(content.get("type", "PLACEMENT")).upper()
        except Exception as exc:
            log.debug("user_ws.order_parse_error", content=str(content)[:200], error=str(exc))
            return

        if not order_id:
            log.debug("user_ws.order_missing_id", content=str(content)[:200])
            return

        ev = OrderEvent(
            order_id=order_id,
            event_type=evt_type,
            side=side,
            token_id=token_id,
            price=price,
            size=size,
            size_matched=size_matched,
            expires_at=expires_at,
            ts_ms=ts_ms,
        )

        if evt_type == "PLACEMENT":
            self._cache.upsert(ev)
        elif evt_type == "UPDATE":
            self._cache.upsert(ev)
        elif evt_type == "CANCELLATION":
            self._cache.remove(order_id)

        log.info("user_ws.order_event",
                 type=evt_type, order_id=order_id,
                 token=token_id[:16], side=side,
                 price=price, size=size, size_matched=size_matched)

        if self._on_order:
            try:
                self._on_order(ev)
            except Exception:
                log.exception("user_ws.on_order callback raised")

    # ------------------------------------------------------------------
    # Trade events — CRITICAL: process maker_orders[] one-by-one
    # ------------------------------------------------------------------

    async def _handle_trade(self, content: dict) -> None:
        """
        A trade event carries `maker_orders[]` — an array of independent
        maker fills.  We fire the callback ONCE PER MAKER ORDER ENTRY,
        using the maker's own asset_id and order_id.

        Expected structure (polymarket CLOB WS):
        {
          "orderID": "taker_order_id",   # our taker order (sell TP)
          "tradeID": "...",
          "status": "MATCHED",
          "price": 0.42,
          "size": 5.0,
          "ts": 1234567890,
          "maker_orders": [
            {
              "orderID": "maker_hash_up_1",
              "assetID": "0x...UP_TOKEN...",
              "price": 0.42,
              "size": 2.5
            },
            {
              "orderID": "maker_hash_down_1",
              "assetID": "0x...DOWN_TOKEN...",
              "price": 0.42,
              "size": 2.5
            }
          ]
        }

        If `maker_orders` is absent (rare), fall back to the top-level fields
        with the top-level `orderID` treated as the maker order id.
        """
        if not self._on_maker_fill:
            return

        maker_orders: list[dict] | None = content.get("maker_orders")
        trade_id = str(content.get("tradeID", "") or content.get("trade_id", ""))
        status = str(content.get("status", "MATCHED")).upper()
        ts_ms = int(content.get("ts", 0)) or int(time.time() * 1000)

        fills_emitted = 0

        if maker_orders:
            # Normal path: iterate each maker fill independently
            for mo in maker_orders:
                try:
                    mo_id = str(mo.get("orderID", "") or mo.get("order_id", ""))
                    asset_id = str(mo.get("assetID", "") or mo.get("asset_id", ""))
                    mo_price = float(mo.get("price", 0))
                    # Polymarket WS payload may use any of: size / size_matched /
                    # matched_size / amount / fill_size. Pull whichever is present
                    # and non-zero. H-V6-12 (2026-07-11): the live WS test showed
                    # size=0 across the board while CLOB reports size_matched=5.71
                    # via REST — so the WS payload key for our account is one of
                    # the alternates below, not the legacy `size` field.
                    mo_size = self._extract_size_from_payload(mo)
                except Exception:
                    log.debug("user_ws.maker_parse_error", mo=str(mo)[:200])
                    continue

                if not mo_id or not asset_id:
                    log.debug("user_ws.maker_missing_fields", mo=str(mo)[:200])
                    continue

                # H-V6-12 fallback: WS payload reported size=0 even though the
                # status is MATCHED/MINED/CONFIRMED. Ask CLOB REST for the
                # authoritative size_matched for this order.
                if mo_size <= 0 and status in ("MATCHED", "MINED", "CONFIRMED"):
                    filled = await self._rest_filled_size(mo_id)
                    if filled and filled > 0:
                        mo_size = filled

                fill = MakerFill(
                    maker_order_id=mo_id,
                    asset_id=asset_id,
                    price=mo_price,
                    size=mo_size,
                    trade_id=trade_id,
                    status=status,
                    ts_ms=ts_ms,
                )

                log.info(
                    "user_ws.maker_fill",
                    trade_id=trade_id[:16],
                    maker_order_id=mo_id,
                    asset_id=asset_id[:16],
                    price=mo_price,
                    size=mo_size,
                    status=status,
                )

                try:
                    self._on_maker_fill(fill)
                    fills_emitted += 1
                except Exception:
                    log.exception("user_ws.on_maker_fill callback raised")

        else:
            # Rare fallback: no maker_orders[], use top-level fields
            # Treat the top-level orderID as the maker order id.
            order_id = str(content.get("orderID", "") or content.get("order_id", ""))
            asset_id = str(content.get("assetID", "") or content.get("asset_id", ""))
            price = float(content.get("price", 0))
            size = self._extract_size_from_payload(content)

            if not order_id:
                log.debug("user_ws.trade_no_makers_no_id", content=str(content)[:200])
                return

            # H-V6-12 fallback (top-level path): same REST recovery.
            if size <= 0 and status in ("MATCHED", "MINED", "CONFIRMED"):
                filled = await self._rest_filled_size(order_id)
                if filled and filled > 0:
                    size = filled

            fill = MakerFill(
                maker_order_id=order_id,
                asset_id=asset_id,
                price=price,
                size=size,
                trade_id=trade_id,
                status=status,
                ts_ms=ts_ms,
            )

            log.info(
                "user_ws.maker_fill_fallback",
                trade_id=trade_id[:16],
                maker_order_id=order_id,
                asset_id=asset_id[:16],
                price=price,
                size=size,
            )

            try:
                self._on_maker_fill(fill)
                fills_emitted += 1
            except Exception:
                log.exception("user_ws.on_maker_fill callback raised")

        if fills_emitted == 0:
            log.debug("user_ws.trade_no_fills", content=str(content)[:200])

    # ------------------------------------------------------------------
    # Helpers (H-V6-12: WS size=0 fallback to REST)
    # ------------------------------------------------------------------

    _SIZE_KEYS = (
        "size_matched",
        "matched_size",
        "matchedSize",
        "fill_size",
        "fillSize",
        "amount",
        "amount_matched",
        "size",
    )

    def _extract_size_from_payload(self, payload: dict) -> float:
        """Try each known WS size key; return the first non-zero value."""
        for size_key in self._SIZE_KEYS:
            v = payload.get(size_key)
            if v is None:
                continue
            try:
                candidate = float(v)
            except (TypeError, ValueError):
                continue
            if candidate > 0:
                return candidate
        return 0.0

    async def _rest_filled_size(self, order_id: str) -> float | None:
        """
        Fall back to CLOB REST `get_order` to read the authoritative
        size_matched. Polymart3 (working sibling project) uses the same
        pattern in its buy_token MATCHED-no-fills recovery path.
        Returns None on error, 0.0 if order exists but isn't filled.
        """
        if not self._clob or not order_id:
            return None
        try:
            return await self._clob.get_order_filled_size(order_id)
        except Exception:
            log.exception(
                "user_ws._rest_filled_size.error", order_id=order_id[:16],
            )
            return None
