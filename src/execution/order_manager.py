"""OrderManager — sole order placement / cancellation interface.

Design principles:
  - Single entry point for all L2 order operations (place, cancel, reconcile).
  - All calls are serialized via an asyncio.Lock to prevent concurrent-order bugs.
  - Tracks all open orders in-memory (server_order_id -> TrackedOrder).
  - Provides reconcile() which re-syncs in-memory state with GET /orders.
  - DRY_RUN has been removed — all orders go to the real CLOB.

Tier tagging scheme:
  - client_order_id = f"{{round_ts}}-{{tier_name}}-{{uuid4[:8]}}"
  - Examples: "581400-T1-a3f12b9c", "581400-TP-c7d4e1f2"
"""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Literal

from src.config import strategy as _s
from src.execution.clob_client import CLOBClient, FillResult
from src.infra.logging import get_logger
from src.market.market_ws import PriceCache

log = get_logger(__name__)


@dataclass
class TrackedOrder:
    client_order_id: str
    server_order_id: str | None
    round_start_ts: int
    side: Literal["BUY", "SELL"]
    token_id: str
    tier_name: str    # "T1" | "T2" | "T3" | "T4" | "TP"
    price: float
    size: float
    lifetime_s: int
    expires_at: float   # Unix timestamp
    placed_at: float
    size_matched: float = 0.0
    status: Literal["placed", "live", "filled", "cancelled", "expired"] = "placed"


class OrderManager:
    """Serialized order executor with in-memory state."""

    # How close to the book to post GTD orders (in ticks).
    # BUY: bid + BUY_STEP ticks → slightly above bid so taker hits us first.
    # SELL: ask - SELL_STEP ticks → slightly below ask so taker hits us first.
    BUY_STEP: float = 0.001
    SELL_STEP: float = 0.001

    def __init__(
        self,
        clob: CLOBClient,
        price_cache: PriceCache | None = None,
    ) -> None:
        self._clob = clob
        self._s = _s()
        self._cache = price_cache
        self._lock = asyncio.Lock()
        self._orders: dict[str, TrackedOrder] = {}          # client_order_id -> order
        self._by_server: dict[str, str] = {}               # server_id -> client_id

    # ------------------------------------------------------------------
    # Price helpers
    # ------------------------------------------------------------------

    def _adjust_buy_price(self, token_id: str, desired_price: float) -> float:
        """
        Snap GTD buy price just above the current best bid so a taker is
        more likely to cross the spread and fill us immediately.
        Falls back to desired_price if the book is unavailable.
        """
        if self._cache:
            bid = self._cache.bid(token_id)
            if bid > 0:
                return round(bid + self.BUY_STEP, 4)
        return desired_price

    def _adjust_sell_price(self, token_id: str, desired_price: float) -> float:
        """
        Snap GTD sell price just below the current best ask so a taker is
        more likely to cross the spread and fill us immediately.
        Falls back to desired_price if the book is unavailable.
        """
        if self._cache:
            ask = self._cache.ask(token_id)
            if ask > 0:
                return round(ask - self.SELL_STEP, 4)
        return desired_price

    # ------------------------------------------------------------------
    # Public read access
    # ------------------------------------------------------------------

    def open_orders_for_round(self, round_start_ts: int) -> list[TrackedOrder]:
        return [
            o for o in self._orders.values()
            if o.round_start_ts == round_start_ts
            and o.status in ("placed", "live")
        ]

    def by_server_id(self, server_order_id: str) -> TrackedOrder | None:
        client_id = self._by_server.get(server_order_id)
        if client_id:
            return self._orders.get(client_id)
        return None

    def by_token(self, token_id: str) -> list[TrackedOrder]:
        return [
            o for o in self._orders.values()
            if o.token_id == token_id and o.status in ("placed", "live")
        ]

    # ------------------------------------------------------------------
    # Place BUY (GTD)
    # ------------------------------------------------------------------

    async def place_buy(
        self,
        token_id: str,
        price: float,
        size: float,
        round_start_ts: int,
        tier_name: str,
        tick_size: float,
    ) -> TrackedOrder | None:
        """Place a GTD BUY order. Returns TrackedOrder or None."""
        async with self._lock:
            lifetime_s = self._s.GTD_LIFETIME_S
            expiration_ts = int(time.time()) + 60 + lifetime_s
            cid = f"{round_start_ts}-{tier_name}-{uuid.uuid4().hex[:8]}"

            # Snap to just above the current bid for better fill probability
            adjusted_price = self._adjust_buy_price(token_id, price)
            result: FillResult = await self._clob.place_gtd_buy(
                token_id=token_id,
                price=adjusted_price,
                size=size,
                lifetime_s=lifetime_s,
                tick_size=tick_size,
            )

            if not result.success:
                log.warning(
                    "order_manager.buy.failed",
                    token=token_id[:16], price=adjusted_price, size=size,
                    tier=tier_name, error=result.error,
                )
                return None

            server_id = result.order_id
            order = TrackedOrder(
                client_order_id=cid,
                server_order_id=server_id,
                round_start_ts=round_start_ts,
                side="BUY",
                token_id=token_id,
                tier_name=tier_name,
                price=adjusted_price,  # the price actually sent to CLOB
                size=size,
                lifetime_s=lifetime_s,
                expires_at=float(expiration_ts),
                placed_at=time.time(),
                status="live",
            )
            self._orders[cid] = order
            if server_id:
                self._by_server[server_id] = cid

            log.info(
                "order_manager.placed",
                client_id=cid,
                server_id=server_id[:16] if server_id else "",
                side="BUY",
                token=token_id[:16],
                price=price,
                size=size,
                filled_tokens=result.tokens,
                fill_price=result.fill_price,
                tier=tier_name,
            )
            return order

    # ------------------------------------------------------------------
    # Place SELL (FAK market)
    # ------------------------------------------------------------------

    async def place_sell(
        self,
        token_id: str,
        size: float,
        price: float,
        round_start_ts: int,
        tier_name: str,
        tick_size: float,
    ) -> TrackedOrder | None:
        """Place a FAK market SELL order."""
        async with self._lock:
            cid = f"{round_start_ts}-{tier_name}-{uuid.uuid4().hex[:8]}"

            # Snap to just below the current ask for better fill probability
            sell_price = self._adjust_sell_price(token_id, price)
            result: FillResult = await self._clob.place_fak_sell(
                token_id=token_id,
                size=size,
                price=sell_price,
                tick_size=tick_size,
                slippage_pct=self._s.SELL_SLIPPAGE_PCT,
            )

            if not result.success:
            log.warning(
                "order_manager.sell.failed",
                token=token_id[:16], size=size, price=sell_price,
                tier=tier_name, error=result.error,
            )
                return None

            server_id = result.order_id
            order = TrackedOrder(
                client_order_id=cid,
                server_order_id=server_id,
                round_start_ts=round_start_ts,
                side="SELL",
                token_id=token_id,
                tier_name=tier_name,
                price=sell_price,  # the adjusted price actually sent to CLOB
                size=size,
                lifetime_s=0,
                expires_at=0,
                placed_at=time.time(),
                status="filled",
            )
            self._orders[cid] = order
            if server_id:
                self._by_server[server_id] = cid

            log.info(
                "order_manager.sell_filled",
                client_id=cid,
                server_id=server_id[:16] if server_id else "",
                token=token_id[:16],
                size=size,
                filled_tokens=result.tokens,
                fill_price=result.fill_price,
                tier=tier_name,
            )
            return order

    # ------------------------------------------------------------------
    # Cancel
    # ------------------------------------------------------------------

    async def cancel(self, client_order_id: str, reason: str = "manual") -> bool:
        """Cancel an order by client_order_id."""
        async with self._lock:
            order = self._orders.get(client_order_id)
            if not order:
                log.warning("order_manager.cancel.not_found", client_id=client_order_id)
                return False

            if order.status in ("filled", "cancelled", "expired"):
                log.info(
                    "order_manager.cancel.already_gone",
                    client_id=client_order_id, status=order.status,
                )
                return False

            server_id = order.server_order_id
            if server_id:
                ok = await self._clob.cancel_order(server_id)
                if ok:
                    order.status = "cancelled"
                    log.info(
                        "order_manager.cancelled",
                        client_id=client_order_id,
                        server_id=server_id[:16],
                        reason=reason,
                    )
                    return True
                return False

            order.status = "cancelled"
            return True

    async def cancel_market(self, round_start_ts: int) -> list[str]:
        """Cancel all open orders for a round. Returns list of cancelled client_ids."""
        async with self._lock:
            to_cancel = self.open_orders_for_round(round_start_ts)
            if not to_cancel:
                return []

            client_ids = [o.client_order_id for o in to_cancel]
            server_ids = [o.server_order_id for o in to_cancel if o.server_order_id]

            if server_ids:
                resp = await self._clob.cancel_orders(server_ids)
                cancelled = resp.get("canceled", [])
                for o in to_cancel:
                    if o.server_order_id in cancelled:
                        o.status = "cancelled"

            for o in to_cancel:
                if o.status != "cancelled":
                    o.status = "cancelled"

            log.info(
                "order_manager.cancel_market",
                round_start_ts=round_start_ts,
                count=len(client_ids),
            )
            return client_ids

    # ------------------------------------------------------------------
    # Reconcile
    # ------------------------------------------------------------------

    async def reconcile(self, condition_id: str | None = None) -> None:
        """
        Re-sync in-memory state with CLOB.
        Marks orders that are no longer on CLOB as cancelled/expired.
        """
        async with self._lock:
            try:
                live_orders = await self._clob.get_open_orders(condition_id)
                live_server_ids = {str(o.get("orderID", "")) for o in live_orders}

                for co in list(self._orders.values()):
                    if co.status not in ("placed", "live"):
                        continue
                    sid = co.server_order_id
                    if sid and sid not in live_server_ids:
                        co.status = "cancelled"
                        log.info(
                            "order_manager.reconcile.gone",
                            client_id=co.client_order_id,
                            server_id=sid[:16],
                        )

            except Exception as exc:
                log.warning("order_manager.reconcile.error: {}", exc)

    # ------------------------------------------------------------------
    # State mutations from callbacks
    # ------------------------------------------------------------------

    def mark_filled(self, server_order_id: str) -> None:
        """Mark an order as filled (called from trade events)."""
        order = self.by_server_id(server_order_id)
        if order:
            order.status = "filled"
            log.info(
                "order_manager.mark_filled",
                client_id=order.client_order_id,
                server_id=server_order_id[:16],
            )

    def mark_size_matched(self, server_order_id: str, size_matched: float) -> None:
        """Update matched size (called from order UPDATE events)."""
        order = self.by_server_id(server_order_id)
        if order:
            order.size_matched = size_matched
