"""GTD expiry watcher — background task that detects expired GTD orders.

Polymarket cancels GTD orders server-side when expiration_ts passes, but
the order event may arrive on the User WS channel several seconds later
(or not at all if the connection dropped). This watcher proactively:
  1. Tracks every placed order's expires_at in OrderManager.
  2. Every poll_interval_s, checks for orders that have crossed their
     expiration without a fill callback.
  3. Notifies the strategy engine so it can:
       - release the tier.placed lock (so re-arm logic can run),
       - cancel the CLOB order (idempotent — already expired is OK),
       - record the tier as expired.

Without this watcher, expired GTD orders leave tier.placed=True forever,
which silently disables the corresponding martingale leg.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from src.execution.order_manager import OrderManager
from src.infra.logging import get_logger
from src.strategy.round_state import RoundState

log = get_logger(__name__)


@dataclass
class ExpiryCallback:
    """Callback invoked when an order expires unhandled."""

    async def __call__(self, client_order_id: str, server_order_id: str, tier: int) -> None: ...


class GtdExpiryWatcher:
    """Periodically detects expired GTD orders and notifies the engine."""

    def __init__(
        self,
        order_manager: OrderManager,
        round_state_provider,
        poll_interval_s: float = 2.0,
    ) -> None:
        self._om = order_manager
        self._get_round = round_state_provider   # callable returning RoundState | None
        self._poll_interval = poll_interval_s
        self._running = False
        self._task: asyncio.Task | None = None
        # server_order_id -> last time we notified (prevent spam)
        self._last_notified: dict[str, float] = {}

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run())
        log.info("gtd_expiry_watcher.started", poll_interval_s=self._poll_interval)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        log.info("gtd_expiry_watcher.stopped")

    async def _run(self) -> None:
        while self._running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("gtd_expiry_watcher.error", error=str(exc))
            await asyncio.sleep(self._poll_interval)

    async def _tick(self) -> None:
        now = time.time()
        rs = self._get_round()
        if rs is None or rs.position is None:
            return

        for tier in rs.position.tiers:
            if not tier.placed or tier.is_complete:
                continue
            if tier.expires_at <= 0 or now < tier.expires_at:
                continue

            # Avoid notifying the same expiry repeatedly
            last = self._last_notified.get(tier.server_order_id or "")
            if last and now - last < 30:
                continue

            self._last_notified[tier.server_order_id or ""] = now
            log.warning(
                "gtd_expiry.detected",
                tier=tier.tier,
                server_order_id=(tier.server_order_id or "")[:16],
                size_filled=tier.size_filled,
                size_expected=tier.size_expected,
                fill_rate=tier.fill_rate,
                expires_at=tier.expires_at,
                age_overdue=now - tier.expires_at,
            )

            # Best-effort CLOB cancel (idempotent if already expired)
            cid = None
            for o in self._om._orders.values():
                if o.server_order_id == tier.server_order_id:
                    cid = o.client_order_id
                    break
            if cid:
                try:
                    await self._om.cancel(cid, reason="gtd_expired")
                except Exception as exc:
                    log.debug("gtd_expiry.cancel_error", client_id=cid, error=str(exc))

            rs.position.mark_expired(tier, reason="gtd_expired")

            asyncio.create_task(self._drop_notify_after(tier.server_order_id or "", 60))

    async def _drop_notify_after(self, server_order_id: str, delay: float) -> None:
        await asyncio.sleep(delay)
        self._last_notified.pop(server_order_id, None)