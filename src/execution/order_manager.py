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
from src.market.pm_client import PolymarketClient

log = get_logger(__name__)

# Two-stage TP: maker-first, fallback to taker.
#
# IMPORTANT: TP_GTD_LIFETIME_S is the DESIRED lifetime — Polymarket CLOB
# enforces a 180-second hard floor (`CLOBClient.GTD_MIN_SEC = 180`), so the
# actual server-side lifetime is `effective_lifetime = max(TP_GTD_LIFETIME_S,
# GTD_MIN_SEC + 1) = 181s`. Setting TP_GTD_LIFETIME_S below 180 does NOT
# shorten the TP wait — it is clamped. If you need shorter TP windows, you
# must accept the CLOB's floor; the bot already does the right thing.
#
# The corresponding fallback-buffer wait in `place_tp_with_fallback` is
# `effective_lifetime - TP_FALLBACK_BUFFER_S = 166s` for current settings.
TP_GTD_LIFETIME_S: int = 90         # desired; clamped to 181s by CLOB
TP_FALLBACK_BUFFER_S: int = 15     # how much buffer before GTD deadline to start fallback


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
    # Authoritative placed size, post-CLOB round-down. Used by the engine
    # to set tier.size_expected so is_complete / cost-cap arithmetic stays
    # consistent with what the CLOB actually accepted (audit M-1).
    placed_size: float = 0.0
    size_matched: float = 0.0
    status: Literal["placed", "live", "filled", "cancelled", "expired"] = "placed"


class OrderManager:
    """Serialized order executor with in-memory state."""

    # How close to the book to post GTD orders (in ticks).
    # M-NEW-4 fix: Polymarket BTC 15m markets use tick_size=0.01; the previous
    # hard-coded BUY_STEP=0.001 was below tick granularity and round-down in
    # the CLOB would collapse it back to bid, defeating the taker-cross intent.
    # Use tick_size-aware stepping so we always land strictly inside the bid→ask
    # band: BUY = bid + tick_size/2, SELL = ask - tick_size/2.
    #
    # The legacy 0.001 values are kept as fallbacks for any market with
    # tick_size <= 0.001 (no such market exists today but keeps the math safe).
    BUY_STEP: float = 0.001
    SELL_STEP: float = 0.001

    @staticmethod
    def _buy_step_for(tick_size: float) -> float:
        """Step size for BUY snap, scaled to the market's tick granularity.

        tick_size=0.01 → step=0.005 (half a tick above bid, round-up).
        tick_size=0.001 → step=0.0005.
        tick_size=0.0001 → step=0.0001 (smaller than half a tick — clamp).
        """
        if tick_size <= 0:
            return OrderManager.BUY_STEP
        return max(tick_size / 2.0, min(tick_size, 0.0001))

    @staticmethod
    def _sell_step_for(tick_size: float) -> float:
        """Step size for SELL snap, mirror of _buy_step_for."""
        if tick_size <= 0:
            return OrderManager.SELL_STEP
        return max(tick_size / 2.0, min(tick_size, 0.0001))

    def __init__(
        self,
        clob: CLOBClient,
        price_cache: PriceCache | None = None,
        pm_client: PolymarketClient | None = None,
    ) -> None:
        self._clob = clob
        self._s = _s()
        self._cache = price_cache
        self._pm = pm_client
        self._lock = asyncio.Lock()
        self._orders: dict[str, TrackedOrder] = {}          # client_order_id -> order
        self._by_server: dict[str, str] = {}               # server_id -> client_id
        # H-NEW-1 fix: queue of (fill_price, size_matched) tuples for server_ids
        # that arrived BEFORE the real TrackedOrder was inserted (CLOBClient
        # fired the fill callback synchronously from inside place_gtd_buy).
        # Drained by resolve_pending_server_id() when the real entry lands.
        self._pending_fills: dict[str, list[tuple[float, float]]] = {}

    # ------------------------------------------------------------------
    # Price helpers
    # ------------------------------------------------------------------

    async def _get_best_bid_ask(self, token_id: str) -> tuple[float, float]:
        """
        Fetch live best bid/ask from REST API (orderbook endpoint).
        Returns (best_bid, best_ask), defaults to (0, 0) on error.
        """
        if not self._pm:
            return 0.0, 0.0
        try:
            ob = await self._pm.get_order_book(token_id)
            if not ob:
                return 0.0, 0.0
            bids = ob.get("bids", [])
            asks = ob.get("asks", [])
            bid = float(bids[0].get("price", 0)) if bids else 0.0
            ask = float(asks[0].get("price", 0)) if asks else 0.0
            return bid, ask
        except Exception:
            return 0.0, 0.0

    async def _adjust_buy_price(self, token_id: str, desired_price: float, tick_size: float = 0.01) -> float:
        """
        Snap GTD buy price just above the current best bid so a taker is
        more likely to cross the spread and fill us immediately.

        M-NEW-4 fix: uses `_buy_step_for(tick_size)` instead of the legacy
        hard-coded BUY_STEP=0.001, which round-down to nothing on
        tick_size=0.01 markets. Now the snap moves us strictly above bid
        by at least half a tick.

        Falls back to desired_price if the book is unavailable.
        """
        bid, _ = await self._get_best_bid_ask(token_id)
        if bid > 0:
            step = self._buy_step_for(tick_size)
            return round(bid + step, 4)
        return desired_price

    async def _adjust_sell_price(self, token_id: str, desired_price: float, tick_size: float = 0.01) -> float:
        """
        Snap GTD sell price just below the current best ask so a taker is
        more likely to cross the spread and fill us immediately.

        M-NEW-4 fix: uses `_sell_step_for(tick_size)`. Symmetric with the
        BUY adjustment.
        """
        _, ask = await self._get_best_bid_ask(token_id)
        if ask > 0:
            step = self._sell_step_for(tick_size)
            return round(ask - step, 4)
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

    # ── H-NEW-1 fix: pending-server-id + pending-fill queue
    # ────────────────────────────────────────────────────
    #
    # CLOBClient.place_gtd_buy polls every 0.5s. If the CLOB fills the order
    # within the first poll cycle, the fill callback fires BEFORE the await
    # returns to OrderManager.place_buy — meaning the server_id -> client_id
    # mapping in `self._by_server` is NOT yet populated when the callback
    # lands. om.by_server_id(server_id) returns None, the fill is logged as
    # "untracked" and silently dropped, and tier.size_filled / _total_cost
    # never advance even though the user has paid.
    #
    # Two-part fix:
    #   (a) `register_pending_server_id(server_id)` lets the CLOBClient
    #       pre-register the server_id the moment the post-response arrives.
    #       We use a sentinel client_order_id; by_server_id() below detects
    #       the sentinel and either returns the eventual TrackedOrder (if
    #       already inserted) or queues the fill arguments for replay.
    #   (b) `_pending_fills[server_id]` stores fill kwargs that arrived
    #       before the real TrackedOrder was inserted; on insertion we
    #       replay them through `apply_fill_event` so size_matched and the
    #       downstream engine state stay coherent.

    def register_pending_server_id(self, server_id: str) -> None:
        """Pre-register a server_order_id before the full TrackedOrder exists.

        Called by CLOBClient as soon as the post-response arrives (i.e. the
        moment we know the server_id). Idempotent: if the server_id is
        already mapped to a real client_order_id, this is a no-op.
        """
        if not server_id or server_id in self._by_server:
            return
        sentinel_cid = f"_pending_{server_id}"
        self._by_server[server_id] = sentinel_cid

    def resolve_pending_server_id(self, real_cid: str, server_id: str) -> None:
        """After place_buy / place_gtd_sell finishes, re-point the server_id
        to the real client_order_id and replay any pending fills.

        Pending fills (queue (b)) are drained in arrival order so that
        cumulative size_matched / _avg_cost reflects every observed fill,
        not just the ones that landed after the TrackedOrder existed.
        """
        if not server_id:
            return
        self._by_server[server_id] = real_cid
        # If a fill raced us, the deduper-driven callback fired while we
        # still pointed at the sentinel. Replay queued fills now so the
        # real TrackedOrder's size_matched / state catch up.
        bucket = getattr(self, "_pending_fills", None)
        pending = bucket.pop(server_id, []) if bucket is not None else []
        if pending:
            order = self._orders.get(real_cid)
            if order:
                for fill_price, size_matched in pending:
                    self.apply_fill_event(server_id, size_matched=size_matched, fill_price=fill_price)
                # If the cumulative matched reached the order's expected size,
                # promote the status to "filled" (mirrors what mark_filled
                # would have done had the lookup succeeded in time).
                if order.size_matched + 1e-9 >= order.size * 0.995:
                    self.mark_filled(server_id)
                log.info(
                    "order_manager.replay_pending_fills",
                    server_id=server_id[:16],
                    count=len(pending),
                    real_cid=real_cid,
                )

    def queue_pending_fill(self, server_id: str, fill_price: float, size_matched: float) -> None:
        """Store a fill observation that arrived before the real TrackedOrder.

        Called from `apply_fill_event` when the sentinel is detected. The
        fill is replayed via `resolve_pending_server_id` once the real
        client_order_id is inserted.
        """
        if not server_id:
            return
        # Tolerate tests that build OrderManager via __new__ without
        # running __init__ (which would have created _pending_fills).
        bucket = getattr(self, "_pending_fills", None)
        if bucket is None:
            self._pending_fills = {}
            bucket = self._pending_fills
        bucket.setdefault(server_id, []).append((fill_price, size_matched))

    def drain_pending_fills(self, server_id: str) -> None:
        """Explicitly drop pending fills for a server_id (e.g. on cancel)."""
        bucket = getattr(self, "_pending_fills", None)
        if bucket is not None:
            bucket.pop(server_id, None)

    def _cleanup_failed_post(self, server_id: str | None) -> None:
        """Drop the sentinel mapping and any queued fills for a failed post.

        B-V4-1 fix (audit v4): `register_pending_server_id` writes a sentinel
        `_pending_<server_id>` to `_by_server` the moment CLOBClient's
        `place_gtd_buy` / `place_gtd_sell` / `place_fak_sell` returns the
        server_order_id. If the post then yields `success=False` (network
        error, size < 5 tokens, poll timeout), the TrackedOrder is never
        inserted and `resolve_pending_server_id` is never called. The
        sentinel + any queued `_pending_fills` would persist forever,
        causing subsequent fill callbacks for the same server_id to be
        re-queued into a dead-letter bucket.

        Callers must invoke this on every failure path that exits with
        `return None` so that partial state is not leaked.
        """
        if not server_id:
            return
        # Only drop sentinel entries; never touch a real cid mapping
        # that a successful post elsewhere may have installed.
        client_id = self._by_server.get(server_id)
        if client_id and client_id.startswith("_pending_"):
            self._by_server.pop(server_id, None)
        self.drain_pending_fills(server_id)

    # ------------------------------------------------------------------
    # Place BUY (GTD)
    # ------------------------------------------------------------------

    async def place_buy_by_notional(
        self,
        token_id: str,
        notional: float,
        ref_price: float,
        round_start_ts: int,
        tier_name: str,
        tick_size: float,
    ) -> TrackedOrder | None:
        """Place a GTD BUY sized by USD notional rather than share count.

        H-V4-1 fix (audit v4): router used to compute
            size = notional / bid_price
        with `bid_price` = engine decision-time WS-cached bid. But the
        CLOB accepts the BUY at `adjusted_price = bid + BUY_STEP`, so
        the actual cost CLOB charges is `size * adjusted_price`, which
        can be 1-3% higher than `notional` per tier (cumulative 5% over
        4 tiers). This wrapper computes `size` AFTER `_adjust_buy_price`
        so the per-tier hard cap (`TIER_NOTIONAL_MAX_MULT`) and the
        eventual `_avg_cost` reflect what the CLOB actually accepted.

        `ref_price` is only used as a fallback when the orderbook is
        unavailable (same semantics as `place_buy(desired_price=...)`).
        """
        # Step 1: ask the orderbook for the snap price (REST, OUTSIDE lock).
        adjusted_price = await self._adjust_buy_price(token_id, ref_price, tick_size)
        if adjusted_price <= 0:
            adjusted_price = ref_price
        if adjusted_price <= 0:
            log.warning(
                "order_manager.buy_by_notional.no_price",
                token=token_id[:16], notional=notional,
            )
            return None
        # Step 2: compute size so cost == notional at the actual CLOB price.
        size = notional / adjusted_price
        return await self.place_buy(
            token_id=token_id,
            price=adjusted_price,
            size=size,
            round_start_ts=round_start_ts,
            tier_name=tier_name,
            tick_size=tick_size,
        )

    async def place_buy(
        self,
        token_id: str,
        price: float,
        size: float,
        round_start_ts: int,
        tier_name: str,
        tick_size: float,
    ) -> TrackedOrder | None:
        """Place a GTD BUY order. Returns TrackedOrder or None.

        B-NEW-2 fix (audit v3): `_adjust_buy_price` performs a REST orderbook
        fetch (1-3s in the worst case). Previously the entire place_buy body
        — adjust + post + bookkeeping — ran inside `async with self._lock`,
        so a slow orderbook fetch serialized all BUY traffic across rounds.
        Now the orderbook fetch happens BEFORE acquiring the lock; the lock
        only serializes the CLOB post + state insert (the section that
        actually needs exclusive access to `self._orders` / `self._by_server`).

        H-V4-1 fix (audit v4): prefer `place_buy_by_notional` over this
        method so that `size` is computed AFTER the orderbook price snap
        instead of before — see `place_buy_by_notional` docstring for
        the rationale. Callers that pass a precomputed `size` (e.g. tests,
        the FAK sell path) keep using this method directly.
        """
        # H-3 fix: the CLOB client enforces a minimum GTD lifetime of
        # GTD_MIN_SEC+1 = 181 s (CLOB-side hard floor). The TrackedOrder
        # must expire at the *same* wall-clock time as the server-side
        # GTD, otherwise the expiry watcher cancels an order that has
        # already expired on the server (or vice versa) — both paths
        # cause cancel-after-fill races. See also CLOBClient.GTD_MIN_SEC.
        from src.execution.clob_client import CLOBClient as _CC
        lifetime_s = self._s.GTD_LIFETIME_S
        effective_lifetime = max(lifetime_s, _CC.GTD_MIN_SEC + 1)
        cid = f"{round_start_ts}-{tier_name}-{uuid.uuid4().hex[:8]}"

        # Snap to just above the current bid for better fill probability.
        # OUTSIDE the lock — REST orderbook can take 1-3s, no need to block
        # other OM operations. B-1 v2 fix: prior to this fix the same
        # `_adjust_buy_price + place_gtd_buy + success-check` block appeared
        # TWICE in this function, which meant every BUY order was posted to
        # the CLOB twice and charged the user 2x the intended notional.
        # The second block was a leftover copy-paste; the dedup is enforced
        # by `tests/test_audit_fixes_2026_07_11.py::test_b1_v2_buy_single_call`.
        adjusted_price = await self._adjust_buy_price(token_id, price, tick_size)

        async with self._lock:
            result: FillResult = await self._clob.place_gtd_buy(
                token_id=token_id,
                price=adjusted_price,
                size=size,
                lifetime_s=lifetime_s,
                tick_size=tick_size,
            )

            if not result.success:
                # B-V4-1 fix: drop the sentinel + pending fills before exiting
                # so a server_id that has already fired a fill callback (and
                # thus installed a `_pending_<server_id>` mapping) does not
                # leak as a dead-letter entry in `_by_server` / `_pending_fills`.
                self._cleanup_failed_post(result.order_id)
                log.warning(
                    "order_manager.buy.failed",
                    token=token_id[:16], price=adjusted_price, size=size,
                    tier=tier_name, error=result.error,
                )
                return None

            server_id = result.order_id
            # H-3 fix: trust the server-side expiration_ts from FillResult
            # so the watcher's `expires_at` matches the CLOB's wall clock
            # exactly (was previously `int(time.time()) + effective_lifetime`
            # computed at this call site, which could drift by a second or
            # two against the value the CLOB actually stored).
            expiration_ts = result.expiration_ts or (
                int(time.time()) + effective_lifetime
            )
            order = TrackedOrder(
                client_order_id=cid,
                server_order_id=server_id,
                round_start_ts=round_start_ts,
                side="BUY",
                token_id=token_id,
                tier_name=tier_name,
                price=adjusted_price,  # the price actually sent to CLOB
                size=size,
                # M-1: CLOB may round-down size_q to a tick boundary; the
                # engine must size tier.size_expected against the size the
                # CLOB actually accepted, not the naive notional/price
                # quotient (which can be off by one tick).
                placed_size=result.tokens if result.tokens > 0 else size,
                lifetime_s=effective_lifetime,
                expires_at=float(expiration_ts),
                placed_at=time.time(),
                status="live",
            )
            self._orders[cid] = order
            # H-NEW-1 fix: re-point the sentinel mapping (if any) to the real
            # client_order_id. If the CLOBClient already pre-registered the
            # server_id via register_pending_server_id(), _by_server already
            # holds `_pending_<server_id>`; resolve_pending_server_id will
            # overwrite it with the real cid. If no race occurred this is
            # equivalent to a plain insert.
            if server_id:
                self.resolve_pending_server_id(cid, server_id)

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
    # Place SELL (GTD limit, maker-first)
    # ------------------------------------------------------------------

    async def place_gtd_sell(
        self,
        token_id: str,
        size: float,
        price: float,
        round_start_ts: int,
        tier_name: str,
        tick_size: float,
        lifetime_s: int | None = None,
        # B-4 fix: when True, skip the ask-side snap and use the engine's
        # exact bid as the order price. Used by the TP path so the actual
        # sale price matches the trigger-condition bid (no spread surprise).
        price_lock: bool = False,
    ) -> TrackedOrder | None:
        """
        Place a GTD limit SELL at `price` (typically the current bid).

        Unlike `place_sell` (which is a FAK taker) this returns *immediately*
        with a TrackedOrder in `live` status. The caller decides when to
        follow up with a taker fallback.

        Returns None on post failure.
        """
        async with self._lock:
            # H-3 fix: enforce the same effective lifetime as the BUY path —
            # CLOBClient clamps to GTD_MIN_SEC+1 = 181 s. The tracked
            # expires_at must mirror that, otherwise the expiry watcher
            # races with the server-side expiration and cancels orders
            # that have already self-expired (or vice versa).
            from src.execution.clob_client import CLOBClient as _CC
            gtd_lifetime = lifetime_s if lifetime_s is not None else TP_GTD_LIFETIME_S
            effective_lifetime = max(gtd_lifetime, _CC.GTD_MIN_SEC + 1)
            cid = f"{round_start_ts}-{tier_name}-{uuid.uuid4().hex[:8]}"

            # B-4 fix: TP path uses engine's exact bid as the limit price
            # so the actual fill aligns with the trigger-condition bid.
            # BUY tiers still snap to ask-SELL_STEP for fill probability.
            adjusted_price = (
                price if price_lock
                else await self._adjust_sell_price(token_id, price, tick_size)
            )
            result: FillResult = await self._clob.place_gtd_sell(
                token_id=token_id,
                price=adjusted_price,
                size=size,
                lifetime_s=gtd_lifetime,
                tick_size=tick_size,
            )

            if not result.success or not result.order_id:
                # B-V4-1 fix: drop sentinel + pending fills so a callback that
                # already fired against the sentinel does not orphan.
                self._cleanup_failed_post(result.order_id)
                log.warning(
                    "order_manager.gtd_sell.failed",
                    token=token_id[:16], size=size, price=adjusted_price,
                    tier=tier_name, error=result.error,
                )
                # Even partial fills are returned in result.tokens; surface them.
                if result.tokens > 0:
                    log.info(
                        "order_manager.gtd_sell.partial_via_failure",
                        filled=result.tokens,
                    )
                return None

            server_id = result.order_id
            # H-3 fix: trust the server-side expiration_ts from FillResult
            # so the watcher's `expires_at` matches the CLOB's wall clock
            # exactly (was previously computed locally and could drift).
            expiration_ts = result.expiration_ts or (
                int(time.time()) + effective_lifetime
            )
            order = TrackedOrder(
                client_order_id=cid,
                server_order_id=server_id,
                round_start_ts=round_start_ts,
                side="SELL",
                token_id=token_id,
                tier_name=tier_name,
                price=adjusted_price,
                size=size,
                placed_size=result.tokens if result.tokens > 0 else size,
                lifetime_s=gtd_lifetime,
                expires_at=float(expiration_ts),
                placed_at=time.time(),
                status="live",
                size_matched=result.tokens,
            )
            self._orders[cid] = order
            # H-NEW-1 fix: same as place_buy — resolve the sentinel mapping
            # pre-registered by CLOBClient when the post-response first
            # arrived. If no fill callback raced us, this is just a plain
            # insert. See register_pending_server_id docstring.
            if server_id:
                self.resolve_pending_server_id(cid, server_id)

            log.info(
                "order_manager.gtd_sell.posted",
                client_id=cid,
                server_id=server_id[:16] if server_id else "",
                token=token_id[:16],
                size=size,
                price=adjusted_price,
                tier=tier_name,
                desired_lifetime_s=gtd_lifetime,
                effective_lifetime_s=effective_lifetime,
                initial_match=result.tokens,
            )
            return order

    # ------------------------------------------------------------------
    # Two-stage TP: place GTD sell, then FAK fallback if not fully filled.
    # ------------------------------------------------------------------

    async def place_tp_with_fallback(
        self,
        token_id: str,
        size: float,
        price: float,
        round_start_ts: int,
        tier_name: str = "TP",
        tick_size: float = 0.01,
    ) -> tuple[TrackedOrder | None, TrackedOrder | None, str, str]:
        """
        TP exit with maker-first / taker-fallback design.

        Returns (gtd, fak, gtd_error, fak_error):
          * gtd_error ∈ {"", "gtd_post_failed"}            — empty on success
          * fak_error ∈ {"", "fak_post_error:...", "fak_no_fills",
                          "below_min_5", "fak_no_response", "fak_cancelled"}

        H-4 fix: stage-level error codes let the router distinguish network
        errors from book-rejected FAKs and choose the right mutex policy.

        B-4 fix: passes price_lock=True into place_gtd_sell so the actual
        GTD limit = the engine's bid at trigger time, not the snapped
        ask-SELL_STEP value (which would push the sale above the trigger).
        """
        gtd: TrackedOrder | None = None
        fak: TrackedOrder | None = None
        gtd_error: str = ""
        fak_error: str = ""

        # ── Stage 1: GTD limit sell at bid ──────────────────────────────
        gtd = await self.place_gtd_sell(
            token_id=token_id,
            size=size,
            price=price,
            round_start_ts=round_start_ts,
            tier_name=tier_name,
            tick_size=tick_size,
            lifetime_s=TP_GTD_LIFETIME_S,
            price_lock=True,   # B-4: use engine's exact bid as limit
        )

        if not gtd:
            gtd_error = "gtd_post_failed"
            log.warning(
                "order_manager.tp.gtd_post_failed_skip_to_fak",
                token=token_id[:16], size=size,
            )
            # Jump straight to FAK fallback
            fak, fak_error = await self._place_fak_with_error(
                token_id=token_id,
                size=size,
                price=price,
                round_start_ts=round_start_ts,
                tier_name=tier_name,
                tick_size=tick_size,
            )
            return (None, fak, gtd_error, fak_error)

        # ── Wait for the GTD to fill or near-expire ──────────────
        # H-3 fix: sleep must align with the *server-side* GTD deadline, not
        # the user-configured TP_GTD_LIFETIME_S. Since the CLOB client clamps
        # lifetime to GTD_MIN_SEC+1 = 181s, the watcher/cancel must wait
        # roughly that long before deciding whether to fire the FAK
        # fallback — otherwise we cancel a still-live maker order mid-life
        # and pay the taker spread unnecessarily. We use the same effective
        # lifetime as place_gtd_sell so client-side and server-side clocks
        # agree.
        from src.execution.clob_client import CLOBClient as _CC
        wait_s = max(TP_GTD_LIFETIME_S, _CC.GTD_MIN_SEC + 1) - TP_FALLBACK_BUFFER_S
        await asyncio.sleep(wait_s)

        # Refresh size_matched before deciding fallback qty.
        server_id = gtd.server_order_id
        if server_id:
            try:
                status = await self._clob._run(self._clob._client.get_order, server_id)
                if status:
                    gtd.size_matched = float(status.get("size_matched") or gtd.size_matched)
            except Exception as exc:
                log.debug("order_manager.tp.status_poll_failed", error=str(exc))

        sold_so_far = gtd.size_matched
        remaining = max(0.0, size - sold_so_far)

        # ── Stage 2: FAK fallback for the residual ───────────────────────
        if remaining >= 5.0:  # CLOB 5-shares minimum
            log.info(
                "order_manager.tp.fak_fallback",
                token=token_id[:16],
                sold=sold_so_far,
                residual=remaining,
                limit_bid=price,
            )
            fak, fak_error = await self._place_fak_with_error(
                token_id=token_id,
                size=remaining,
                price=price,
                round_start_ts=round_start_ts,
                tier_name=tier_name,
                tick_size=tick_size,
            )
        else:
            log.info(
                "order_manager.tp.gtd_full",
                token=token_id[:16], sold=sold_so_far, target=size,
            )

        # Promote GTD to filled if it's now fully matched
        if gtd.size_matched >= size * 0.995:
            self.mark_filled(gtd.client_order_id)

        return (gtd, fak, gtd_error, fak_error)

    async def _place_fak_with_error(
        self,
        token_id: str,
        size: float,
        price: float,
        round_start_ts: int,
        tier_name: str,
        tick_size: float,
    ) -> tuple[TrackedOrder | None, str]:
        """Wrapper around place_sell that maps failures to stage-level error tags.

        Returns (tracked_or_None, error_tag). error_tag is "" on success.
        H-4 fix: distinguishes network errors from book-rejected FAKs so the
        router can choose the right mutex policy.
        """
        if size < 5.0:
            return (None, "below_min_5")
        try:
            tracked = await self.place_sell(
                token_id=token_id,
                size=size,
                price=price,
                round_start_ts=round_start_ts,
                tier_name=tier_name,
                tick_size=tick_size,
            )
        except Exception as exc:
            log.warning("order_manager.tp.fak_caller_error", error=str(exc))
            return (None, f"fak_caller_error:{exc}")
        if tracked is None:
            # place_sell returns None on failure but swallows the cause;
            # we conservatively treat any None as a transient post error
            # and let the router keep the mutex.
            return (None, "fak_post_error")
        return (tracked, "")

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
            sell_price = await self._adjust_sell_price(token_id, price, tick_size)
            result: FillResult = await self._clob.place_fak_sell(
                token_id=token_id,
                size=size,
                price=sell_price,
                tick_size=tick_size,
                slippage_pct=self._s.SELL_SLIPPAGE_PCT,
            )

            if not result.success:
                # B-V4-1 fix: drop sentinel + pending fills so FAK partial
                # fills that fired via the post-response callback are not
                # orphaned as dead-letter entries.
                self._cleanup_failed_post(result.order_id)
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
            # H-NEW-1 fix: same resolve — FAK sells also race against the
            # synchronous _fire_fill_callback inside place_fak_sell (which
            # fires immediately on the post-response). resolve_pending here
            # turns the sentinel mapping into the real cid so the deduper
            # bookkeeping lands on the right TrackedOrder.
            if server_id:
                self.resolve_pending_server_id(cid, server_id)

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
                    # M-V4-3 fix: drop any queued pending fills for this
                    # server_id (partial fills that arrived before the
                    # cancel request would otherwise sit in `_pending_fills`
                    # forever and accumulate over a long session).
                    self.drain_pending_fills(server_id)
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

            # M-V4-3 fix: drain pending fills for any server_id we just
            # cancelled so the dead-letter queue does not accumulate stale
            # entries from previous partial-fill events.
            for o in to_cancel:
                if o.server_order_id:
                    self.drain_pending_fills(o.server_order_id)

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
                log.warning("order_manager.reconcile.error", error=str(exc))

    async def purge_round(self, round_start_ts: int) -> int:
        """Drop in-memory state for orders belonging to a finished round.

        M-V4-1 fix (audit v4): without this, `_orders` / `_by_server` /
        `_pending_fills` grow monotonically across rounds — at 96
        rounds/day × ~16 TrackedOrders each, the maps accumulate ~50k
        entries in a month, and `reconcile()` ends up scanning every
        historical TrackedOrder on every poll.

        Called from `_on_new_round` once the previous round has been
        cancelled. Returns the number of TrackedOrders purged for
        observability.
        """
        async with self._lock:
            stale = [
                cid for cid, o in self._orders.items()
                if o.round_start_ts == round_start_ts
            ]
            for cid in stale:
                o = self._orders.pop(cid, None)
                if o is None:
                    continue
                sid = o.server_order_id
                if sid:
                    self._by_server.pop(sid, None)
                    self.drain_pending_fills(sid)
            if stale:
                log.info(
                    "order_manager.purge_round",
                    round_start_ts=round_start_ts,
                    purged=len(stale),
                    remaining=len(self._orders),
                )
            return len(stale)

    # ------------------------------------------------------------------
    # State mutations from callbacks
    # ------------------------------------------------------------------

    def mark_filled(self, server_order_id: str) -> bool:
        """Mark an order as filled only if cumulative matched >= expected size.

        Returns True if the order was promoted to filled status.
        Returns False if the order is unknown, already terminal, or partially
        filled (caller should keep watching for more fill events).
        """
        order = self.by_server_id(server_order_id)
        if not order:
            return False
        if order.status in ("filled", "cancelled", "expired"):
            return order.status == "filled"
        # Guard against treating partial fills as fully filled.
        # CLOB sends a fill event on EVERY trade that crosses our order, so
        # if we have only received N of M tokens we are not done yet.
        if order.size_matched > 0 and order.size_matched + 1e-9 < order.size * 0.995:
            log.debug(
                "order_manager.partial_fill_skip_mark_filled",
                client_id=order.client_order_id,
                matched=order.size_matched,
                expected=order.size,
            )
            return False
        order.status = "filled"
        log.info(
            "order_manager.mark_filled",
            client_id=order.client_order_id,
            server_id=server_order_id[:16],
            matched=order.size_matched,
            expected=order.size,
        )
        return True

    def apply_fill_event(
        self,
        server_order_id: str,
        size_matched: float,
        fill_price: float | None = None,
    ) -> None:
        """Update matched size for a GTD order and report any newly-completed status."""
        order = self.by_server_id(server_order_id)
        if not order:
            # H-NEW-1 fix: if CLOBClient pre-registered a sentinel
            # `_pending_<server_id>` mapping but the real TrackedOrder is
            # not yet inserted, queue this fill so resolve_pending_server_id
            # can replay it once the real entry lands.
            client_id = self._by_server.get(server_order_id)
            if client_id and client_id.startswith("_pending_"):
                self.queue_pending_fill(
                    server_order_id, fill_price or 0.0, size_matched
                )
                log.debug(
                    "order_manager.apply_fill.queued_for_replay",
                    server_id=server_order_id[:16],
                    size_matched=size_matched,
                )
            return
        # size_matched is monotonic — keep the max
        if size_matched > order.size_matched:
            order.size_matched = size_matched

    def mark_size_matched(self, server_order_id: str, size_matched: float) -> None:
        """Update matched size (called from order UPDATE events)."""
        order = self.by_server_id(server_order_id)
        if order:
            order.size_matched = size_matched
