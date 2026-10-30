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
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from src.config import strategy as _s
from src.execution.clob_client import CLOBClient, FillResult, _round_down
from src.infra.logging import get_logger
from src.market.market_ws import PriceCache
from src.market.pm_client import PolymarketClient

log = get_logger(__name__)

# TP GTD lifetime. Polymarket CLOB enforces a 180-second hard floor
# (`CLOBClient.GTD_MIN_SEC = 180`). The client clamps to
# `max(TP_GTD_LIFETIME_S, GTD_MIN_SEC + 1)` so values below 180 are
# widened to 181. The default 300 s matches the BUY-side lifetime so
# a TP GTD posted by the strategy tick has the same cancellation
# budget as the BUY GTDs it might replace.
#
# H-NEW-2026-07-15 (audit follow-up): the previous two-stage maker/FAK
# design (`place_tp_with_fallback`) was removed. Production now uses
# `place_gtd_sell_at_price` exclusively, with the strategy tick
# cancelling the TP GTD when mid retreats below the fire line (instead
# of waiting for natural expiry and burning 5 minutes of maker
# allowance). The TP_FALLBACK_BUFFER_S constant is retained only for
# backward compatibility with imports in old test stubs.
TP_GTD_LIFETIME_S: int = 300        # desired; CLOB client clamps to ≥ 181s
TP_FALLBACK_BUFFER_S: int = 15      # legacy constant; no longer used by OrderManager


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

    def __init__(
        self,
        clob: CLOBClient,
        price_cache: PriceCache | None = None,
        pm_client: PolymarketClient | None = None,
        db: object | None = None,
    ) -> None:
        self._clob = clob
        self._s = _s()
        self._cache = price_cache
        self._pm = pm_client
        # Optional `src.persistence.db.DB`. When present, every order and
        # fill is mirrored into SQLite so the web dashboard can render
        # history. Kept optional and fully exception-guarded: a DB hiccup
        # must never block order placement or cancellation.
        self._db = db
        self._lock = asyncio.Lock()
        self._orders: dict[str, TrackedOrder] = {}          # client_order_id -> order
        self._by_server: dict[str, str] = {}               # server_id -> client_id
        # H-NEW-1 fix: queue of (fill_price, size_matched) tuples for server_ids
        # that arrived BEFORE the real TrackedOrder was inserted (CLOBClient
        # fired the fill callback synchronously from inside place_gtd_buy).
        # Drained by resolve_pending_server_id() when the real entry lands.
        self._pending_fills: dict[str, list[tuple[float, float]]] = {}
        # H-NEW-2026-07-11 (audit v6-12 production revisit): the bot
        # crashes/restarts can leave `order_status=invalid` GTDs hanging
        # on the CLOB, occupying balance/allowance. Once one is dangling
        # the next SELL is rejected with
        #   "not enough balance / allowance: ... balance: 5000000,
        #    sum of matched orders: 5000000, order amount: 5000000"
        # until the hold expires. Fix: on startup, fetch open orders and
        # cancel anything that is not already terminal (`order_status`
        # in {LIVE, INVALID, UNKNOWN}). Idempotent and safe to call
        # repeatedly; logging-only on cancellation failure.
        self._startup_reconciled: bool = False

    # ------------------------------------------------------------------
    # Persistence hooks (optional — see `db` in __init__)
    # ------------------------------------------------------------------

    def _persist_order(self, order: TrackedOrder) -> None:
        """Mirror a freshly created TrackedOrder into SQLite (best effort)."""
        db = getattr(self, "_db", None)
        if db is None:
            return
        try:
            db.insert_order(
                client_order_id=order.client_order_id,
                server_order_id=order.server_order_id,
                round_start_ts=order.round_start_ts,
                side=order.side,
                token_id=order.token_id,
                tier_name=order.tier_name,
                price=order.price,
                size=order.placed_size if order.placed_size > 0 else order.size,
                lifetime_s=int(order.lifetime_s),
                expires_at=int(order.expires_at),
                placed_at=int(order.placed_at),
            )
            if order.status != "placed" or order.size_matched > 0:
                self._persist_status(order)
        except Exception as exc:  # noqa: BLE001
            # Never let dashboard persistence break the trading path.
            log.warning("order_manager.db_persist_order_failed", error=str(exc))

    def _persist_status(self, order: TrackedOrder) -> None:
        """Mirror status / size_matched changes into SQLite (best effort)."""
        db = getattr(self, "_db", None)
        if db is None:
            return
        try:
            db.update_order_status(
                order.client_order_id, order.status, order.size_matched
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("order_manager.db_persist_status_failed", error=str(exc))

    def _persist_fill(
        self, order: TrackedOrder, delta_size: float, fill_price: float
    ) -> None:
        """Record an incremental fill (best effort, idempotent by fill_id)."""
        db = getattr(self, "_db", None)
        if db is None or delta_size <= 0:
            return
        try:
            db.insert_fill(
                # Cumulative matched size makes the id stable across the
                # WS UPDATE / TierWatcher-resync feeds that both mirror
                # the same fill into `apply_fill_event`.
                fill_id=f"{order.client_order_id}:{order.size_matched:.6f}",
                order_id=order.client_order_id,
                round_start_ts=order.round_start_ts,
                token_id=order.token_id,
                side=order.side,
                price=fill_price,
                size=delta_size,
                ts_ms=int(time.time() * 1000),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("order_manager.db_persist_fill_failed", error=str(exc))

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

    async def _find_conflicting_open_order(
        self,
        token_id: str,
        side: str,
        price: float,
        tick_size: float,
    ) -> str | None:
        """Return the server id of a live order at the same asset/side/price.

        Why this exists: a POST can be accepted by the CLOB while the client
        sees a timeout/reset and records a failure. The order is then live on
        the book but absent from `self._orders`; the next strategy tick would
        post a SECOND order at the same tier price. Scanning open orders by
        (asset, side, price) before every post closes that orphan window.

        Returns None when no conflict exists. Raises when the CLOB query
        itself fails — callers treat that as "cannot verify" and refuse to
        place (fail-closed), because a blind post is exactly how duplicates
        happen.

        Test doubles / older clients without `get_open_orders` are treated
        as "verification unavailable" (None); the local in-memory duplicate
        check remains the protection there.
        """
        if not token_id:
            return None
        settings = getattr(self, "_s", None)
        if settings is not None and not getattr(
            settings, "VERIFY_NO_EXISTING_ORDER", True
        ):
            return None
        getter = getattr(self._clob, "get_open_orders", None)
        if getter is None:
            return None
        try:
            orders = await getter(asset_id=token_id)
        except TypeError:
            # Fake/older signature without the asset_id keyword.
            orders = await getter()
        if orders is None:
            raise RuntimeError("open-orders query returned None")

        tolerance = max(abs(float(tick_size)) / 2.0, 1e-9)
        live_statuses = {"", "LIVE", "INVALID", "UNKNOWN", "PLACED", "OPEN"}
        for order in orders:
            order_asset = str(
                order.get("asset_id")
                or order.get("assetId")
                or order.get("token_id")
                or ""
            )
            if order_asset and order_asset != token_id:
                continue
            if str(order.get("side") or "").upper() != side.upper():
                continue
            status = str(order.get("status") or "").upper()
            if status not in live_statuses:
                continue
            try:
                order_price = float(order.get("price", 0) or 0)
            except (TypeError, ValueError):
                continue
            if abs(order_price - float(price)) <= tolerance + 1e-12:
                return str(
                    order.get("id") or order.get("orderID") or "unknown"
                )
        return None

    # ------------------------------------------------------------------
    # Place BUY (GTD) at an EXACT strategy price (no orderbook snap)
    # ------------------------------------------------------------------

    async def place_buy_by_strategy_price(
        self,
        token_id: str,
        strategy_price: float,
        notional: float,
        round_start_ts: int,
        tier_name: str,
        tick_size: float,
        lifetime_s: int | None = None,
    ) -> TrackedOrder | None:
        """Place a GTD BUY at the engine's exact strategy price.

        Refactor (H-NEW-2026-07-11): replaces `place_buy_by_notional`. The
        previous implementation called `_adjust_buy_price` (REST /book fetch
        to snap `bid + half_tick`) so the order would fill faster. We
        removed the orderbook fetch entirely:

          - All BUY GTDs are now posted at the **strategy price** (no
            adjustment). The new trigger model pre-arms the GTD early
            (strategy_price * (1 + margin)) so price typically CROSSES
            through strategy_price on the way down — the maker sits on
            the book at the limit price and gets filled when taker hits it.
          - No REST /book call → faster place_buy (~50 ms vs ~500 ms).
          - One less moving part in the data path.

        `size = notional / strategy_price`. This is rounded down to the
        tick granularity by CLOBClient; the TrackedOrder.placed_size
        captures the actual accepted shares so the engine knows exactly
        how many shares correspond to "this tier is complete".

        Lifetime defaults to `GTD_LIFETIME_S` from config (180 s, the CLOB
        minimum). TierWatcher tracks balance + GTD expiry after this
        returns; no client-side poll loop here.
        """
        from src.execution.clob_client import CLOBClient as _CC
        lifetime = max(int(lifetime_s or self._s.GTD_LIFETIME_S), _CC.GTD_MIN_SEC + 1)

        price = float(strategy_price)
        if price <= 0:
            log.warning(
                "order_manager.buy_by_strategy_price.no_price",
                token=token_id[:16], strategy_price=strategy_price,
            )
            return None

        size = notional / price
        # H-NEW-2026-07-11 (B-NEW-4): refuse if there's already an open BUY
        # for the same (round, tier_name). Without this, two concurrent
        # ActionRouter invocations could each call place_buy and post two
        # GTDs on the book, double-spending the tier's notional budget.
        # Cost is mechanical: an `open_orders_for_round` scan + early return.
        for o in self.open_orders_for_round(round_start_ts):
            if o.tier_name == tier_name and o.status in ("placed", "live"):
                log.warning(
                    "order_manager.buy_by_strategy_price.duplicate_tier",
                    token=token_id[:16],
                    tier=tier_name,
                    round=round_start_ts,
                    existing_client_id=o.client_order_id,
                )
                return None
        return await self.place_buy(
            token_id=token_id,
            price=price,
            size=size,
            round_start_ts=round_start_ts,
            tier_name=tier_name,
            tick_size=tick_size,
            lifetime_s=lifetime,
            # Skip the orderbook snap — keep the exact strategy price.
            price_lock=True,
        )

    async def place_gtd_sell_at_price(
        self,
        token_id: str,
        size: float,
        price: float,
        round_start_ts: int,
        tier_name: str,
        tick_size: float,
        lifetime_s: int | None = None,
    ) -> TrackedOrder | None:
        """Place a GTD SELL at an exact price (no orderbook snap).

        Refactor (H-NEW-2026-07-11): symmetric with
        `place_buy_by_strategy_price`. The previous implementation
        snapped to `ask - SELL_STEP`; we now post at the engine's exact
        target. TierWatcher handles the rest.

        Used by the TP path: `price` = `tp_target` so the actual sell
        price equals the trigger-condition price.
        """
        from src.execution.clob_client import CLOBClient as _CC
        gtd_lifetime = max(int(lifetime_s or TP_GTD_LIFETIME_S), _CC.GTD_MIN_SEC + 1)
        return await self.place_gtd_sell(
            token_id=token_id,
            size=size,
            price=float(price),
            round_start_ts=round_start_ts,
            tier_name=tier_name,
            tick_size=tick_size,
            lifetime_s=gtd_lifetime,
            price_lock=True,   # use engine's exact price (no ask snap)
        )

    # H-NEW-2026-07-15 (audit follow-up): `place_buy_by_notional` was
    # removed. It was a thin wrapper around `place_buy` that pre-divided
    # `notional/ref_price` for callers that wanted to think in USD
    # budget instead of share count. The current production path uses
    # `place_buy_by_strategy_price` exclusively (which sizes shares
    # from the strategy price + tier notional internally), and the only
    # remaining callers were a couple of legacy tests that now also
    # use `place_buy_by_strategy_price`. Removing the wrapper eliminates
    # the maintenance hazard of two parallel sizing paths.
    #
    async def place_buy(
        self,
        token_id: str,
        price: float,
        size: float,
        round_start_ts: int,
        tier_name: str,
        tick_size: float,
        lifetime_s: int | None = None,
        price_lock: bool = False,
    ) -> TrackedOrder | None:
        """Place a GTD BUY order. Returns TrackedOrder or None.

        B-NEW-2 fix (audit v3): `_adjust_buy_price` performs a REST orderbook
        fetch (1-3s in the worst case). Previously the entire place_buy body
        — adjust + post + bookkeeping — ran inside `async with self._lock`,
        so a slow orderbook fetch serialized all BUY traffic across rounds.
        Now the orderbook fetch happens BEFORE acquiring the lock; the lock
        only serializes the CLOB post + state insert (the section that
        actually needs exclusive access to `self._orders` / `self._by_server`).

        H-NEW-2026-07-15 (audit follow-up): the previous H-V4-1 hint
        recommending `place_buy_by_notional` is moot — that wrapper
        was removed because the strategy tick now uses
        `place_buy_by_strategy_price` exclusively (which sizes shares
        from the strategy price + tier notional internally and skips
        the post-only price snap entirely). `place_buy` remains for
        callers that want to pass a precomputed `size` (legacy tests
        and any direct internal wiring).
        """
        # H-3 fix: the CLOB client enforces a minimum GTD lifetime of
        # GTD_MIN_SEC+1 = 181 s (CLOB-side hard floor). The TrackedOrder
        # must expire at the *same* wall-clock time as the server-side
        # GTD, otherwise the expiry watcher cancels an order that has
        # already expired on the server (or vice versa) — both paths
        # cause cancel-after-fill races. See also CLOBClient.GTD_MIN_SEC.
        from src.execution.clob_client import CLOBClient as _CC
        # H-NEW-2026-07-15 (audit follow-up): respect caller-supplied
        # `lifetime_s` before falling back to config. Previously this line
        # unconditionally read `self._s.GTD_LIFETIME_S`, which silently
        # swallowed every override — including `place_buy_by_strategy_price`'s
        # `lifetime=...` argument. `place_gtd_sell_at_price` happens to also
        # use 300 s today, so production never noticed, but the contract was
        # broken (any future change to `TP_GTD_LIFETIME_S` would have been
        # silently ignored by the BUY path). Now: if the caller passes
        # lifetime_s, use it; otherwise use the config default.
        cfg_lifetime = self._s.GTD_LIFETIME_S
        lifetime_s = int(lifetime_s) if lifetime_s is not None else cfg_lifetime
        effective_lifetime = max(lifetime_s, _CC.GTD_MIN_SEC + 1)
        cid = f"{round_start_ts}-{tier_name}-{uuid.uuid4().hex[:8]}"

        # H-NEW-2026-07-11: production callers always pass
        # `price_lock=True` via `place_buy_by_strategy_price`, so we post
        # at the engine's exact strategy price. The legacy
        # `_adjust_buy_price` orderbook fetch has been removed entirely.
        adjusted_price = float(price)
        # Quantize the share count to the same precision the CLOB will use.
        # TrackedOrder.size / placed_size then equal the server-accepted
        # order exactly: no "expected 5.000 but server took 4.99" drift, and
        # the tier budget can never be overspent by rounding.
        size_q = (
            _round_down(size, Decimal(str(tick_size)))
            if tick_size and tick_size > 0
            else float(size)
        )

        async with self._lock:
            # Authoritative duplicate guard. The same check exists in
            # `place_buy_by_strategy_price` before the lock, but that one is
            # TOCTOU: two concurrent callers can both pass it before either
            # acquires the lock. Re-check under the lock so a tier can never
            # have two live BUY GTDs (double-spending its notional).
            for existing in self.open_orders_for_round(round_start_ts):
                if existing.tier_name == tier_name:
                    log.warning(
                        "order_manager.buy.duplicate_tier_rejected",
                        token=token_id[:16],
                        tier=tier_name,
                        round=round_start_ts,
                        existing_client_id=existing.client_order_id,
                    )
                    return None

            # Orphan guard: catch an order that reached the CLOB but whose
            # POST response never reached us (timeout / reset). Fail closed
            # if the open-orders query itself cannot be verified.
            try:
                conflict = await self._find_conflicting_open_order(
                    token_id, "BUY", adjusted_price, tick_size
                )
            except Exception as exc:
                log.warning(
                    "order_manager.buy.verify_failed",
                    token=token_id[:16],
                    price=adjusted_price,
                    tier=tier_name,
                    error=str(exc),
                    note="refusing to post without orphan verification",
                )
                return None
            if conflict:
                log.error(
                    "order_manager.buy.existing_order_on_book",
                    token=token_id[:16],
                    price=adjusted_price,
                    tier=tier_name,
                    existing_server_id=conflict[:16],
                    note="an open order already exists at this asset/side/"
                         "price; refusing to place a duplicate",
                )
                return None

            result: FillResult = await self._clob.place_gtd_buy(
                token_id=token_id,
                price=adjusted_price,
                size=size_q,
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
                    token=token_id[:16], price=adjusted_price, size=size_q,
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
                size=size_q,
                # M-1: CLOBClient returns the post-round-down accepted size
                # in `tokens` for a BUY post (not a fill). Fall back to our
                # own size_q for older/fake clients that return 0.
                placed_size=result.tokens if result.tokens > 0 else size_q,
                lifetime_s=effective_lifetime,
                expires_at=float(expiration_ts),
                placed_at=time.time(),
                status="live",
            )
            self._orders[cid] = order
            self._persist_order(order)
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
                adjusted_price=adjusted_price,
                size=size_q,
                accepted_size=result.tokens,
                fill_price=result.fill_price,
                tier=tier_name,
                desired_lifetime_s=lifetime_s,
                effective_lifetime_s=effective_lifetime,
                expires_at=expiration_ts,
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

            # H-NEW-2026-07-11: production callers always pass
            # `price_lock=True` via `place_gtd_sell_at_price`, so we post
            # at the engine's exact tp/target price. The legacy
            # `_adjust_sell_price` orderbook fetch has been removed.
            adjusted_price = float(price) if price_lock else float(price)
            # CLOBClient rounds SELL size to 0.0001 shares. Quantize here
            # too so the watcher's expected_size matches the real order.
            size_q = _round_down(size, Decimal("0.0001"))

            # Orphan guard for the TP side: a lost POST response can leave a
            # live SELL at this exact price while the local bookkeeping
            # thinks the post failed. Fail closed on query errors.
            try:
                conflict = await self._find_conflicting_open_order(
                    token_id, "SELL", adjusted_price, tick_size
                )
            except Exception as exc:
                log.warning(
                    "order_manager.gtd_sell.verify_failed",
                    token=token_id[:16],
                    price=adjusted_price,
                    tier=tier_name,
                    error=str(exc),
                    note="refusing to post without orphan verification",
                )
                return None
            if conflict:
                log.error(
                    "order_manager.gtd_sell.existing_order_on_book",
                    token=token_id[:16],
                    price=adjusted_price,
                    tier=tier_name,
                    existing_server_id=conflict[:16],
                    note="an open SELL already exists at this asset/price; "
                         "refusing to place a duplicate",
                )
                return None

            result: FillResult = await self._clob.place_gtd_sell(
                token_id=token_id,
                price=adjusted_price,
                size=size_q,
                lifetime_s=gtd_lifetime,
                tick_size=tick_size,
            )

            if not result.success or not result.order_id:
                # B-V4-1 fix: drop sentinel + pending fills so a callback that
                # already fired against the sentinel does not orphan.
                self._cleanup_failed_post(result.order_id)
                log.warning(
                    "order_manager.gtd_sell.failed",
                    token=token_id[:16], size=size_q, price=adjusted_price,
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
                size=size_q,
                placed_size=size_q,
                lifetime_s=gtd_lifetime,
                expires_at=float(expiration_ts),
                placed_at=time.time(),
                status="live",
                size_matched=result.tokens,
            )
            self._orders[cid] = order
            self._persist_order(order)
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
                size=size_q,
                price=adjusted_price,
                tier=tier_name,
                desired_lifetime_s=gtd_lifetime,
                effective_lifetime_s=effective_lifetime,
                expires_at=expiration_ts,
                initial_match=result.tokens,
                initial_fill_rate=(result.tokens / size) if size > 0 else 0.0,
            )
            return order

    # ------------------------------------------------------------------
    # Two-stage TP: place GTD sell, then FAK fallback if not fully filled.
    # ------------------------------------------------------------------

    # H-NEW-2026-07-15 (audit follow-up): `place_tp_with_fallback` and
    # `_place_fak_with_error` were removed. They implemented a two-stage
    # maker-first / FAK-fallback TP design that production no longer
    # uses — the strategy tick now posts a single GTD via
    # `place_gtd_sell_at_price` and cancels it (via `ActionCancelTier`
    # with `tier=-1` sentinel) when mid retreats below the fire line.
    # Removing the dead-code paths eliminates the maintenance hazard
    # where a future contributor might re-enable them and reintroduce
    # the 5-minute passive wait bug.
    #
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

            # H-NEW-2026-07-11: TP SELL uses the engine's exact tp target price
            # (passed in via `price`); no orderbook snap.
            sell_price = float(price)
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
            self._persist_order(order)
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
            return await self._cancel_locked(client_order_id, reason)

    async def _cancel_locked(
        self, client_order_id: str, reason: str = "manual"
    ) -> bool:
        """Cancel implementation. Caller MUST hold `self._lock`.

        Extracted from `cancel()` so that `cancel_by_server_id()` — which
        already holds the (non-reentrant) asyncio.Lock — does not deadlock
        by calling the public `cancel()`.
        """
        order = self._orders.get(client_order_id)
        if not order:
            log.warning("order_manager.cancel.not_found", client_id=client_order_id)
            return False

        if order.status in ("filled", "cancelled", "expired"):
            log.info(
                "order_manager.cancel.already_gone",
                client_id=client_order_id, status=order.status,
                side=order.side, tier=order.tier_name,
            )
            return False

        server_id = order.server_order_id
        if server_id:
            ok = await self._clob.cancel_order(server_id)
            if ok:
                order.status = "cancelled"
                self._persist_status(order)
                # M-V4-3 fix: drop any queued pending fills for this
                # server_id (partial fills that arrived before the
                # cancel request would otherwise sit in `_pending_fills`
                # forever and accumulate over a long session).
                self.drain_pending_fills(server_id)
                log.info(
                    "order_manager.cancelled",
                    client_id=client_order_id,
                    server_id=server_id[:16],
                    side=order.side,
                    tier=order.tier_name,
                    reason=reason,
                    matched=order.size_matched,
                    expected=order.size,
                    residual=max(0.0, order.size - order.size_matched),
                )
                return True
            # H-V6-2 fix: previously a failed cancel returned False
            # silently and operators could not tell whether the order
            # was actually still live. Surface the failure with the
            # reason (passed by caller — e.g. "gtd_expired", "round_end",
            # "manual") so logs can be filtered.
            log.warning(
                "order_manager.cancel.failed",
                client_id=client_order_id,
                server_id=server_id[:16],
                side=order.side,
                tier=order.tier_name,
                reason=reason,
            )
            return False

        order.status = "cancelled"
        self._persist_status(order)
        log.info(
            "order_manager.cancelled.no_server_id",
            client_id=client_order_id,
            side=order.side,
            tier=order.tier_name,
            reason=reason,
        )
        return True

    async def cancel_by_server_id(
        self,
        server_order_id: str,
        reason: str = "tick_reconcile",
    ) -> bool:
        """Cancel a live GTD identified by server_order_id.

        H-NEW-2026-07-15 tick-driven refactor: the strategy tick
        (`StrategyEngine._reconcile_inflight_buys`) is the sole source of
        BUY-side cancel decisions. The engine only has the tier's
        `server_order_id` in hand, so this helper resolves it to the
        internal TrackedOrder and reuses the same cancel logic as the
        client-id-keyed `cancel()`.

        Returns True iff the CLOB accepted the cancel AND the
        TrackedOrder was transitioned to `cancelled`. False on
        not-found, already-terminal, or CLOB reject.
        """
        async with self._lock:
            client_id = self._by_server.get(server_order_id)
            if not client_id or client_id.startswith("_pending_"):
                # Sentinel mapping only (post in flight) or unmapped —
                # nothing to cancel from our bookkeeping perspective.
                log.warning(
                    "order_manager.cancel_by_server_id.unmapped",
                    server_id=server_order_id[:16],
                    reason=reason,
                )
                return False
            # Call the lock-held implementation directly: asyncio.Lock is
            # NOT reentrant, so `await self.cancel(...)` here would deadlock
            # forever and freeze the DecisionLoop + round rollover.
            return await self._cancel_locked(client_id, reason=reason)

    async def cancel_market(self, round_start_ts: int) -> list[str]:
        """Cancel all open orders for a round. Returns list of cancelled client_ids."""
        async with self._lock:
            to_cancel = self.open_orders_for_round(round_start_ts)
            if not to_cancel:
                return []

            client_ids = [o.client_order_id for o in to_cancel]
            server_ids = [o.server_order_id for o in to_cancel if o.server_order_id]

            # H-V6-2 fix: log per-order snapshot so operators can see which
            # GTDs are about to be cancelled and how full each one was at
            # round-end. The previous single summary log only printed count.
            for o in to_cancel:
                log.info(
                    "order_manager.cancel_market.target",
                    client_id=o.client_order_id,
                    server_id=(o.server_order_id or "")[:16],
                    side=o.side,
                    tier=o.tier_name,
                    status=o.status,
                    matched=o.size_matched,
                    expected=o.size,
                    residual=max(0.0, o.size - o.size_matched),
                )

            cancelled_on_clob: list[str] = []
            not_cancelled: list[str] = []
            if server_ids:
                resp = await self._clob.cancel_orders(server_ids)
                cancelled_on_clob = list(resp.get("canceled", []) or [])
                not_cancelled = list(resp.get("not_canceled", []) or [])
                for o in to_cancel:
                    if o.server_order_id in cancelled_on_clob:
                        o.status = "cancelled"
                    elif o.server_order_id in not_cancelled:
                        log.warning(
                            "order_manager.cancel_market.not_cancelled",
                            client_id=o.client_order_id,
                            server_id=o.server_order_id[:16],
                            side=o.side, tier=o.tier_name,
                            note="CLOB did not confirm the cancel; order "
                                 "remains tracked/live so late fills are "
                                 "still attributed",
                        )
                    else:
                        log.warning(
                            "order_manager.cancel_market.unconfirmed",
                            client_id=o.client_order_id,
                            server_id=o.server_order_id[:16],
                            side=o.side, tier=o.tier_name,
                            note="cancel response did not mention this id; "
                                 "keeping live status",
                        )

            # Orders with no server id were never on the CLOB — they can be
            # marked cancelled locally. Orders whose cancel was not confirmed
            # stay in "live" status on purpose: force-marking them cancelled
            # would hide a real fill from the engine.
            for o in to_cancel:
                if not o.server_order_id and o.status != "cancelled":
                    o.status = "cancelled"

            # M-V4-3 fix: drain pending fills for any server_id we just
            # cancelled so the dead-letter queue does not accumulate stale
            # entries from previous partial-fill events.
            for o in to_cancel:
                if o.server_order_id and o.status == "cancelled":
                    self.drain_pending_fills(o.server_order_id)

            # Mirror the final status (cancelled / still-live) into SQLite.
            for o in to_cancel:
                self._persist_status(o)

            log.info(
                "order_manager.cancel_market",
                round_start_ts=round_start_ts,
                count=len(client_ids),
                cancelled_on_clob=len(cancelled_on_clob),
                not_cancelled_on_clob=len(not_cancelled),
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
                if live_orders is None:
                    # Query failed — we do NOT know which orders are gone.
                    # Marking everything cancelled here (the old behaviour,
                    # caused by get_open_orders swallowing errors into [])
                    # would leave live GTDs on the book while the engine
                    # believes the slots are free.
                    log.warning(
                        "order_manager.reconcile.query_failed",
                        note="skipping state reconciliation; live orders kept",
                    )
                    return
                live_server_ids = {
                    str(o.get("id") or o.get("orderID") or "")
                    for o in live_orders
                }

                for co in list(self._orders.values()):
                    if co.status not in ("placed", "live"):
                        continue
                    sid = co.server_order_id
                    if sid and sid not in live_server_ids:
                        co.status = "cancelled"
                        self._persist_status(co)
                        log.info(
                            "order_manager.reconcile.gone",
                            client_id=co.client_order_id,
                            server_id=sid[:16],
                        )

            except Exception as exc:
                log.warning("order_manager.reconcile.error", error=str(exc))

    async def startup_reconcile(self) -> None:
        """Force-cancel any leftover CLOB orders that survived a bot restart.

        H-NEW-2026-07-11 (audit v6-12 production revisit): without this,
        a bot crash or a previous round whose `cancel_market` failed
        (e.g. `_safe_cancel` SDK signature bug) leaves the CLOB holding
        `order_status=invalid` GTDs that *still consume the maker
        allowance*. The next `place_gtd_sell` is then permanently
        rejected with
          "not enough balance / allowance: ... balance: 5000000,
           sum of matched orders: 5000000, order amount: 5000000"

        Idempotent: if nothing is open it does nothing. If the cancel
        itself fails (signature bug, network, etc.) we log and continue
        so a partial reconciliation does not block startup.

        Production symptom 2026-07-11T12:04:46: stale GTD
        `0x3ede40a1f6c744` (status=invalid) blocked every subsequent
        SELL while bot still believed it held 5 DOWN tokens; the real
        sell executed on the UI side without ever being reflected in
        OrderManager.

        R8 (audit 2026-07-12): this method handles CLOB-ORDER lifetime
        cleanup only — it does NOT replay historical fills into the
        engine's position state. That gap is closed by `on_new_round`
        constructing a fresh `Position()` per round (each Tier 1..3
        starts at size_filled=0). The trade-off: any fills that
        occurred during the previous round and were never reflected
        in the engine before the bot restarted are accepted as "gone"
        on the next round — we re-arm fresh entries off the new
        mid. This is intentional because reconstructing pre-restart
        fills would require reading CLOB trade history filtered by
        market, which (a) adds latency at boot and (b) is prone to
        double-count races with the WS TRADE channel that fires as
        soon as User Channel subscribes. The v7 audit test
        `test_r8_on_new_round_resets_position_state` pins the
        invariant that no previous-round flag leaks across.
        """
        if self._startup_reconciled:
            return
        settings = getattr(self, "_s", None)
        if settings is not None and not getattr(
            settings, "STARTUP_RECONCILE_CANCEL_ALL", True
        ):
            self._startup_reconciled = True
            log.info(
                "order_manager.startup_reconcile.disabled",
                note="STARTUP_RECONCILE_CANCEL_ALL=False; leaving existing "
                     "account orders untouched (shared-wallet mode)",
            )
            return
        try:
            open_orders = await self._clob.get_open_orders()
        except Exception as exc:
            log.warning(
                "order_manager.startup_reconcile.list_failed",
                error=str(exc),
                note="cannot enumerate stale CLOB orders; "
                     "balance lock may persist — manual intervention needed",
            )
            return

        if open_orders is None:
            log.warning(
                "order_manager.startup_reconcile.query_failed",
                note="open-orders query failed; cannot distinguish our "
                     "orders from other account activity — skipping",
            )
            return

        if not open_orders:
            self._startup_reconciled = True
            log.info("order_manager.startup_reconcile", cancelled=0, note="no open orders on CLOB")
            return

        # H-NEW-2026-07-12: py-clob-client-v2 returns each open order with
        # the field name `id` (the full hash), NOT `orderID` (V1 name).
        # Same shape came back from a previous v1 client; the V2 migration
        # rename is what the official "Migrating to CLOB V2" guide calls
        # out: `get_orders()` → `get_open_orders()` and `id` is the
        # canonical key. Fall back to `orderID` for safety in case a
        # future SDK roll-back drops v1-style fields.
        # Also filter out rows whose CLOB status is already terminal
        # (MATCHED / CANCELED / EXPIRED / matched >= original_size):
        # cancelling them is wasted work and noisy in logs. We only
        # cancel LIVE / INVALID / UNKNOWN rows that may still hold
        # maker allowance.
        live_statuses = {"LIVE", "INVALID", "UNKNOWN"}
        def _sid(o: dict) -> str:
            return str(o.get("id") or o.get("orderID") or "")
        cancel_candidates = [
            (o, _sid(o))
            for o in open_orders
            if _sid(o) and (o.get("status") in live_statuses or not o.get("status"))
        ]
        if len(cancel_candidates) < len(open_orders):
            skipped = len(open_orders) - len(cancel_candidates)
            log.info(
                "order_manager.startup_reconcile.terminal_skipped",
                skipped=skipped,
                statuses_seen=sorted({
                    str(o.get("status", "NONE")) for o in open_orders
                }),
            )
        server_ids = [sid for _, sid in cancel_candidates]
        if not server_ids:
            self._startup_reconciled = True
            log.info(
                "order_manager.startup_reconcile",
                cancelled=0,
                note="open orders are all terminal; nothing to cancel",
            )
            return
        log.warning(
            "order_manager.startup_reconcile.stale_orders",
            count=len(server_ids),
            server_ids=[sid[:16] for sid in server_ids],
            note="bot restart detected dangling CLOB orders; "
                 "force-cancelling to free maker allowance",
        )

        cancelled: list[str] = []
        not_cancelled: list[str] = []
        try:
            resp = await self._clob.cancel_orders(server_ids)
            cancelled = list(resp.get("canceled", []) or [])
            not_cancelled = list(resp.get("not_canceled", []) or [])
        except Exception as exc:
            log.warning(
                "order_manager.startup_reconcile.batch_cancel_failed",
                error=str(exc),
                note="best-effort single-order cancel may still recover",
            )
            # Fallback: try cancel_order per id (uses OrderPayload path)
            for sid in server_ids:
                try:
                    ok = await self._clob.cancel_order(sid)
                    if ok:
                        cancelled.append(sid)
                    else:
                        not_cancelled.append(sid)
                except Exception as exc2:
                    log.warning(
                        "order_manager.startup_reconcile.single_cancel_failed",
                        server_id=sid[:16], error=str(exc2),
                    )
                    not_cancelled.append(sid)

        log.info(
            "order_manager.startup_reconcile",
            requested=len(server_ids),
            cancelled_on_clob=len(cancelled),
            not_cancelled=len(not_cancelled),
        )

        # Mark in-memory orders cancelled so subsequent fill callbacks
        # for these ids are dropped.
        async with self._lock:
            for sid in cancelled:
                cid = self._by_server.get(sid)
                if not cid:
                    continue
                co = self._orders.get(cid)
                if co and co.status in ("placed", "live"):
                    co.status = "cancelled"
                    self.drain_pending_fills(sid)

        # Resolve in-memory vs CLOB drift.
        await self.reconcile()
        self._startup_reconciled = True

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
            # Only purge terminal state. A previous-round order whose cancel
            # was not confirmed by the CLOB must stay tracked: deleting the
            # mapping would turn a late fill into an "untracked" event and
            # lose it from the books.
            #
            # `<=` (not `==`) so terminal orders from older rounds are also
            # collected — otherwise a single never-confirmed cancel could
            # keep an old-round entry alive forever.
            stale = [
                cid for cid, o in self._orders.items()
                if o.round_start_ts <= round_start_ts
                and o.status not in ("placed", "live")
            ]
            skipped_live = sum(
                1 for o in self._orders.values()
                if o.round_start_ts <= round_start_ts
                and o.status in ("placed", "live")
            )
            for cid in stale:
                o = self._orders.pop(cid, None)
                if o is None:
                    continue
                sid = o.server_order_id
                if sid:
                    self._by_server.pop(sid, None)
                    self.drain_pending_fills(sid)
            if stale or skipped_live:
                log.info(
                    "order_manager.purge_round",
                    round_start_ts=round_start_ts,
                    purged=len(stale),
                    skipped_live=skipped_live,
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
            log.debug(
                "order_manager.mark_filled.unknown_order",
                server_id=server_order_id[:16],
            )
            return False
        if order.status in ("filled", "cancelled", "expired"):
            log.debug(
                "order_manager.mark_filled.skip_already_terminal",
                client_id=order.client_order_id,
                status=order.status,
                server_id=server_order_id[:16],
            )
            return order.status == "filled"
        # Guard against treating partial fills as fully filled.
        # CLOB sends a fill event on EVERY trade that crosses our order, so
        # if we have only received N of M tokens we are not done yet.
        #
        # H-NEW-2026-07-15 (audit follow-up): compare against `placed_size`
        # (the CLOB round-down accepted share count, M-1 fix) instead of
        # `size` (the naive notional/price quotient). When the CLOB rounds
        # down by one tick the two values differ by a single share, so a
        # clean fill at `placed_size` would never trigger the filled
        # promotion under the old check, leaving `TrackedOrder.status`
        # stuck at `live` for the rest of the round. Fall back to `size`
        # when `placed_size` is unavailable (defensive — covers any
        # legacy code path that bypassed the M-1 wiring).
        expected_threshold = (
            order.placed_size if order.placed_size > 0 else order.size
        )
        # NOTE: no `size_matched > 0` escape hatch here. A zero-matched order
        # is *not* filled; the old condition let mark_filled() promote an
        # order that had received no fills at all.
        if order.size_matched + 1e-9 < expected_threshold * 0.995:
            log.debug(
                "order_manager.partial_fill_skip_mark_filled",
                client_id=order.client_order_id,
                matched=order.size_matched,
                expected=expected_threshold,
                placed_size=order.placed_size,
                raw_size=order.size,
            )
            return False
        order.status = "filled"
        self._persist_status(order)
        log.info(
            "order_manager.mark_filled",
            client_id=order.client_order_id,
            server_id=server_order_id[:16],
            side=order.side,
            tier=order.tier_name,
            token=order.token_id[:16],
            matched=order.size_matched,
            expected=expected_threshold,
            fill_rate=order.size_matched / expected_threshold if expected_threshold > 0 else 0.0,
            lifetime_s=order.lifetime_s,
            placed_age_s=max(0.0, time.time() - order.placed_at),
        )
        return True

    def apply_fill_event(
        self,
        server_order_id: str,
        size_matched: float,
        fill_price: float | None = None,
    ) -> None:
        """Update matched size for a GTD order and report any newly-completed status.

        R7 (audit 2026-07-12): this method is the MIRROR-ONLY path used by
        the WS UPDATE and TierWatcher-resync feeds. It deliberately does
        NOT call `eng.on_order_filled` — that path is reserved for the
        WS TRADE callback which carries per-trade deltas (and is deduped
        by trade_id). Accidentally calling apply_fill_event followed by
        eng.on_order_filled on the SAME delta would double-count
        (size_filled += delta twice, _total_qty += delta twice). The
        `__call_mirror_only` assertion at exit makes that regression a
        hard failure in tests rather than a silent accounting drift.

        Constraints:
          * `size_matched` is MONOTONIC — must not regress.
          * `size_matched` carries the cumulative matched shares straight
            from CLOB; a negative or NaN input is treated as garbage.
        """
        # R7: sanity checks (cheap, fail-loud).
        if size_matched < 0 or size_matched != size_matched:  # NaN-safe
            log.warning(
                "order_manager.apply_fill_event.invalid_size",
                server_id=server_order_id[:16],
                size_matched=size_matched,
                note="ignoring non-monotonic / NaN size_matched feed",
            )
            return

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
            else:
                log.warning(
                    "order_manager.apply_fill.untracked",
                    server_id=server_order_id[:16],
                    size_matched=size_matched,
                    fill_price=fill_price,
                )
            return
        # size_matched is monotonic — keep the max
        if size_matched > order.size_matched:
            prev_matched = order.size_matched
            order.size_matched = size_matched
            # Dashboard history: record the delta. This method is the single
            # mirror point for the WS-UPDATE and TierWatcher-resync feeds,
            # and the fill_id is keyed on the cumulative size, so a duplicate
            # observation of the same fill is dropped by the INSERT OR IGNORE.
            self._persist_fill(
                order,
                delta_size=size_matched - prev_matched,
                fill_price=(
                    fill_price if fill_price is not None and fill_price > 0
                    else order.price
                ),
            )
            self._persist_status(order)
            # H-V6-2 fix: log every incremental fill so operators can see
            # GTD lifetime progress (previously this method only updated
            # `size_matched` silently; the only fill records were
            # `user_ws.maker_fill` in user_ws.py and `router.maker_fill` in
            # main.py, both of which use [:16] prefixes). Operators
            # debugging fill accounting need a per-event row keyed by the
            # full client_id so they can grep by tier.
            log.info(
                "order_manager.fill_record",
                client_id=order.client_order_id,
                side=order.side,
                tier=order.tier_name,
                token=order.token_id[:16],
                prev_matched=prev_matched,
                new_matched=size_matched,
                delta=size_matched - prev_matched,
                fill_price=fill_price,
                expected=order.size,
                fills_remaining=max(0.0, order.size - size_matched),
            )
