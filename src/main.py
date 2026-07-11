"""Polymarket BTC 15m Up/Down — GTD Martingale Bot.

Entry point. Coordinates:
  1. WindowManager — tracks 15-min window boundaries
  2. BinanceFeed   — BTC spot WS + ring buffer
  3. MarketWS      — CLOB market data WS (last_trade_price)
  4. MarketDiscovery — Gamma REST to find market metadata
  5. StrategyEngine — decision loop (emits typed Actions)
  6. OrderManager   — serialised order executor
  7. TierWatcher    — one per in-flight GTD, polls balance + cancels
  8. HeartbeatLoop  — independent keepalive

Usage:
  pip install -e .              # install dependencies
  cp .env.example .env          # fill in credentials
  python -m src.main            # start bot

All trading is live. DRY_RUN has been removed.

H-NEW-2026-07-11 refactor:
  - BUY/TP GTDs post at the **strategy price exactly** (no orderbook snap).
  - Trigger lines use `PRICE_PRE_MARGIN` to pre-arm GTDs early:
      BUY : mid <= strategy_price * (1 + margin)  → post at strategy_price
      SELL: mid >= strategy_price * (1 - margin)  → post at strategy_price
  - Each GTD is monitored by a TierWatcher (token-balance polling) that
    cancels the GTD once enough shares have been filled, when the GTD
    expires, or when the next tier's zone has been crossed.
  - The orderbook (WS bba + REST /book fallback) is no longer consulted
    on the BUY/SELL paths — only the midpoint is used for triggering.
"""
from __future__ import annotations

import asyncio
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from src.config import general, binance, log_config, strategy
from src.clob_l1_auth import create_or_derive_clob_api_creds, ClobL1Error
from src.execution.clob_client import CLOBClient
from src.execution.gtd_expiry_watcher import GtdExpiryWatcher
from src.execution.heartbeat import HeartbeatLoop
from src.execution.order_manager import OrderManager
from src.execution.tier_watcher import TierWatcher, TierOutcome, WatcherResult
from src.feeds.binance_spot import BinanceCache, BinanceFeed, BinanceSpotService, RingBuffer
from src.infra.logging import configure_logging, get_logger
from src.market.market_service import MarketService
from src.market.models import MarketInfo
from src.market.pm_client import PolymarketClient
from src.market.window_manager import WindowManager
from src.market.market_ws import MarketWS, PriceCache
from src.market.user_ws import UserWS
from src.persistence.db import DB, init_db
from src.strategy.engine import (
    Action,
    ActionOpenT1,
    ActionOpenTN,
    ActionTakeProfit,
    StrategyEngine,
)
from src.strategy.loop import DecisionLoop

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Window-open BTC baseline plumbing
# ---------------------------------------------------------------------------

def _on_window_baseline(asset: str, price: float, source: str) -> None:
    """WindowManager -> GateKeeper bridge.

    Receives the BTC spot captured at each window's open (K-line backtrack),
    and applies it as the gate baseline. Source tag identifies the call site
    so logs can attribute each baseline update.

    Sync callback (WindowManager.tick() is async but this fires from inside
    the synchronous portion after await fetch_at_window_open). Mutation is
    idempotent: set_binance_baseline overwrites the existing baseline every
    time it fires, so we add a small guard so we don't reset on every tick
    of the same window — only the first applied value per round sticks.
    """
    if asset != "BTC":
        return
    eng = STATE.engine
    if not eng:
        return
    # Idempotency: only apply when the engine's current round round_start_ts
    # differs from the last baseline we applied. Without this, every
    # window_discovery within the same 15-min window would reset the
    # baseline to the same K-line value and re-flush logs.
    if getattr(eng, "_last_applied_baseline_round_ts", None) == STATE.round_start_ts:
        return
    eng.set_binance_baseline(price)
    eng._last_applied_baseline_round_ts = STATE.round_start_ts
    log.info(
        "gate.baseline_applied",
        asset=asset,
        baseline=price,
        source=source,
        round_start_ts=STATE.round_start_ts,
        btc_now=STATE.binance_cache.last_price,
    )


# ---------------------------------------------------------------------------
# Shared mutable state
# ---------------------------------------------------------------------------

class AppState:
    """Bundles all shared state for the application."""

    def __init__(self) -> None:
        self.round_start_ts: int = 0
        self.binance_cache = BinanceCache()
        self.price_cache = PriceCache()
        self.ring = RingBuffer(retention_s=binance().BUFFER_RETENTION_S)
        self.db_conn = init_db()
        self.db = DB(self.db_conn)
        self.current_market = None   # MarketInfo | None
        self.engine: StrategyEngine | None = None
        self.binance_feed: BinanceFeed | None = None
        self.market_ws: MarketWS | None = None
        self.user_ws: UserWS | None = None
        self.order_manager: OrderManager | None = None
        self.heartbeat: HeartbeatLoop | None = None
        self.gtd_expiry: GtdExpiryWatcher | None = None
        self.decision_loop: DecisionLoop | None = None
        self.clob: CLOBClient | None = None  # reference for fill_callback wiring
        # H-NEW-2026-07-11: ActionRouter reference (set in __init__ of
        # ActionRouter). _on_new_round uses it to stop all TierWatchers
        # before the next round starts so stale watcher callbacks cannot
        # mutate the new round's position state.
        self.router = None
        self.pm_client: PolymarketClient | None = None
        self.market_svc: MarketService | None = None
        self.wm: WindowManager | None = None  # window lifecycle manager
        self.binance_spot: BinanceSpotService | None = None  # BTC spot via REST
        self._shutdown = asyncio.Event()
        # B-3 v2 fix: hold strong references to fire-and-forget background
        # tasks (currently the TP runner spawned by ActionRouter) so they
        # cannot be garbage-collected before they finish. Without this, a
        # CPython GC pass happening between `asyncio.create_task(...)` and
        # the task's first `await` can drop the task entirely — silently
        # aborting the TP sell path even though the GTD BUY is live on the
        # CLOB. Mirrors the same pattern used in `GtdExpiryWatcher._bg_tasks`.
        self._bg_tasks: set[asyncio.Task[None]] = set()


STATE = AppState()


# ---------------------------------------------------------------------------
# Action router — bridges StrategyEngine -> OrderManager + Position state
# ---------------------------------------------------------------------------


async def _maybe_realize_closed(token_id: str, eng, pos) -> None:
    """If the CLOB reports zero token balance for `token_id`, instruct
    `Position.realize_position_closed()` to drop the phantom inventory.

    H-NEW-2026-07-11 (audit v6-12 production revisit): the bot used to
    re-fire trigger_tp into a hot retry loop when the user's wallet had
    been drained by a manual UI-side SELL, because (a) the UserWS still
    delivered `size=0` fill events and (b) `_check_take_profit` had no
    backoff. This helper is the authoritative reconciliation path: it
    asks the CLOB "how many of this token do I actually hold?" and, if
    the answer is zero, resets the position state so the next STATUS
    line is honest and trigger_tp stops firing.

    Best-effort: a transient REST failure does NOT clear state — we
    only act on a definitive balance==0.
    """
    try:
        bal = await eng._state.clob.get_token_balance(token_id)
    except Exception as exc:
        log.debug(
            "router.realize_check.failed",
            token=token_id[:16], error=str(exc),
            note="REST balance check errored; keeping in-memory position as-is",
        )
        return

    # balance is in shares; epsilon-comparison with the in-memory qty.
    if bal <= 0.5 and pos.total_qty > 0.5:
        log.warning(
            "router.realize_check.drained",
            token=token_id[:16],
            clob_balance_shares=bal,
            in_memory_qty=pos.total_qty,
            note="CLOB balance is 0 but engine still thinks we hold tokens; "
                 "calling realize_position_closed()",
        )
        pos.realize_position_closed(
            source="router.realize_check",
            note=(
                "REST get_token_balance returned 0 during TP-rejection "
                "reconciliation; user-side UI sell (or upstream bot crash "
                "consuming the inventory) is the most likely cause"
            ),
        )
    elif bal + 0.5 < pos.total_qty:
        # Partial drain — log it but DON'T auto-clear, since we may still
        # be able to sell the residual via TP. The bot will retry at the
        # next backoff elapse.
        log.warning(
            "router.realize_check.partial_drain",
            token=token_id[:16],
            clob_balance_shares=bal,
            in_memory_qty=pos.total_qty,
            note="CLOB balance < in-memory qty but > 0; leaving position "
                 "intact, backoff will retry sell",
        )


class ActionRouter:
    """Converts StrategyEngine Actions into OrderManager calls and wires fill events.

    H-NEW-2026-07-11 refactor:
      - BUY actions post the GTD at the engine's strategy price (no orderbook
        snap). OrderManager.place_buy_by_strategy_price handles this.
      - TP actions post a single GTD SELL at the engine's tp_target. No more
        FAK fallback — the TierWatcher cancels the residual if mid never
        reaches tp_target.
      - For every GTD placed, a TierWatcher is spawned. The watcher polls
        the CLOB token balance and decides when to cancel the GTD (filled,
        expired, or crossed-into-next-tier).
    """

    def __init__(self, state: AppState) -> None:
        self._state = state
        # Strong refs to the spawned TierWatchers per (round, tier). Lets us
        # cancel them on round-end without losing the references.
        self._watchers: dict[tuple[int, int], TierWatcher] = {}
        self._tp_watchers: dict[int, TierWatcher] = {}
        # Convenience handle so other modules (e.g. _on_new_round cleanup)
        # can call stop_all_watchers() without going through the engine.
        state.router = self

    # ------------------------------------------------------------------
    # TierWatcher lifecycle helpers
    # ------------------------------------------------------------------

    def _spawn_tier_watcher(
        self,
        round_start_ts: int,
        tier: int,
        server_order_id: str,
        token_id: str,
        strategy_price: float,
        size_expected: float,
        expires_at: float,
        cross_threshold: float | None = None,
    ) -> TierWatcher:
        """Spawn a TierWatcher for a BUY tier's GTD and remember it.

        `cross_threshold` = the next tier's pre-arm fire line. When the
        watched token's mid drops at or below this line, the watcher
        cancels the GTD early so the next tier can fire cleanly.

        H-NEW-2026-07-11 (B-NEW-4): guard against accidental double-spawn
        by short-circuiting if a watcher for the same (round, tier) is
        already running. Otherwise (e.g. an `_evaluate` tick fires while
        the previous spawn's place_buy await is still in flight) we would
        post a second GTD on the book and double-spend the tier's budget.
        """
        existing = self._watchers.get((round_start_ts, tier))
        if existing is not None and not existing.result:
            # Watcher is still running (or pending). Refuse the duplicate.
            log.warning(
                "router.duplicate_tier_watcher_rejected",
                round=round_start_ts,
                tier=tier,
                server_id=server_order_id[:16],
                active_server_id=(existing._server_order_id or "")[:16],
            )
            return existing

        cfg = strategy()
        eng = self._state.engine

        async def on_done(result: WatcherResult) -> None:
            eng = self._state.engine
            if not eng or not eng._round:
                return
            tier_snap = eng._round.position.tiers[tier - 1]
            # H-NEW-2026-07-11: forward dedup_key + real avg_fill_price so
            # apply_tier_watcher_result can dedup terminal states across
            # multiple fan-in paths and price its missed-fill fix-up
            # against the CLOB-reported average (not the strategy price).
            eng._round.position.apply_tier_watcher_result(
                tier_snap,
                result.outcome.value,
                result.filled_qty,
                dedup_key=result.dedup_key,
                avg_fill_price=result.avg_fill_price,
            )

        async def mid_provider() -> float:
            return self._state.price_cache.get_midpoint(token_id)

        def is_active() -> bool:
            eng = self._state.engine
            return bool(eng and eng._round and eng._round.round_start_ts == round_start_ts)

        watcher = TierWatcher(
            clob=self._state.clob,
            server_order_id=server_order_id,
            token_id=token_id,
            side="BUY",
            strategy_price=strategy_price,
            expected_size=size_expected,
            expires_at=expires_at,
            poll_s=cfg.TIER_WATCHER_POLL_S,
            fill_ratio=cfg.TIER_WATCHER_FILL_RATIO,
            cross_threshold=cross_threshold,
            mid_price_provider=mid_provider,
            on_complete=on_done,
            is_still_active=is_active,
        )
        task = watcher.start()
        # Keep a strong ref so the task cannot be GC'd before its first await.
        self._state._bg_tasks.add(task)
        task.add_done_callback(self._state._bg_tasks.discard)
        self._watchers[(round_start_ts, tier)] = watcher
        # Mark the tier as "being watched" so the engine's mutex holds.
        if eng and eng._round:
            tier_snap = eng._round.position.tiers[tier - 1]
            tier_snap.watching = True
        return watcher

    def _spawn_tp_watcher(
        self,
        round_start_ts: int,
        server_order_id: str,
        token_id: str,
        tp_price: float,
        size: float,
        expires_at: float,
    ) -> TierWatcher:
        """Spawn a TierWatcher for a TP SELL GTD."""
        cfg = strategy()
        eng = self._state.engine

        async def on_done(result: WatcherResult) -> None:
            eng = self._state.engine
            if not eng or not eng._round:
                return
            pos = eng._round.position
            # Treat a watcher complete as a TP fill (full or partial).
            filled_qty = result.filled_qty
            if result.outcome in (
                TierOutcome.FILLED,
                TierOutcome.OVER_FILLED,
                TierOutcome.EXPIRED,
                TierOutcome.CROSSED,
                TierOutcome.CANCELLED,
            ) and filled_qty > 0:
                # record_tp_fill reduces _total_qty / _total_cost and
                # clears _tp_inflight.
                pos.record_tp_fill(result.server_order_id, result.strategy_price, filled_qty)
            else:
                # No fill (zero qty) → just clear the mutex so the next
                # tick can re-arm TP.
                pos._tp_inflight = False

        async def mid_provider() -> float:
            return self._state.price_cache.get_midpoint(token_id)

        def is_active() -> bool:
            eng = self._state.engine
            return bool(eng and eng._round and eng._round.round_start_ts == round_start_ts)

        watcher = TierWatcher(
            clob=self._state.clob,
            server_order_id=server_order_id,
            token_id=token_id,
            side="SELL",
            strategy_price=tp_price,
            expected_size=size,
            expires_at=expires_at,
            poll_s=cfg.TIER_WATCHER_POLL_S,
            fill_ratio=cfg.TIER_WATCHER_FILL_RATIO,
            cross_threshold=None,  # TP has no "next tier" — exits cleanly on fill
            mid_price_provider=mid_provider,
            on_complete=on_done,
            is_still_active=is_active,
        )
        task = watcher.start()
        self._state._bg_tasks.add(task)
        task.add_done_callback(self._state._bg_tasks.discard)
        self._tp_watchers[round_start_ts] = watcher
        return watcher

    async def stop_all_watchers(self) -> None:
        """Cancel every active watcher (called on shutdown / round-end)."""
        for w in list(self._watchers.values()):
            await w.stop()
        self._watchers.clear()
        for w in list(self._tp_watchers.values()):
            await w.stop()
        self._tp_watchers.clear()

    # ------------------------------------------------------------------
    # Action dispatch
    # ------------------------------------------------------------------

    async def __call__(self, action: Action) -> None:
        eng = self._state.engine
        om = self._state.order_manager
        if not eng or not om:
            return

        tick_size = eng._round.tick_size if eng._round else 0.01
        round_ts = self._state.round_start_ts

        if isinstance(action, ActionOpenT1):
            # H-NEW-2026-07-11: post at strategy_price exactly (the price the
            # engine decided to enter at). No orderbook fetch, no half-tick
            # snap. Size = notional / strategy_price.
            order = await om.place_buy_by_strategy_price(
                token_id=action.token,
                strategy_price=action.price,
                notional=action.notional,
                round_start_ts=round_ts,
                tier_name="T1",
                tick_size=tick_size,
            )
            if order:
                expires = order.expires_at
                eng.bind_server_order_id(
                    tier=1,
                    server_order_id=order.server_order_id or "",
                    token_id=action.token,
                    expires_at=expires,
                    expected_price=order.price,
                    placed_size=order.placed_size if order.placed_size > 0 else None,
                )
                self._state.clob.fill_callback = self._make_fill_callback()
                size_expected = order.placed_size if order.placed_size > 0 else (action.notional / action.price)
                # T2 fire line: trigger_price(T2) * (1 + margin). We don't
                # know the realised baseline yet, so use the design
                # baseline (= action.price itself for T1; the engine sets
                # baseline_target_price to action.price via bind).
                # The watcher will compare against the live midpoint; if
                # the next-tier zone hasn't dropped yet, it just keeps
                # waiting — no harm.
                margin = float(strategy().PRICE_PRE_MARGIN or 0.0)
                t2_design_price = action.price * (1.0 - strategy().TIER2_DROP_PCT)
                t2_fire = t2_design_price * (1.0 + margin)
                self._spawn_tier_watcher(
                    round_start_ts=round_ts,
                    tier=1,
                    server_order_id=order.server_order_id or "",
                    token_id=action.token,
                    strategy_price=order.price,
                    size_expected=size_expected,
                    expires_at=expires,
                    cross_threshold=t2_fire,
                )
                log.info(
                    "router.t1_placed",
                    side=action.side,
                    token=action.token[:16],
                    strategy_price=action.price,
                    size=order.size,
                    server_id=order.server_order_id or "none",
                    watcher=True,
                )
            else:
                log.error(
                    "router.t1_failed",
                    side=action.side,
                    token=action.token[:16],
                    strategy_price=action.price,
                    notional=action.notional,
                    reason="om.place_buy_by_strategy_price returned None",
                )
                eng._round.position.entry_side = None
                eng._round.position.entry_token = None

        elif isinstance(action, ActionOpenTN):
            order = await om.place_buy_by_strategy_price(
                token_id=action.token,
                strategy_price=action.price,
                notional=action.notional,
                round_start_ts=round_ts,
                tier_name=f"T{action.tier}",
                tick_size=tick_size,
            )
            if order:
                expires = order.expires_at
                eng.bind_server_order_id(
                    tier=action.tier,
                    server_order_id=order.server_order_id or "",
                    token_id=action.token,
                    expires_at=expires,
                    expected_price=order.price,
                    placed_size=order.placed_size if order.placed_size > 0 else None,
                )
                self._state.clob.fill_callback = self._make_fill_callback()
                size_expected = order.placed_size if order.placed_size > 0 else (action.notional / action.price)
                # Next tier's fire line — only meaningful for T2/T3 (T4 is last).
                cross_thr: float | None = None
                if action.tier < 4:
                    cfg = strategy()
                    margin = float(cfg.PRICE_PRE_MARGIN or 0.0)
                    drops = cfg.tier_drop_pcts
                    if action.tier < len(drops) and drops[action.tier] is not None:
                        next_design = order.price * (1.0 - drops[action.tier])
                        cross_thr = next_design * (1.0 + margin)
                self._spawn_tier_watcher(
                    round_start_ts=round_ts,
                    tier=action.tier,
                    server_order_id=order.server_order_id or "",
                    token_id=action.token,
                    strategy_price=order.price,
                    size_expected=size_expected,
                    expires_at=expires,
                    cross_threshold=cross_thr,
                )
                log.info(
                    "router.tN_placed",
                    tier=action.tier,
                    token=action.token[:16],
                    strategy_price=action.price,
                    size=order.size,
                    server_id=order.server_order_id or "none",
                    watcher=True,
                )
            else:
                log.error(
                    "router.tN_failed",
                    tier=action.tier,
                    token=action.token[:16],
                    strategy_price=action.price,
                    notional=action.notional,
                    reason="om.place_buy_by_strategy_price returned None",
                )
                tier_snap = getattr(eng._round.position, f"tier{action.tier}", None)
                if tier_snap is not None:
                    tier_snap.placed = False
                    tier_snap.watching = False
                    tier_snap.server_order_id = None
                    tier_snap.expires_at = 0.0

        elif isinstance(action, ActionTakeProfit):
            # H-NEW-2026-07-11: a single GTD SELL at tp_target, monitored
            # by a TierWatcher. No FAK fallback — when the GTD expires
            # un-filled, the watcher reports EXPIRED and we clear the
            # mutex so the next tick can re-arm if mid is still above
            # the pre-arm fire line.
            #
            # H-NEW-2026-07-11 (audit v6-12 production revisit): on
            # POST-failure we MUST tell the engine to backoff, otherwise
            # the next 0.25s `_evaluate` tick re-fires trigger_tp and we
            # re-POST the same unfulfillable order. Production saw 11
            # retries in <3s. We also REST-check the token balance: if
            # the on-chain inventory is actually 0 (e.g. user sold via
            # UI while we still believed in 5 tokens), we tell the
            # Position to forget the phantom inventory.
            async def _tp_runner() -> None:
                try:
                    if not eng._round:
                        return
                    pos = eng._round.position
                    order = await om.place_gtd_sell_at_price(
                        token_id=action.token,
                        size=action.qty,
                        price=action.price,
                        round_start_ts=round_ts,
                        tier_name="TP",
                        tick_size=tick_size,
                    )
                    if not order:
                        pos._tp_inflight = False
                        log.warning(
                            "router.tp_failed",
                            token=action.token[:16],
                            strategy_price=action.price,
                            qty=action.qty,
                            reason="om.place_gtd_sell_at_price returned None",
                        )
                        # Engage engine backoff + reconcile inventory.
                        if hasattr(eng, "notify_tp_post_error"):
                            try:
                                eng.notify_tp_post_error(
                                    reason="place_gtd_sell_at_price returned None"
                                )
                            except Exception:
                                pass
                        if hasattr(eng, "_state"):
                            try:
                                await _maybe_realize_closed(
                                    action.token, eng, pos
                                )
                            except Exception:
                                pass
                        return
                    eng.notify_tp_recovered()
                    pos.register_tp(
                        server_order_id=order.server_order_id or "",
                        token_id=action.token,
                        target_qty=action.qty,
                    )
                    self._spawn_tp_watcher(
                        round_start_ts=round_ts,
                        server_order_id=order.server_order_id or "",
                        token_id=action.token,
                        tp_price=order.price,
                        size=action.qty,
                        expires_at=order.expires_at,
                    )
                    log.info(
                        "router.tp_placed",
                        token=action.token[:16],
                        strategy_price=action.price,
                        qty=action.qty,
                        server_id=order.server_order_id or "none",
                        watcher=True,
                    )
                except Exception as exc:
                    log.exception("router.tp_runner_crashed", error=str(exc))
                    # Defensive release of _tp_inflight. Even if
                    # notify_tp_post_error / realize check paths below
                    # fail (e.g. test stubs that don't implement
                    # notify_tp_post_error), the mutex MUST come down so
                    # subsequent ticks don't re-fire trigger_tp on a
                    # permanently-failed attempt.
                    if eng._round:
                        eng._round.position._tp_inflight = False
                    # H-NEW-2026-07-11 (audit v6-12 production revisit):
                    # engage the backoff gate so trigger_tp is suppressed
                    # for `_tp_backoff_s` seconds (default 60s). Skip
                    # silently on stubs (real production engines always
                    # have this method).
                    if hasattr(eng, "notify_tp_post_error"):
                        try:
                            eng.notify_tp_post_error(
                                reason=f"tp_runner exception: {exc!r}"
                            )
                        except Exception:
                            log.debug(
                                "router.tp_runner.notify_failed",
                                note="notify_tp_post_error raised; "
                                     "continuing with mutex already cleared",
                            )
                    if eng._round and hasattr(eng, "_state"):
                        try:
                            await _maybe_realize_closed(
                                action.token, eng, eng._round.position
                            )
                        except Exception:
                            log.debug(
                                "router.tp_runner.realize_failed",
                                note="_maybe_realize_closed raised; "
                                     "in-memory state may be stale until "
                                     "next reconcile",
                            )

            task = asyncio.create_task(_tp_runner())
            self._state._bg_tasks.add(task)
            task.add_done_callback(self._state._bg_tasks.discard)

    def _make_fill_callback(self):
        """Create a fill callback that routes to engine (used by CLOBClient polling).

        CLOBClient now uses a FillDeduper to translate cumulative observations
        into per-event deltas. The callback receives `(token_id, order_id,
        fill_price, fill_delta_size, fill_total_size)`:

          * `fill_delta_size`  — strict increment since the last fire (B-3 fix;
            the engine must treat this as a delta, not a cumulative).
          * `fill_total_size`  — cumulative matched size; used by OrderManager
            to keep its tracked-order state in monotonic lock-step with the
            server. Always > 0 once deduper has accepted the observation.

        The engine end (on_order_filled -> record_fill) already does
        `size_filled += size`, which under delta semantics gives an exact sum,
        regardless of how many times WS / polling / FAK re-observe the order.
        """
        eng = self._state.engine
        om = self._state.order_manager

        def on_fill(
            token_id: str,
            order_id: str,
            fill_price: float,
            fill_delta_size: float,
            fill_total_size: float,
        ):
            if not eng or not om:
                return
            if fill_delta_size <= 0 or fill_total_size <= 0:
                # Defensive: deduper should already have filtered these out.
                return

            # 1) Promote tracked-order status to "filled" (no-op if partial).
            om.mark_filled(order_id)

            # 2) Feed the engine position state with the DELTA. record_fill
            #    does `size_filled += size` which under delta semantics is
            #    exactly correct — no double counting (B-3). We also pass
            #    cumulative to the OrderManager for its own bookkeeping.
            om.apply_fill_event(
                order_id,
                size_matched=fill_total_size,
                fill_price=fill_price,
            )
            eng.on_order_filled(
                token_id, order_id, fill_price, fill_delta_size
            )

            log.info(
                "router.fill",
                token=token_id[:16],
                order_id=order_id[:16],
                fill_price=fill_price,
                fill_delta_size=fill_delta_size,
                fill_total_size=fill_total_size,
            )
        return on_fill


# ---------------------------------------------------------------------------
# Round lifecycle
# ---------------------------------------------------------------------------

async def _on_binance_connect() -> None:
    log.debug("binance_feed.connected")


def _on_market_discovered(asset: str, mkt) -> None:
    """Called by WindowManager as soon as market info (token IDs) is resolved."""
    state = STATE

    # Detect if this is a window transition (condition_id changed)
    prev_up = getattr(state.current_market, "token_up_id", None) if state.current_market else None
    prev_cid = getattr(state.current_market, "condition_id", "") if state.current_market else ""
    is_transition = prev_cid and prev_cid != mkt.condition_id

    # Clear old token prices from cache to avoid stale data
    if state.current_market and state.price_cache:
        old_up = getattr(state.current_market, "token_up_id", None)
        old_down = getattr(state.current_market, "token_down_id", None)
        for old_t in [old_up, old_down]:
            if old_t and old_t != mkt.token_up_id and old_t != mkt.token_down_id:
                state.price_cache.clear_token(old_t)

    if state.market_ws:
        state.market_ws.subscribe_tokens([mkt.token_up_id, mkt.token_down_id])
    state.current_market = mkt

    if is_transition:
        log.info(
            "market.transition",
            asset=asset,
            slug=mkt.slug or "",
            cid=mkt.condition_id[:16],
            up=mkt.token_up_id[:16],
            down=mkt.token_down_id[:16],
            round_start_ts=mkt.round_start_ts,
            window_start=mkt.window_start.isoformat(),
            window_end=mkt.window_end.isoformat(),
            prev_cid=prev_cid[:16],
            prev_up=(prev_up[:16] if prev_up else "none"),
        )
    else:
        log.info(
            "market.discovered",
            asset=asset,
            slug=mkt.slug or "",
            cid=mkt.condition_id[:16],
            up=mkt.token_up_id[:16],
            down=mkt.token_down_id[:16],
        )


async def _on_new_round(round_start_ts: int) -> None:
    """
    Called by WindowManager on every 15-min boundary.

    Handles end-of-round cleanup. WindowManager's tick() also calls
    fetch_at_window_open() which triggers `_on_window_baseline` to seed
    the GateKeeper — so we no longer wait for a baseline here. Returning
    promptly is important: this callback is awaited from WM's boundary
    poller, and the previous 5-min `compute_baseline` wait loop blocked
    the event loop for up to 5 minutes on every round transition.
    """
    state = STATE

        # End previous round
    if state.round_start_ts > 0:
        prev = state.round_start_ts
        # H-NEW-2026-07-11: stop any active TierWatchers so they don't
        # fire after the round has rolled over. Watchers that already
        # finished are no-ops.
        router = getattr(state, "router", None)
        if router is not None:
            try:
                await router.stop_all_watchers()
            except Exception as exc:
                log.warning("router.stop_all_watchers.error", error=str(exc))
        if state.engine:
            await state.engine.on_round_end()
        if state.order_manager:
            await state.order_manager.cancel_market(prev)
            # M-V4-1 fix: drop the previous round's TrackedOrders /
            # server_id mappings / pending fills so the maps do not grow
            # unbounded across rounds.
            purged = await state.order_manager.purge_round(prev)
            if purged:
                log.info(
                    "round.purged_prev_state",
                    prev_round_start=prev, purged=purged,
                )
        log.info("round.transition", prev_round_start=prev, new_round_start=round_start_ts)

    state.round_start_ts = round_start_ts

    # ── Use WindowManager for market discovery ─────────────────────────────────
    # Note: market subscription (token IDs) is handled in _on_market_discovered.
    # Here we just check that market is available.
    if state.wm:
        ctx = state.wm.get("BTC")
        if not (ctx and ctx.market):
            log.warning("round.market_not_found", round_start_ts=round_start_ts)
    else:
        state.current_market = None
        log.warning("round.market_not_found.wm_unavailable", round_start_ts=round_start_ts)

    # Notify engine of new round. Baseline is now seeded by
    # `_on_window_baseline` (called from WM.tick after fetch_at_window_open)
    # — see main.py / window_manager.py.
    if state.engine:
        await state.engine.on_new_round(round_start_ts, state.current_market)

    # Persist round start
    mkt = state.current_market
    state.db.upsert_round(
        round_start_ts=round_start_ts,
        slug=mkt.slug if mkt else None,
        condition_id=mkt.condition_id if mkt else None,
        up_token=mkt.up_token if mkt else None,
        down_token=mkt.down_token if mkt else None,
        tick_size=mkt.tick_size if mkt else 0.01,
    )



# ---------------------------------------------------------------------------
# REST midpoint poller (stable prices for strategy decisions)
# ---------------------------------------------------------------------------

async def _midpoint_poll_loop() -> None:
    """Poll /midpoint REST API every 2s to keep stable price cache updated."""
    last_condition_id = ""
    last_up_token = ""
    last_down_token = ""
    while True:
        await asyncio.sleep(2.0)
        s = STATE
        if not s.pm_client:
            continue

        wm = s.wm
        if not wm:
            continue

        ctx = wm.get("BTC")
        mkt = ctx.market if ctx else None
        if not mkt or not mkt.token_up_id or not mkt.token_down_id:
            continue

        condition_id = mkt.condition_id or ""
        up_token = mkt.token_up_id
        down_token = mkt.token_down_id

        # Log window transition when condition_id changes
        if condition_id != last_condition_id and last_condition_id:
            log.info(
                "midpoint.window_changed",
                prev_condition_id=last_condition_id[:16],
                new_condition_id=condition_id[:16],
                up_token=up_token[:16],
                down_token=down_token[:16],
                round_start_ts=mkt.round_start_ts,
            )
        last_condition_id = condition_id
        last_up_token = up_token
        last_down_token = down_token

        try:
            up_mid = await s.pm_client.get_midpoint(up_token)
            down_mid = await s.pm_client.get_midpoint(down_token)
            s.price_cache.midpoint[up_token] = up_mid
            s.price_cache.midpoint[down_token] = down_mid

            # B-1 fix (single-driver): the strategy decision loop is now
            # driven solely by DecisionLoop on a 250ms tick. The midpoint
            # poll here only refreshes the price cache; it no longer
            # re-enters _evaluate. Previously both this loop and
            # DecisionLoop would invoke _evaluate, which combined with the
            # engine's _eval_inflight drop-on-re-entry guard produced
            # redundant work and confusing log timing.
        except Exception as exc:
            log.warning("midpoint.poll error", condition_id=condition_id[:16], up_token=up_token[:16], down_token=down_token[:16], error=str(exc))


# ---------------------------------------------------------------------------
# Status printer
# ---------------------------------------------------------------------------

async def _status_loop() -> None:
    cfg = log_config()
    last_status_ts = 0.0
    while True:
        await asyncio.sleep(cfg.HEARTBEAT_S)
        s = STATE

        # Throttle status log to every 5s
        now = time.time()
        if now - last_status_ts < 5.0:
            continue
        last_status_ts = now

        bc = s.binance_cache.last_price
        gate_str = ""
        if s.engine and s.engine._gate and bc > 0:
            gs = s.engine._gate.state(bc)
            flag = "PASS" if gs.within_limit else "FAIL"
            # H-V6-1 fix: print baseline as well as drift so the log line
            # is self-contained — operators can sanity-check the gate
            # without cross-referencing earlier events. The previous
            # format only printed drift, hiding the case where the
            # baseline was never set (gate_str="").
            gate_str = (
                f"baseline={gs.binance_baseline:,.1f} drift={gs.drift:.4f}[{flag}] |"
            )
        elif s.engine and s.engine._round:
            # H-V6-1 fix: explicitly surface the case where baseline is
            # still missing so the operator can distinguish "gate PASS"
            # from "baseline not seeded yet" — both produce an empty
            # gate_str in the legacy format.
            gate_str = "baseline=pending |"

        wm = s.wm
        mkt = wm.get("BTC").market if wm and wm.get("BTC") else None
        up_token = mkt.up_token if mkt else ""
        down_token = mkt.down_token if mkt else ""
        up_last = s.price_cache.get_midpoint(up_token) if up_token else 0
        down_last = s.price_cache.get_midpoint(down_token) if down_token else 0

        next_in = wm.seconds_until_next() if wm else 0
        period_s = wm.window_period_s() if wm else 900
        # H-V6-15: STATUS no longer shows absolute window_end timestamp —
        # operators found it useless (had to mentally diff against current
        # time) and the rich status (e.g. `window_end=08:15:00(272s)`)
        # crowded out the more useful "are we still inside the T1 entry
        # window?" signal. Replace with:
        #   - `remain=Xs/Ys`  — X = seconds left in current window,
        #                        Y = total window length (e.g. 900 for BTC 15m)
        #   - `window=[IN_T1|PAST_T1]` — IN_T1 means T1 entry is still
        #                                allowed (elapsed < T1_ENTRY_WINDOW_S);
        #                                PAST_T1 means we've crossed the
        #                                time gate and no fresh T1 will fire
        #                                this window.
        # Clamp `next_in` to [0, period_s] because the 5s STATUS interval
        # can sample right at the rollover where the boundary calc briefly
        # returns a tiny negative number.
        remain_s = max(0, min(period_s, next_in))
        elapsed_s = max(0, period_s - remain_s)
        t1_entry_window_s = strategy().T1_ENTRY_WINDOW_S
        # `ENTER_OK` is the canonical IN_T1 label — it is the same predicate
        # the engine uses inside `_check_t1_entry` to gate entry, so STATUS
        # and engine are guaranteed to agree.
        t1_status = "IN_T1" if elapsed_s <= t1_entry_window_s else "PAST_T1"
        # When the window is within 1s of ending, STATUS flips to `END` so
        # operators immediately see "this round is over" without having to
        # read the seconds count.
        if remain_s < 1.0:
            t1_status = "END"
        window_str = f"window=[{t1_status}] remain={remain_s:.0f}s/{period_s:.0f}s"

        # Position state
        pos_str = ""
        if s.engine and s.engine._round and s.engine._round.position:
            pos = s.engine._round.position
            tiers_str = ",".join(
                f"T{t.tier}={'✓' if t.is_complete else ('⏳' if t.placed else '○')}"
                for t in pos.tiers
            )
            # H-V6-12 (operator request): STATUS should show live position
            # cost basis + unrealized PnL%, so operators can see fill impact
            # at a glance instead of cross-referencing position.fill_recorded
            # events. Up / Down midpoint is already on the line as up_last /
            # down_last; pick the one matching position.entry_side.
            held_mid = 0.0
            if pos.entry_side == "UP":
                held_mid = up_last
            elif pos.entry_side == "DOWN":
                held_mid = down_last
            has_pos = pos.avg_cost > 0 and pos.total_qty > 0 and held_mid > 0
            if has_pos:
                unreal_pnl = (held_mid - pos.avg_cost) * pos.total_qty
                unreal_pnl_pct = (held_mid - pos.avg_cost) / pos.avg_cost
                pnl_pct_str = f"({unreal_pnl_pct * 100:+.1f}%)"
                sign = "+" if unreal_pnl > 0 else ("-" if unreal_pnl < 0 else "")
                upnl_str = f"${sign}{abs(unreal_pnl):.2f}{pnl_pct_str}"
            else:
                upnl_str = "$0.00"
            # H-NEW-2026-07-11: also show the live take-profit target so
            # operators see "how far away from TP" without cross-referencing
            # position.fill_recorded events. Returns None when avg_cost<=0.
            tp_target = pos.tp_target()
            tp_str = f" tpTarget={tp_target:.4f}" if tp_target is not None else ""
            side_str = pos.entry_side or "-"
            pos_str = (
                f" | side={side_str} tiers=[{tiers_str}]"
                f" qty={pos.total_qty:.2f}"
                f" avg={pos.avg_cost:.4f}"
                f" cost=${pos.total_cost:.2f}"
                f" mid={held_mid:.4f}"
                f" uPnL={upnl_str}"
                f"{tp_str}"
            )

        log.info(
            f"STATUS  binance={bc:,.1f}  {gate_str}"
            f"  {window_str}  "
            f"UP={up_last:.4f}  DOWN={down_last:.4f}{pos_str}"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    configure_logging()
    s = STATE
    log.info("bot.starting", version="0.1.0")

    cfg_strategy = strategy()

    # ── CLOB credentials ─────────────────────────────────────────────────
    # Priority: env L2 creds > L1 private key derivation
    api_key = cfg_strategy.API_KEY
    api_secret = cfg_strategy.API_SECRET
    api_passphrase = cfg_strategy.API_PASSPHRASE

    if not (api_key and api_secret and api_passphrase):
        if cfg_strategy.WALLET_PRIVATE_KEY:
            try:
                creds = create_or_derive_clob_api_creds(
                    host=cfg_strategy.POLYMARKET_HOST,
                    private_key=cfg_strategy.WALLET_PRIVATE_KEY,
                    chain_id=137,
                )
                api_key = creds.api_key
                api_secret = creds.api_secret
                api_passphrase = creds.api_passphrase
                log.info(
                    "clob_l1_auth.derived",
                    api_key=api_key[:8],
                )
            except ClobL1Error as exc:
                log.error(
                    "clob_l1_auth.failed — trading disabled",
                    error=str(exc),
                )
        else:
            log.warning(
                "CLOB credentials missing: set API_KEY/API_SECRET/API_PASSPHRASE "
                "or WALLET_PRIVATE_KEY in .env"
            )

    # ── CLOB client ──────────────────────────────────────────────────────
    s.clob = CLOBClient(
        host=cfg_strategy.POLYMARKET_HOST,
        private_key=cfg_strategy.WALLET_PRIVATE_KEY,
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
        signature_type=cfg_strategy.SIGNATURE_TYPE,
        funder_address=cfg_strategy.FUNDER_ADDRESS,
    )

    # ── Market / Window management ────────────────────────────────────────────
    # PolymarketClient: Gamma + CLOB public API (market discovery, prices, resolution)
    s.pm_client = PolymarketClient(
        gamma_api="https://gamma-api.polymarket.com",
        polymarket_host=cfg_strategy.POLYMARKET_HOST,
    )
    s.market_svc = MarketService(
        pm=s.pm_client,
        threshold=0.5,
        slug_interval="15m",
        window_sec=900,
    )
    # BinanceSpotService: REST-based BTC spot (K-line backtrack for window-open price)
    s.binance_spot = BinanceSpotService()
    # WindowManager: unified window lifecycle (discovery + price sync + BTC drift guard)
    # WindowManager internally polls 15-min boundaries and fires _on_new_round callback.
    s.wm = WindowManager(
        market_svc=s.market_svc,
        symbols=["BTC"],
        on_new_round=_on_new_round,
        on_market_discovered=_on_market_discovered,
        binance_spot=s.binance_spot,
        entry_cfg=None,
        # H-V6-1 fix: forward the K-line backfilled BTC spot at window_open
        # to the strategy engine's GateKeeper. Replaces the previous lazy
        # baseline seed (latest WS trade at the moment WM polled the
        # boundary) which sat 1-15s off from the true window_open price and
        # made the gate FAIL right after every round transition.
        on_window_baseline=_on_window_baseline,
    )

    # ── Execution layer ─────────────────────────────────────────────────
    s.order_manager = OrderManager(s.clob, price_cache=s.price_cache, pm_client=s.pm_client)
    # H-NEW-2026-07-11 (audit v6-12 production revisit): if a previous
    # bot process crashed (or `_safe_cancel` lost its race against SDK
    # signature bugs), dangling GTDs on the CLOB still occupy the maker
    # allowance and block any subsequent SELL with `not enough
    # balance/allowance`. Force-cancel them BEFORE the first round
    # starts so an old stuck `0x3ede40a1f6c744`-style order cannot
    # wedge the account into "I still hold 5 tokens but cannot sell".
    try:
        await s.order_manager.startup_reconcile()
    except Exception as exc:
        log.warning(
            "main.startup_reconcile.failed",
            error=str(exc),
            note="continuing — partial reconciliation may leave a stale order on CLOB",
        )
    # H-NEW-1 fix: tell CLOBClient how to pre-register a server_order_id
    # the moment the post-response arrives. This closes the race where a
    # fill callback fires from inside place_gtd_buy / place_gtd_sell /
    # place_fak_sell BEFORE the awaited call returns to place_buy / etc.,
    # by which time the real TrackedOrder is not yet in OrderManager._orders.
    # See OrderManager.register_pending_server_id docstring.
    s.clob.register_pending = s.order_manager.register_pending_server_id
    s.heartbeat = HeartbeatLoop(s.clob)

    # ── GTD expiry watcher ──────────────────────────────────────────────
    s.gtd_expiry = GtdExpiryWatcher(
        order_manager=s.order_manager,
        round_state_provider=lambda: s.engine._round if s.engine else None,
        poll_interval_s=2.0,
    )

    # ── Action router + Strategy engine ──────────────────────────────────
    router = ActionRouter(s)
    s.engine = StrategyEngine(s.binance_cache, s.price_cache, router)

    # ── Decision loop — drives StrategyEngine._evaluate() on a fixed tick ──
    # Fixes audit B-1: before this loop existed, nothing invoked _evaluate,
    # so the entire decision core was dead code.
    s.decision_loop = DecisionLoop(s.engine, tick_interval_s=0.25)

    # ── Feeds ───────────────────────────────────────────────────────────
    s.binance_feed = BinanceFeed(
        cache=s.binance_cache,
        ring=s.ring,
        on_connect=_on_binance_connect,
    )
    s.market_ws = MarketWS(
        cache=s.price_cache,
        on_connect=None,
        on_disconnect=None,
    )

    # ── User WebSocket (order events) ────────────────────────────────────
    if cfg_strategy.API_KEY and cfg_strategy.API_SECRET:
        s.user_ws = UserWS(
            api_key=cfg_strategy.API_KEY,
            api_secret=cfg_strategy.API_SECRET,
            api_passphrase=cfg_strategy.API_PASSPHRASE,
            on_maker_fill=_make_maker_fill_callback(s),
            on_order=_make_user_ws_order_callback(s),
            clob=s.clob,  # H-V6-12: REST fallback when WS size=0
        )
    else:
        log.warning(
            "bot.no_credentials",
            msg="API_KEY not set — UserWS disabled, fill events will not be received"
        )
        s.user_ws = None

    # ── Start services ──────────────────────────────────────────────────
    await s.binance_feed.start()
    await s.market_ws.start()
    await s.wm.start()
    s.gtd_expiry.start()
    if s.decision_loop:
        s.decision_loop.start()

    if s.user_ws:
        await s.user_ws.start()

    if s.clob.is_configured():
        await s.heartbeat.start()
    else:
        log.error(
            "bot.no_clob_credentials",
            msg="CLOB not configured — cannot start heartbeat. "
                "Set API_KEY / API_SECRET / API_PASSPHRASE in .env",
        )

    # Status printer + REST midpoint poller
    status_task = asyncio.create_task(_status_loop())
    midpoint_task = asyncio.create_task(_midpoint_poll_loop())

    # ── Graceful shutdown ───────────────────────────────────────────────
    def _signal_handler() -> None:
        log.info("bot.shutdown_signal_received")
        s._shutdown.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except (NotImplementedError, OSError):
            pass

    await s._shutdown.wait()

    # ── Cleanup ─────────────────────────────────────────────────────────
    log.info("bot.shutdown_initiated")
    status_task.cancel()
    midpoint_task.cancel()

    # B-3 v2 fix: await any in-flight TP runners so they finish cleanly
    # before tearing down dependencies (clob / order_manager). Otherwise a
    # TP sell could fire after `_shutdown` is set and race against cleanup.
    if STATE._bg_tasks:
        await asyncio.gather(*list(STATE._bg_tasks), return_exceptions=True)
        STATE._bg_tasks.clear()

    if s.gtd_expiry:
        await s.gtd_expiry.stop()
    if s.decision_loop:
        await s.decision_loop.stop()
    if s.wm:
        await s.wm.stop()
    if s.market_ws:
        await s.market_ws.stop()
    if s.user_ws:
        await s.user_ws.stop()
    if s.binance_feed:
        await s.binance_feed.stop()
    if s.heartbeat:
        await s.heartbeat.stop()
    if s.binance_spot:
        await s.binance_spot.close()
    if s.pm_client:
        await s.pm_client.close()
    s.db_conn.close()
    log.info("bot.shutdown_complete")


# ---------------------------------------------------------------------------
# User WS callbacks — route order/trade events to engine
# ---------------------------------------------------------------------------

def _make_maker_fill_callback(state: AppState):
    """
    Route each individual maker fill to engine.

    This is the primary fill path from the UserWS.  Each call corresponds to one
    maker_orders[] entry with a full order_id and its own asset_id (UP or DOWN).
    We pass the full order_id — NO prefix truncation.
    """
    eng = state.engine
    om = state.order_manager

    def on_maker_fill(fill) -> None:
        if not eng or not om:
            return

        order_id = fill.maker_order_id
        asset_id = fill.asset_id
        fill_price = fill.price
        # polymarket WS reports per-trade size (incremental). We must
        # accumulate into the order's matched size before promoting to filled.
        fill_size_inc = fill.size

        if not order_id or fill_size_inc <= 0:
            # H-V6-12 (audit m-2): surface this as info, not debug, so
            # operators can see when upstream delivered a zero-size fill.
            # The router itself can't tell whether the upstream was the
            # WS-only path or WS+REST fallback — user_ws logs the
            # fallback decision in `user_ws.rest_filled_size` separately.
            log.info(
                "router.maker_fill.skip",
                order_id=order_id, fill_size=fill_size_inc,
                reason="upstream delivered size=0 fill event",
            )
            return

        tracked = om.by_server_id(order_id)
        if not tracked:
            # H-NEW-2026-07-11 (audit v6-12): was DEBUG. Production symptom
            # 2026-07-11 11:15:30: UserWS delivered a TRADE event with
            # asset_id=DOWN token and a maker_order_id that did not match
            # any tracked BUY GTD. Because the size field was 0 the skip
            # path was logged at INFO — but if a future WS payload arrives
            # with size>0 and an untracked order_id, the DEBUG-level skip
            # below would silently drop the fill with no breadcrumb.
            # Operators need to see untracked order_ids so they can spot
            # WS payload mis-routing before it costs a position.
            log.info(
                "router.maker_fill.untracked",
                order_id=order_id, asset_id=asset_id[:16],
                fill_size=fill_size_inc, fill_price=fill_price,
                reason="no TrackedOrder matches server_order_id",
            )
            return

        # Decide what *increment* to forward to the engine. The UserWS
        # delivers one maker-fill row per trade — most of the time the
        # increment is exactly fill.size. But the exchange can also send a
        # MOMENTUM / UPDATE event that reports the order's NEW cumulative
        # matched size (not a delta). We unify on delta semantics here:
        # `prev_cumulative + delta == new_cumulative`, so `delta = max(0,
        # new_cumulative - prev_cumulative)`. Anything beyond that is
        # treated as already accounted for and dropped. (B-3 fix — earlier
        # versions ignored this and double-counted on UPDATE-style events.)
        prev_cumulative = tracked.size_matched or 0.0
        new_cumulative = prev_cumulative + fill_size_inc
        delta = max(0.0, fill_size_inc)
        # Only emit when there is net new matched size to report.
        if new_cumulative - prev_cumulative <= 1e-9:
            log.debug(
                "router.maker_fill.no_new",
                order_id=order_id, prev=prev_cumulative, inc=fill_size_inc,
            )
            return

        om.apply_fill_event(
            order_id,
            size_matched=new_cumulative,
            fill_price=fill_price,
        )
        om.mark_filled(order_id)

        # Route to engine with authoritative asset_id. Engine treats
        # `fill_size` as the per-event delta after the B-3 deduper / Lock
        # refactor (see engine.on_order_filled docstring).
        eng.on_order_filled(asset_id, order_id, fill_price, delta)

        log.info(
            "router.maker_fill",
            maker_order_id=order_id,    # full id — never truncate
            asset_id=asset_id[:16],
            fill_price=fill_price,
            fill_size=delta,
            fill_total_size=new_cumulative,
            status=fill.status,
        )

    return on_maker_fill


def _make_user_ws_order_callback(state: AppState):
    """
    Route CLOB order events to engine.

    Handles PLACEMENT (bind server order id), UPDATE (size_matched),
    and CANCELLATION events from the UserWS order channel.
    """
    eng = state.engine

    def on_order(ev) -> None:  # ev: OrderEvent from user_ws.py
        if not eng:
            return

        order_id = ev.order_id
        evt_type = ev.event_type.upper()

        if evt_type == "CANCELLATION":
            eng.on_order_cancelled(order_id, reason="user_ws")
        elif evt_type == "UPDATE":
            # Partial fill update — size_matched changed
            # Engine tracks fills via on_order_filled separately,
            # but UPDATE gives us the authoritative size_matched for the order.
            om = state.order_manager
            if om:
                om.apply_fill_event(order_id, size_matched=ev.size_matched)
                om.mark_filled(order_id)
            log.debug(
                "router.order_update",
                order_id=order_id, size_matched=ev.size_matched,
            )

    return on_order


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
