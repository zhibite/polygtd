"""Polymarket BTC 15m Up/Down — GTD Martingale Bot.

Entry point. Coordinates:
  1. WindowManager — tracks 15-min window boundaries
  2. BinanceFeed   — BTC spot WS + ring buffer
  3. MarketWS      — CLOB market data WS (best_bid / last_trade_price)
  4. MarketDiscovery — Gamma REST to find market metadata
  5. StrategyEngine — decision loop (emits typed Actions)
  6. OrderManager   — serialised order executor
  7. UserWS        — CLOB user WS for fill / cancel events
  8. GtdExpiryWatcher — detects expired GTD orders
  9. HeartbeatLoop  — independent keepalive

Usage:
  pip install -e .              # install dependencies
  cp .env.example .env          # fill in credentials
  python -m src.main            # start bot

All trading is live. DRY_RUN has been removed.
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

class ActionRouter:
    """Converts StrategyEngine Actions into OrderManager calls and wires fill events."""

    def __init__(self, state: AppState) -> None:
        self._state = state

    async def __call__(self, action: Action) -> None:
        eng = self._state.engine
        om = self._state.order_manager
        if not eng or not om:
            return

        tick_size = eng._round.tick_size if eng._round else 0.01
        round_ts = self._state.round_start_ts

        if isinstance(action, ActionOpenT1):
            # H-V4-1 fix: route via `place_buy_by_notional` so the share
            # count is computed AFTER the orderbook price snap instead of
            # before. This prevents each tier from over-spending by 1-3%
            # versus the planned `notional`.
            order = await om.place_buy_by_notional(
                token_id=action.token,
                notional=action.notional,
                ref_price=action.price,
                round_start_ts=round_ts,
                tier_name="T1",
                tick_size=tick_size,
            )
            if order:
                # Update engine state
                eng._round.position.entry_side = action.side
                eng._round.position.entry_token = action.token
                expires = order.expires_at
                # Use adjusted_price (the price actually sent to CLOB, after bid+BUY_STEP)
                # so size_expected matches the notional CLOB will charge us with.
                eng.bind_server_order_id(
                    tier=1,
                    server_order_id=order.server_order_id or "",
                    token_id=action.token,
                    expires_at=expires,
                    expected_price=order.price,
                    # M-1: pass the CLOB-accepted placed_size so size_expected
                    # is exact (avoid notional/price round-trip mismatch).
                    placed_size=order.placed_size if order.placed_size > 0 else None,
                )
                # Wire fill callback so engine knows when order fills
                self._state.clob.fill_callback = self._make_fill_callback()
                log.info(
                    "router.t1_placed",
                    side=action.side,
                    token=action.token[:16],
                    price=order.price,
                    size=order.size,
                    server_id=order.server_order_id or "none",
                )
            else:
                # B-V4-2 fix: H-1 v2 wrote `entry_token` / `entry_side`
                # BEFORE awaiting the handler. If `place_buy` returns None
                # (CLOB post error / size below 5 tokens / poll timeout),
                # the engine is now stuck in a state where the round thinks
                # T1 was attempted but no TrackedOrder / server_id was bound.
                # Subsequent `_evaluate` would skip both `_check_t1_entry`
                # (entry_token is set) and `_check_average_down`
                # (baseline_set is False) — the round silently idles until
                # the next window transition. Roll back the engine state so
                # a future evaluate can re-attempt or the next round starts
                # cleanly.
                log.error(
                    "router.t1_failed",
                    side=action.side,
                    token=action.token[:16],
                    price=action.price,
                    notional=action.notional,
                    reason="om.place_buy returned None",
                )
                eng._round.position.entry_side = None
                eng._round.position.entry_token = None

        elif isinstance(action, ActionOpenTN):
            # H-V4-1 fix: same as T1 — size after the price snap.
            order = await om.place_buy_by_notional(
                token_id=action.token,
                notional=action.notional,
                ref_price=action.price,
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
                    # M-1: use CLOB-accepted placed_size for size_expected.
                    placed_size=order.placed_size if order.placed_size > 0 else None,
                )
                self._state.clob.fill_callback = self._make_fill_callback()
                log.info(
                    "router.tN_placed",
                    tier=action.tier,
                    token=action.token[:16],
                    price=order.price,
                    size=order.size,
                    server_id=order.server_order_id or "none",
                )
            else:
                # B-V4-2 fix: tier.placed / tier.server_order_id were not
                # mutated (those happen inside `bind_server_order_id`), but
                # `_check_average_down` already passed the `if tier.placed:
                # continue` check — meaning a subsequent evaluate in the
                # same 250ms tick could re-trigger T_N. Reset the tier
                # state and log the failure.
                log.error(
                    "router.tN_failed",
                    tier=action.tier,
                    token=action.token[:16],
                    price=action.price,
                    notional=action.notional,
                    reason="om.place_buy returned None",
                )
                tier_snap = getattr(eng._round.position, f"tier{action.tier}", None)
                if tier_snap is not None:
                    tier_snap.placed = False
                    tier_snap.server_order_id = None
                    tier_snap.expires_at = 0.0

        elif isinstance(action, ActionTakeProfit):
            # Two-stage TP: GTD limit sell at bid (maker), then FAK fallback
            # after ~75 seconds for any residual qty. Avoids the "FAK reload
            # loop" where a shallow book lets TP fill part-by-part and each
            # round walks down one more bid level.
            #
            # H-1 fix: place_tp_with_fallback may take 75–90s end-to-end.
            # Awaiting it directly stalls the action router and the entire
            # _evaluate loop. Fire the placement as a background task and
            # bind the resulting server_order_ids back into the engine so
            # subsequent fill events route into Position.record_tp_fill
            # (B-2 fix). The in-flight mutex (`pos._tp_inflight`) remains
            # the single source of truth for "TP is in flight, do not
            # re-arm yet" — it is cleared either by the deduper-driven
            # fill callback or by the failure-recovery branch at the end
            # of the spawned task.
            async def _tp_runner() -> None:
                try:
                    gtd, fak, gtd_err, fak_err = await om.place_tp_with_fallback(
                        token_id=action.token,
                        size=action.qty,
                        price=action.price,
                        round_start_ts=round_ts,
                        tier_name="TP",
                        tick_size=tick_size,
                    )
                except Exception as exc:
                    # H-1 fix: any uncaught exception in the TP runner
                    # would leave pos._tp_inflight stuck True forever.
                    # Release the mutex and surface the crash so the next
                    # tick can decide whether to re-arm.
                    log.exception("router.tp_runner_crashed", error=str(exc))
                    if eng._round:
                        pos = eng._round.position
                        pos._tp_inflight = False
                        # B-V5-2 fix: clear _tp_snapshots so any stale entries
                        # from a partially-completed TP run cannot leak across
                        # retries / round boundaries. Without this, the dict
                        # grows monotonically (one entry per TP attempt) over
                        # a long session.
                        pos._tp_snapshots.clear()
                    return

                if not eng._round:
                    return
                pos = eng._round.position
                if gtd and gtd.server_order_id:
                    pos.register_tp(
                        server_order_id=gtd.server_order_id,
                        token_id=action.token,
                        target_qty=action.qty,
                    )
                if fak and fak.server_order_id:
                    # FAK target_qty is the residual; if it returned a
                    # tracked order the actual sold qty may differ.
                    pos.register_tp(
                        server_order_id=fak.server_order_id,
                        token_id=action.token,
                        target_qty=fak.size,
                    )

                # H-4 fix: classify outcome for observability. The mutex
                # policy is decided by whether we observed any tracked
                # order; engine-side fill callbacks will clear the mutex
                # for partial/full fills. Only a "no order posted at all"
                # outcome releases the mutex here.
                no_order_posted = (gtd is None and fak is None)
                if no_order_posted:
                    pos._tp_inflight = False
                    log.warning(
                        "router.tp_failed",
                        token=action.token[:16],
                        price=action.price,
                        qty=action.qty,
                        gtd_err=gtd_err,
                        fak_err=fak_err,
                    )
                    return

                if gtd and not fak:
                    log.info(
                        "router.tp_gtd_only",
                        token=action.token[:16],
                        price=action.price,
                        qty=action.qty,
                        gtd_id=(gtd.server_order_id or "")[:16],
                    )
                elif fak and not gtd:
                    log.info(
                        "router.tp_fak_only",
                        token=action.token[:16],
                        price=action.price,
                        qty=action.qty,
                        fak_id=(fak.server_order_id or "")[:16],
                        gtd_err=gtd_err,
                        fak_err=fak_err,
                    )
                else:
                    log.info(
                        "router.tp_full",
                        token=action.token[:16],
                        price=action.price,
                        qty=action.qty,
                        gtd_id=(gtd.server_order_id or "")[:16] if gtd else "",
                        fak_id=(fak.server_order_id or "")[:16] if fak else "",
                    )

            # B-3 v2 fix: retain a strong reference to the spawned task so it
            # cannot be GC'd before its first `await` yields control. Mirrors
            # the cleanup pattern in `GtdExpiryWatcher._bg_tasks` — when the
            # task finishes, it removes itself from the set.
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
        window_end = wm.next_round_start() if wm else 0
        window_end_str = datetime.fromtimestamp(window_end, tz=timezone.utc).strftime("%H:%M:%S")

        # Position state
        pos_str = ""
        if s.engine and s.engine._round and s.engine._round.position:
            pos = s.engine._round.position
            tiers_str = ",".join(
                f"T{t.tier}={'✓' if t.is_complete else ('⏳' if t.placed else '○')}"
                for t in pos.tiers
            )
            pos_str = f" | tiers=[{tiers_str}] qty={pos.total_qty:.2f} cost=${pos.total_cost:.2f}"

        # H-V6-9: surface the cached best_bid for UP/DOWN so the operator
        # can tell from STATUS whether the WS bba path is actually feeding
        # the strategy. Before this, REST midpoint was visible but best_bid
        # was hidden — leaving us blind to the fact that `cache.best_bid`
        # was 0 while the WS was pumping `best_bid_ask` events we never
        # parsed (production symptom: 2026-07-11 01:17 round 01:15).
        if up_token and down_token:
            up_bid = s.price_cache.best_bid.get(up_token, 0.0)
            down_bid = s.price_cache.best_bid.get(down_token, 0.0)
            bid_str = f" | bids=UP={up_bid:.4f}/DOWN={down_bid:.4f}"
        else:
            bid_str = ""

        log.info(
            f"STATUS  binance={bc:,.1f}  {gate_str}"
            f"  window_end={window_end_str}({next_in:.0f}s)  "
            f"UP={up_last:.4f}  DOWN={down_last:.4f}{bid_str}{pos_str}"
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

    # ── Strategy engine ──────────────────────────────────────────────────
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

    # Status printer + midpoint poller
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
            log.debug(
                "router.maker_fill.skip",
                order_id=order_id, fill_size=fill_size_inc,
            )
            return

        tracked = om.by_server_id(order_id)
        if not tracked:
            log.debug(
                "router.maker_fill.untracked",
                order_id=order_id,
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
