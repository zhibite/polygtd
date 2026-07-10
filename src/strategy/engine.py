"""Strategy engine — decision loop that watches prices and emits actions.

The engine is purely reactive:
  - Receives price ticks via on_binance_tick() and on_token_tick()
  - Maintains RoundState internally
  - Calls the supplied callbacks for each action

Order state is driven by the fill/cancel/expire callbacks. A tier's
placed=True flag is cleared only when the GTD either:
  a) fills completely (is_complete=True), or
  b) expires without a fill (on_order_expired -> mark_expired)
This ensures a tier cannot be triggered twice.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Callable, Literal, Protocol

from src.config import strategy as _s
from src.feeds.binance_spot import BinanceCache
from src.market.models import MarketInfo
from src.market.market_ws import PriceCache
from src.strategy.gatekeeper import GateKeeper
from src.strategy.round_state import Position, RoundState, TierSnapshot
from src.infra.logging import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Action types
# ---------------------------------------------------------------------------


@dataclass
class ActionOpenT1:
    side: Literal["UP", "DOWN"]
    token: str
    price: float    # best_bid at moment of decision
    notional: float


@dataclass
class ActionOpenTN:
    tier: int        # 2, 3, 4
    token: str
    price: float
    notional: float
    trigger_price: float


@dataclass
class ActionTakeProfit:
    token: str
    price: float    # best_bid at moment of decision
    qty: float     # total position qty


@dataclass
class ActionCancelTier:
    tier: int           # which tier to cancel
    server_order_id: str
    reason: str


Action = ActionOpenT1 | ActionOpenTN | ActionTakeProfit | ActionCancelTier


class ActionHandler(Protocol):
    """Protocol for callbacks that consume strategy actions."""

    async def __call__(self, action: Action) -> None: ...


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class StrategyEngine:
    """Reactive strategy that monitors prices and emits action callbacks."""

    def __init__(
        self,
        binance_cache: BinanceCache,
        price_cache: PriceCache,
        handler: ActionHandler,
    ) -> None:
        self._bc = binance_cache
        self._pc = price_cache
        self._handler = handler
        self._round: RoundState | None = None
        self._gate: GateKeeper | None = None
        self._s = _s()
        # B-1 fix: prevents concurrent tick sources (midpoint poll loop +
        # binance + WS book ticker) from re-entering _evaluate while the
        # previous run is awaiting an action handler. Drop-on-re-entry.
        self._eval_inflight: bool = False

    # ------------------------------------------------------------------
    # Round lifecycle
    # ------------------------------------------------------------------

    async def on_new_round(self, round_start_ts: int, market: MarketInfo | None = None) -> None:
        """Called when the 15-min window rolls over."""
        log.info("engine.new_round", round_start_ts=round_start_ts)
        self._round = RoundState(round_start_ts=round_start_ts, market=market)
        self._gate = None

    def set_binance_baseline(self, baseline: float) -> None:
        """Call after collecting the round-start Binance median."""
        if self._round:
            self._round.binance_baseline = baseline
        self._gate = GateKeeper(baseline)
        log.info("engine.baseline_set", baseline=baseline)

    async def on_round_end(self) -> None:
        """Called at round close."""
        if self._round:
            self._round.finished = True
            log.info(
                "engine.round_ended",
                round_start_ts=self._round.round_start_ts,
                position=self._round.position,
            )
        self._round = None
        self._gate = None

    # ------------------------------------------------------------------
    # Price tick handlers
    # ------------------------------------------------------------------

    async def on_binance_tick(self) -> None:
        """Re-evaluate all decisions whenever a new Binance trade arrives."""
        await self._evaluate()

    async def on_token_tick(self, token: str) -> None:
        """Re-evaluate all decisions whenever a token's price changes."""
        await self._evaluate()

    # ------------------------------------------------------------------
    # Order event handlers — these drive tier state machine
    # ------------------------------------------------------------------

    def on_order_filled(
        self,
        token_id: str,
        server_order_id: str,
        fill_price: float,
        fill_size: float,
    ) -> None:
        """
        Called when a confirmed fill delta is received.

        Args:
            token_id:        authoritative asset_id of the filled maker order (UP or DOWN)
            server_order_id: complete server order_id hash (NOT a prefix)
            fill_price:      price of this fill
            fill_size:       tokens filled IN THIS EVENT (a delta, NOT a
                              cumulative). Multiple sources (WS, polling,
                              FAK response) may observe the same order — a
                              FillDeduper inside CLOBClient ensures this is
                              only called with the un-processed delta so that
                              `record_fill`'s `size_filled += size` does not
                              double-count (B-3 fix).
        """
        if not self._round:
            log.debug(
                "engine.fill_ignored no_round",
                token=token_id[:16], order_id=server_order_id,
            )
            return

        if self._round.position.is_empty:
            log.debug(
                "engine.fill_ignored no_position",
                token=token_id[:16], order_id=server_order_id,
            )
            return

        pos = self._round.position
        for tier in pos.tiers:
            if tier.server_order_id == server_order_id:
                # Verify asset_id matches — reject mismatches (cross-token contamination)
                if token_id and token_id != tier.token_id and tier.token_id:
                    log.warning(
                        "engine.fill_asset_mismatch",
                        token=token_id[:16], expected=tier.token_id[:16], order_id=server_order_id,
                    )
                    return

                pos.record_fill(tier, fill_price, fill_size)
                log.info(
                    "engine.tier_filled",
                    tier=tier.tier,
                    token_id=token_id[:16],
                    fill_price=fill_price,
                    fill_size=fill_size,
                    cumulative_size=tier.size_filled,
                    expected_size=tier.size_expected,
                    avg_cost=pos.avg_cost,
                    total_qty=pos.total_qty,
                    tp_target=pos.tp_target(),
                )
                return

        # Not a BUY tier fill — check if it is a registered TP (SELL) fill.
        if pos.record_tp_fill(server_order_id, fill_price, fill_size):
            log.info(
                "engine.tp_filled",
                server_order_id=server_order_id[:16],
                token_id=token_id[:16],
                fill_price=fill_price,
                fill_size=fill_size,
                total_qty=pos.total_qty,
                avg_cost=pos.avg_cost,
                tp_target=pos.tp_target(),
            )
            return

        log.debug("engine.fill_unknown", order_id=server_order_id)

    def on_order_cancelled(self, server_order_id: str, reason: str = "manual") -> None:
        """
        Called when an order is cancelled (manual, deviation, or cancel-all).
        Resets tier.placed so the strategy can re-arm if conditions still hold.
        """
        if not self._round:
            return
        for tier in self._round.position.tiers:
            if tier.server_order_id == server_order_id:
                tier.placed = False
                tier.server_order_id = None
                log.info(
                    "engine.order_cancelled",
                    tier=tier.tier,
                    server_order_id=server_order_id,
                    reason=reason,
                )
                return

    def on_order_expired(self, server_order_id: str) -> None:
        """
        Called when a GTD order has expired server-side without a fill.
        Mark the tier as expired so the strategy can re-arm.
        """
        if not self._round:
            return
        for tier in self._round.position.tiers:
            if tier.server_order_id == server_order_id:
                self._round.position.mark_expired(tier, reason="gtd_expired")
                tier.placed = False
                tier.server_order_id = None
                log.warning(
                    "engine.tier_expired",
                    tier=tier.tier,
                    server_order_id=server_order_id,
                    size_filled=tier.size_filled,
                    size_expected=tier.size_expected,
                )
                return

    def bind_server_order_id(
        self,
        tier: int,
        server_order_id: str,
        token_id: str,
        expires_at: float,
        expected_price: float,
        placed_size: float | None = None,
    ) -> None:
        """
        Called by the action router after an order is placed.

        Records the authoritative token_id so fill events can be validated.
        size_expected is the CLOB-accepted placed_size when supplied (M-1
        fix — the CLOB may round down size to the nearest tick, so
        notional / expected_price can be off by one tick and cause
        is_complete to never flip on a clean fill). Falls back to the
        notional/expected_price quotient if placed_size is missing.
        """
        if not self._round:
            return
        t = self._round.position.tiers[tier - 1]
        t.placed = True
        t.server_order_id = server_order_id
        t.token_id = token_id
        t.expires_at = expires_at
        if placed_size is not None and placed_size > 0:
            t.size_expected = placed_size
        elif expected_price > 0:
            t.size_expected = t.notional / expected_price
        else:
            t.size_expected = 0.0

        # H-NEW-3 fix: anchor the martingale trigger baseline to the price
        # the CLOB actually accepted (`expected_price` here is the adjusted
        # price from TrackedOrder, after bid+BUY_STEP snap). Previously
        # _check_t1_entry pre-set baseline_target_price from the WS-cached
        # `bid_price`, which can be stale by the time the order lands on
        # the CLOB and produces a different adjusted_price. Re-anchoring
        # here (idempotent overwrite with the same value during T1, and
        # meaningful for T2..T4) keeps the martingale math aligned with
        # what we are actually paying per share.
        if tier == 1 and expected_price > 0:
            self._round.position.baseline_target_price = expected_price

    # ------------------------------------------------------------------
    # Core evaluation
    # ------------------------------------------------------------------

    async def _evaluate(self) -> None:
        """Re-evaluate the round state and emit at most one action.

        Wrapped in an in-flight guard so concurrent tick sources (B-1 fix:
        midpoint poll, WS book ticker, binance trades) cannot re-enter the
        decision loop while a previous evaluate is awaiting the action
        handler. Without this, two ticks arriving in the same window could
        each emit ActionOpenT1 / ActionOpenTN and the router would queue
        two placements.
        """
        if not self._round or not self._gate:
            return

        # Single-flight: drop re-entry rather than queue it. The next tick
        # will pick up the new state and call _evaluate again.
        if self._eval_inflight:
            log.debug(
                "engine.evaluate.skip re_entry",
                round_start_ts=self._round.round_start_ts,
            )
            return
        self._eval_inflight = True
        try:
            bc_now = self._bc.last_price
            if bc_now <= 0:
                return

            gate_state = self._gate.state(bc_now)

            new_state = "PASS" if gate_state.within_limit else "FAIL"
            if self._round.gate_last_state != new_state:
                self._round.gate_last_state = new_state
                log.info("engine.gate", drift=gate_state.drift, state=new_state)

            if not gate_state.within_limit:
                return  # drift out of bounds — no new orders

            pos = self._round.position

            # ── Not yet entered → check T1 entry ────────────────────────────
            if pos.entry_token is None:
                await self._check_t1_entry(bc_now)

            # ── In position → check averaging-down and take-profit ───────────
            if pos.entry_token is not None:
                await self._check_average_down(bc_now)
                await self._check_take_profit(bc_now)
        finally:
            self._eval_inflight = False

    async def _check_t1_entry(self, bc_now: float) -> None:
        """Check both sides for T1 entry trigger."""
        if not self._round or not self._gate or not self._round.up_token:
            return

        pos = self._round.position
        if pos.entry_token is not None:
            return  # already entered

        s = self._s
        up_tok = self._round.up_token
        down_tok = self._round.down_token

        for side, token in [("UP", up_tok), ("DOWN", down_tok)]:
            mid_price = self._pc.get_midpoint(token)
            if mid_price <= 0:
                continue

            # Price cap check
            if mid_price > s.ENTRY_PRICE_CAP:
                log.debug("engine.t1_skip_too_expensive", side=side, mid_price=mid_price)
                continue

            # ── T1 entry time gate ───────────────────────────────────────
            # T1 must enter within T1_ENTRY_WINDOW_S of window open.
            # This prevents chasing at the very end of the 15-min window.
            elapsed_in_window = time.time() - self._round.round_start_ts
            if elapsed_in_window > s.T1_ENTRY_WINDOW_S:
                # Throttle: log once when gate closes, then stay quiet
                now = time.time()
                last = getattr(self, "_t1_gate_closed_log_ts", 0.0)
                if now - last > 60:
                    self._t1_gate_closed_log_ts = now
                    log.info(
                        "engine.t1_gate_closed elapsed={:.0f}s > limit={:.0f}s",
                        elapsed_in_window, s.T1_ENTRY_WINDOW_S,
                    )
                continue

            # BTC drift gate
            if not self._gate.can_enter(bc_now, mid_price):
                continue

            bid_price = self._pc.bid(token)
            if bid_price <= 0:
                continue

            # Stake cap check — don't overcommit
            if pos.stake_cap_remaining < s.TIER1_NOTIONAL:
                log.debug(
                    "engine.t1_skip_cap_reached",
                    stake_remaining=pos.stake_cap_remaining, needed=s.TIER1_NOTIONAL,
                )
                continue

            log.info(
                "engine.trigger_t1",
                side=side,
                token=token[:16],
                mid=mid_price,
                best_bid=bid_price,
                drift=self._gate.state(bc_now).drift,
            )

            # H-1 v2 fix: claim the entry slot BEFORE awaiting the handler.
            # Previously `_round.position.entry_token` was written by the
            # router only AFTER `await place_buy` resolved — during that
            # yield window a concurrent `_evaluate` tick could observe
            # `entry_token is None` and emit a duplicate ActionOpenT1.
            # Writing it here, under the same `_eval_inflight` guard that
            # is still held, closes the race. The router will skip its own
            # `entry_token = action.token` write because the field is no
            # longer None (idempotent overwrite with the same value).
            #
            # H-NEW-3 fix: `baseline_target_price` is now anchored inside
            # `bind_server_order_id` to the actual adjusted_price (the price
            # the CLOB accepted). Previously it was set here to the
            # WS-cached bid which can be stale by 200-500ms vs the REST
            # orderbook the CLOB saw. The bind-server-id path runs after
            # the order has been posted, so we get a more accurate anchor.
            pos.entry_side = side
            pos.entry_token = token
            await self._handler(ActionOpenT1(
                side=side,
                token=token,
                price=bid_price,
                notional=s.TIER1_NOTIONAL,
            ))
            return  # enter only once per round

    async def _check_average_down(self, bc_now: float) -> None:
        """
        Check if the next averaging tier should be triggered.

        Mutex rules:
          - A tier is skipped if it is already *complete* (is_complete=True)
          - A tier is skipped if it is currently *in-flight* (placed=True but not complete)
          - A tier is triggered only if it is *not placed yet*

        This prevents double-triggering the same tier.
        """
        if not self._round or not self._gate:
            return

        pos = self._round.position
        if not pos.baseline_set or pos.entry_token is None:
            return

        # H-NEW-2 fix: while a TP runner is in flight (pos._tp_inflight=True),
        # suppress averaging-down. Reason: the TP runner is working a GTD
        # sell at the bid; opening a new BUY would (a) widen the position we
        # intend to close and (b) interact with B-NEW-1's old bug where BUY
        # fills incorrectly cleared _tp_inflight. Suppressing averaging here
        # keeps the close path deterministic — once TP resolves (fill or
        # GTD expiry → _tp_inflight cleared), the next evaluate() resumes
        # normal averaging-down decisions.
        if pos._tp_inflight:
            return

        mid_price = self._pc.get_midpoint(pos.entry_token)
        if mid_price <= 0:
            return

        # H-3 v2 fix: anchor the averaging trigger lines to the INTENDED
        # entry price (the bid at T1 decision time), not to the actual
        # fill average. This keeps martingale spacing stable across T1
        # slippage — if T1 fills 5% above the bid, T2 still triggers at
        # the design -20% from the original bid, not -20% from a
        # slippage-shifted avg. Falls back to the actual baseline for
        # older round-state objects constructed before this field existed.
        baseline = pos.baseline_target_price or pos.baseline_price
        s = self._s

        for tier in pos.tiers[1:]:  # T2, T3, T4
            # Skip if already complete or currently in-flight
            if tier.is_complete:
                continue
            if tier.placed:
                # M-V4-2 fix: `tier.placed=True` but `server_order_id is None`
                # is a stale state — it can only arise if a previous
                # `bind_server_order_id` call did not run (router-level
                # failure, see B-V4-2) or was unwound. Treat this as a
                # half-placed tier and reset so we can re-fire cleanly,
                # rather than skipping forever or duplicating the order.
                if not tier.server_order_id:
                    log.warning(
                        "engine.tier_stale_placed_reset",
                        tier=tier.tier,
                        size_filled=tier.size_filled,
                        cost_filled=tier.cost_filled,
                    )
                    tier.placed = False
                    tier.expires_at = 0.0
                else:
                    continue

            # Per-tier hard cap: refuse to (re)place this tier if it has
            # already absorbed > tier.notional * TIER_NOTIONAL_MAX_MULT.
            # This stops GTD loops / repeated partial fills from silently
            # spending into the next tier's budget.
            if pos.tier_is_saturated(tier):
                log.warning(
                    "engine.tier_saturated_skip",
                    tier=tier.tier,
                    cost_filled=tier.cost_filled,
                    cap=tier.notional * s.TIER_NOTIONAL_MAX_MULT,
                    planned=tier.notional,
                )
                continue

            trigger = tier.trigger_price(baseline)
            if trigger is None:
                continue

            # Price must have dropped to or below the trigger
            if mid_price > trigger:
                continue

            bid_price = self._pc.bid(pos.entry_token)
            if bid_price <= 0:
                continue

            # BTC drift gate for averaging.
            # Drift is bypassed when the price has already dropped to the *next*
            # tier's trigger line (e.g. checking T2 but price already hit T3's -40%).
            # In that case price momentum justifies the averaging entry.
            next_drop = TierSnapshot._next_tier_drop(tier.tier)
            if not self._gate.can_double_down(bc_now, mid_price, tier.tier, next_drop, baseline):
                continue

            # Per-tier remaining cap (defense-in-depth)
            tier_remaining = pos.tier_notional_remaining(tier)
            if tier_remaining < tier.notional * 0.5:
                log.warning(
                    "engine.tier_remaining_low_skip",
                    tier=tier.tier, tier_remaining=tier_remaining,
                )
                continue

            # Stake cap check — don't overcommit
            if pos.stake_cap_remaining < tier.notional:
                log.debug(
                    "engine.tN_skip_cap",
                    tier=tier.tier, remaining=pos.stake_cap_remaining, needed=tier.notional,
                )
                continue

            log.info(
                "engine.trigger_tN",
                tier=tier.tier,
                mid=mid_price,
                trigger=trigger,
                baseline=baseline,
                bid=bid_price,
                notional=tier.notional,
            )

            await self._handler(ActionOpenTN(
                tier=tier.tier,
                token=pos.entry_token,
                price=bid_price,
                notional=tier.notional,
                trigger_price=trigger,
            ))
            return  # place one tier at a time

    async def _check_take_profit(self, bc_now: float) -> None:
        """Check if take-profit should trigger.

        Mutex: while a TP sell is in-flight (awaiting CLOB ack / partial fill
        callback), do not enqueue a new TP action. The flag is cleared by the
        next fill event so residual position can re-arm if mid is still
        above the target.
        """
        if not self._round or not self._gate:
            return

        pos = self._round.position
        if not pos.entry_token or pos.total_qty <= 0:
            return

        if pos._tp_inflight:
            return

        mid_price = self._pc.get_midpoint(pos.entry_token)
        if mid_price <= 0:
            return

        tp_target = pos.tp_target()
        if tp_target is None:
            return

        # B-4 fix: trigger on bid, not mid. With mid/bid gap of ~1-3% in
        # the 0.95-zone tokens, mid-based triggers over-state the profit
        # window and lead to actual sale prices that miss the target.
        # Using bid here keeps trigger-condition and sale-price identical.
        bid_price = self._pc.bid(pos.entry_token)
        if bid_price <= 0:
            return

        if bid_price < tp_target:
            return

        if not self._gate.can_act(bc_now):
            return

        # Set mutex BEFORE emitting the action so re-entrant _evaluate()
        # calls (called again from any subsequent token/bnb tick before the
        # TP sell's fill callback lands) do not stack duplicate actions.
        pos._tp_inflight = True

        log.info(
            "engine.trigger_tp",
            mid=mid_price,
            bid=bid_price,
            tp_target=tp_target,
            tp_pct=pos.tp_pct_for_current_state(),
            avg_cost=pos.avg_cost,
            qty=pos.total_qty,
        )

        await self._handler(ActionTakeProfit(
            token=pos.entry_token,
            price=bid_price,
            qty=pos.total_qty,
        ))
