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
from src.feeds.binance_trades import BinanceCache
from src.market.discovery import MarketInfo
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

    # ------------------------------------------------------------------
    # Round lifecycle
    # ------------------------------------------------------------------

    async def on_new_round(self, round_start_ts: int, market: MarketInfo | None = None) -> None:
        """Called when the 15-min window rolls over."""
        log.info("engine.new_round", round_start_ts=round_start_ts)
        self._round = RoundState(round_start_ts=round_start_ts)
        if market:
            self._round.slug = market.slug
            self._round.condition_id = market.condition_id
            self._round.up_token = market.up_token
            self._round.down_token = market.down_token
            self._round.tick_size = market.tick_size
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
        Called when a fill event is received.

        Args:
            token_id:        authoritative asset_id of the filled maker order (UP or DOWN)
            server_order_id: complete server order_id hash (NOT a prefix)
            fill_price:      price of this fill
            fill_size:       tokens filled in this event
        """
        if not self._round:
            log.debug(
                "engine.fill_ignored no_round token={} order_id={}",
                token_id[:16], server_order_id,
            )
            return

        if self._round.position.is_empty:
            log.debug(
                "engine.fill_ignored no_position token={} order_id={}",
                token_id[:16], server_order_id,
            )
            return

        pos = self._round.position
        for tier in pos.tiers:
            if tier.server_order_id == server_order_id:
                # Verify asset_id matches — reject mismatches (cross-token contamination)
                if token_id and token_id != tier.token_id and tier.token_id:
                    log.warning(
                        "engine.fill_asset_mismatch token_id={} expected={} order_id={}",
                        token_id[:16], tier.token_id[:16], server_order_id,
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

        log.debug("engine.fill_unknown order_id={}", server_order_id)

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
    ) -> None:
        """
        Called by the action router after an order is placed.

        Records the authoritative token_id so fill events can be validated.
        size_expected is computed from expected_price (bid at placement), not from
        avg_fill_price (still None at this point — it's only set on first fill).
        """
        if not self._round:
            return
        t = self._round.position.tiers[tier - 1]
        t.placed = True
        t.server_order_id = server_order_id
        t.token_id = token_id
        t.expires_at = expires_at
        t.size_expected = t.notional / expected_price if expected_price > 0 else 0.0

    # ------------------------------------------------------------------
    # Core evaluation
    # ------------------------------------------------------------------

    async def _evaluate(self) -> None:
        if not self._round or not self._gate:
            return

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
            last_price = self._pc.last(token)
            bid_price = self._pc.bid(token)
            if last_price <= 0 or bid_price <= 0:
                continue

            # Price cap check
            if last_price > s.ENTRY_PRICE_CAP:
                log.debug("engine.t1_skip_too_expensive side={} last={}", side, last_price)
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
            if not self._gate.can_enter(bc_now, last_price):
                continue

            # Stake cap check — don't overcommit
            if pos.stake_cap_remaining < s.TIER1_NOTIONAL:
                log.debug(
                    "engine.t1_skip_cap_reached stake_remaining={} needed={}",
                    pos.stake_cap_remaining, s.TIER1_NOTIONAL,
                )
                continue

            log.info(
                "engine.trigger_t1",
                side=side,
                token=token[:16],
                last_trade=last_price,
                best_bid=bid_price,
                drift=self._gate.state(bc_now).drift,
            )

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

        last_price = self._pc.last(pos.entry_token)
        if last_price <= 0:
            return

        baseline = pos.baseline_price
        s = self._s

        for tier in pos.tiers[1:]:  # T2, T3, T4
            # Skip if already complete or currently in-flight
            if tier.is_complete:
                continue
            if tier.placed:
                continue

            trigger = tier.trigger_price(baseline)
            if trigger is None:
                continue

            # Price must have dropped to or below the trigger
            if last_price > trigger:
                continue

            bid_price = self._pc.bid(pos.entry_token)
            if bid_price <= 0:
                continue

            # BTC drift gate for averaging.
            # Drift is bypassed when the price has already dropped to the *next*
            # tier's trigger line (e.g. checking T2 but price already hit T3's -40%).
            # In that case price momentum justifies the averaging entry.
            next_drop = TierSnapshot._next_tier_drop(tier.tier)
            if not self._gate.can_double_down(bc_now, last_price, tier.tier, next_drop, baseline):
                continue

            # Stake cap check — don't overcommit
            if pos.stake_cap_remaining < tier.notional:
                log.debug(
                    "engine.tN_skip_cap tier={} remaining={} needed={}",
                    tier.tier, pos.stake_cap_remaining, tier.notional,
                )
                continue

            log.info(
                "engine.trigger_tN",
                tier=tier.tier,
                last_trade=last_price,
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
        """Check if take-profit should trigger."""
        if not self._round or not self._gate:
            return

        pos = self._round.position
        if not pos.entry_token or pos.total_qty <= 0:
            return

        last_price = self._pc.last(pos.entry_token)
        if last_price <= 0:
            return

        tp_target = pos.tp_target()
        if tp_target is None:
            return

        # Use last_price for trigger, bid for order placement
        if last_price < tp_target:
            return

        bid_price = self._pc.bid(pos.entry_token)
        if bid_price <= 0:
            return

        if not self._gate.can_act(bc_now):
            return

        log.info(
            "engine.trigger_tp",
            last_trade=last_price,
            tp_target=tp_target,
            avg_cost=pos.avg_cost,
            qty=pos.total_qty,
        )

        await self._handler(ActionTakeProfit(
            token=pos.entry_token,
            price=bid_price,
            qty=pos.total_qty,
        ))
