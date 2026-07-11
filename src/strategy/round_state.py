"""Round and position state for the strategy engine.

Key changes vs the original:
  - TierSnapshot now distinguishes between *placed*, *partial fill*, and *fully filled*.
  - size_filled accumulates across partial fills; filled flag only flips once
    the expected stake (notional / price) has been delivered.
  - A baseline_tracker records the running weighted-avg fill price so
    averaging tiers can be triggered against the real cost basis.
  - expected_size/expected_cost are recorded at placement time so we know
    when a partial fill is "complete enough".
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from src.config import strategy as _s
from src.infra.logging import get_logger
from src.infra.logging import get_logger

if TYPE_CHECKING:
    from src.market.models import MarketInfo

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# TierSnapshot
# ---------------------------------------------------------------------------


@dataclass
class TierSnapshot:
    """State for a single martingale tier (T1..T4).

    Lifecycle:
        placed=False, watching=False   -> not yet sent
        placed=True,  watching=True    -> GTD live on CLOB, watcher polling balance
        watching=False, filled=False   -> GTD cancelled/expired, no fill
        filled=True                    -> done (size_filled >= size_expected * 0.995)

    size_filled accumulates across partial fills; filled flag only flips once
    the expected stake has been delivered. The TierWatcher writes
    `placed_qty` from the authoritative CLOB balance so that `is_complete`
    reflects ground truth even when WS fill events are missed.

    Two parallel paths drive `is_complete`:
      - UserWS / CLOBClient fill callbacks → `record_fill` increments
        `size_filled`, which feeds `is_complete`.
      - TierWatcher → sets `filled=True` directly when the watched
        balance crosses `expected_size * fill_ratio` (independent of fill
        events). This is the primary path under the new model.

    A tier can be "complete" via either path; both feed the same flag.
    """

    tier: int                         # 1, 2, 3, 4
    notional: float                   # USD amount for this tier
    placed: bool = False
    # H-NEW-2026-07-11: True while a TierWatcher is actively polling this
    # tier's GTD. Set when the watcher spawns, cleared when the watcher
    # terminates. The engine uses `watching` (not `placed`) to decide
    # whether a tier is "in flight" — a cancelled/expired GTD keeps
    # `placed=True` for the cancel-grace window but `watching=False`.
    watching: bool = False
    filled: bool = False
    server_order_id: str | None = None
    avg_fill_price: float | None = None
    size_filled: float = 0.0          # total shares filled (sum of partial fills)
    size_expected: float = 0.0        # expected shares = notional / price at placement
    # H-NEW-2026-07-11: authoritative ground-truth fill count from
    # TierWatcher polling get_token_balance. Always >= size_filled when
    # fills were missed by the WS path. Used by `is_complete` as a
    # second source of truth.
    placed_qty: float = 0.0
    cost_filled: float = 0.0          # running notional of fills = Σ(px * sz)
    placed_at: float = 0.0
    expires_at: float = 0.0
    last_fill_ts: float = 0.0
    cancel_reason: str = ""
    token_id: str | None = None       # bound to the server order so fills can be matched
    # Optional: id of the spawned TierWatcher task (for diagnostics only).
    watcher_task: object | None = None

    def trigger_price(self, baseline: float) -> float | None:
        cfg = _s()
        drops = cfg.tier_drop_pcts  # [None, 0.20, 0.40, 0.60]
        drop = drops[self.tier - 1]
        if drop is None:
            return None
        return baseline * (1 - drop)

    @staticmethod
    def _next_tier_drop(tier: int) -> float | None:
        """Drop-pct of the *next* tier, or None if this is the last tier (T4)."""
        cfg = _s()
        drops = cfg.tier_drop_pcts  # 0-indexed: drops[1]=T2, drops[2]=T3, drops[3]=T4
        if tier >= len(drops):
            return None
        return drops[tier]  # tier 1 -> drops[1] = T2's drop = 0.20, etc.

    @property
    def fill_rate(self) -> float:
        """Fraction of expected size that has been filled (0..1)."""
        if self.size_expected <= 0:
            return 0.0
        return min(1.0, self.size_filled / self.size_expected)

    @property
    def is_complete(self) -> bool:
        """True if the tier has fully filled (>= 99.5% of expected size).

        H-NEW-2026-07-11: consults BOTH fill-callback state (`size_filled`)
        and TierWatcher ground truth (`placed_qty`). Whichever reports
        "complete" first wins — this guarantees `is_complete` flips even
        when the UserWS missed fill events and only the watcher
        observation caught it.

        For SELL tiers (TP), `placed_qty` decreases over time (we are
        selling), so we use `size_filled` only — the watcher's job is just
        to know when to cancel the GTD (i.e. when balance hits 0).
        """
        if self.size_expected <= 0:
            return self.filled
        # Watcher-driven path: balance >= 99.5% of expected.
        if self.placed_qty + 1e-9 >= self.size_expected * 0.995:
            return True
        # Fill-callback-driven path: still kept as a backstop.
        return self.size_filled >= self.size_expected * 0.995


# ---------------------------------------------------------------------------
# Take-profit snapshot
# ---------------------------------------------------------------------------


@dataclass
class TpSnapshot:
    """State for a single take-profit (SELL) order.

    Sell orders are not martingale tiers and do not drive the round's
    averaging-down logic, but they still need authoritative book-keeping so
    that fills can be matched back to a specific server_order_id and routed
    into the Position's _total_qty / _total_cost reducer (fixes B-2 — without
    this, TP sells never update position state, _tp_inflight never clears,
    and the engine deadlocks after one TP).

    Lifecycle:
        placed=False  -> registered but no fill yet (GTD still open)
        placed=True   -> at least one fill has been accounted for

    size_sold accumulates across partial fills. The Position reduces
    _total_qty / _total_cost as fills land; when size_sold reaches target,
    the entry is dropped from the registry.
    """

    server_order_id: str           # complete server order_id hash
    token_id: str                  # token we are selling
    target_qty: float              # shares we intended to sell
    placed: bool = False           # True once we have seen any fill
    size_sold: float = 0.0         # cumulative shares sold (sum of partial fills)
    usdc_received: float = 0.0     # cumulative proceeds (Σ price * size)


# ---------------------------------------------------------------------------
# Position
# ---------------------------------------------------------------------------

# Helpers must be defined at module level so default_factory lambdas
# can reference them without capturing _s() at class-definition time.
def _tier1_factory() -> TierSnapshot:
    return TierSnapshot(tier=1, notional=_s().TIER1_NOTIONAL)


def _tier2_factory() -> TierSnapshot:
    return TierSnapshot(tier=2, notional=_s().TIER2_NOTIONAL)


def _tier3_factory() -> TierSnapshot:
    return TierSnapshot(tier=3, notional=_s().TIER3_NOTIONAL)


def _tier4_factory() -> TierSnapshot:
    return TierSnapshot(tier=4, notional=_s().TIER4_NOTIONAL)


@dataclass
class Position:
    """Live position within a round."""

    entry_side: Literal["UP", "DOWN"] | None = None
    entry_token: str | None = None

    tier1: TierSnapshot = field(default_factory=_tier1_factory)
    tier2: TierSnapshot = field(default_factory=_tier2_factory)
    tier3: TierSnapshot = field(default_factory=_tier3_factory)
    tier4: TierSnapshot = field(default_factory=_tier4_factory)

    baseline_price: float | None = None   # anchor: avg fill price after T1 fills
    baseline_set: bool = False             # becomes True once baseline_price is anchored
    # H-3 v2 fix: baseline_target_price records the INTENDED entry price
    # (the bid at the moment _check_t1_entry decided to enter) so that
    # the martingale averaging-tier triggers stay anchored to the design
    # price, not to the actual fill average. The actual fill price still
    # drives `_avg_cost` and TP via `baseline_price` — only the T2/T3/T4
    # trigger lines use this target.
    baseline_target_price: float | None = None
    # H-V6-14 fix: baseline_realized_price is the T1 avg fill price we
    # actually paid (Σ price*size / Σ size across all T1 partial fills),
    # used as the authoritative baseline for T2/T3/T4 trigger lines once
    # at least one T1 fill has landed. Before any T1 fill, the engine falls
    # back to baseline_target_price (the intended bid) — this preserves the
    # "don't fire T2 before T1 is partially filled" guard from H-NEW-2 but
    # rebases the trigger math to the *real* cost basis, not the design
    # price. Symptom this fixes: prod 2026-07-11 around the 17m-ago round,
    # T1 GTD submitted @ 0.41, fill landed @ 0.34, T2 then triggered at
    # 0.41*0.80 = 0.328 (in user's words: "why did T2 fire at 0.33? it
    # should have waited for a real 20% drop from 0.34 → 0.27"). Now that
    # baseline is 0.34 once T1 has at least one fill, T2's real trigger
    # is 0.272 — i.e. it would not have fired at 0.33.
    baseline_realized_price: float | None = None
    baseline_set_realized: bool = False

    # Cached position-level stats (recalculated after each fill)
    _total_qty: float = 0.0
    _total_cost: float = 0.0
    _avg_cost: float = 0.0

    # TP in-flight mutex — prevents the engine from issuing a new TP
    # action while a previous TP sell is still being processed by the
    # OrderManager / CLOB. Resets whenever _total_qty changes (i.e. a fill
    # landed, whether that fill is the TP itself or residual positioning).
    _tp_inflight: bool = False
    _last_qty_signature: float = 0.0

    # In-flight TP sells keyed by server_order_id. Lets engine.on_order_filled
    # match a SELL fill back to a specific sell order so it can:
    #   1) reduce _total_qty / _total_cost / _avg_cost by the fill delta;
    #   2) clear _tp_inflight so the engine can re-fire a residual TP on
    #      the next tick if mid is still above target.
    _tp_snapshots: dict[str, "TpSnapshot"] = field(default_factory=dict)

    @property
    def tiers(self) -> list[TierSnapshot]:
        return [self.tier1, self.tier2, self.tier3, self.tier4]

    @property
    def has_inflight_tier(self) -> bool:
        """True if any averaging tier is currently in-flight (placed but not complete).

        H-V5-1: used by engine._check_take_profit to suppress TP while an
        averaging tier is mid-fill, since tp_target() relies on _avg_cost
        which is stale until the in-flight tier actually fills.
        """
        return any(t.placed and not t.is_complete for t in self.tiers[1:])

    @property
    def is_empty(self) -> bool:
        return all(not t.placed and not t.filled for t in self.tiers) and self._total_qty == 0

    @property
    def total_qty(self) -> float:
        return self._total_qty

    @property
    def total_cost(self) -> float:
        return self._total_cost

    @property
    def avg_cost(self) -> float:
        return self._avg_cost

    @property
    def total_notional_used(self) -> float:
        """Sum of notional actually spent (= cost filled so far)."""
        return self._total_cost

    @property
    def total_notional_committed(self) -> float:
        """Sum of notional placed but not yet completed (in-flight + filled)."""
        return sum(t.notional for t in self.tiers if t.placed)

    @property
    def stake_cap_remaining(self) -> float:
        """How much more USD could be deployed before hitting total_max_notional.

        H-V4-2 fix (audit v4): uses `total_notional_committed` (placed +
        filled) instead of `_total_cost` (filled only). Without this, an
        in-flight tier's `notional` does not consume the cap, so the
        check is ineffective while a tier sits between post and fill.
        The "one tier in-flight at a time" invariant currently hides
        this — but the cap is structural defence-in-depth and should
        hold even if that invariant ever changes.
        """
        return max(0.0, _s().total_max_notional - self.total_notional_committed)

    def tier_is_saturated(self, tier: TierSnapshot) -> bool:
        """True if this tier has already absorbed > tier.notional * max_mult."""
        cap = tier.notional * _s().TIER_NOTIONAL_MAX_MULT
        return tier.cost_filled >= cap - 1e-9

    def tier_notional_remaining(self, tier: TierSnapshot) -> float:
        """How much more USD this tier may still receive before saturation."""
        cap = tier.notional * _s().TIER_NOTIONAL_MAX_MULT
        return max(0.0, cap - tier.cost_filled)

    def register_tp(
        self,
        server_order_id: str,
        token_id: str,
        target_qty: float,
    ) -> None:
        """Register an in-flight take-profit order so its fills can be matched.

        Idempotent on server_order_id (a re-registration of the same order is
        a no-op). Called from ActionRouter when a GTD or FAK TP sell is placed.
        """
        if not server_order_id or target_qty <= 0:
            return
        if server_order_id in self._tp_snapshots:
            return
        self._tp_snapshots[server_order_id] = TpSnapshot(
            server_order_id=server_order_id,
            token_id=token_id,
            target_qty=target_qty,
        )

    def record_tp_fill(
        self,
        server_order_id: str,
        price: float,
        size: float,
    ) -> bool:
        """
        Reduce position state by a take-profit (SELL) fill.

        Returns:
            True  if this fill was matched to a registered TP snapshot
                  and position state was updated (caller should refresh
                  TP state via the response; _tp_inflight is cleared as
                  a side-effect).
            False if no matching snapshot exists.

        Side-effects:
            - _total_qty     -= size  (clamped at 0)
            - _total_cost    -= avg-cost * size  (proportional, weighted)
            - _avg_cost      recomputed from the residual
            - _tp_inflight   cleared (so engine can re-fire TP on residual)
            - snapshot       updated; dropped once size_sold >= target_qty
        """
        if size <= 0 or price <= 0:
            return False
        snap = self._tp_snapshots.get(server_order_id)
        if snap is None:
            return False

        snap.placed = True
        snap.size_sold += size
        snap.usdc_received += price * size

        # Reduce position. Cap each component to zero so a sell of "more than
        # we hold" (defensive: should never happen, but FAK can over-shoot
        # in shallow books) cannot push totals negative.
        prev_qty = self._total_qty
        prev_cost = self._total_cost
        sold = min(size, prev_qty)
        if sold <= 0:
            # No inventory to reduce. Don't touch avg_cost.
            self._tp_inflight = False
            if snap.size_sold + 1e-9 >= snap.target_qty:
                self._tp_snapshots.pop(server_order_id, None)
            return True

        # Pro-rata cost removal keeps the avg_cost basis intact.
        avg_before = prev_cost / prev_qty if prev_qty > 1e-9 else 0.0
        self._total_qty = max(0.0, prev_qty - sold)
        self._total_cost = max(0.0, prev_cost - avg_before * sold)
        if self._total_qty > 1e-9:
            self._avg_cost = self._total_cost / self._total_qty
        else:
            self._total_cost = 0.0
            self._avg_cost = 0.0

        # Any TP fill means a previous TP either fully drained the
        # inventory or partially filled — either way, release the mutex so
        # the engine can re-arm if mid is still above target.
        self._tp_inflight = False

        log.info(
            "position.tp_fill_recorded",
            server_order_id=server_order_id[:16],
            fill_price=price,
            fill_size=size,
            size_sold=snap.size_sold,
            target_qty=snap.target_qty,
            total_qty=self._total_qty,
            total_cost=self._total_cost,
            avg_cost=self._avg_cost,
            avg_before=avg_before,
        )

        # Snapshot is "done" once we have hit target; drop it so the next
        # fill for the same server_order_id is ignored. Keeps registry clean.
        if snap.size_sold + 1e-9 >= snap.target_qty:
            self._tp_snapshots.pop(server_order_id, None)

        return True

    def record_fill(self, tier: TierSnapshot, price: float, size: float) -> None:
        """Called when a tier receives (more) fills — supports partial fills.

        Strategy: size_filled and cost_filled always accumulate; filled flag
        only flips once size_filled has reached >= 99.5% of expected_size
        (via `tier.is_complete` which also consults `placed_qty` from the
        TierWatcher).

        For tier 1, baseline_price is set ONCE (on first partial fill) so
        averaging tiers can be triggered against the real cost basis.
        """
        if size <= 0 or price <= 0:
            return

        # If this is the first time we see a fill for this tier, mark placed=True.
        if not tier.placed:
            tier.placed = True

        tier.size_filled += size
        tier.cost_filled += price * size
        tier.avg_fill_price = tier.cost_filled / tier.size_filled if tier.size_filled > 1e-9 else price
        tier.last_fill_ts = __import__("time").time()

        if tier.is_complete:
            tier.filled = True

        # Position-level stats
        self._total_qty += size
        self._total_cost += price * size
        if self._total_qty > 0:
            self._avg_cost = self._total_cost / self._total_qty

        # Anchor baseline on first T1 fill (any partial fill counts).
        # NOTE: BUY-side fills must NOT clear _tp_inflight here — that flag
        # tracks a SELL-side TP runner in flight, and an averaging BUY fill
        # landing while TP is waiting is independent of TP's progress. Clearing
        # it here would let the next _evaluate() re-arm a second TP sell while
        # the first GTD sell is still live on the CLOB (B-NEW-1, audit v3).
        # _tp_inflight is cleared only inside record_tp_fill() when a real
        # TP fill actually lands.
        if tier.tier == 1 and not self.baseline_set and tier.size_filled > 0:
            self.baseline_price = tier.avg_fill_price or price
            self.baseline_set = True

        # H-V6-14: baseline_realized_price is the AUTHORITATIVE baseline for
        # T2/T3/T4 trigger lines once T1 has any fills. We compute it as the
        # LIVE volume-weighted average fill price on every T1 partial fill
        # (= Σ price*size / Σ size at the moment of this fill), so that
        # subsequent trigger evaluations are anchored to the real cost basis
        # even when T1 fills in many small partials (or slips between
        # partials against a thin orderbook).
        #
        # The first time baseline_set_realized flips True is the moment T2+
        # trigger math rebases from "design price" to "real cost basis"; the
        # transition is observable in logs as a one-shot `baseline.rebased`
        # event. After that, baseline_realized_price continues to track the
        # live T1 VWAP (so a continued drop on T1 — e.g. another 6-share
        # partial at 0.30 — would shift the baseline lower, and T2 trigger
        # re-evaluates against the new value on the next tick).
        #
        # IMPORTANT: this only updates on T1 fills. T2/T3/T4 fills do NOT
        # change baseline_realized_price — the anchor reflects what we paid
        # for our *initial position size*, not the running average across
        # all averaging tiers (which lives in _avg_cost for PnL math
        # already; we don't want averaging tiers to rebaseline themselves
        # — that would defeat the whole martingale math).
        if tier.tier == 1 and tier.size_filled > 1e-9:
            new_realized = tier.cost_filled / tier.size_filled
            if self.baseline_realized_price is None or abs(new_realized - self.baseline_realized_price) > 1e-9:
                self.baseline_realized_price = new_realized
            if not self.baseline_set_realized:
                self.baseline_set_realized = True

        log.info(
            "position.fill_recorded",
            tier=tier.tier,
            fill_price=price,
            fill_size=size,
            cumulative_size=tier.size_filled,
            expected_size=tier.size_expected,
            avg_fill_price=tier.avg_fill_price,
            tier_filled=tier.filled,
            total_qty=self._total_qty,
            avg_cost=self._avg_cost,
            baseline_price=self.baseline_price,
        )

    def mark_expired(self, tier: TierSnapshot, reason: str = "gtd_expired") -> None:
        """Mark a tier as expired (e.g. GTD expired without filling).

        We do NOT clear size_filled/cost_filled — partial fills must still
        be counted. We just reset placed so the strategy can decide whether
        to try again at the next averaging opportunity.

        H-NEW-2026-07-11: also clear `watching` so the engine does not
        treat this tier as in-flight any longer.
        """
        tier.placed = False
        tier.watching = False
        tier.server_order_id = None
        tier.cancel_reason = reason
        log.warning(
            "position.tier_expired",
            tier=tier.tier,
            size_filled=tier.size_filled,
            size_expected=tier.size_expected,
            reason=reason,
        )

    def apply_tier_watcher_result(
        self,
        tier: TierSnapshot,
        outcome: str,
        filled_qty: float,
    ) -> None:
        """Commit TierWatcher terminal state into the tier / position.

        Called by the ActionRouter's watcher callback when a tier's GTD
        has reached a final state (filled / expired / crossed / cancelled
        / over_filled / error).

        The watcher is the ground-truth source for "how many shares do I
        actually hold"; this method reconciles that into:

          * tier.placed_qty = max(prior, filled_qty)   # monotonic
          * tier.filled     = True if outcome == "filled" (or over_filled)
          * tier.watching   = False (watcher is done)
          * position totals _total_qty / _total_cost if filled_qty >
            tier.size_filled (catches fills missed by the WS path).
        """
        from src.execution.tier_watcher import TierOutcome

        # Monotonic — fill events may have already incremented size_filled.
        if filled_qty > tier.placed_qty:
            tier.placed_qty = filled_qty

        if outcome in (TierOutcome.FILLED.value, TierOutcome.OVER_FILLED.value):
            tier.filled = True
            # If the watcher caught fills that the WS path missed, advance
            # position-level totals so STATUS / TP math see them.
            if filled_qty > tier.size_filled + 1e-9:
                # We don't know the exact per-fill price here, so use
                # tier.avg_fill_price (set by record_fill) or the strategy
                # price as a fallback. This is a best-effort fix-up for
                # missed fill events; the size_filled path remains
                # authoritative when it works.
                fill_px = tier.avg_fill_price or 0.0
                if fill_px <= 0 and tier.size_expected > 0 and tier.notional > 0:
                    # Strategy price back-derivation = notional / size_expected.
                    fill_px = tier.notional / tier.size_expected
                if fill_px <= 0:
                    fill_px = 0.0
                missed = filled_qty - tier.size_filled
                # Only add positive missed qty; never inflate.
                if missed > 1e-9 and fill_px > 0:
                    self._total_qty += missed
                    self._total_cost += missed * fill_px
                    if self._total_qty > 0:
                        self._avg_cost = self._total_cost / self._total_qty
                    tier.size_filled += missed
                    tier.cost_filled += missed * fill_px
                    log.info(
                        "position.watcher_reconciled_missed_fills",
                        tier=tier.tier,
                        missed=missed,
                        fill_px=fill_px,
                        total_qty=self._total_qty,
                    )

            # Anchor baseline_realized on first T1 watcher fill (same as
            # record_fill does for the size_filled path).
            if tier.tier == 1 and tier.size_filled > 1e-9:
                new_realized = tier.cost_filled / tier.size_filled
                if (
                    self.baseline_realized_price is None
                    or abs(new_realized - self.baseline_realized_price) > 1e-9
                ):
                    self.baseline_realized_price = new_realized
                self.baseline_set_realized = True

        elif outcome == TierOutcome.EXPIRED.value:
            # Cancel-after-expiry — do not mark filled (it didn't fill).
            # keep partial fills in size_filled/cost_filled; just close out.
            pass
        elif outcome == TierOutcome.CROSSED.value:
            # Cancel because mid dropped past the next tier's zone. The
            # next tier watcher will fire from the next _evaluate tick.
            pass
        elif outcome in (TierOutcome.CANCELLED.value, TierOutcome.ERROR.value):
            # External / unexpected — keep partial fills, close out.
            pass

        tier.watching = False

        log.info(
            "position.tier_watcher_done",
            tier=tier.tier,
            outcome=outcome,
            filled_qty=filled_qty,
            placed_qty=tier.placed_qty,
            size_filled=tier.size_filled,
            size_expected=tier.size_expected,
            filled=tier.filled,
        )

    def tp_target(self) -> float | None:
        """Take-profit trigger price. None if no position.

        Decrements as more averaging tiers fire so the strategy captures
        profit sooner when already deep in a martingale sequence.
        Formula:
            TP_pct = max(TP_MIN_PCT,
                         TP_PROFIT_PCT - N * TP_DECREMENT_PCT)
        where N = number of completed tiers beyond T1
                 (T1 only → N=0; T1+T2 → N=1; ... T1..T4 → N=3).
        """
        if self._avg_cost <= 0:
            return None
        cfg = _s()
        # Count completed averaging tiers (tiers 2..4 that have fully filled).
        n = sum(1 for t in self.tiers[1:] if t.is_complete)
        tp_pct = cfg.TP_PROFIT_PCT - n * cfg.TP_DECREMENT_PCT
        tp_pct = max(cfg.TP_MIN_PCT, tp_pct)
        return self._avg_cost * (1 + tp_pct)

    def tp_pct_for_current_state(self) -> float:
        """The TP percentage currently in effect (for logging / tests).

        Same formula as `tp_target` but always returns a float even when
        `_avg_cost <= 0`. Useful for diagnostic logs that want to show the
        current TP target regardless of position state.
        """
        cfg = _s()
        n = sum(1 for t in self.tiers[1:] if t.is_complete)
        tp_pct = cfg.TP_PROFIT_PCT - n * cfg.TP_DECREMENT_PCT
        return max(cfg.TP_MIN_PCT, tp_pct)


# ---------------------------------------------------------------------------
# RoundState
# ---------------------------------------------------------------------------


@dataclass
class RoundState:
    """Complete per-round state machine."""

    round_start_ts: int
    market: "MarketInfo | None" = field(default=None, repr=False)

    # Derived from market — init=False so they don't appear in __init__ signature.
    # Defaults are set in __post_init__.
    slug: str | None = field(init=False, default=None)
    condition_id: str | None = field(init=False, default=None)
    up_token: str | None = field(init=False, default=None)
    down_token: str | None = field(init=False, default=None)
    tick_size: float = field(init=False, default=0.01)

    position: Position = field(default_factory=Position)
    finished: bool = False
    binance_baseline: float | None = None
    gate_last_state: str = ""

    def __post_init__(self) -> None:
        # Derived fields are set here because init=False excludes them from
        # the __init__ signature. When market is None they retain their defaults.
        if self.market is not None:
            self.slug = self.market.slug
            self.condition_id = self.market.condition_id
            self.up_token = self.market.up_token
            self.down_token = self.market.down_token
            self.tick_size = self.market.tick_size