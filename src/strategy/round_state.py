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
        placed=False   -> not yet sent
        placed=True, filled=False -> in-flight (possibly partially filled)
        placed=True, filled=True  -> fully filled

    size_filled accumulates across multiple partial fills; expected_size
    is captured at placement time (= notional / price) so we can detect
    when a tier is "complete enough" to flip filled=True.
    """

    tier: int                         # 1, 2, 3, 4
    notional: float                   # USD amount for this tier
    placed: bool = False
    filled: bool = False
    server_order_id: str | None = None
    avg_fill_price: float | None = None
    size_filled: float = 0.0          # total shares filled (sum of partial fills)
    size_expected: float = 0.0        # expected shares = notional / price at placement
    cost_filled: float = 0.0          # running notional of fills = Σ(px * sz)
    placed_at: float = 0.0
    expires_at: float = 0.0
    last_fill_ts: float = 0.0
    cancel_reason: str = ""
    token_id: str | None = None       # bound to the server order so fills can be matched

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
        """True if the tier has fully filled (>= 99.5% of expected size)."""
        if self.size_expected <= 0:
            return self.filled
        return self.size_filled >= self.size_expected * 0.995

    @property
    def is_expired(self) -> bool:
        """True if GTD has expired and the tier was never completed."""
        if self.expires_at <= 0:
            return False
        import time as _t
        return _t.time() >= self.expires_at and not self.is_complete


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
    def filled_tiers(self) -> list[TierSnapshot]:
        return [t for t in self.tiers if t.is_complete]

    @property
    def next_open_tier(self) -> TierSnapshot | None:
        """The first tier that is not yet fully filled (placed or pending)."""
        for t in self.tiers:
            if not t.is_complete:
                return t
        return None

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
        """How much more USD could be deployed before hitting total_max_notional."""
        return max(0.0, _s().total_max_notional - self._total_cost)

    def tier_notional_used(self, tier: TierSnapshot) -> float:
        """USD already filled into this tier (not including in-flight)."""
        return tier.cost_filled

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
        only flips once size_filled has reached >= 99.5% of expected_size.

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

        # Any fill landing while TP is in-flight means the previous TP did
        # not consume the full position (FAK partial), so allow the engine
        # to re-fire TP on the next tick if mid is still above target.
        if self._tp_inflight:
            self._tp_inflight = False
            log.info(
                "position.tp_inflight_cleared_by_fill",
                tier=tier.tier, total_qty=self._total_qty,
            )

        # Anchor baseline on first T1 fill (any partial fill counts)
        if tier.tier == 1 and not self.baseline_set and tier.size_filled > 0:
            self.baseline_price = tier.avg_fill_price or price
            self.baseline_set = True

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
        """
        tier.placed = False
        tier.server_order_id = None
        tier.cancel_reason = reason
        log.warning(
            "position.tier_expired",
            tier=tier.tier,
            size_filled=tier.size_filled,
            size_expected=tier.size_expected,
            reason=reason,
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

    def max_notional_exposed(self) -> float:
        """Sum of all tier notionals (worst-case if every tier fires)."""
        return sum(t.notional for t in self.tiers)


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