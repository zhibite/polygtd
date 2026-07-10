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
from typing import Literal

from src.config import strategy as _s
from src.infra.logging import get_logger

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
    def is_expired(self, now: float | None = None) -> bool:
        """True if GTD has expired and the tier was never completed."""
        if self.expires_at <= 0:
            return False
        if now is None:
            import time as _t
            now = _t.time()
        return now >= self.expires_at and not self.is_complete


# ---------------------------------------------------------------------------
# Position
# ---------------------------------------------------------------------------


@dataclass
class Position:
    """Live position within a round."""

    entry_side: Literal["UP", "DOWN"] | None = None
    entry_token: str | None = None

    tier1: TierSnapshot = field(
        default_factory=lambda: TierSnapshot(tier=1, notional=_s().TIER1_NOTIONAL)
    )
    tier2: TierSnapshot = field(
        default_factory=lambda: TierSnapshot(tier=2, notional=_s().TIER2_NOTIONAL)
    )
    tier3: TierSnapshot = field(
        default_factory=lambda: TierSnapshot(tier=3, notional=_s().TIER3_NOTIONAL)
    )
    tier4: TierSnapshot = field(
        default_factory=lambda: TierSnapshot(tier=4, notional=_s().TIER4_NOTIONAL)
    )

    baseline_price: float | None = None   # anchor: avg fill price after T1 fills
    baseline_set: bool = False             # becomes True once baseline_price is anchored

    # Cached position-level stats (recalculated after each fill)
    _total_qty: float = 0.0
    _total_cost: float = 0.0
    _avg_cost: float = 0.0

    @property
    def tiers(self) -> list[TierSnapshot]:
        return [self.tier1, self.tier2, self.tier3, self.tier4]

    @property
    def filled_tiers(self) -> list[TierSnapshot]:
        return [t for t in self.tiers if t.is_complete]

    @property
    def next_open_tier(self) -> TierSnapshot | None:
        """The first tier that is placed but not yet filled, or next to place."""
        for t in self.tiers:
            if not t.is_complete:
                return t
        return None

    @property
    def next_unfilled_tier(self) -> TierSnapshot | None:
        """The first tier that has not been placed or has only partially filled."""
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
        """Take-profit trigger price. None if no position."""
        if self._avg_cost <= 0:
            return None
        return self._avg_cost * (1 + _s().TP_PROFIT_PCT)

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
    slug: str | None = None
    condition_id: str | None = None
    up_token: str | None = None
    down_token: str | None = None
    tick_size: float = 0.01

    position: Position = field(default_factory=Position)
    finished: bool = False
    binance_baseline: float | None = None

    # Snapshot of gate when it last changed (for logging)
    gate_last_state: str = ""