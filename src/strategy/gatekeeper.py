"""GateKeeper — BTC spot drift gate for all order actions.

The GateKeeper enforces the Binance spot drift limit relative to the
round-start baseline.  No order (open / average-down / TP) is placed
unless the current drift is within the configured threshold.

Drift bypass for averaging tiers:
  When a tier's price trigger has been reached (e.g. T2 drops 20% from
  baseline), the trade is justified by price movement itself, not direction.
  In that case drift is bypassed — the GateKeeper signals "force allow".
  This lets averaging proceed even when BTC has drifted out of the ±0.15% band,
  as long as the martingale entry condition is met.

Two separate "baseline" concepts:
  - binance_baseline : used ONLY for drift check (open vs round start)
  - position.baseline (avg_fill_price) : used for tier drop-pct triggers
"""
from __future__ import annotations

from dataclasses import dataclass
from src.config import strategy as _s
from src.infra.logging import get_logger

log = get_logger(__name__)


@dataclass
class GateState:
    binance_baseline: float
    binance_now: float
    drift: float          # |now - baseline| / baseline
    within_limit: bool

    def __str__(self) -> str:
        flag = "PASS" if self.within_limit else "FAIL"
        return (
            f"baseline={self.binance_baseline:,.1f}  "
            f"now={self.binance_now:,.1f}  "
            f"drift={self.drift:.4f}  "
            f"limit={_s().BASELINE_DRIFT_LIMIT:.4f}  "
            f"[{flag}]"
        )


class GateKeeper:
    """
    Decides whether any new order action is permitted by spot drift.

    Entry (T1):    drift must be within limit (full gate)
    Average-down:   drift is bypassed when the tier's trigger price is reached
    Take-profit:    drift must be within limit (full gate)
    """

    def __init__(self, binance_baseline: float) -> None:
        self._baseline = binance_baseline
        self._drift_limit = _s().BASELINE_DRIFT_LIMIT

    @property
    def baseline(self) -> float:
        return self._baseline

    def drift(self, binance_now: float) -> float:
        if binance_now <= 0 or self._baseline <= 0:
            return float("inf")
        return abs(binance_now - self._baseline) / self._baseline

    def state(self, binance_now: float) -> GateState:
        d = self.drift(binance_now)
        return GateState(
            binance_baseline=self._baseline,
            binance_now=binance_now,
            drift=d,
            within_limit=d <= self._drift_limit,
        )

    def can_act(self, binance_now: float) -> bool:
        """True when current spot is within drift limit vs round-start baseline."""
        return self.state(binance_now).within_limit

    def can_enter(self, binance_now: float, token_price: float) -> bool:
        """
        Full entry check for T1: drift OK + token price within cap.
        Drift is always enforced for first entry.
        """
        cap = _s().ENTRY_PRICE_CAP
        return self.can_act(binance_now) and token_price <= cap

    def can_double_down(
        self,
        binance_now: float,
        token_price: float,
        current_tier: int,
        next_tier_drop_pct: float | None,
        t1_entry: float,
    ) -> bool:
        """
        Average-down check for T2/T3/T4.

        H-NEW-2026-07-15 (audit follow-up — naming): the `t1_entry` argument
        is the token T1 fill-weighted average price, NOT the BTC/binance
        baseline used by `GateKeeper.__init__`. The two share the legacy
        name "baseline" but mean very different things; renamed here to
        remove the BTC-vs-token confusion.

        Drift bypass: if price has already dropped to the *next* tier's trigger
        (e.g. checking T2 but price has already hit T3's -40% line), the trade
        is justified by price momentum itself and drift is ignored.

        T4 has no next tier — drift is always checked.
        """
        if next_tier_drop_pct is not None:
            next_trigger = t1_entry * (1 - next_tier_drop_pct)
            if token_price <= next_trigger:
                log.debug(
                    "gatekeeper.drift_bypass",
                    tier=current_tier, token=token_price, next_trigger=next_trigger,
                )
                return True

        return self.can_act(binance_now)
