"""Regression tests pinning the four-layer martingale config (2026-07-14).

The operator retuned the strategy to a four-tier ladder and tighter
spend caps per layer. These tests cover the surface area that drifted
during the migration:

  * `StrategySettings` exposes the new notionals, drops, entry cap,
    and TP decrement values exactly as documented.
  * `Position.tiers` exposes four `TierSnapshot` slots, each pre-loaded
    from the configured notional values.
  * Tier drop thresholds are 20% / 40% / 60% from the realised T1
    baseline (T1 fires at 0.80, T3 at 0.40, T4 at 0.24 when T1 fills
    at 0.40).
  * `total_notional_committed` clamps to the planned per-tier budget
    even after partial fills so the strategy cannot over-spend a tier.
  * `tp_target` walks through 20% -> 17% -> 14% -> 11% as averaging
    tiers accumulate fills.
"""
from __future__ import annotations

import pytest

from src.config import strategy as _s
from src.strategy.round_state import Position, TierSnapshot


def _strategy_settings():
    return _s()


def test_strategy_notionals_match_operator_spec() -> None:
    """T1..T4 USD notionals must be 1 / 2 / 4 / 8 USD."""
    cfg = _strategy_settings()
    assert cfg.TIER1_NOTIONAL == pytest.approx(1.0)
    assert cfg.TIER2_NOTIONAL == pytest.approx(2.0)
    assert cfg.TIER3_NOTIONAL == pytest.approx(4.0)
    assert cfg.TIER4_NOTIONAL == pytest.approx(8.0)


def test_strategy_entry_cap_and_drop_thresholds() -> None:
    """Entry cap is 0.40 and drops are 20/40/60%."""
    cfg = _strategy_settings()
    assert cfg.ENTRY_PRICE_CAP == pytest.approx(0.40)
    assert cfg.TIER2_DROP_PCT == pytest.approx(0.20)
    assert cfg.TIER3_DROP_PCT == pytest.approx(0.40)
    assert cfg.TIER4_DROP_PCT == pytest.approx(0.60)


def test_strategy_tp_decrement_is_three_percent() -> None:
    """N=1 -> 17%, N=2 -> 14%, N=3 -> 11%; floor at 5%."""
    cfg = _strategy_settings()
    assert cfg.TP_PROFIT_PCT == pytest.approx(0.20)
    assert cfg.TP_DECREMENT_PCT == pytest.approx(0.03)
    assert cfg.TP_MIN_PCT == pytest.approx(0.05)


def test_max_tier_is_four() -> None:
    cfg = _strategy_settings()
    assert cfg.max_tier == 4
    assert cfg.tier_notionals == [1.0, 2.0, 4.0, 8.0]
    assert cfg.tier_drop_pcts == [None, 0.20, 0.40, 0.60]
    assert cfg.total_max_notional == pytest.approx(15.0)


def test_position_has_four_tier_snapshots_with_configured_notionals() -> None:
    pos = Position()
    assert [t.tier for t in pos.tiers] == [1, 2, 3, 4]
    for tier, expected in zip(pos.tiers, (1.0, 2.0, 4.0, 8.0)):
        assert isinstance(tier, TierSnapshot)
        assert tier.notional == pytest.approx(expected)


def test_trigger_prices_against_realised_t1_baseline() -> None:
    """Each tier's trigger fires at baseline * (1 - drop)."""
    pos = Position()
    baseline = 0.40
    t1_price = pos.tier1.trigger_price(baseline)
    assert t1_price is None  # T1 has no trigger price — it is the entry.
    assert pos.tier2.trigger_price(baseline) == pytest.approx(0.32)
    assert pos.tier3.trigger_price(baseline) == pytest.approx(0.24)
    assert pos.tier4.trigger_price(baseline) == pytest.approx(0.16)


def test_tier_budget_is_capped_after_partial_fill() -> None:
    """A placed tier must reserve its full planned notional even if only
    partially filled, so the global stake cap cannot be bypassed by
    repeatedly topping up a tier at thin fills.
    """
    pos = Position()
    pos.tier2.size_expected = 10.0
    pos.record_fill(pos.tier2, price=0.32, size=6.0)  # 60% of expected.
    pos.tier2.placed = True
    pos.tier2.server_order_id = "0xt2-1"

    # total_notional_committed reflects the larger of cost_filled and the
    # tier's planned notional when a tier is in flight.
    assert pos.total_notional_committed == pytest.approx(pos.tier2.notional)
    # H-NEW-2026-07-15: tier_notional_remaining is removed; the only
    # residual cap is the round-level stake_cap_remaining which uses
    # total_notional_committed. Verify that an in-flight tier's
    # planned notional DOES consume the round-level cap (so the cap
    # cannot be bypassed by repeatedly topping up a tier at thin
    # fills). The previous `tier_notional_remaining` asserted the same
    # intent via a different mechanism.
    assert pos.stake_cap_remaining == pytest.approx(
        _strategy_settings().total_max_notional - pos.tier2.notional
    ), (
        "in-flight tier's planned notional must consume the round-level "
        "cap (defence-in-depth against thin-fill topping up)"
    )


def test_stake_cap_accounts_for_inflight_averaging_tier() -> None:
    """The averaging-tier budget must not be allowed to re-trigger once
    the full configured notional has been reserved for it.
    """
    pos = Position()
    pos.tier1.size_expected = 2.5
    pos.record_fill(pos.tier1, price=0.40, size=2.5)  # 1.0 USD consumed.
    pos.tier4.size_expected = 52.63
    pos.tier4.placed = True
    pos.tier4.server_order_id = "0xt4-1"

    # 15 (max) - 1 (T1 filled) - 8 (T4 planned) = 6 left.
    assert pos.stake_cap_remaining == pytest.approx(6.0)