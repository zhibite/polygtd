"""Tests for tier-decrementing take-profit (TP_PROFIT_PCT - N * TP_DECREMENT_PCT).

Formula:
    TP_pct = max(TP_MIN_PCT, TP_PROFIT_PCT - N * TP_DECREMENT_PCT)
    TP_target = avg_cost * (1 + TP_pct)
where N = number of averaging tiers with fills (T2..T4).

With TP_DECREMENT_PCT=0.03, the targets are 20%, 17%, 14%, and 11%.
"""
from __future__ import annotations

from src.config import strategy as _s
from src.strategy.round_state import Position


def _make_position_with_avg_cost(avg: float) -> Position:
    """Build a Position whose avg_cost property evaluates to `avg`."""
    pos = Position()
    if avg > 0:
        # Use tier1 record_fill so total_qty/total_cost/avg_cost are coherent.
        # avg = total_cost / total_qty ⇒ if avg=0.40, total_qty=10, total_cost=4.
        qty = 10.0
        pos.record_fill(pos.tier1, price=avg, size=qty)
    return pos


def test_tp_t1_only_is_20_pct() -> None:
    """N=0 (no averaging tiers filled) → TP = 20%."""
    pos = _make_position_with_avg_cost(0.40)
    cfg = _s()
    expected = 0.40 * (1 + cfg.TP_PROFIT_PCT)  # = 0.48
    assert pos.tp_target() == expected
    assert pos.tp_pct_for_current_state() == cfg.TP_PROFIT_PCT


def test_tp_t2_filled_is_17_pct() -> None:
    """N=1 (T2 has fills) → TP = 17%."""
    pos = _make_position_with_avg_cost(0.40)
    cfg = _s()
    # Mark tier2 as fully filled via record_fill on a 99.5%+ fill.
    pos.tier2.size_expected = 10.0
    pos.record_fill(pos.tier2, price=0.32, size=10.0)  # ≥99.5% triggers is_complete

    expected_pct = cfg.TP_PROFIT_PCT - 1 * cfg.TP_DECREMENT_PCT  # 0.17
    assert pos.tp_pct_for_current_state() == pytest.approx(expected_pct)
    expected = pos.avg_cost * (1 + expected_pct)
    assert pos.tp_target() == pytest.approx(expected)


def test_tp_t3_filled_is_14_pct() -> None:
    """N=2 (T2 and T3 have fills) → TP = 14%."""
    pos = _make_position_with_avg_cost(0.40)
    cfg = _s()
    for t in (pos.tier2, pos.tier3):
        t.size_expected = 10.0
        pos.record_fill(t, price=0.20, size=10.0)

    expected_pct = cfg.TP_PROFIT_PCT - 2 * cfg.TP_DECREMENT_PCT  # 0.14
    assert pos.tp_pct_for_current_state() == pytest.approx(expected_pct)


def test_tp_t4_filled_is_11_pct() -> None:
    """N=3 (all three averaging tiers have fills) → TP = 11%."""
    pos = _make_position_with_avg_cost(0.40)
    cfg = _s()
    for tier in (pos.tier2, pos.tier3, pos.tier4):
        tier.size_expected = 10.0
        pos.record_fill(tier, price=0.16, size=10.0)

    expected_pct = cfg.TP_PROFIT_PCT - 3 * cfg.TP_DECREMENT_PCT
    assert pos.tp_pct_for_current_state() == pytest.approx(expected_pct)
    assert pos.tp_target() == pytest.approx(pos.avg_cost * (1 + expected_pct))


def test_tp_partial_tier_counts_as_one_averaging_fill() -> None:
    """A partially filled averaging order still counts toward N once capital is deployed."""
    pos = _make_position_with_avg_cost(0.40)
    cfg = _s()
    pos.tier2.size_expected = 100.0
    pos.record_fill(pos.tier2, price=0.32, size=80.0)

    assert pos.tier2.is_complete is False
    expected_pct = cfg.TP_PROFIT_PCT - cfg.TP_DECREMENT_PCT
    assert pos.tp_pct_for_current_state() == pytest.approx(expected_pct)


def test_tp_floor_at_min_pct(monkeypatch) -> None:
    """When the natural formula would underflow, TP must clamp at TP_MIN_PCT.

    We can't easily mutate the pydantic BaseSettings instance, so we
    patch the module-level `_s()` reference inside round_state to return
    a stand-in with TP_DECREMENT_PCT=0.10.
    """
    import src.strategy.round_state as rs_mod

    class StubCfg:
        TIER1_NOTIONAL = 2.0
        TIER2_NOTIONAL = 3.0
        TIER3_NOTIONAL = 5.0
        TIER4_NOTIONAL = 6.0
        TP_PROFIT_PCT = 0.20
        TP_DECREMENT_PCT = 0.10
        TP_MIN_PCT = 0.05
        # H-NEW-2026-07-15: TIER_NOTIONAL_MAX_MULT removed.
        total_max_notional = 16.0

    monkeypatch.setattr(rs_mod, "_s", lambda: StubCfg())

    pos = _make_position_with_avg_cost(0.40)
    # Mark T2 complete (the prior fills are stale under stub cfg — recompute)
    pos.tier2.size_expected = 10.0
    pos.record_fill(pos.tier2, price=0.20, size=10.0)
    assert pos.tier2.is_complete is True
    # N=1 → max(0.05, 0.20 - 1*0.10) = max(0.05, 0.10) = 0.10
    assert pos.tp_pct_for_current_state() == 0.10

    # Mark T3 complete → N=2 → max(0.05, 0.20 - 0.20) = max(0.05, 0) = 0.05 (floor)
    pos.tier3.size_expected = 10.0
    pos.record_fill(pos.tier3, price=0.20, size=10.0)
    assert pos.tp_pct_for_current_state() == 0.05

    # Once the floor is reached it remains in force after T4 fills.
    pos.tier4.size_expected = 10.0
    pos.record_fill(pos.tier4, price=0.20, size=10.0)
    assert pos.tp_pct_for_current_state() == 0.05


def test_tp_target_returns_none_when_no_position() -> None:
    """tp_target returns None if no avg_cost (no fills)."""
    pos = Position()
    assert pos._avg_cost == 0.0
    assert pos.tp_target() is None


def test_tp_pct_helper_works_without_position() -> None:
    """tp_pct_for_current_state always returns a float (diagnostic-safe)."""
    pos = Position()
    cfg = _s()
    # N=0 → 20% regardless of avg_cost
    assert pos.tp_pct_for_current_state() == cfg.TP_PROFIT_PCT


import pytest  # noqa: E402  (placed at bottom for readability of test list above)