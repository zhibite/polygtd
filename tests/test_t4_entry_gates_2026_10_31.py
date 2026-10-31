"""Tests for the T4 entry gates added 2026-10-31.

T4 is the largest ladder step (8 of the 15 USD planned exposure) and sits
60% below T1. A fixed price drop alone cannot tell an overreaction from a
decisive trend, so T4 is additionally gated on:

  * window time left (`T4_MIN_REMAINING_S`),
  * BTC distance to the window strike (`T4_MAX_STRIKE_Z`),
  * not arming after a gap far through the trigger
    (`T4_MAX_GAP_BELOW_TRIGGER_PCT`).

The strike gate must also bind the drift-FAIL time bypass, otherwise a
trending window is exactly where the bypass would add the largest layer.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from src.strategy.engine import StrategyEngine
from src.strategy.round_state import Position


def _engine(*, btc_price: float = 100_000.0, baseline: float = 100_000.0):
    eng = StrategyEngine.__new__(StrategyEngine)
    eng._bc = SimpleNamespace(
        last_price=btc_price, ts_ms=int(time.time() * 1000)
    )
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.155)
    eng._gate = SimpleNamespace(
        baseline=baseline,
        can_enter=lambda bc, mid: True,
        can_act=lambda bc: True,
        can_double_down=lambda *a, **k: True,
        state=lambda bc: SimpleNamespace(drift=0.0, within_limit=True),
    )
    eng._s = SimpleNamespace(
        ENTRY_PRICE_CAP=0.40,
        PRICE_PRE_MARGIN=0.10,
        CANCEL_HYSTERESIS_PCT=0.02,
        TP_PROFIT_PCT=0.20,
        TP_DECREMENT_PCT=0.03,
        TP_MIN_PCT=0.05,
        T4_MIN_REMAINING_S=240.0,
        T4_STRIKE_GATE_ENABLED=True,
        T4_MAX_STRIKE_Z=2.0,
        T4_STRIKE_SIGMA_PCT_PER_MIN=0.05,
        T4_MAX_GAP_BELOW_TRIGGER_PCT=0.05,
    )
    eng._t1_skip_last_log_ts = {}
    eng._t1_cap_reject_first_seen = {}
    eng._t1_cap_reject_summary_logged = {}
    eng._tN_skip_last_log_ts = {}
    eng._tp_last_post_error_ts = 0.0
    eng._tp_post_failure_count = 0
    eng._tp_backoff_s = 5.0
    actions: list = []

    async def handler(action) -> None:  # noqa: ANN001
        actions.append(action)

    eng._handler = handler
    eng._actions = actions
    return eng


def _position(side: str = "DOWN") -> Position:
    """T1..T3 filled (7 USD committed), leaving the 8-USD T4 budget."""
    pos = Position()
    pos.entry_token = side
    pos.entry_side = side
    pos.baseline_realized_price = 0.38
    pos.baseline_set_realized = True
    pos._total_qty = 2.5 + 6.41 + 17.09
    pos._total_cost = 1.0 + 2.0 + 4.0
    pos._avg_cost = pos._total_cost / pos._total_qty
    for tier, filled, cost in (
        (pos.tier1, 2.5, 1.0),
        (pos.tier2, 6.41, 2.0),
        (pos.tier3, 17.09, 4.0),
    ):
        tier.size_filled = filled
        tier.cost_filled = cost
        tier.filled = True
    return pos


def _round(pos: Position, seconds_left: float):
    return SimpleNamespace(
        position=pos,
        round_start_ts=int(time.time() - 300),
        up_token="UP",
        down_token="DOWN",
        market=SimpleNamespace(
            window_end=datetime.now(timezone.utc)
            + timedelta(seconds=seconds_left)
        ),
    )


# ---------------------------------------------------------------------------
# Time floor
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t4_skipped_when_window_too_late() -> None:
    eng = _engine()
    eng._round = _round(_position(), 180)  # < T4_MIN_REMAINING_S (240)

    await eng._check_average_down(bc_now=100_000.0)

    assert eng._actions == []


@pytest.mark.asyncio
async def test_t4_allowed_when_time_and_strike_ok() -> None:
    eng = _engine()
    eng._round = _round(_position("DOWN"), 300)  # z = 0 (BTC at the strike)

    await eng._check_average_down(bc_now=100_000.0)

    assert len(eng._actions) == 1
    assert eng._actions[0].tier == 4
    assert eng._actions[0].price == pytest.approx(0.152)


# ---------------------------------------------------------------------------
# Distance-to-strike gate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t4_strike_gate_blocks_adverse_btc_trend() -> None:
    eng = _engine()
    eng._round = _round(_position("DOWN"), 300)
    # +0.25% above the strike with 5 min left → z ≈ 2.24 > 2.0.
    await eng._check_average_down(bc_now=100_250.0)

    assert eng._actions == []


@pytest.mark.asyncio
async def test_t4_strike_gate_is_mirrored_for_up_positions() -> None:
    up = _engine()
    up._round = _round(_position("UP"), 300)
    # −0.25% below the strike is adverse for UP → blocked.
    await up._check_average_down(bc_now=99_750.0)
    assert up._actions == []

    down = _engine()
    down._round = _round(_position("DOWN"), 300)
    # The same move is favourable for DOWN → allowed.
    await down._check_average_down(bc_now=99_750.0)
    assert len(down._actions) == 1


@pytest.mark.asyncio
async def test_t4_strike_gate_binds_the_drift_bypass() -> None:
    """The drift-FAIL time bypass must not ignore the strike distance."""
    blocked = _engine()
    blocked._gate.can_double_down = lambda *a, **k: False
    blocked._round = _round(_position("DOWN"), 600)  # bypass time is met
    # +0.4% with 10 min left → z ≈ 2.53 > 2.0.
    await blocked._check_average_down(bc_now=100_400.0)
    assert blocked._actions == []

    allowed = _engine()
    allowed._gate.can_double_down = lambda *a, **k: False
    allowed._round = _round(_position("DOWN"), 600)
    await allowed._check_average_down(bc_now=100_050.0)
    assert len(allowed._actions) == 1


@pytest.mark.asyncio
async def test_t4_strike_gate_can_be_disabled() -> None:
    eng = _engine()
    eng._s.T4_STRIKE_GATE_ENABLED = False
    eng._round = _round(_position("DOWN"), 300)

    await eng._check_average_down(bc_now=101_000.0)  # far above the strike

    assert len(eng._actions) == 1


@pytest.mark.asyncio
async def test_t4_strike_gate_fails_open_without_inputs() -> None:
    """A missing BTC baseline must not wedge the ladder."""
    eng = _engine(baseline=0.0)
    eng._round = _round(_position("DOWN"), 300)

    await eng._check_average_down(bc_now=200_000.0)

    assert len(eng._actions) == 1


# ---------------------------------------------------------------------------
# Gap-through gate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t4_gap_gate_waits_after_a_gap_through() -> None:
    eng = _engine()
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.10)  # 34% below 0.152
    eng._round = _round(_position(), 300)

    await eng._check_average_down(bc_now=100_000.0)

    assert eng._actions == []


@pytest.mark.asyncio
async def test_t4_gap_gate_allows_a_small_gap() -> None:
    eng = _engine()
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.15)  # 1.3% below 0.152
    eng._round = _round(_position(), 300)

    await eng._check_average_down(bc_now=100_000.0)

    assert len(eng._actions) == 1


# ---------------------------------------------------------------------------
# Sigma estimate
# ---------------------------------------------------------------------------


def test_btc_sigma_uses_the_sixty_second_range() -> None:
    eng = _engine()
    now = time.time()
    eng._btc_samples = [
        (now - 50, 100_000.0),
        (now - 30, 100_200.0),
        (now - 20, 100_100.0),
        (now - 5, 100_150.0),
    ]
    # 0.2% range over the minute → σ ≈ 0.1% per minute (range / 2).
    assert eng._btc_sigma_1m() == pytest.approx(0.001)

    eng._btc_samples = []
    # Thin sample → the configured floor (0.05% = 0.0005).
    assert eng._btc_sigma_1m() == pytest.approx(0.0005)
