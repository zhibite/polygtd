"""Tests for the BTC feed-health and short-term volatility guards.

Added after the 2026-10-30 flash moves:
  * `BTC_STALE_MAX_AGE_S` — the Binance WS feed going quiet makes the drift
    gate meaningless; new entries/averaging pause (fail-closed) while TP
    exits stay enabled.
  * `BTC_VOL_MAX_MOVE_PCT` over `BTC_VOL_WINDOW_S` — a sharp rise/drop
    (flash move) blocks new entries/averaging for `BTC_VOL_COOLDOWN_S` and
    cancels resting BUY GTDs so they cannot fill mid-spike.
"""
from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from src.strategy.engine import ActionCancelTier, ActionTakeProfit, StrategyEngine
from src.strategy.round_state import Position


def _engine(*, btc_price: float = 100_000.0, ts_ms: int | None = None):
    eng = StrategyEngine.__new__(StrategyEngine)
    eng._bc = SimpleNamespace(last_price=btc_price, ts_ms=ts_ms or 0)
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.35)
    eng._gate = SimpleNamespace(
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


def _round(pos: Position, *, age_s: float = 10.0):
    return SimpleNamespace(
        position=pos,
        round_start_ts=int(time.time() - age_s),
        up_token="UP",
        down_token="DOWN",
    )


# ---------------------------------------------------------------------------
# B — feed staleness
# ---------------------------------------------------------------------------


def test_btc_feed_freshness() -> None:
    fresh = _engine(ts_ms=int(time.time() * 1000))
    stale = _engine(ts_ms=int((time.time() - 30) * 1000))
    unknown = _engine()

    assert fresh._btc_feed_stale() is False
    assert stale._btc_feed_stale() is True
    # No ts_ms (test stubs / feed never updated) is "unknown" → fail-open.
    assert unknown._btc_feed_stale() is False


@pytest.mark.asyncio
async def test_stale_feed_blocks_t1_entry() -> None:
    eng = _engine(ts_ms=int((time.time() - 30) * 1000))
    eng._round = _round(Position())

    await eng._check_t1_entry(bc_now=100_000.0)

    assert eng._actions == [], "stale BTC feed must not open T1"


@pytest.mark.asyncio
async def test_stale_feed_blocks_averaging() -> None:
    eng = _engine(ts_ms=int((time.time() - 30) * 1000))
    pos = Position()
    pos.entry_token = "UP"
    pos.entry_side = "UP"
    pos.baseline_realized_price = 0.40
    pos.baseline_set_realized = True
    pos._total_qty = 5.0
    pos._total_cost = 2.0
    pos._avg_cost = 0.40
    eng._round = _round(pos)

    await eng._check_average_down(bc_now=100_000.0)

    assert eng._actions == [], "stale BTC feed must not average down"


# ---------------------------------------------------------------------------
# A — short-term volatility blackout
# ---------------------------------------------------------------------------


def test_volatility_guard_arms_blackout_on_sharp_move() -> None:
    eng = _engine()
    now = time.time()
    eng._btc_samples = [(now - 30, 100_000.0), (now - 20, 100_050.0)]

    # (100_600 - 100_000) / 100_000 = 0.6% > 0.3% threshold.
    eng._update_btc_volatility(100_600.0)

    assert eng._btc_vol_blocked() is True


def test_volatility_guard_ignores_slow_drift() -> None:
    eng = _engine()
    now = time.time()
    eng._btc_samples = [(now - 30, 100_000.0), (now - 20, 100_050.0)]

    # (100_200 - 100_000) / 100_000 = 0.2% < 0.3% threshold.
    eng._update_btc_volatility(100_200.0)

    assert eng._btc_vol_blocked() is False


@pytest.mark.asyncio
async def test_vol_blackout_blocks_t1_and_averaging() -> None:
    eng = _engine()
    eng._btc_vol_block_until = time.time() + 60

    pos = Position()
    eng._round = _round(pos)
    await eng._check_t1_entry(bc_now=100_000.0)
    assert eng._actions == []

    pos.entry_token = "UP"
    pos.entry_side = "UP"
    pos.baseline_realized_price = 0.40
    pos.baseline_set_realized = True
    pos._total_qty = 5.0
    pos._total_cost = 2.0
    pos._avg_cost = 0.40
    await eng._check_average_down(bc_now=100_000.0)
    assert eng._actions == []


@pytest.mark.asyncio
async def test_vol_blackout_cancels_resting_buy_orders() -> None:
    eng = _engine()
    pos = Position()
    pos.entry_token = "UP"
    pos.tier1.placed = True
    pos.tier1.server_order_id = "s1"
    pos.tier2.placed = True
    pos.tier2.server_order_id = "s2"
    pos.tier3.placed = True
    pos.tier3.server_order_id = "s3"
    pos.tier3.filled = True  # complete tiers have nothing on the book
    pos.tier3.size_filled = 10.0
    pos.tier3.size_expected = 10.0
    eng._round = _round(pos)

    await eng._cancel_placed_buys("btc_vol_spike")

    assert [a.tier for a in eng._actions] == [1, 2]
    assert all(isinstance(a, ActionCancelTier) for a in eng._actions)
    assert all(a.reason == "btc_vol_spike" for a in eng._actions)
    assert pos.tier1.cancel_pending is True
    assert pos.tier2.cancel_pending is True


@pytest.mark.asyncio
async def test_vol_blackout_does_not_block_take_profit() -> None:
    """Exits must stay enabled during a volatility blackout."""
    eng = _engine()
    eng._btc_vol_block_until = time.time() + 60
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.40)

    pos = Position()
    pos.entry_token = "UP"
    pos.entry_side = "UP"
    pos._total_qty = 10.0
    pos._total_cost = 3.0
    pos._avg_cost = 0.30
    pos.tier1.size_filled = 10.0
    pos.tier1.cost_filled = 3.0
    pos.baseline_realized_price = 0.30
    pos.baseline_set_realized = True
    eng._round = _round(pos)

    await eng._check_take_profit(bc_now=100_000.0)

    assert len(eng._actions) == 1
    assert isinstance(eng._actions[0], ActionTakeProfit)


# ---------------------------------------------------------------------------
# T4 time-based drift bypass
# ---------------------------------------------------------------------------


def _t4_position() -> Position:
    pos = Position()
    pos.entry_token = "UP"
    pos.entry_side = "UP"
    pos.baseline_realized_price = 0.38
    pos.baseline_set_realized = True
    pos._total_qty = 36.77
    pos._total_cost = 9.68
    pos._avg_cost = 9.68 / 36.77
    for tier, filled, cost in (
        (pos.tier1, 5.0, 1.90),
        (pos.tier2, 9.86, 2.958),
        (pos.tier3, 21.92, 4.822),
    ):
        tier.size_filled = filled
        tier.cost_filled = cost
        tier.filled = True
    return pos


def _round_with_end(pos: Position, seconds_left: float):
    from datetime import datetime, timedelta, timezone

    return SimpleNamespace(
        position=pos,
        round_start_ts=int(time.time() - 300),
        up_token="UP",
        down_token="DOWN",
        market=SimpleNamespace(
            window_end=datetime.now(timezone.utc) + timedelta(seconds=seconds_left)
        ),
    )


@pytest.mark.asyncio
async def test_t4_time_bypass_places_t4_despite_drift_fail() -> None:
    """Window has 10 min left → T4 may fire even though drift FAILs."""
    eng = _engine()
    eng._gate.can_double_down = lambda *a, **k: False
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.10)  # below T4 fire line
    eng._round = _round_with_end(_t4_position(), 600)

    await eng._check_average_down(bc_now=100_000.0)

    assert len(eng._actions) == 1
    assert eng._actions[0].tier == 4
    assert eng._actions[0].price == pytest.approx(0.152)


@pytest.mark.asyncio
async def test_t4_time_bypass_does_not_apply_with_little_time_left() -> None:
    """Window has only 5 min left → T4 stays blocked by the drift gate."""
    eng = _engine()
    eng._gate.can_double_down = lambda *a, **k: False
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.10)
    eng._round = _round_with_end(_t4_position(), 300)

    await eng._check_average_down(bc_now=100_000.0)

    assert eng._actions == []


@pytest.mark.asyncio
async def test_t4_time_bypass_can_be_disabled() -> None:
    eng = _engine()
    eng._s.T4_DRIFT_BYPASS_MIN_REMAINING_S = 0.0
    eng._gate.can_double_down = lambda *a, **k: False
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.10)
    eng._round = _round_with_end(_t4_position(), 600)

    await eng._check_average_down(bc_now=100_000.0)

    assert eng._actions == []
