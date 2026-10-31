"""Tests for the post-take-profit entry block.

Operator rule (2026-10-31): once a round has taken profit, T1 must NOT
enter again in the same 15-minute window — even when the TP filled with
plenty of time left. Re-entering after a profit was judged too risky.
`entry_token` is deliberately kept by `realize_position_closed`, which is
the guard `_check_t1_entry` uses to block a fresh ladder.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from src.strategy.engine import StrategyEngine
from src.strategy.round_state import Position


def _engine(**config):
    eng = StrategyEngine.__new__(StrategyEngine)
    eng._bc = SimpleNamespace(last_price=100_000.0, ts_ms=0)
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.40)
    eng._gate = SimpleNamespace(
        can_act=lambda bc: True,
        can_enter=lambda bc, mid: True,
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
        **config,
    )
    eng._tp_last_post_error_ts = 0.0
    eng._tp_post_failure_count = 0
    eng._tp_backoff_s = 5.0
    return eng


def _residual_position() -> Position:
    """Position just after a full TP: floating-point residual ~0.004."""
    pos = Position()
    pos.entry_token = "UP"
    pos.entry_side = "UP"
    pos._total_qty = 0.0041
    pos._total_cost = 0.001
    pos._avg_cost = 0.25
    pos._tp_sold_qty = 5.0
    pos.baseline_realized_price = 0.38
    pos.baseline_set_realized = True
    pos.tier1.size_filled = 5.0
    pos.tier1.cost_filled = 1.9
    pos.tier1.filled = True
    return pos


def _round_with_end(pos: Position, seconds_left: float):
    return SimpleNamespace(
        position=pos,
        round_start_ts=int(time.time() - 60),
        up_token="UP",
        down_token="DOWN",
        market=SimpleNamespace(
            window_end=datetime.now(timezone.utc) + timedelta(seconds=seconds_left)
        ),
    )


@pytest.mark.asyncio
async def test_early_tp_blocks_t1_for_the_rest_of_the_window() -> None:
    """An early TP (13 min left) must still end this window's entries."""
    eng = _engine()
    pos = _residual_position()
    eng._round = _round_with_end(pos, 800)

    await eng._check_take_profit(bc_now=100_000.0)

    assert pos.entry_token == "UP", (
        "a TP exit must block T1 until the next window, even with time left"
    )
    assert pos.entry_side == "UP"
    assert pos._total_qty == 0.0
    assert pos._tp_sold_qty == 0.0
    assert pos._position_closed is True
    assert all(t.size_filled == 0.0 for t in pos.tiers)
    assert all(not t.placed and not t.filled for t in pos.tiers)


@pytest.mark.asyncio
async def test_late_tp_keeps_single_entry_per_window() -> None:
    eng = _engine()
    pos = _residual_position()
    eng._round = _round_with_end(pos, 600)  # only 10 min left: not early

    await eng._check_take_profit(bc_now=100_000.0)

    assert pos.entry_token == "UP", (
        "late TP must keep the round's entry_token so T1 cannot re-enter"
    )
    assert pos._total_qty == 0.0
    # The full-close cleanup resets the tier panel too (the dashboard must
    # show all-idle once the position is gone).
    assert pos.tier1.size_filled == 0.0
    assert pos.tier1.filled is False
    assert pos.tier1.server_order_id is None


@pytest.mark.asyncio
async def test_tp_uses_a_taker_sell_below_5_shares() -> None:
    """A sole 1-USD T1 is ~2.5 shares; a GTD SELL would be rejected.

    The engine must emit a taker TP action (FAK market sell) instead of
    posting a doomed GTD order.
    """
    captured: list = []

    async def handler(action) -> None:  # noqa: ANN001
        captured.append(action)

    eng = _engine()
    eng._handler = handler
    pos = Position()
    pos.entry_token = "UP"
    pos.entry_side = "UP"
    pos._total_qty = 2.5
    pos._total_cost = 1.0
    pos._avg_cost = 0.40
    pos.baseline_realized_price = 0.40
    pos.baseline_set_realized = True
    pos.tier1.size_filled = 2.5
    pos.tier1.cost_filled = 1.0
    pos.tier1.filled = True
    eng._round = _round_with_end(pos, 600)
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.50)  # above TP

    await eng._check_take_profit(bc_now=100_000.0)

    assert len(captured) == 1
    action = captured[0]
    assert action.taker is True
    assert action.qty == pytest.approx(2.5)
    assert action.price == pytest.approx(0.50)
    assert pos._tp_inflight is True


@pytest.mark.asyncio
async def test_exact_zero_tp_close_cleans_tier_state_once() -> None:
    """A TP that sells the exact quantity leaves total_qty == 0 (not an FP
    residual). The cleanup must still run — once — or the dashboard keeps
    showing filled tiers after the position is gone (production
    2026-10-31 09:02)."""
    from unittest.mock import patch

    eng = _engine()
    pos = Position()
    pos.entry_token = "UP"
    pos.entry_side = "UP"
    pos._total_qty = 0.0  # exact zero, not 0.004
    pos.tier1.size_filled = 5.0
    pos.tier1.size_expected = 5.0
    pos.tier1.filled = True
    pos.tier1.placed = True
    pos.tier1.server_order_id = "0xabc"
    eng._round = _round_with_end(pos, 600)  # too late for re-entry

    with patch.object(
        pos, "realize_position_closed", wraps=pos.realize_position_closed
    ) as mock_rpc:
        await eng._check_take_profit(bc_now=100_000.0)
        assert mock_rpc.call_count == 1

        assert pos.tier1.size_filled == 0.0
        assert pos.tier1.size_expected == 0.0
        assert pos.tier1.filled is False
        assert pos.tier1.placed is False
        assert pos.tier1.server_order_id is None
        assert pos._position_closed is True
        assert pos.entry_token == "UP"  # no re-entry: too little time left

        # A second tick must NOT repeat the cleanup.
        await eng._check_take_profit(bc_now=100_000.0)
        assert mock_rpc.call_count == 1


@pytest.mark.asyncio
async def test_pending_t1_is_not_treated_as_closed_position() -> None:
    """entry_token is set at T1 decision time; before the fill lands the
    position is still empty. The full-close cleanup must NOT run then, or
    the pending T1 order would be wiped and the round could re-arm."""
    from unittest.mock import patch

    eng = _engine()
    pos = Position()
    pos.entry_token = "UP"
    pos.entry_side = "UP"
    pos._total_qty = 0.0
    pos.tier1.placed = True
    pos.tier1.server_order_id = "0xpending"
    eng._round = _round_with_end(pos, 800)  # early window: re-arm eligible

    with patch.object(
        pos, "realize_position_closed", wraps=pos.realize_position_closed
    ) as mock_rpc:
        await eng._check_take_profit(bc_now=100_000.0)
        assert mock_rpc.call_count == 0, (
            "a pending T1 (no fills yet) must not be cleaned up as a closed position"
        )

    assert pos.entry_token == "UP"
    assert pos.tier1.server_order_id == "0xpending"
