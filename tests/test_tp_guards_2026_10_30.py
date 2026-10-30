"""TP guard regressions from the 14:45 round (2026-10-30).

Production sequence:
  1. T1 filled 5 @0.37 and the TP sold all 5 @0.45 (profit $0.40).
  2. The (then unfixed) post-TP re-entry gap let T2/T3 buy again.
  3. Mid rose back above the TP fire line, but the TP never posted:
       * `engine.tp_blocked_avg_cost_unreconciled` fired every tick —
         sum(tier fills)=37.65 vs total_qty=32.65; the 5-share gap was
         the shares the earlier TP had already SOLD, not watcher lag.
       * BTC drift was FAIL, so `can_act()` would have blocked it too.
  4. The position rode the price down.

These tests pin the two fixes:
  * shares sold by TP are added back in the avg-cost reconciliation;
  * TP exits are not gated by BTC drift unless explicitly configured.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.strategy.engine import ActionTakeProfit, StrategyEngine
from src.strategy.round_state import Position


def _position(
    *,
    qty: float,
    avg_cost: float,
    tier_fills: tuple[float, float, float, float],
    tp_sold: float = 0.0,
) -> Position:
    pos = Position()
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos._total_qty = qty
    pos._total_cost = qty * avg_cost
    pos._avg_cost = avg_cost
    pos._tp_sold_qty = tp_sold
    for i, filled in enumerate(tier_fills):
        pos.tiers[i].size_filled = filled
        pos.tiers[i].cost_filled = filled * avg_cost if filled else 0.0
    pos.baseline_realized_price = 0.37
    pos.baseline_set_realized = True
    pos.baseline_target_price = 0.37
    return pos


def _engine(pos: Position, *, mid: float, gate_ok: bool, **config):
    eng = StrategyEngine.__new__(StrategyEngine)
    eng._round = SimpleNamespace(position=pos, round_start_ts=1_793_371_500)
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: mid)
    eng._gate = SimpleNamespace(
        can_act=lambda ts: gate_ok,
        state=lambda ts: SimpleNamespace(drift=0.003, within_limit=gate_ok),
    )
    eng._s = SimpleNamespace(
        PRICE_PRE_MARGIN=0.10, CANCEL_HYSTERESIS_PCT=0.02, **config
    )
    eng._tp_last_post_error_ts = 0.0
    eng._tp_post_failure_count = 0
    eng._tp_backoff_s = 5.0
    return eng


async def _run(eng: StrategyEngine) -> list:
    actions: list = []

    async def handler(action) -> None:  # noqa: ANN001
        actions.append(action)

    eng._handler = handler
    await eng._check_take_profit(bc_now=100_000.0)
    return actions


@pytest.mark.asyncio
async def test_shares_sold_by_tp_do_not_block_the_next_tp() -> None:
    """sum(tier fills) = total_qty + tp_sold_qty must be treated as reconciled."""
    pos = _position(
        qty=32.65,
        avg_cost=0.209,
        tier_fills=(5.0, 10.13, 22.52, 0.0),
        tp_sold=5.0,  # the T1 TP exit earlier in the round
    )
    eng = _engine(pos, mid=0.25, gate_ok=True)  # above the TP fire line

    actions = await _run(eng)

    assert len(actions) == 1
    assert isinstance(actions[0], ActionTakeProfit)


@pytest.mark.asyncio
async def test_genuine_unreconciled_tier_fills_still_block_tp() -> None:
    """Without a TP sale, a tier/position gap must still defer the TP."""
    pos = _position(
        qty=10.0,
        avg_cost=0.20,
        tier_fills=(10.0, 5.0, 0.0, 0.0),  # 15 filled vs 10 held
        tp_sold=0.0,
    )
    eng = _engine(pos, mid=0.35, gate_ok=True)

    actions = await _run(eng)

    assert actions == []


@pytest.mark.asyncio
async def test_tp_is_not_blocked_by_btc_drift_by_default() -> None:
    """Exits must still happen while BTC spot drift is out of range."""
    pos = _position(
        qty=10.0, avg_cost=0.30, tier_fills=(10.0, 0.0, 0.0, 0.0),
    )
    eng = _engine(pos, mid=0.40, gate_ok=False)  # drift gate FAILING

    actions = await _run(eng)

    assert len(actions) == 1, (
        "TP is an exit; the BTC drift entry filter must not suppress it"
    )
    assert isinstance(actions[0], ActionTakeProfit)


@pytest.mark.asyncio
async def test_tp_drift_gate_can_be_opted_into() -> None:
    pos = _position(
        qty=10.0, avg_cost=0.30, tier_fills=(10.0, 0.0, 0.0, 0.0),
    )
    eng = _engine(pos, mid=0.40, gate_ok=False, TP_REQUIRE_DRIFT_GATE=True)

    actions = await _run(eng)

    assert actions == []
