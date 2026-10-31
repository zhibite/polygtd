"""T4 post-entry stop-loss.

Operator rule (2026-10-31): once T4 fills (max planned exposure), a
continuing collapse is no longer a dip the ladder can average into.
Stage 1 sells a fraction below T4's fill price after a confirmation
window; stage 2 exits the remainder on a deeper drop or near expiry.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from src.execution.order_manager import TrackedOrder
from src.main import ActionRouter
from src.strategy.engine import ActionStopLoss, StrategyEngine
from src.strategy.round_state import Position


def _engine(**config):
    eng = StrategyEngine.__new__(StrategyEngine)
    eng._bc = SimpleNamespace(last_price=100_000.0, ts_ms=0)
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.10)
    eng._gate = SimpleNamespace(
        can_act=lambda bc: True,
        can_enter=lambda bc, mid: True,
        can_double_down=lambda *a, **k: True,
        state=lambda bc: SimpleNamespace(drift=0.0, within_limit=True),
    )
    cfg = {
        "STOP_LOSS_ENABLED": True,
        "STOP_LOSS_T4_DROP_PCT": 0.45,
        "STOP_LOSS_CONFIRM_S": 20.0,
        "STOP_LOSS_STAGE1_SELL_PCT": 0.60,
        "STOP_LOSS_STAGE2_EXTRA_DROP_PCT": 0.30,
        "STOP_LOSS_TIME_EXIT_S": 120.0,
        "STOP_LOSS_FLOOR": 0.05,
        "STOP_LOSS_FINAL_EXIT_S": 60.0,
        "STOP_LOSS_FINAL_MID": 0.50,
        "STOP_LOSS_RETRY_S": 3.0,
    }
    cfg.update(config)
    eng._s = SimpleNamespace(**cfg)
    eng._stop_loss_stage = 0
    eng._stop_loss_below_since = None
    eng._stop_loss_retry_after_ts = 0.0
    eng._stop_loss_inflight = False
    actions: list = []

    async def handler(action) -> None:  # noqa: ANN001
        actions.append(action)

    eng._handler = handler
    eng._actions = actions
    return eng


def _t4_position(qty: float = 72.19, cost: float = 15.0, avg: float = 0.2078):
    pos = Position()
    pos.entry_token = "UP"
    pos.entry_side = "UP"
    pos._total_qty = qty
    pos._total_cost = cost
    pos._avg_cost = avg
    pos.tier1.size_filled = 2.5
    pos.tier1.cost_filled = 1.0
    pos.tier1.filled = True
    pos.tier2.size_filled = 6.41
    pos.tier2.cost_filled = 2.0
    pos.tier2.filled = True
    pos.tier3.size_filled = 17.09
    pos.tier3.cost_filled = 4.0
    pos.tier3.filled = True
    pos.tier4.size_filled = qty - 26.0
    pos.tier4.cost_filled = cost - 7.0
    pos.tier4.avg_fill_price = pos.tier4.cost_filled / pos.tier4.size_filled
    pos.tier4.filled = True
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
async def test_stop_loss_is_not_armed_before_t4_fills() -> None:
    eng = _engine()
    pos = _t4_position()
    pos.tier4.size_filled = 0.0
    pos.tier4.cost_filled = 0.0
    eng._round = _round_with_end(pos, 600)
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.01)

    await eng._check_stop_loss(bc_now=100_000.0)

    assert eng._actions == []


@pytest.mark.asyncio
async def test_stage1_sells_after_confirmation() -> None:
    eng = _engine()
    pos = _t4_position()
    eng._round = _round_with_end(pos, 600)
    # T4 avg = 8/46.19 = 0.1732; stage-1 line = 0.1732 × 0.55 = 0.0953.
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.09)

    await eng._check_stop_loss(bc_now=100_000.0)
    assert eng._actions == [], "first observation arms the timer only"
    assert eng._stop_loss_below_since is not None

    # Skip the 20s wall-clock wait: backdate the confirmation timestamp.
    eng._stop_loss_below_since = time.time() - 21
    await eng._check_stop_loss(bc_now=100_000.0)

    assert len(eng._actions) == 1
    action = eng._actions[0]
    assert isinstance(action, ActionStopLoss)
    assert action.stage == 1
    assert action.qty == pytest.approx(72.19 * 0.60, abs=1e-4)
    assert action.round_start_ts == eng._round.round_start_ts


@pytest.mark.asyncio
async def test_confirmation_resets_when_mid_recovers() -> None:
    eng = _engine()
    pos = _t4_position()
    eng._round = _round_with_end(pos, 600)
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.09)

    await eng._check_stop_loss(bc_now=100_000.0)
    assert eng._stop_loss_below_since is not None

    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.20)
    await eng._check_stop_loss(bc_now=100_000.0)
    assert eng._stop_loss_below_since is None
    assert eng._actions == []


@pytest.mark.asyncio
async def test_stage2_sells_the_remainder_on_a_deeper_drop() -> None:
    eng = _engine()
    pos = _t4_position()
    eng._round = _round_with_end(pos, 600)
    eng.notify_stop_filled(1, 43.314)
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.05)  # below 0.7 × line

    await eng._check_stop_loss(bc_now=100_000.0)

    assert len(eng._actions) == 1
    action = eng._actions[0]
    assert action.stage == 2
    assert action.qty == pytest.approx(pos.total_qty)
    assert action.reason == "stage2_drop"


@pytest.mark.asyncio
async def test_final_minute_exit_sells_a_losing_position() -> None:
    eng = _engine()
    pos = _t4_position()
    eng._round = _round_with_end(pos, 30)
    eng.notify_stop_filled(1, 43.314)
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.40)

    await eng._check_stop_loss(bc_now=100_000.0)

    assert len(eng._actions) == 1
    assert eng._actions[0].stage == 2
    assert eng._actions[0].reason == "final_exit"


@pytest.mark.asyncio
async def test_inflight_stop_does_not_emit_a_duplicate() -> None:
    eng = _engine()
    pos = _t4_position()
    eng._round = _round_with_end(pos, 600)
    eng._stop_loss_inflight = True
    eng._stop_loss_below_since = time.time() - 999
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.01)

    await eng._check_stop_loss(bc_now=100_000.0)

    assert eng._actions == []


@pytest.mark.asyncio
async def test_stop_loss_can_be_disabled() -> None:
    eng = _engine(STOP_LOSS_ENABLED=False)
    pos = _t4_position()
    eng._round = _round_with_end(pos, 600)
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: 0.01)

    await eng._check_stop_loss(bc_now=100_000.0)

    assert eng._actions == []
    assert eng._stop_loss_below_since is None


# ---------------------------------------------------------------------------
# Router wiring
# ---------------------------------------------------------------------------


class _StopEngine:
    def __init__(self, pos: Position, round_ts: int) -> None:
        self._round = SimpleNamespace(
            tick_size=0.01, position=pos, round_start_ts=round_ts
        )
        self.stop_filled: list[tuple] = []
        self.stop_error: list[tuple] = []

    def notify_stop_filled(self, stage, qty):
        self.stop_filled.append((stage, qty))

    def notify_stop_error(self, stage, reason):
        self.stop_error.append((stage, reason))


class _StopOM:
    def __init__(self, order: TrackedOrder) -> None:
        self.order = order
        self.cancelled: list[int] = []
        self.sell_kwargs: dict | None = None

    async def cancel_market(self, round_start_ts):
        self.cancelled.append(round_start_ts)
        return ["c-tp"]

    async def place_market_sell(self, **kwargs):
        self.sell_kwargs = kwargs
        return self.order


@pytest.mark.asyncio
async def test_router_stop_loss_cancels_orders_and_sells() -> None:
    round_ts = int(time.time() - 60)
    pos = _t4_position()
    eng = _StopEngine(pos, round_ts)
    order = TrackedOrder(
        client_order_id="c-sl",
        server_order_id="s-sl",
        round_start_ts=round_ts,
        side="SELL",
        token_id="tok",
        tier_name="SL1",
        price=0.10,
        size=43.3,
        placed_size=43.3,
        size_matched=43.3,
        lifetime_s=0,
        expires_at=0.0,
        placed_at=time.time(),
        status="filled",
    )
    om = _StopOM(order)
    state = SimpleNamespace(
        engine=eng,
        order_manager=om,
        clob=SimpleNamespace(fill_callback=None),
        round_start_ts=round_ts,
        killswitch=None,
        _bg_tasks=set(),
    )
    router = ActionRouter(state)  # type: ignore[arg-type]

    await router(ActionStopLoss(
        token="UP", price=0.10, qty=43.3, stage=1,
        reason="t4_drop", round_start_ts=round_ts,
    ))
    for _ in range(5):
        pending = [t for t in state._bg_tasks if not t.done()]
        if not pending:
            break
        await asyncio.gather(*pending)

    assert om.cancelled == [round_ts]
    assert om.sell_kwargs is not None
    assert om.sell_kwargs["size"] == pytest.approx(43.3)
    assert pos.total_qty == pytest.approx(72.19 - 43.3, abs=1e-4)
    assert eng.stop_filled == [(1, pytest.approx(43.3))]
    assert eng.stop_error == []
