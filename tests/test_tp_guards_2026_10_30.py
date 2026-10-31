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


# ---------------------------------------------------------------------------
# Order-status promotion on the final-reconcile path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_final_reconcile_promotes_fully_filled_order() -> None:
    """A fill discovered only by the final reconcile must mark the order.

    Production 2026-10-30 15:00 round: T1 filled via the watcher's
    spawn-time fast path; the final reconcile recorded the fill but never
    called `mark_filled`, so OrderManager kept the order as `live` and the
    GTD invariant warned every 5 s for the rest of the round.
    """
    from src.main import ActionRouter
    from src.strategy.round_state import Position

    class _OM:
        def __init__(self) -> None:
            self.applied: list[tuple] = []
            self.marked: list[str] = []

        def by_server_id(self, sid):  # noqa: ANN001
            return SimpleNamespace(
                side="BUY", token_id="tok", price=0.40,
                size_matched=0.0, placed_size=5.0, size=5.0,
            )

        def apply_fill_event(
            self, sid, size_matched, fill_price=None, trader_side=None,
        ) -> None:
            self.applied.append((sid, size_matched, fill_price))

        def reconcile_fill_terms(
            self, sid, cumulative, fill_price=None, trader_side=None,
        ) -> bool:
            return True

        def mark_filled(self, sid) -> bool:  # noqa: ANN001
            self.marked.append(sid)
            return True

    class _Clob:
        async def get_order_fill_state(self, sid):  # noqa: ANN001
            return {"size_matched": 5.0, "avg_price": 0.40}

    class _Eng:
        def __init__(self) -> None:
            self._round = SimpleNamespace(position=Position(), round_start_ts=123)
            self.fills: list[tuple] = []

        def on_order_filled(self, token, sid, price, size) -> None:  # noqa: ANN001
            self.fills.append((token, sid, price, size))

    om, eng = _OM(), _Eng()
    state = SimpleNamespace(
        engine=eng, order_manager=om, clob=_Clob(),
        round_start_ts=123, _bg_tasks=set(),
    )
    router = ActionRouter(state)

    await router._final_reconcile_before_close("s1")

    assert om.applied == [("s1", 5.0, 0.40)]
    assert om.marked == ["s1"], (
        "a fully matched order discovered by the final reconcile must be "
        "promoted from 'live' to 'filled'"
    )


@pytest.mark.asyncio
async def test_final_reconcile_rechecks_cumulative_after_await() -> None:
    """A fill booked while the reconcile REST call is in flight must not be
    credited to the engine twice.

    Production 2026-10-30 15:17: T3 filled 20.83 exactly as the strategy
    cancelled the tier. The watcher's on_done booked the fill, then the
    final reconcile credited it AGAIN because `prev` had been captured
    before its `await`; the position read 56.02 while the wallet held
    35.19. OrderManager's monotonic max kept its own value correct, so
    only the engine drifted.
    """
    from src.main import ActionRouter

    class _OM:
        def __init__(self) -> None:
            self.tracked = SimpleNamespace(
                side="BUY", token_id="tok", price=0.24,
                size_matched=0.0, placed_size=20.83, size=20.83,
            )
            self.applied: list[float] = []

        def by_server_id(self, sid):  # noqa: ANN001
            return self.tracked

        def apply_fill_event(
            self, sid, size_matched, fill_price=None, trader_side=None,
        ) -> None:
            self.applied.append(size_matched)
            self.tracked.size_matched = max(
                self.tracked.size_matched, size_matched
            )

        def reconcile_fill_terms(
            self, sid, cumulative, fill_price=None, trader_side=None,
        ) -> bool:
            return True

        def mark_filled(self, sid) -> bool:  # noqa: ANN001
            return True

    class _Clob:
        def __init__(self, om: "_OM") -> None:
            self.om = om

        async def get_order_fill_state(self, sid):  # noqa: ANN001
            # The watcher's terminal callback books the fill while this
            # REST call is in flight.
            self.om.tracked.size_matched = 20.83
            return {"size_matched": 20.83, "avg_price": 0.24}

    class _Eng:
        def __init__(self) -> None:
            self._round = SimpleNamespace(position=Position(), round_start_ts=123)
            self.fills: list[float] = []

        def on_order_filled(self, token, sid, price, size) -> None:  # noqa: ANN001
            self.fills.append(size)

    om, eng = _OM(), _Eng()
    state = SimpleNamespace(
        engine=eng, order_manager=om, clob=_Clob(om),
        round_start_ts=123, _bg_tasks=set(),
    )
    router = ActionRouter(state)

    await router._final_reconcile_before_close("s1")

    assert om.applied == [], "no new cumulative was observed after the await"
    assert eng.fills == [], (
        "the engine was credited twice for one fill (stale `prev` across await)"
    )


# ---------------------------------------------------------------------------
# TP terminal fill mirroring / zero-balance reconciliation
# ---------------------------------------------------------------------------


def test_tp_terminal_fill_mirrors_into_order_manager() -> None:
    """A TP that sold must not stay 'live' in OrderManager.

    Production 2026-10-30 15:21: the TP sold the whole 35.19-share
    position; the watcher's on_done only updated the Position, so the
    dashboard still listed the order as open and the fills table had no
    SELL row.
    """
    from src.main import ActionRouter

    class _OM:
        def __init__(self) -> None:
            self.applied: list[tuple] = []
            self.marked: list[str] = []

        def apply_fill_event(
            self, sid, size_matched, fill_price=None, trader_side=None,
        ) -> None:
            self.applied.append((sid, size_matched, fill_price))

        def reconcile_fill_terms(
            self, sid, cumulative, fill_price=None, trader_side=None,
        ) -> bool:
            return True

        def mark_filled(self, sid) -> bool:  # noqa: ANN001
            self.marked.append(sid)
            return True

    om = _OM()
    router = ActionRouter.__new__(ActionRouter)
    router._state = SimpleNamespace(order_manager=om)
    result = SimpleNamespace(
        server_order_id="s1", filled_qty=35.19,
        avg_fill_price=0.30, strategy_price=0.305,
    )

    router._mirror_tp_terminal_fill(result)

    assert om.applied == [("s1", 35.19, pytest.approx(0.30))]
    assert om.marked == ["s1"]


@pytest.mark.asyncio
async def test_maybe_realize_closed_clears_phantom_with_clob_argument() -> None:
    """The helper must be reachable now that it takes the clob directly.

    The old signature read `eng._state.clob`; StrategyEngine has no
    `_state`, so every call was gated out and a fully-drained wallet left
    a phantom position that kept retrying doomed sells.
    """
    from src.main import _maybe_realize_closed

    class _Clob:
        def __init__(self) -> None:
            self.calls = 0

        async def get_token_balance(self, token_id):  # noqa: ANN001
            self.calls += 1
            return 0.0

    pos = Position()
    pos.entry_token = "tok"
    pos._total_qty = 20.83
    pos._total_cost = 5.58
    pos._avg_cost = 0.2676
    clob = _Clob()

    await _maybe_realize_closed("tok", clob, pos)

    assert clob.calls == 2, "zero balance must be confirmed on a re-read"
    assert pos.total_qty == 0.0
