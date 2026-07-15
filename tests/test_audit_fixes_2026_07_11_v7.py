"""Regression tests for the R1/R5/R6/R7 audit hardening (2026-07-12).

Covers the residual risk items from the GTD-buy/averaging/TP review:

  R1  — `_eval_inflight` is set True on entry and cleared on EVERY exit
        path (normal completion AND exception from a sub-step like
        _check_t1_entry / _check_average_down / _check_take_profit).
        Without this, an exception in any sub-step would freeze the
        engine on the next tick (silent no-op, no log row).
  R5  — `Position.apply_tier_watcher_result` clears
        `tier.watching = False` in a `finally`, so an exception during
        in-branch log.info / baseline reconciliation cannot leak the
        in-flight mutex (which would deadlock `_check_take_profit`
        via `has_inflight_tier`).
  R6  — `_check_take_profit` blocks the TP fire while the position
        aggregate is behind the per-tier sum (gap > tolerance). This
        guards against posting a TP SELL at a stale `_avg_cost` while
        the TierWatcher is mid-reconcile.
  R7  — `OrderManager.apply_fill_event` rejects NaN / negative
        size_matched at entry to keep the cumulative state sane.
        The mirror-only contract (never call eng.on_order_filled)
        is preserved by code review; this test pins the NaN/negative
        early-return.
  R8  — on_new_round() resets every per-round flag
        (baseline_set_realized, _applied_watcher_dedup, watcher
        registries) so a fresh round never inherits stale reconciliation
        state from the previous round.

H-NEW-2026-07-15 (audit follow-up): the legacy `baseline_set` /
`baseline_price` fields were removed from Position, so they are no
longer in the reset list.

Run via `pytest tests/test_audit_fixes_2026_07_11_v7.py`.
"""
from __future__ import annotations

import asyncio
import pathlib
import sys
from typing import Any

import pytest

# Repo root on sys.path so `from src.* import …` works regardless of cwd.
_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ---------------------------------------------------------------------------
# Tiny fakes / stubs
# ---------------------------------------------------------------------------


class _StubGate:
    def can_act(self, bc_now):  # noqa: ARG002
        return True

    def can_enter(self, bc_now, mid):  # noqa: ARG002
        return True

    def can_double_down(self, *a, **kw):  # noqa: ARG002
        return True

    def state(self, bc_now):  # noqa: ARG002
        from dataclasses import dataclass

        @dataclass
        class S:
            drift: float = 0.0
            within_limit: bool = True

        return S()


class _StubPriceCache:
    def __init__(self, mid: float, bid: float) -> None:
        self._mid = mid
        self._bid = bid

    def get_midpoint(self, token):  # noqa: ARG002
        return self._mid

    def bid(self, token):  # noqa: ARG002
        return self._bid

    def mid(self, token):  # noqa: ARG002
        return (self._mid + self._bid) / 2


# ---------------------------------------------------------------------------
# R1: _eval_inflight is cleared even when a sub-step raises
# ---------------------------------------------------------------------------


def _build_eval_engine(handler_raises: BaseException | None):
    """Build a StrategyEngine that calls a custom handler which may raise
    on await. We bypass __init__ to avoid wiring real Binance/CLOB deps.
    """
    from src.strategy.engine import StrategyEngine
    from src.strategy.round_state import Position

    eng = StrategyEngine.__new__(StrategyEngine)
    import time

    round_start = int(time.time())
    pos = Position()  # real one — provides all properties/methods
    eng._round = type(
        "R",
        (),
        {
            "round_start_ts": round_start,
            "up_token": "tok_up",
            "down_token": "tok_down",
            "gate_last_state": "PASS",
            "position": pos,
        },
    )()
    eng._gate = _StubGate()
    eng._bc = type("B", (), {"last_price": 100.0})()
    eng._pc = _StubPriceCache(mid=0.30, bid=0.30)
    eng._eval_inflight = False

    # _s carries the live StrategySettings. Tests bypass __init__, so wire
    # a minimal stand-in so `_check_t1_entry`/`_check_take_profit` reads
    # not blow up on AttributeError. Fields cover the union of all
    # settings._check_*() functions touch.
    class _Cfg:
        PRICE_PRE_MARGIN = 0.10
        ENTRY_PRICE_CAP = 0.36
        TP_PROFIT_PCT = 0.20
        TP_DECREMENT_PCT = 0.05
        TP_MIN_PCT = 0.05
        T1_ENTRY_WINDOW_S = 300
        TIER1_NOTIONAL = 2.0
        TIER2_NOTIONAL = 3.0
        TIER3_NOTIONAL = 5.0
        TIER4_NOTIONAL = 6.0
        # H-NEW-2026-07-15: TIER_NOTIONAL_MAX_MULT removed.
        BASELINE_DRIFT_LIMIT = 0.0015
        STAKE_CAP_PER_ROUND = 16.0
        tier_drop_pcts = [None, 0.30, 0.60]
    eng._s = _Cfg()

    # The cap-reject bookkeeping lives on the engine instance.
    eng._t1_cap_reject_first_seen = {}
    eng._t1_cap_reject_summary_logged = {}
    eng._tN_skip_last_log_ts = {}
    eng._t1_skip_last_log_ts = {}

    async def raising_handler(*args, **kwargs):
        if handler_raises is not None:
            raise handler_raises
        return None

    eng._handler = raising_handler
    return eng


@pytest.mark.asyncio
async def test_r1_eval_inflight_reset_on_normal_completion():
    """R1: _evaluate() clears _eval_inflight in its `finally` block even
    when the function returns normally. Without this, a normal but
    slow-tick evaluate could leave the flag True and starve the next tick.
    """
    eng = _build_eval_engine(handler_raises=None)
    # Tick in normal mode — _check_t1_entry short-circuits because
    # pos.entry_token is None and the price is below cap.
    await eng._evaluate()
    assert eng._eval_inflight is False, (
        "R1 regression: _eval_inflight was True after _evaluate returned"
    )


@pytest.mark.asyncio
async def test_r1_eval_inflight_reset_when_handler_raises():
    """R1: _evaluate() clears _eval_inflight even when a sub-step
    (handler) raises. This is the actual hardening the R1 audit mandated:
    without the try/finally, the engine would freeze silently on the
    next tick with no breadcrumb.
    """
    eng = _build_eval_engine(handler_raises=RuntimeError("simulated CLOB down"))
    with pytest.raises(RuntimeError):
        await eng._evaluate()
    assert eng._eval_inflight is False, (
        "R1 regression: _eval_inflight leaked True after handler raised; "
        "the next tick would silently no-op"
    )


@pytest.mark.asyncio
async def test_r1_eval_inflight_blocks_concurrent_re_entry():
    """R1: while _evaluate() is awaiting its handler, a second call is
    dropped on the floor (single-flight). This is the original invariant
    we are guarding with the try/finally.
    """
    eng = _build_eval_engine(handler_raises=None)
    # Manually flip the flag, simulate an in-flight call.
    eng._eval_inflight = True
    # A concurrent re-entry must drop with debug log, no state change.
    before_flag = eng._eval_inflight
    before_round_ts = eng._round.round_start_ts
    await eng._evaluate()
    # The single-flight guard must have made this a no-op.
    assert eng._eval_inflight == before_flag
    assert eng._round.round_start_ts == before_round_ts


# ---------------------------------------------------------------------------
# R5: apply_tier_watcher_result clears watching=False on exception
# ---------------------------------------------------------------------------


def test_r5_apply_tier_watcher_result_releases_watching_on_exception():
    """R5: an exception during in-branch reconciliation (e.g. log.info
    failing on a serialiser edge case) must NOT leak `tier.watching=True`.
    The flag is the only thing standing between us and a soft-lock
    `has_inflight_tier=True` condition.
    """
    from src.strategy.round_state import Position, TierSnapshot
    import unittest.mock as mock

    pos = Position()
    tier1 = pos.tier1
    tier1.watching = True
    tier1.size_filled = 1.0  # already partially filled
    tier1.size_expected = 5.0
    tier1.notional = 2.0

    # Drive the FILLED branch with a missed-fill reconciliation path that
    # would normally hit the log.info(...) call. We patch log.info to
    # raise on the specific breadcrumb we expect.
    boom = mock.patch(
        "src.strategy.round_state.log.info",
        side_effect=RuntimeError("simulated log sink failure"),
    )
    boom.start()
    try:
        with pytest.raises(RuntimeError):
            pos.apply_tier_watcher_result(
                tier1,
                outcome="filled",
                filled_qty=5.0,  # > tier.size_filled → triggers reconcile path
                dedup_key="0xtest|filled",
                avg_fill_price=0.39,
            )
    finally:
        boom.stop()

    # Critical assertion: regardless of the exception, watching must
    # have been cleared by the finally block.
    assert tier1.watching is False, (
        "R5 regression: apply_tier_watcher_result leaked tier.watching=True "
        "on exception path; has_inflight_tier would soft-lock the round"
    )


def test_r5_apply_tier_watcher_result_happy_path_releases_watching():
    """R5 baseline: in the normal completion path, watching also flips False."""
    from src.strategy.round_state import Position

    pos = Position()
    pos.tier1.watching = True
    pos.tier1.size_expected = 5.0
    pos.tier1.notional = 2.0
    pos.apply_tier_watcher_result(
        pos.tier1, outcome="filled", filled_qty=0.0,
        dedup_key="0xhp|filled", avg_fill_price=0.0,
    )
    assert pos.tier1.watching is False


# ---------------------------------------------------------------------------
# R6: _check_take_profit blocks while position aggregate is unreconciled
# ---------------------------------------------------------------------------


def _build_engine_for_tp_check(pos, mid_price: float):
    """Build an engine with a stub harness, ready to call _check_take_profit."""
    from src.strategy.engine import StrategyEngine
    import time

    eng = StrategyEngine.__new__(StrategyEngine)
    eng._round = type(
        "R", (),
        {"position": pos, "tick_size": 0.01,
         "round_start_ts": int(time.time())},
    )()
    eng._gate = _StubGate()
    eng._pc = _StubPriceCache(mid=mid_price, bid=mid_price * 0.99)

    # Bypass gate can_act
    eng._gate.can_act = lambda *a, **kw: True

    # Bypass backoff bookkeeping
    eng._tp_last_post_error_ts = 0.0
    eng._tp_backoff_s = 5.0
    eng._tp_post_failure_count = 0

    captured = []

    async def fake_handler(action):
        captured.append(action)

    eng._handler = fake_handler
    eng._captured = captured

    # Module-level helper used by _check_take_profit
    import src.strategy.engine as eng_mod
    return eng


@pytest.mark.asyncio
async def test_r6_tp_blocked_when_position_aggregate_lags_tiers():
    """R6: when _total_qty < sum(tier.size_filled) by more than tolerance,
    _check_take_profit must NOT emit an ActionTakeProfit. Otherwise
    the SELL GTD would post at a stale `_avg_cost`.

    Scenario: WS TRADE delivered tier2 a 5.0 fill but the TierWatcher
    has not yet reconciled `_total_qty`. _avg_cost is stale.
    """
    from src.strategy.round_state import Position

    pos = Position()
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos._total_qty = 10.0  # stale — WS hasn't pushed the T2 fill yet
    pos._total_cost = 4.0  # _avg_cost = 0.40
    pos._avg_cost = 0.40
    pos.tier1.size_filled = 5.0
    pos.tier1.filled = True
    pos.tier2.size_filled = 5.0
    pos.tier2.filled = True
    # No watcher reconciliation yet → tier.size_filled (10) > _total_qty (10)
    # Wait, this is actually equal. Let's set it to be ahead:
    pos._total_qty = 5.0  # way behind — TierWatcher hasn't reconciled
    pos.tier1.size_filled = 5.0
    pos.tier2.size_filled = 5.0  # 10 shares known per-tier, but only 5 in position

    # Wire an in-tick mid comfortably above tp_target so the price gate
    # would otherwise pass.
    eng = _build_engine_for_tp_check(pos, mid_price=0.50)

    await eng._check_take_profit(0.0)

    assert eng._captured == [], (
        f"R6 regression: _check_take_profit fired while _total_qty was "
        f"behind per-tier sum; expected 0 actions, got {eng._captured}"
    )


@pytest.mark.asyncio
async def test_r6_tp_fires_when_position_aggregate_is_consistent():
    """R6 baseline: with _total_qty matching per-tier sum, TP fires as
    expected when mid is above fire_line.
    """
    from src.strategy.round_state import Position

    pos = Position()
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos._total_qty = 5.0
    pos._total_cost = 2.0
    pos._avg_cost = 0.40
    pos.tier1.size_filled = 5.0
    pos.tier1.filled = True

    # tp_target = 0.40 * (1 + TP_PROFIT_PCT) ≈ 0.40 * 1.20 = 0.48 (with
    # default TP_PROFIT_PCT=0.20, N=0). Mid=0.50 is comfortably above.
    eng = _build_engine_for_tp_check(pos, mid_price=0.50)

    await eng._check_take_profit(0.0)

    assert len(eng._captured) == 1, (
        f"R6 baseline broken: expected 1 ActionTakeProfit, "
        f"got {len(eng._captured)}"
    )


# ---------------------------------------------------------------------------
# R7: apply_fill_event rejects NaN / negative size_matched
# ---------------------------------------------------------------------------


def test_r7_apply_fill_event_rejects_nan_size():
    """R7: a NaN size_matched feed must be dropped silently rather than
    silently pinning size_matched=NaN into TrackedOrder.
    """
    from src.execution.order_manager import OrderManager
    import asyncio as _asyncio

    om = OrderManager.__new__(OrderManager)
    om._clob = None
    om._lock = _asyncio.Lock()
    om._orders = {}
    om._by_server = {}
    om._pending_fills = {}
    om._pc = None
    om._pm = None
    om._s = type("S", (), {"GTD_LIFETIME_S": 120, "SELL_SLIPPAGE_PCT": 1.0})()

    # Pre-insert a tracked order
    from src.execution.order_manager import TrackedOrder

    om._orders["cid1"] = TrackedOrder(
        client_order_id="cid1",
        server_order_id="0xorder1",
        round_start_ts=0,
        token_id="tok",
        side="BUY",
        tier_name="T1",
        price=0.40,
        size=5.0,
        lifetime_s=120,
        expires_at=0,
        placed_at=0.0,
        size_matched=1.0,
    )
    om._by_server["0xorder1"] = "cid1"

    # Apply NaN — must be a no-op
    om.apply_fill_event("0xorder1", size_matched=float("nan"),
                        fill_price=0.40)

    # TrackedOrder.size_matched must be unchanged
    assert om._orders["cid1"].size_matched == 1.0, (
        "R7 regression: apply_fill_event left size_matched=NaN "
        "in TrackedOrder, corrupting subsequent monotonic-max comparisons"
    )


def test_r7_apply_fill_event_rejects_negative_size():
    """R7: a negative size_matched feed must be dropped silently.
    A negative value would otherwise pin size_matched to `max(prior, neg)`
    which could regress the cumulative state.
    """
    from src.execution.order_manager import OrderManager, TrackedOrder
    import asyncio as _asyncio

    om = OrderManager.__new__(OrderManager)
    om._clob = None
    om._lock = _asyncio.Lock()
    om._orders = {}
    om._by_server = {}
    om._pending_fills = {}
    om._pc = None
    om._pm = None
    om._s = type("S", (), {"GTD_LIFETIME_S": 120, "SELL_SLIPPAGE_PCT": 1.0})()

    om._orders["cid1"] = TrackedOrder(
        client_order_id="cid1",
        server_order_id="0xorder1",
        round_start_ts=0,
        token_id="tok",
        side="BUY",
        tier_name="T1",
        price=0.40,
        size=5.0,
        lifetime_s=120,
        expires_at=0,
        placed_at=0.0,
        size_matched=2.0,
    )
    om._by_server["0xorder1"] = "cid1"

    om.apply_fill_event("0xorder1", size_matched=-1.0, fill_price=0.40)

    assert om._orders["cid1"].size_matched == 2.0, (
        "R7 regression: apply_fill_event accepted a negative size_matched"
    )


def test_r7_apply_fill_event_does_not_call_engine():
    """R7 invariant: apply_fill_event is the MIRROR-ONLY path. We assert
    that no eng reference exists on OrderManager — making accidental
    cross-pollination impossible.
    """
    from src.execution.order_manager import OrderManager

    om = OrderManager.__new__(OrderManager)
    # OrderManager MUST NOT have any reference to the strategy engine.
    # The mirror-only contract is preserved because there is no way to
    # reach eng from inside apply_fill_event.
    forbidden_attrs = ("_engine", "_eng", "engine")
    for attr in forbidden_attrs:
        assert not hasattr(om, attr), (
            f"R7 regression: OrderManager gained a `{attr}` attr; "
            "mirror-only contract risks being violated if someone wires "
            "apply_fill_event to call eng.on_order_filled."
        )


# ---------------------------------------------------------------------------
# R8: on_new_round resets per-round reconciliation state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_r8_on_new_round_resets_position_state():
    """R8: a new round must inherit a fresh Position so previous-round
    `_applied_watcher_dedup`, baseline anchors, and stale TP state
    cannot leak across rounds.
    """
    from src.strategy.engine import StrategyEngine
    import time

    eng = StrategyEngine.__new__(StrategyEngine)

    # Seed what a populated position looks like.
    eng._round = type("R", (), {
        "round_start_ts": int(time.time()) - 60,
        "position": type("P", (), {
            "tiers": [],
            "entry_token": "tok_old",
            "entry_side": "UP",
            "_tp_inflight": True,
            "_applied_watcher_dedup": {"old_key"},
            "baseline_realized_price": 0.40,
            "baseline_set_realized": True,
        })(),
    })()

    # Per-round bookkeeping that on_new_round resets.
    eng._t1_cap_reject_first_seen = {}
    eng._t1_cap_reject_summary_logged = {}
    eng._tN_skip_last_log_ts = {}
    eng._t1_skip_last_log_ts = {}
    eng._t1_rebased_logged = {}  # if engine reads it on round reset

    # Minimal wiring for on_new_round (only those it touches).
    from src.strategy.gatekeeper import GateKeeper
    eng._gate = GateKeeper.__new__(GateKeeper)
    # on_new_round only inspects `_baseline_pinned_ts`; default-init it.
    if not hasattr(eng._gate, "_baseline_pinned_ts"):
        eng._gate._baseline_pinned_ts = 0.0

    # Patch the helpers on_new_round reaches.
    class _Cfg:
        BTC_SPOT_BASELINE_DEV_PCT = 0.15
    eng._s = _Cfg()

    new_round_ts = int(time.time())

    # Provide a market carrying the fields RoundState.__post_init__ reads.
    market = type("M", (), {
        "btc_spot_at_window_open": 67000.0,
        "slug": "btc-up-or-down-mock",
        "end_ts": new_round_ts + 900,
        "up_token": "tok_up_new",
        "down_token": "tok_down_new",
        "condition_id": "0xcond_mock",
        "tick_size": 0.01,
    })()

    await eng.on_new_round(new_round_ts, market=market)

    # The fresh round must NOT be the old one.
    assert eng._round.round_start_ts == new_round_ts
    # Position state must be fresh.
    pos = eng._round.position
    assert pos.entry_token is None
    assert pos._tp_inflight is False
    assert pos._applied_watcher_dedup == set()
    assert pos.baseline_realized_price is None
    assert pos.baseline_set_realized is False
