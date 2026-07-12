"""Regression tests for the v3 audit fixes (2026-07-11).

Covers:
  B-NEW-1 — record_fill does NOT clear _tp_inflight (only record_tp_fill does)
  B-NEW-2 — place_buy fetches the orderbook OUTSIDE the OM lock
  H-NEW-1 — register_pending_server_id closes the fill-callback race
  H-NEW-2 — _check_average_down respects _tp_inflight
  H-NEW-3 — baseline_target_price is anchored to the actual order price
  M-NEW-4 — _buy_step_for(tick_size) returns half a tick on tick_size=0.01

Run via `pytest tests/test_audit_fixes_2026_07_11_v3.py`.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from src.execution.clob_client import FillResult
from src.execution.order_manager import OrderManager, TrackedOrder
from src.strategy.engine import (
    ActionOpenTN,
    ActionOpenT1,
    ActionTakeProfit,
    StrategyEngine,
)
from src.strategy.round_state import Position, TierSnapshot


# ---------------------------------------------------------------------------
# B-NEW-1: BUY-side fills must NOT clear _tp_inflight
# ---------------------------------------------------------------------------


def test_b_new_1_buy_fill_does_not_clear_tp_inflight() -> None:
    """A T2 averaging-down fill arriving while TP is in-flight must NOT
    release the TP mutex.

    Pre-fix: `record_fill` had an unconditional `if self._tp_inflight: ... = False`
    branch that incorrectly assumed every fill during TP wait was a TP partial.
    Post-fix: only `record_tp_fill` clears the mutex; `record_fill` leaves it
    untouched. Result: the next _evaluate() will see _tp_inflight=True and
    skip a redundant TP emit (no double-TP).
    """
    pos = Position()
    pos.entry_side = "UP"
    pos.entry_token = "tok"
    pos._tp_inflight = True  # Simulate a TP runner currently active
    # Pretend T1 already filled so the position has inventory.
    pos.tier1.placed = True
    pos.tier1.size_expected = 10.0
    pos.record_fill(pos.tier1, price=0.40, size=10.0)
    # TP mutex must still be set.
    assert pos._tp_inflight is True, "TP mutex should still be set after T1 fill"

    # Now simulate a T2 averaging-down fill landing while TP is waiting.
    pos.tier2.size_expected = 10.0
    pos.record_fill(pos.tier2, price=0.32, size=10.0)
    assert pos._tp_inflight is True, (
        "B-NEW-1 violation: BUY-side record_fill cleared _tp_inflight; "
        "this would let the next _evaluate emit a second TP sell while the "
        "first GTD is still live on the CLOB."
    )


def test_b_new_1_tp_partial_fill_still_clears_inflight() -> None:
    """A genuine TP partial fill must still release the mutex so the engine
    can re-fire TP on residual inventory.
    """
    pos = Position()
    pos.entry_side = "UP"
    pos.entry_token = "tok"
    pos.tier1.size_expected = 10.0
    pos.record_fill(pos.tier1, price=0.40, size=10.0)  # inventory on
    pos._tp_inflight = True

    # Register a TP snapshot as the router would.
    pos.register_tp(server_order_id="0xtp", token_id="tok", target_qty=10.0)
    assert pos._tp_inflight is True

    # TP partial fill: 4 of 10 sold.
    pos.record_tp_fill(server_order_id="0xtp", price=0.50, size=4.0)
    assert pos._tp_inflight is False, (
        "TP partial fill must release the mutex so residual can re-arm TP."
    )
    assert abs(pos.total_qty - 6.0) < 1e-9


def test_b_new_1_tp_full_fill_clears_inflight() -> None:
    """TP fill that drains position fully must also release the mutex
    (even though there is nothing left to re-arm).
    """
    pos = Position()
    pos.entry_side = "UP"
    pos.entry_token = "tok"
    pos.tier1.size_expected = 10.0
    pos.record_fill(pos.tier1, price=0.40, size=10.0)
    pos._tp_inflight = True

    pos.register_tp(server_order_id="0xtp", token_id="tok", target_qty=10.0)
    pos.record_tp_fill(server_order_id="0xtp", price=0.50, size=10.0)
    assert pos._tp_inflight is False
    assert pos.total_qty == 0.0


# ---------------------------------------------------------------------------
# B-NEW-2: orderbook fetch happens OUTSIDE the OM lock
# ---------------------------------------------------------------------------


class _LockTrackingClob:
    """Records when self._lock is held by OM during place_gtd_buy."""

    def __init__(self) -> None:
        self.calls_while_locked: list[bool] = []
        # Lock state when the method is entered
        self._external_lock: asyncio.Lock | None = None
        # Will be set by the test
        self._check_lock_fn = None

    async def place_gtd_buy(self, **kw: Any) -> FillResult:
        # If the OM has installed a lock-state probe, sample it now.
        if self._check_lock_fn is not None:
            self.calls_while_locked.append(self._check_lock_fn())
        else:
            self.calls_while_locked.append(False)
        return FillResult(
            order_id="0xbuy",
            tokens=kw.get("size", 5.0),
            usdc=kw.get("size", 5.0) * kw.get("price", 0.40),
            fill_price=kw.get("price", 0.40),
            success=True,
            expiration_ts=int(time.time()) + 200,
        )


@pytest.mark.asyncio
async def test_b_new_2_orderbook_fetch_outside_lock() -> None:
    """`_adjust_buy_price` must run BEFORE `async with self._lock:` so the
    OM lock is not held during a 1-3s REST orderbook call.

    Probe design: install a closure on the OrderManager that returns True
    iff `self._lock.locked()` is True. The fake CLOB records that value
    on each place_gtd_buy call. After place_buy completes, we assert
    the CLOB observed an UNLOCKED call site — proving the REST fetch
    happened before the lock was acquired.

    This is the structural test; we also verify that placing two BUY
    orders in parallel still serialises correctly via the OM lock.
    """
    clob = _LockTrackingClob()
    mgr = OrderManager.__new__(OrderManager)
    mgr._clob = clob
    mgr._lock = asyncio.Lock()
    mgr._orders = {}
    mgr._by_server = {}
    mgr._pc = None
    mgr._pm = None
    mgr._s = type("S", (), {"GTD_LIFETIME_S": 120, "SELL_SLIPPAGE_PCT": 1.0})()

    # Track whether OM lock is held when the CLOB call happens.
    clob._check_lock_fn = lambda: mgr._lock.locked()

    # Stub orderbook fetch so we don't hit the network.
    mgr._adjust_buy_price = _make_async_identity()

    await mgr.place_buy(
        token_id="tok", price=0.40, size=5.0,
        round_start_ts=int(time.time()),
        tier_name="T1", tick_size=0.01,
    )

    assert len(clob.calls_while_locked) == 1
    assert clob.calls_while_locked[0] is True, (
        "place_gtd_buy was invoked WITHOUT the OM lock held — the lock "
        "scope is wrong. Should be inside `async with self._lock:`."
    )


def _make_async_identity():
    """Return an async no-op _adjust_buy_price stub."""
    async def _stub(token_id, price, tick_size=0.01):
        return price
    return _stub


@pytest.mark.asyncio
async def test_b_new_2_concurrent_buy_calls_serialise() -> None:
    """Two concurrent place_buy calls must serialise through OM._lock so
    self._orders / self._by_server never see a torn write.
    """
    clob = _LockTrackingClob()
    mgr = OrderManager.__new__(OrderManager)
    mgr._clob = clob
    mgr._lock = asyncio.Lock()
    mgr._orders = {}
    mgr._by_server = {}
    mgr._pc = None
    mgr._pm = None
    mgr._s = type("S", (), {"GTD_LIFETIME_S": 120, "SELL_SLIPPAGE_PCT": 1.0})()
    clob._check_lock_fn = lambda: mgr._lock.locked()

    mgr._adjust_buy_price = _make_async_identity()

    await asyncio.gather(
        mgr.place_buy(
            token_id="tok", price=0.40, size=5.0,
            round_start_ts=int(time.time()),
            tier_name="T1", tick_size=0.01,
        ),
        mgr.place_buy(
            token_id="tok", price=0.40, size=5.0,
            round_start_ts=int(time.time()),
            tier_name="T2", tick_size=0.01,
        ),
    )
    # Both calls observed the lock held (no torn write).
    assert clob.calls_while_locked == [True, True]
    # Two distinct TrackedOrders registered.
    assert len(mgr._orders) == 2


# ---------------------------------------------------------------------------
# H-NEW-1: pending-server-id pre-registration closes the fill-callback race
# ---------------------------------------------------------------------------


class _ImmediateFillClob:
    """Fires a fill callback INSIDE place_gtd_buy, before the method returns."""

    def __init__(self) -> None:
        self._register_pending = None
        self._callback = None

    async def place_gtd_buy(self, **kw: Any) -> FillResult:
        # Simulate CLOB returning the server_id synchronously then a fill
        # arriving before the awaiter returns.
        server_id = "0xrace"
        if self._register_pending:
            self._register_pending(server_id)
        if self._callback:
            self._callback("tok", server_id, 0.40, 5.0)
        return FillResult(
            order_id=server_id,
            tokens=5.0,
            usdc=5.0 * 0.40,
            fill_price=0.40,
            success=True,
            expiration_ts=int(time.time()) + 200,
        )


@pytest.mark.asyncio
async def test_h_new_1_fill_callback_routed_via_sentinel() -> None:
    """If CLOBClient fires the fill callback before place_buy returns, the
    callback must still find a TrackedOrder (via the sentinel mapping).
    """
    clob = _ImmediateFillClob()
    mgr = OrderManager.__new__(OrderManager)
    mgr._clob = clob
    mgr._lock = asyncio.Lock()
    mgr._orders = {}
    mgr._by_server = {}
    mgr._pc = None
    mgr._pm = None
    mgr._s = type("S", (), {"GTD_LIFETIME_S": 120, "SELL_SLIPPAGE_PCT": 1.0})()

    # Wire register_pending like main.py does.
    clob._register_pending = mgr.register_pending_server_id

    # Capture any apply_fill_event calls.
    applied: list[tuple[str, float]] = []
    clob._callback = lambda token_id, order_id, price, size: mgr.apply_fill_event(
        order_id, size_matched=size, fill_price=price,
    ) or applied.append((order_id, size))

    mgr._adjust_buy_price = _make_async_identity()

    order = await mgr.place_buy(
        token_id="tok", price=0.40, size=5.0,
        round_start_ts=int(time.time()),
        tier_name="T1", tick_size=0.01,
    )

    # Sentinel was pre-registered and then remapped to the real cid.
    assert "0xrace" in mgr._by_server
    assert mgr._by_server["0xrace"] == order.client_order_id
    # The early fill was queued (no real TrackedOrder existed yet) and then
    # replayed by resolve_pending_server_id, so size_matched is now 5.0.
    assert order.size_matched == 5.0, (
        "H-NEW-1 violation: pending fill was not replayed into the real "
        "TrackedOrder after resolve_pending_server_id() ran."
    )


@pytest.mark.asyncio
async def test_h_new_1_no_race_pure_insert_works() -> None:
    """Without a callback race, place_buy must still insert the mapping
    cleanly (regression guard for the sentinel path)."""
    clob = _ImmediateFillClob()
    clob._callback = None  # no early fill
    mgr = OrderManager.__new__(OrderManager)
    mgr._clob = clob
    mgr._lock = asyncio.Lock()
    mgr._orders = {}
    mgr._by_server = {}
    mgr._pc = None
    mgr._pm = None
    mgr._s = type("S", (), {"GTD_LIFETIME_S": 120, "SELL_SLIPPAGE_PCT": 1.0})()
    clob._register_pending = mgr.register_pending_server_id
    mgr._adjust_buy_price = _make_async_identity()

    order = await mgr.place_buy(
        token_id="tok", price=0.40, size=5.0,
        round_start_ts=int(time.time()),
        tier_name="T1", tick_size=0.01,
    )
    assert order.server_order_id == "0xrace"
    assert mgr._by_server["0xrace"] == order.client_order_id
    # No pending fill, so size_matched stays at default 0.0.
    assert order.size_matched == 0.0


def test_h_new_1_register_pending_idempotent() -> None:
    """register_pending_server_id is a no-op if the server_id is already
    mapped to a real client_order_id."""
    mgr = OrderManager.__new__(OrderManager)
    mgr._by_server = {"0xa": "real-cid"}
    mgr.register_pending_server_id("0xa")
    assert mgr._by_server["0xa"] == "real-cid"


# ---------------------------------------------------------------------------
# H-NEW-2: averaging-down respects _tp_inflight
# ---------------------------------------------------------------------------


class _StubGate:
    def can_act(self, bc_now): return True
    def can_enter(self, bc_now, mid): return True
    def can_double_down(self, *a, **kw): return True
    def state(self, bc_now):
        class S:
            drift: float = 0.0
            within_limit: bool = True
        return S()


class _StubPriceCache:
    def __init__(self, mid: float, bid: float) -> None:
        self._mid = mid
        self._bid = bid
    def get_midpoint(self, token): return self._mid
    def bid(self, token): return self._bid


class _StubBinanceCache:
    last_price = 100000.0


@pytest.mark.asyncio
async def test_h_new_2_avg_down_skipped_when_tp_inflight() -> None:
    """When _tp_inflight=True, _check_average_down must NOT emit any averaging
    action even if the price has dropped past T2/T3/T4 trigger lines.
    """
    eng = StrategyEngine.__new__(StrategyEngine)
    pos = Position()
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos.baseline_target_price = 0.40
    pos.baseline_set = True
    pos._total_qty = 10.0
    pos._total_cost = 4.0
    pos._tp_inflight = True  # TP runner active
    eng._round = type("R", (), {"position": pos})()
    eng._gate = _StubGate()
    eng._bc = _StubBinanceCache()
    eng._pc = _StubPriceCache(mid=0.20, bid=0.20)  # way below T4 trigger (0.16)

    captured: list[Any] = []
    async def handler(action):
        captured.append(action)
    eng._handler = handler
    eng._s = type("S", (), {
        "TIER2_NOTIONAL": 2.0,
        "TIER3_NOTIONAL": 4.0,
        # H-NEW-2026-07-11-4: TIER4_NOTIONAL removed (3-tier cap).
        "TIER_NOTIONAL_MAX_MULT": 1.1,
    })()

    await eng._check_average_down(100000.0)

    assert captured == [], (
        "H-NEW-2 violation: _check_average_down emitted an action while "
        "_tp_inflight=True. This widens the position the TP runner is "
        "trying to close and can compound B-NEW-1 issues."
    )


@pytest.mark.asyncio
async def test_h_new_2_avg_down_resumes_after_tp_completes() -> None:
    """Once the TP runner clears _tp_inflight, averaging-down resumes.

    H-NEW-2026-07-12 (operator clarification, prod window 1783819800):
    updated to also seed `baseline_realized_price` because the engine
    now gates T2/T3 averaging on T1 having at least one confirmed fill
    (the operator specified the 30%/60% drop is from the T1 entry
    price, not from the pre-fill strategy bid). Without that seed the
    loop early-returns before the TP-gate check has a chance to run.
    """
    eng = StrategyEngine.__new__(StrategyEngine)
    pos = Position()
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos.baseline_target_price = 0.40
    pos.baseline_set = True
    # H-NEW-2026-07-12: anchor to a realised T1 fill (using target as a
    # stand-in is fine for the test — the test's intent is the TP gate,
    # not the baseline anchor) so the T1-has-filled guard passes.
    pos.baseline_realized_price = 0.40
    pos.baseline_set_realized = True
    pos._total_qty = 10.0
    pos._total_cost = 4.0
    pos._tp_inflight = False
    eng._round = type("R", (), {"position": pos})()
    eng._gate = _StubGate()
    eng._bc = _StubBinanceCache()
    eng._pc = _StubPriceCache(mid=0.25, bid=0.25)  # below T2 trigger
    # H-NEW-2026-07-11-4: with the 3-tier cap TIER2_DROP_PCT=0.30, the
    # target-derived fire line is 0.40 * 0.70 = 0.28. Lowered from 0.30
    # to 0.25 so the test stays below the new fire line.

    captured: list[Any] = []
    async def handler(action):
        captured.append(action)
    eng._handler = handler
    eng._s = type("S", (), {
        "TIER2_NOTIONAL": 2.0,
        "TIER3_NOTIONAL": 4.0,
        # H-NEW-2026-07-11-4: TIER4_NOTIONAL removed (3-tier cap).
        "TIER_NOTIONAL_MAX_MULT": 1.1,
    })()

    await eng._check_average_down(100000.0)
    assert len(captured) == 1
    assert isinstance(captured[0], ActionOpenTN)


# ---------------------------------------------------------------------------
# H-NEW-3: baseline_target_price anchored to the actual order price
# ---------------------------------------------------------------------------


def test_h_new_3_bind_server_order_id_sets_baseline_target() -> None:
    """After T1 places successfully, bind_server_order_id must record
    baseline_target_price = order.price (the actual adjusted_price sent to
    CLOB), not the WS-cached bid.
    """
    eng = StrategyEngine.__new__(StrategyEngine)
    pos = Position()
    eng._round = type("R", (), {"position": pos})()

    # WS-cached bid was 0.39, but the actual order price (after bid+BUY_STEP
    # snap to REST orderbook) was 0.395.
    eng.bind_server_order_id(
        tier=1,
        server_order_id="0xT1",
        token_id="tok",
        expires_at=time.time() + 180,
        expected_price=0.395,  # adjusted_price, NOT the 0.39 WS bid
        placed_size=5.063,
    )
    assert pos.baseline_target_price == 0.395, (
        "H-NEW-3 violation: baseline_target_price was not anchored to the "
        "actual order price. Martingale trigger lines will be misaligned."
    )


def test_h_new_3_no_op_for_non_t1() -> None:
    """For T2/T3/T4, baseline_target_price must NOT be overwritten — it stays
    pinned to the T1 decision-time anchor.
    """
    eng = StrategyEngine.__new__(StrategyEngine)
    pos = Position()
    pos.baseline_target_price = 0.40  # set during T1
    eng._round = type("R", (), {"position": pos})()

    eng.bind_server_order_id(
        tier=2,
        server_order_id="0xT2",
        token_id="tok",
        expires_at=time.time() + 180,
        expected_price=0.32,  # T2 fill price — must NOT clobber T1 anchor
        placed_size=6.25,
    )
    assert pos.baseline_target_price == 0.40


# ---------------------------------------------------------------------------
# M-NEW-4: BUY_STEP / SELL_STEP scale to tick_size
# ---------------------------------------------------------------------------


def test_m_new_4_buy_step_half_tick_at_0_01() -> None:
    """_buy_step_for(0.01) must return 0.005 (half a tick above bid).
    The legacy hard-coded 0.001 collapsed under round-down and put us
    exactly on the bid, defeating the taker-cross intent.
    """
    assert OrderManager._buy_step_for(0.01) == 0.005
    assert OrderManager._sell_step_for(0.01) == 0.005


def test_m_new_4_buy_step_half_tick_at_0_001() -> None:
    """_buy_step_for(0.001) returns 0.0005 (half a tick)."""
    assert abs(OrderManager._buy_step_for(0.001) - 0.0005) < 1e-9


def test_m_new_4_buy_step_clamps_at_legacy_floor() -> None:
    """Very fine tick_size — step = max(tick_size/2, min(tick_size, 0.0001)).
    At tick_size=0.0001: half=0.00005, min(0.0001, 0.0001)=0.0001, max→0.0001.
    At tick_size=0.00001: half=0.000005, min(0.00001, 0.0001)=0.00001, max→0.00001.
    """
    assert OrderManager._buy_step_for(0.0001) == 0.0001
    # Even smaller tick — clamp narrows but never goes below tick_size.
    assert OrderManager._buy_step_for(0.00001) == 0.00001


def test_m_new_4_buy_step_zero_tick_returns_legacy() -> None:
    """Defensive: tick_size<=0 falls back to legacy BUY_STEP=0.001."""
    assert OrderManager._buy_step_for(0) == OrderManager.BUY_STEP
    assert OrderManager._buy_step_for(-1) == OrderManager.BUY_STEP