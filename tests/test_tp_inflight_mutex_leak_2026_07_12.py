"""
Regression: H-NEW-2026-07-12 TP-mutex-leak guard.

Production at 2026-07-12T08:36:07Z saw `_check_average_down` emit
`engine.t2_diag.silent_return_tp_inflight` ~4 rows/sec for an entire
15m round — the symptom was that every T2/T3 averaging-down tick
short-circuited on `pos._tp_inflight == True`. The flag should have
been released by the watcher's `on_done` callback, but a code path
that bypassed the watcher (or an unhandled exception in `_handler`)
left it stuck.

This test fixes three invariants:

  1. SUCCESS path — after `_handler` returns normally, `pos._tp_inflight`
     stays True so the re-entrant `_evaluate` (B-1 single-flight aside)
     cannot stack a duplicate TP. The watcher is responsible for
     releasing it later.

  2. EXCEPTION path — if `_handler` raises (e.g. handler dispatch
     crash, a test double that raises), `pos._tp_inflight` must come
     back down to False before the exception propagates. This is the
     leak guard.

  3. `_check_average_down` no longer emits the per-tick
     `engine.t2_diag.silent_return_tp_inflight` warning — that breadcrumb
     was a DIAG-2026-07-11 diagnostic and has been removed because
     flooding the operator log with one row per 250ms tick drowned out
     the real warnings (`router.tp_failed`, `engine.tp_backoff_engaged`,
     `router.tp_runner_crashed`).

The engine is built via `__new__` (bypassing `__init__`) to keep these
tests independent of the real `pm_client` / `market_ws` bootstrapping.
"""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import patch

import pytest

from src.strategy.engine import (
    ActionTakeProfit,
    StrategyEngine,
)


# ---------------------------------------------------------------------------
# Minimal fakes
# ---------------------------------------------------------------------------


class _FakeTier:
    """The fields `_check_average_down` / `_check_take_profit` read."""

    def __init__(
        self,
        tier: int = 1,
        placed: bool = False,
        watching: bool = False,
        is_complete: bool = False,
        server_order_id: str | None = None,
    ) -> None:
        self.tier = tier
        self.placed = placed
        self.watching = watching
        self.is_complete = is_complete
        self.server_order_id = server_order_id
        self.size_filled = 0.0
        self.size_expected = 10.0
        self.notional = 1.0


class _FakePos:
    """Position stub exposing only the attributes touched by the code under test."""

    def __init__(self) -> None:
        self.entry_token = "0xtok"
        self.entry_side = "DOWN"
        self._tp_inflight = False
        self._total_qty = 5.55
        self.total_qty = 5.55
        self.avg_cost = 0.36
        self.baseline_set_realized = True
        self.baseline_realized_price = 0.36
        self.baseline_target_price = 0.36
        self.baseline_price = 0.36
        # 3-tier cap (H-NEW-2026-07-11-4). T1 is "complete" with its size
        # filled already (so `sum(tier.size_filled) == pos.total_qty` and
        # the R6 avg_cost-sanity gate falls through).
        self.tiers = [
            _FakeTier(tier=1, is_complete=True, server_order_id="0xT1srv"),
            _FakeTier(tier=2),
            _FakeTier(tier=3),
        ]
        # R6 sanity gate wants `sum(tier.size_filled) ≈ pos.total_qty`,
        # so populate T1's size_filled here.
        self.tiers[0].size_filled = self.total_qty

    def tp_target(self) -> float | None:
        return self.avg_cost * 1.2

    def tp_pct_for_current_state(self) -> float:
        return 0.20

    @property
    def has_inflight_tier(self) -> bool:
        return any(t.placed or t.watching for t in self.tiers)


class _FakeGate:
    """GateKeeper stub — every check returns True so the code falls through."""

    def can_act(self, _ts: float) -> bool:
        return True

    def can_enter(self, _ts: float, _mid: float) -> bool:
        return True

    def can_double_down(self, _ts: float, _mid: float, *_args: Any, **_kw: Any) -> bool:
        return True

    def state(self, _ts: float) -> Any:
        class _S:
            within_limit = True
            drift = 0.0
        return _S()


class _FakeRound:
    round_start_ts = 1783818900

    def __init__(self) -> None:
        self.position = _FakePos()


class _FakePC:
    def __init__(self, mid: float = 0.5) -> None:
        self._mid = mid
        self.midpoint = {"0xtok": mid}

    def get_midpoint(self, token: str) -> float:
        return self.midpoint.get(token, 0.0)


def _make_engine(mid: float = 0.5) -> Any:
    """Build a StrategyEngine instance via __new__, bypassing __init__."""
    eng = StrategyEngine.__new__(StrategyEngine)
    eng._round = _FakeRound()
    eng._pc = _FakePC(mid=mid)
    eng._gate = _FakeGate()
    eng._tp_last_post_error_ts = 0.0
    eng._tp_backoff_s = 5.0
    eng._tp_post_failure_count = 0
    eng._t2_diag_last_log_ts = {}
    eng._t2_diag_last_fingerprint = {}
    return eng


# ---------------------------------------------------------------------------
# Test 1: SUCCESS path leaves the mutex held
# ---------------------------------------------------------------------------


def test_tp_inflight_stays_true_on_success_path() -> None:
    """
    After `_handler` returns normally, `pos._tp_inflight` must remain True.
    The watcher (or downstream router) is responsible for releasing it —
    the engine must NOT preempt that contract.
    """
    eng = _make_engine(mid=0.5)

    async def _handler(action: ActionTakeProfit) -> None:
        return None

    eng._handler = _handler

    async def _run() -> None:
        await eng._check_take_profit(bc_now=0.0)

    asyncio.run(_run())

    assert eng._round.position._tp_inflight is True, (
        "success path must leave _tp_inflight=True so the watcher owns "
        "the release contract"
    )


# ---------------------------------------------------------------------------
# Test 2: EXCEPTION path clears the mutex (the leak guard)
# ---------------------------------------------------------------------------


def test_tp_inflight_cleared_when_handler_raises() -> None:
    """
    If `_handler` raises mid-emission, the leak guard must clear
    `_tp_inflight` before re-raising. Otherwise `_check_average_down`
    is wedged in the silent-return path for the rest of the round.
    """
    eng = _make_engine(mid=0.5)

    class _BoomError(RuntimeError):
        pass

    async def _handler(action: ActionTakeProfit) -> None:
        raise _BoomError("router dispatch crash")

    eng._handler = _handler

    async def _run() -> None:
        try:
            await eng._check_take_profit(bc_now=0.0)
        except _BoomError:
            return
        raise AssertionError(
            "_handler's exception must propagate out of _check_take_profit"
        )

    asyncio.run(_run())

    assert eng._round.position._tp_inflight is False, (
        "exception path must clear _tp_inflight so the next averaging-down "
        "tick can fall through the silent-return gate"
    )


# ---------------------------------------------------------------------------
# Test 3: CancelledError still propagates through the leak guard
# ---------------------------------------------------------------------------


def test_cancelled_error_is_not_swallowed_by_leak_guard() -> None:
    """
    `CancelledError` is a `BaseException` subclass in Py 3.8+. The
    `except Exception` clause must NOT catch it — asyncio shutdown
    semantics depend on that.
    """
    eng = _make_engine(mid=0.5)

    async def _handler(action: ActionTakeProfit) -> None:
        raise asyncio.CancelledError("parent task cancelled mid-emit")

    eng._handler = _handler

    async def _run() -> None:
        try:
            await eng._check_take_profit(bc_now=0.0)
        except asyncio.CancelledError:
            return
        raise AssertionError("CancelledError must propagate, not be swallowed")

    asyncio.run(_run())

    assert eng._round.position._tp_inflight is False, (
        "CancelledError path must still release the mutex"
    )


# ---------------------------------------------------------------------------
# Test 4: averaging-down no longer emits per-tick `silent_return_tp_inflight`
# ---------------------------------------------------------------------------


def test_check_average_down_does_not_emit_tp_inflight_warning() -> None:
    """
    The H-NEW-2026-07-11 diagnostic log `engine.t2_diag.silent_return_tp_inflight`
    fired every 250ms while `_tp_inflight` was True — a 15m round produced
    ~3600 identical rows. With the warning removed, the operator log is
    no longer drowned by the breadcrumb.
    """
    eng = _make_engine(mid=0.255)
    # Force the silent-return path: set the mutex and pre-populate the T2
    # throttle state so the engine falls straight through the early-return.
    eng._round.position._tp_inflight = True
    eng._round.position.tiers[1].placed = True
    eng._round.position.tiers[1].watching = True
    eng._round.position.tiers[1].server_order_id = "0xsrv"

    captured_warning_events: list[str] = []

    def _capture_warning(event: str, *args: Any, **fields: Any) -> None:
        captured_warning_events.append(event)

    with patch("src.strategy.engine.log") as mock_log:
        mock_log.warning.side_effect = _capture_warning
        mock_log.info.side_effect = lambda *a, **kw: None
        mock_log.error.side_effect = lambda *a, **kw: None
        mock_log.exception.side_effect = lambda *a, **kw: None

        async def _run() -> None:
            await eng._check_average_down(bc_now=0.0)

        asyncio.run(_run())

    spam_events = [
        e for e in captured_warning_events
        if "silent_return_tp_inflight" in e
        or "silent_return_mid_zero" in e
    ]
    assert spam_events == [], (
        f"_check_average_down must NOT emit the per-tick DIAG breadcrumb; "
        f"got: {spam_events!r}"
    )


# ---------------------------------------------------------------------------
# Test 5: averaging-down ALSO does not emit `silent_return_mid_zero`
# ---------------------------------------------------------------------------


def test_check_average_down_does_not_emit_mid_zero_warning() -> None:
    """Same as Test 4 but for the `silent_return_mid_zero` warning."""
    eng = _make_engine(mid=0.0)  # forces the mid <= 0 branch
    eng._round.position.tiers[1].placed = False
    eng._round.position.tiers[1].watching = False

    captured_warning_events: list[str] = []

    def _capture_warning(event: str, *args: Any, **fields: Any) -> None:
        captured_warning_events.append(event)

    with patch("src.strategy.engine.log") as mock_log:
        mock_log.warning.side_effect = _capture_warning
        mock_log.info.side_effect = lambda *a, **kw: None
        mock_log.error.side_effect = lambda *a, **kw: None
        mock_log.exception.side_effect = lambda *a, **kw: None

        async def _run() -> None:
            await eng._check_average_down(bc_now=0.0)

        asyncio.run(_run())

    spam_events = [
        e for e in captured_warning_events
        if "silent_return_mid_zero" in e
    ]
    assert spam_events == [], (
        f"_check_average_down must NOT emit the silent_return_mid_zero "
        f"breadcrumb; got: {spam_events!r}"
    )
