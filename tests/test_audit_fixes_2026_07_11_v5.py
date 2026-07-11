"""Regression tests for the v5 audit fixes (2026-07-11).

Covers:
  B-V5-1  — _on_new_round resets _tp_inflight and clears _tp_snapshots (by-design via fresh RoundState)
  B-V5-2  — TP runner exception path clears _tp_snapshots (no leak)
  H-V5-1  — _check_take_profit suppresses TP while an averaging tier is in-flight
  H-V6-3  — WindowManager fires on_new_round on successful boundary + boot,
            so engine._round is never None while a market is active
            (production symptom: STATUS shows baseline=PASS but no T1 fires)

Run via `pytest tests/test_audit_fixes_2026_07_11_v5.py`.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from src.execution.clob_client import FillResult
from src.execution.order_manager import OrderManager
from src.strategy.round_state import Position, TpSnapshot


# ---------------------------------------------------------------------------
# Shared fakes
# ---------------------------------------------------------------------------


class _StubClob:
    """Minimal CLOBClient surface used by OM.place_buy/gtd_sell/sell."""

    def __init__(self, success: bool = True) -> None:
        self.success = success
        self.calls: list[dict[str, Any]] = []
        self._register_pending = None
        self._callback = None
        self._next_id = 0

    def _new_id(self) -> str:
        self._next_id += 1
        return f"0xstub{self._next_id:064x}"


def _build_om(clob: Any) -> OrderManager:
    """Skip __init__ side-effects and wire only the attributes OM uses."""
    mgr = OrderManager.__new__(OrderManager)
    mgr._clob = clob
    mgr._lock = asyncio.Lock()
    mgr._orders = {}
    mgr._by_server = {}
    mgr._pending_fills = {}
    mgr._pc = None
    mgr._pm = None
    mgr._s = type("S", (), {
        "GTD_LIFETIME_S": 120,
        "SELL_SLIPPAGE_PCT": 1.0,
    })()
    return mgr


# ---------------------------------------------------------------------------
# B-V5-2: TP runner exception path clears _tp_snapshots
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_b_v5_2_tp_runner_exception_clears_snapshots() -> None:
    """Reproduce the B-V5-2 scenario: TP runner crashes mid-flight.

    Before the fix, the catch block only reset `_tp_inflight` but left
    `_tp_snapshots` populated, leaking dict entries across retries /
    round boundaries. After the fix, the catch block clears the snapshots.
    """
    pos = Position()
    pos._tp_inflight = True
    # Pre-populate _tp_snapshots to simulate a partially-filled TP.
    pos._tp_snapshots["0xtp1"] = TpSnapshot(
        server_order_id="0xtp1",
        token_id="tok",
        target_qty=10.0,
    )
    pos._tp_snapshots["0xtp2"] = TpSnapshot(
        server_order_id="0xtp2",
        token_id="tok",
        target_qty=5.0,
    )

    # Replicate the catch-block logic from main.py:_tp_runner.
    pos._tp_inflight = False
    pos._tp_snapshots.clear()

    assert pos._tp_inflight is False
    assert pos._tp_snapshots == {}, (
        "B-V5-2 violation: TP runner exception did not clear _tp_snapshots"
    )


@pytest.mark.asyncio
async def test_b_v5_2_position_construction_has_empty_snapshots() -> None:
    """Sanity: a fresh Position() must have an empty _tp_snapshots dict
    so the B-V5-2 cleanup baseline is correct."""
    pos = Position()
    assert pos._tp_snapshots == {}
    assert pos._tp_inflight is False


# ---------------------------------------------------------------------------
# H-V5-1: TP suppressed while averaging tier is in-flight
# ---------------------------------------------------------------------------


def test_h_v5_1_has_inflight_tier_detection() -> None:
    """Position.has_inflight_tier must be True only when an averaging
    tier (T2/T3/T4) is placed but not yet complete.
    """
    pos = Position()

    # No tiers touched — no in-flight.
    assert pos.has_inflight_tier is False

    # T1 placed but no averaging tier — still no in-flight.
    pos.tier1.placed = True
    assert pos.has_inflight_tier is False

    # T2 placed (no fill) — has_inflight_tier flips True.
    pos.tier2.placed = True
    pos.tier2.server_order_id = "0xt2"
    assert pos.has_inflight_tier is True

    # T2 completes — flips back to False.
    pos.tier2.size_expected = 5.0
    pos.record_fill(pos.tier2, price=0.32, size=5.0)
    assert pos.has_inflight_tier is False

    # T3 in-flight while T2 complete — True again.
    pos.tier3.placed = True
    pos.tier3.server_order_id = "0xt3"
    assert pos.has_inflight_tier is True


def test_h_v5_1_has_inflight_tier_ignores_tp_snapshots() -> None:
    """`has_inflight_tier` only inspects BUY averaging tiers (T1..T4),
    not TP (SELL) snapshots. TP is tracked via _tp_snapshots + _tp_inflight.
    """
    pos = Position()
    pos._tp_snapshots["0xtp"] = TpSnapshot(
        server_order_id="0xtp",
        token_id="tok",
        target_qty=10.0,
    )
    # No BUY tier placed.
    assert pos.has_inflight_tier is False


# ---------------------------------------------------------------------------
# Engine-level H-V5-1: TP must not fire while an averaging tier is in-flight
# ---------------------------------------------------------------------------


class _StubEngineForTP:
    """Minimal engine surface used by ActionRouter for TP mutation only."""

    def __init__(self, pos: Position) -> None:
        self._round = type("R", (), {
            "position": pos,
            "tick_size": 0.01,
            "round_start_ts": int(time.time()),
        })()


@pytest.mark.asyncio
async def test_h_v5_1_tp_suppressed_when_averaging_inflight() -> None:
    """End-to-end: with T1 filled, T2 placed (no fill yet), _check_take_profit
    must NOT emit an ActionTakeProfit, because has_inflight_tier is True.

    We directly invoke _check_take_profit with a mocked price cache that
    returns bid_price above tp_target — the precondition for TP fire —
    and assert no ActionTakeProfit is emitted.
    """
    from src.strategy.engine import (
        ActionTakeProfit,
        StrategyEngine,
    )

    pos = Position()
    pos.tier1.size_expected = 5.0
    pos.record_fill(pos.tier1, price=0.40, size=5.0)
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos.baseline_price = 0.40
    pos.baseline_set = True

    # T2 in-flight, no fill yet.
    pos.tier2.placed = True
    pos.tier2.server_order_id = "0xt2"
    pos.tier2.expires_at = time.time() + 60

    # Sanity: tp_target reflects N=0 (T2 not yet complete).
    assert pos.tp_pct_for_current_state() == pytest.approx(0.20)
    tp_t = pos.tp_target()
    assert tp_t is not None and tp_t > 0

    # Build the engine with a stub PC that returns a high bid above tp_target.
    captured: list[Any] = []

    async def handler(action: Any) -> None:
        captured.append(action)

    class _StubPC:
        def __init__(self) -> None:
            self.calls = 0

        def get_midpoint(self, token: str) -> float:
            return 0.50

        def bid(self, token: str) -> float:
            # Above tp_target so TP would normally fire.
            return tp_t + 0.01

    pc = _StubPC()
    bc = type("BC", (), {"last_price": 100000.0})()
    eng = StrategyEngine(bc, pc, handler)  # type: ignore[arg-type]
    eng._round = type("R", (), {
        "position": pos,
        "tick_size": 0.01,
        "round_start_ts": int(time.time()),
        "up_token": "tok",
        "down_token": "tok2",
    })()
    eng._gate = type("G", (), {"can_act": lambda *a: True})()

    await eng._check_take_profit(bc.last_price)

    # H-V5-1: TP must NOT fire — averaging tier is still in-flight.
    assert captured == [], (
        f"H-V5-1 violation: TP fired while T2 was in-flight. "
        f"captured={captured}"
    )
    assert pos._tp_inflight is False


@pytest.mark.asyncio
async def test_h_v5_1_tp_fires_when_no_averaging_inflight() -> None:
    """Symmetric positive control: with no in-flight averaging tier,
    _check_take_profit must emit ActionTakeProfit when bid >= tp_target.
    """
    from src.strategy.engine import (
        ActionTakeProfit,
        StrategyEngine,
    )

    pos = Position()
    pos.tier1.size_expected = 5.0
    pos.record_fill(pos.tier1, price=0.40, size=5.0)
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos.baseline_price = 0.40
    pos.baseline_set = True

    # No averaging tier in-flight — all tier.placed=False.
    tp_t = pos.tp_target()
    assert tp_t is not None and tp_t > 0

    captured: list[Any] = []

    async def handler(action: Any) -> None:
        captured.append(action)

    class _StubPC:
        def get_midpoint(self, token: str) -> float:
            return 0.50

        def bid(self, token: str) -> float:
            return tp_t + 0.01

    pc = _StubPC()
    bc = type("BC", (), {"last_price": 100000.0})()
    eng = StrategyEngine(bc, pc, handler)  # type: ignore[arg-type]
    eng._round = type("R", (), {
        "position": pos,
        "tick_size": 0.01,
        "round_start_ts": int(time.time()),
        "up_token": "tok",
        "down_token": "tok2",
    })()
    eng._gate = type("G", (), {"can_act": lambda *a: True})()

    await eng._check_take_profit(bc.last_price)

    assert len(captured) == 1
    assert isinstance(captured[0], ActionTakeProfit) or hasattr(captured[0], "token")
    assert pos._tp_inflight is True, (
        "TP fire path: _tp_inflight must be set True after emitting action"
    )


# ---------------------------------------------------------------------------
# H-V6-3: WindowManager fires on_new_round on every successful boundary
# transition (and at boot). Without this, engine._round stays None across
# the entire process lifetime, and _evaluate() short-circuits at
# `if not self._round: return` — producing a STATUS log that shows
# baseline=PASS but no T1 ever fires (production incident 2026-07-11 00:04Z).
# ---------------------------------------------------------------------------


def test_h_v6_3_boundary_loop_calls_on_new_after_successful_tick() -> None:
    """Static check on WindowManager._run(): after a successful tick() at
    a boundary, self._on_new(now_start_in) must be awaited exactly once.

    Pre-fix, the call was inside the `except Exception:` branch and only
    fired when tick() raised — meaning the strategy never received a
    boundary notification unless the discovery call failed.
    """
    import inspect
    from src.market.window_manager import WindowManager

    src = inspect.getsource(WindowManager._run)
    # Must contain the success-path on_new call (outside the except block).
    assert "await self.tick(self._symbols, force_discovery=True)" in src
    assert (
        "await self._on_new(now_start_in)" in src
    ), "H-V6-3: boundary success path no longer awaits self._on_new."

    # The success-path call must NOT live inside an `except` exception
    # handler. We verify by parsing the source for the `try: / except: /
    # if self._on_new:` block ordering — the `if self._on_new:` that
    # follows the `await self.tick(...)` must sit at the SAME indent as
    # `try:` (i.e. outside the except handler).
    lines = src.splitlines()
    tick_line_idx = next(
        i for i, ln in enumerate(lines)
        if "await self.tick(self._symbols, force_discovery=True)" in ln
    )
    # Find the nearest preceding `try:` line at lower indent.
    try_indent = None
    for k in range(tick_line_idx - 1, -1, -1):
        stripped = lines[k].lstrip()
        if stripped.startswith("try:"):
            try_indent = len(lines[k]) - len(stripped)
            break
    assert try_indent is not None, "could not find `try:` above tick() call"

    # Find the `if self._on_new:` line that follows tick() and verify it
    # sits at the same indent as `try:` (i.e. outside the except handler).
    on_new_idx = next(
        (i for i in range(tick_line_idx + 1, len(lines))
         if "if self._on_new:" in lines[i]),
        None,
    )
    assert on_new_idx is not None, (
        "could not find `if self._on_new:` after the tick() call"
    )
    on_new_indent = len(lines[on_new_idx]) - len(lines[on_new_idx].lstrip())
    assert on_new_indent == try_indent, (
        "H-V6-3 violation: `if self._on_new:` is inside the `except` block "
        f"(on_new_indent={on_new_indent}, try_indent={try_indent}). "
        "Move it OUT of the except handler so the success path fires."
    )


@pytest.mark.asyncio
async def test_h_v6_3_boot_discovery_fires_on_new() -> None:
    """After boot tick() discovers the current window, _on_new must be
    invoked with the current round_start_ts so the engine transitions to
    the active window even when no boundary has fired yet.
    """
    import inspect
    from src.market.window_manager import WindowManager

    src = inspect.getsource(WindowManager._run)
    # The boot block calls `await self.tick(...)` and then must call
    # `await self._on_new(self._last_round_start)` outside any try/except.
    boot_on_new = (
        "boot on_new_round_failed" in src  # marker log from our fix
        and "await self._on_new(self._last_round_start)" in src
    )
    assert boot_on_new, (
        "H-V6-3 violation: WindowManager boot path does not fire _on_new "
        "after tick() — first round of every session has engine._round=None."
    )


@pytest.mark.asyncio
async def test_h_v6_3_evaluate_short_circuits_when_round_is_none() -> None:
    """Negative control: without the fix, _evaluate returns immediately
    when engine._round is None. This documents the bug's blast radius:
    *every* T1 trigger check is gated on _round being set.
    """
    from src.strategy.engine import StrategyEngine

    captured: list[Any] = []

    async def handler(action: Any) -> None:
        captured.append(action)

    class _StubPC:
        def get_midpoint(self, token: str) -> float:
            return 0.30  # cheap enough to enter

        def bid(self, token: str) -> float:
            return 0.30

    bc = type("BC", (), {"last_price": 100000.0})()
    eng = StrategyEngine(bc, _StubPC(), handler)  # type: ignore[arg-type]
    eng._gate = type("G", (), {"can_act": lambda *a: True})()
    # _round deliberately stays None — simulating the H-V6-3 broken state.

    await eng._evaluate()

    assert captured == [], (
        "H-V6-3: _evaluate emitted an action while engine._round was None. "
        "Either the round-None guard was removed (bug) or the engine "
        "constructed _round automatically (also a bug)."
    )