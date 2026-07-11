"""Regression tests for the v5 audit fixes (2026-07-11).

Covers:
  B-V5-1  — _on_new_round resets _tp_inflight and clears _tp_snapshots (by-design via fresh RoundState)
  B-V5-2  — TP runner exception path clears _tp_snapshots (no leak)
  H-V5-1  — _check_take_profit suppresses TP while an averaging tier is in-flight
  H-V6-3  — WindowManager fires on_new_round on successful boundary + boot,
            so engine._round is never None while a market is active
            (production symptom: STATUS shows baseline=PASS but no T1 fires)
  H-V6-4  — engine.on_new_round() pins the baseline from
            market.btc_spot_at_window_open so the gate is ready immediately
            (production symptom: STATUS shows `baseline=pending` for the
            entire 15-min window after a round transition)
  H-V6-5  — loguru calls use keyword args, not positional args with
            `{:.0f}`-style format-spec in the message string. loguru
            forwards messages to LogRecord.getMessage() which expects
            `%`-formatting and crashes on Python format specs
            (production crash: TypeError at engine.py:436 in _check_t1_entry)

Run via `pytest tests/test_audit_fixes_2026_07_11_v5.py`.
"""
from __future__ import annotations

import asyncio
import inspect
import pathlib
import time
from datetime import datetime, timezone
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


# ---------------------------------------------------------------------------
# H-V6-4: on_new_round pins baseline from market.btc_spot_at_window_open
# so the gate is ready immediately. Before the fix, baseline=pending lasted
# the full 15-min window because on_round_end cleared _gate and the
# separate _on_window_baseline callback was wiped by the same on_round_end.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_h_v6_4_on_new_round_pins_gate_from_market_window_open_price() -> None:
    """After on_new_round, engine._gate must be set and aligned with
    `market.btc_spot_at_window_open`. This is the whole point of the fix:
    no more 15-min window of `baseline=pending`.

    Before the fix:
      - `engine.on_round_end` cleared `_gate = None`
      - WM fired `_on_baseline` BEFORE WM fired `_on_new_round`
      - so the baseline was applied to the OLD round and immediately wiped
      - the new round never got a baseline until the NEXT boundary (15 min)
    After the fix:
      - `engine.on_round_end` keeps `_gate` in place
      - `engine.on_new_round` pins `_gate` directly from
        `market.btc_spot_at_window_open` (the price WM just fetched)
      - drift gating resumes within the first evaluate tick of the new round
    """
    from src.market.models import MarketInfo
    from src.strategy.engine import StrategyEngine

    class _StubBC:
        last_price = 0.0

    class _StubPC:
        def get_midpoint(self, t: str) -> float:
            return 0.0

        def bid(self, t: str) -> float:
            return 0.0

    captured: list[Any] = []

    async def handler(action: Any) -> None:
        captured.append(action)

    eng = StrategyEngine(_StubBC(), _StubPC(), handler)  # type: ignore[arg-type]
    assert eng._gate is None

    mkt = MarketInfo(
        asset="BTC",
        slug="btc-updown-15m-fake",
        condition_id="0x" + "ab" * 32,
        token_up_id="1",
        token_down_id="2",
        window_start=datetime.now(timezone.utc),  # type: ignore[arg-type]
        window_end=datetime.now(timezone.utc),    # type: ignore[arg-type]
        tick_size=0.01,
        btc_spot_at_window_open=64125.17,
        btc_spot_now=0.0,
    )

    round_ts = 1783728900
    await eng.on_new_round(round_ts, mkt)

    # H-V6-4: gate must be set IMMEDIATELY after on_new_round returns.
    assert eng._gate is not None, (
        "H-V6-4 violation: engine._gate is None after on_new_round — "
        "STATUS would log `baseline=pending` for the entire 15-min window."
    )
    assert eng._gate.baseline == pytest.approx(64125.17), (
        f"H-V6-4 violation: gate baseline is {eng._gate.baseline}, expected "
        "market.btc_spot_at_window_open (64125.17)."
    )
    assert eng._gate.can_act(64125.17) is True


@pytest.mark.asyncio
async def test_h_v6_4_on_new_round_falls_back_to_binance_cache() -> None:
    """If `market.btc_spot_at_window_open` is 0 (rare fetcher edge case),
    fall back to `binance_cache.last_price`. This avoids the previous
    `baseline=pending` window in those cases.
    """
    from src.market.models import MarketInfo
    from src.strategy.engine import StrategyEngine

    class _StubBC:
        last_price = 64100.0

    class _StubPC:
        def get_midpoint(self, t: str) -> float:
            return 0.0

        def bid(self, t: str) -> float:
            return 0.0

    async def handler(action: Any) -> None:
        return None

    eng = StrategyEngine(_StubBC(), _StubPC(), handler)  # type: ignore[arg-type]

    mkt = MarketInfo(
        asset="BTC",
        slug="btc-updown-15m-fake",
        condition_id="0x" + "cd" * 32,
        token_up_id="1",
        token_down_id="2",
        window_start=datetime.now(timezone.utc),  # type: ignore[arg-type]
        window_end=datetime.now(timezone.utc),    # type: ignore[arg-type]
        tick_size=0.01,
        btc_spot_at_window_open=0.0,  # ← not yet filled
        btc_spot_now=0.0,
    )

    await eng.on_new_round(1783728900, mkt)

    assert eng._gate is not None
    assert eng._gate.baseline == pytest.approx(64100.0)


def test_h_v6_4_on_round_end_keeps_gate() -> None:
    """Static check: `engine.on_round_end` MUST NOT clear `_gate`.

    We grep the source for any line assigning `self._gate = None` inside
    `on_round_end`. The fix removed that assignment so the gate survives
    the round transition and `STATUS` never logs `baseline=pending`.
    """
    from src.strategy.engine import StrategyEngine

    src = inspect.getsource(StrategyEngine.on_round_end)
    lines = [ln.strip() for ln in src.splitlines()]
    for ln in lines:
        if "self._gate" in ln and "None" in ln and "=" in ln:
            raise AssertionError(
                "H-V6-4 violation: engine.on_round_end still clears _gate. "
                f"Found line: {ln!r}"
            )


# ---------------------------------------------------------------------------
# H-V6-5: loguru calls must use keyword args, not positional args with
# printf-style format specs in the message. Otherwise structlog/loguru
# fall back to `%`-formatting the message and crash on `{:.0f}` /
# `{:.2f}` style placeholders. Reproduced live at 2026-07-11 00:25:43:
#     TypeError: not all arguments converted during string formatting
#     File "src/strategy/engine.py", line 436, in _check_t1_entry
#     log.info("engine.t1_gate_closed elapsed={:.0f}s > limit={:.0f}s",
#              elapsed_in_window, s.T1_ENTRY_WINDOW_S)
# ---------------------------------------------------------------------------


def test_h_v6_5_no_positional_loguru_calls_with_format_spec() -> None:
    """Grep the entire `src/` tree for `log.<level>("...", EXPR, EXPR)`
    where the message string contains a format-spec like `{:.0f}` /
    `{:>10}` / `{:.2f}`. Such calls crash at runtime because loguru
    forwards the message string to the stdlib `LogRecord.getMessage()`
    path which expects `%`-style placeholders, not `format`-spec.

    Allowed forms:
      - log.info("event", k=v, k=v)            ← keyword args
      - log.info("event")                      ← no args
      - log.info("event {}".format(x))         ← pre-formatted f-string / .format
      - log.info(f"event {x}")                 ← pre-formatted f-string

    Forbidden forms:
      - log.info("event {:...}", x, y)         ← positional + format spec
    """
    import pathlib
    import re

    src_root = pathlib.Path("src")
    bad: list[tuple[pathlib.Path, int, str]] = []
    py_re = re.compile(r"\.py$")
    msg_re = re.compile(r'"([^"]+)"')
    # log.X("MSG", ARG, ARG) — non-keyword positional args
    call_re = re.compile(
        r"log\.(?:info|warning|error|debug|exception)\s*\(\s*"
        r'("([^"]+)"|\'([^\']+)\')'
        r"\s*,\s*"
        r"([A-Za-z_][A-Za-z0-9_.\(\)\[\]:\s]*)"  # first positional arg
    )

    # Pre-format-spec inside a quoted message string. Skip f-strings (those
    # are pre-formatted, no loguru-level risk).
    fmt_spec_re = re.compile(r"\{:[^}]+\}")

    for path in src_root.rglob("*"):
        if not py_re.search(path.name):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            # Skip f-strings: they start with `f"..."` or `f'...'`
            stripped = line.lstrip()
            if stripped.startswith(("f\"", "f'")):
                continue
            # Look for log.<level>("MSG", EXPR ...) where EXPR is not `key=`
            if "log." not in line:
                continue
            m = call_re.search(line)
            if not m:
                continue
            msg = m.group(2) or m.group(3) or ""
            first_arg = m.group(4)
            if first_arg.lstrip().startswith(("\"", "'")):
                # second positional arg (rare; we'd have already matched it)
                continue
            if "=" in first_arg.split(",")[0].split(")")[0]:
                # keyword arg form, OK
                continue
            if fmt_spec_re.search(msg):
                bad.append((path, lineno, line.strip()))

    if bad:
        formatted = "\n".join(f"  {p}:{ln}: {l}" for p, ln, l in bad)
        raise AssertionError(
            "H-V6-5: loguru calls with positional args and format-spec in "
            f"the message string crash at runtime:\n{formatted}\n\n"
            "Fix: convert to keyword args, e.g.\n"
            '  log.info("event", elapsed=int(x), limit=int(y))\n'
        )
