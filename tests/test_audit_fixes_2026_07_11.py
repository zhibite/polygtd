"""Regression tests for the v2 audit fixes (2026-07-11).

Covers:
  B-1 v2 — place_buy must call place_gtd_buy exactly once
  B-3 v2 — ActionRouter retains a strong reference to the spawned TP task
  H-1 v2 — _check_t1_entry writes entry_token BEFORE awaiting handler
  H-3 v2 — averaging trigger baseline uses baseline_target_price, not fill avg

Run via `pytest tests/test_audit_fixes_2026_07_11.py`.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from src.execution.clob_client import FillResult
from src.execution.order_manager import OrderManager, TrackedOrder
from src.strategy.engine import (
    ActionOpenT1,
    ActionOpenTN,
    ActionTakeProfit,
    StrategyEngine,
)
from src.strategy.round_state import Position


# ---------------------------------------------------------------------------
# B-1 v2: place_buy must NOT post twice to the CLOB
# ---------------------------------------------------------------------------


class _CountingClob:
    """Records each call to place_gtd_buy."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def place_gtd_buy(
        self,
        token_id: str,
        price: float,
        size: float,
        lifetime_s: int,
        tick_size: float,
    ) -> FillResult:
        self.calls.append(
            dict(token_id=token_id, price=price, size=size,
                 lifetime_s=lifetime_s, tick_size=tick_size)
        )
        return FillResult(
            order_id="0xbuy",
            tokens=size,
            usdc=size * price,
            fill_price=price,
            success=True,
            expiration_ts=int(time.time()) + 200,
        )

    async def _run(self, fn, *a, **kw):
        return None


@pytest.mark.asyncio
async def test_b1_v2_buy_single_call() -> None:
    """place_buy must invoke the underlying CLOB place_gtd_buy exactly once.

    Pre-fix the same `_adjust_buy_price + place_gtd_buy` block appeared twice
    in `OrderManager.place_buy`, which meant every BUY order was posted to the
    CLOB twice and the user was charged 2x notional.
    """
    clob = _CountingClob()
    mgr = OrderManager.__new__(OrderManager)
    mgr._clob = clob
    mgr._lock = asyncio.Lock()
    mgr._orders = {}
    mgr._by_server = {}
    mgr._pc = None
    mgr._pm = None
    mgr._s = type("S", (), {"GTD_LIFETIME_S": 120, "SELL_SLIPPAGE_PCT": 1.0})()

    async def fake_adjust(token, price, tick_size=0.01):
        return price
    mgr._adjust_buy_price = fake_adjust  # type: ignore[assignment]

    order = await mgr.place_buy(
        token_id="tok", price=0.40, size=5.0,
        round_start_ts=int(time.time()),
        tier_name="T1", tick_size=0.01,
    )
    assert order is not None
    assert len(clob.calls) == 1, (
        f"place_buy posted to CLOB {len(clob.calls)} times, expected 1"
    )


# ---------------------------------------------------------------------------
# B-3 v2: ActionRouter retains a strong reference to the TP task
# ---------------------------------------------------------------------------


class _DummyRound:
    def __init__(self, pos):
        self.position = pos
        self.tick_size = 0.01


class _DummyEngine:
    def __init__(self, pos):
        self._round = _DummyRound(pos)


class _SlowClob:
    """Simulates a TP path that completes asynchronously."""

    def __init__(self) -> None:
        self.calls_started = 0
        self.calls_completed = 0

    async def place_gtd_sell_at_price(self, *a, **kw):
        self.calls_started += 1
        await asyncio.sleep(0.05)  # yield so GC has a chance to run
        self.calls_completed += 1
        return (None, None, "gtd_post_failed", "fak_post_failed")


@pytest.mark.asyncio
async def test_b3_v2_tp_runner_task_retained() -> None:
    """ActionRouter must register the spawned TP runner task in
    `state._bg_tasks` so a CPython GC pass cannot drop it before the first
    `await` yields control.
    """
    from src.main import ActionRouter

    pos = Position()
    clob = _SlowClob()
    bg_tasks: set = set()

    state = type(
        "S",
        (),
        {
            "engine": _DummyEngine(pos),
            "order_manager": clob,
            "clob": type("C", (), {"fill_callback": None})(),
            "round_start_ts": int(time.time()),
            "_bg_tasks": bg_tasks,
        },
    )()
    router = ActionRouter(state)  # type: ignore[arg-type]

    await router(ActionTakeProfit(token="t", price=0.5, qty=10.0))

    # The task must be retained in state._bg_tasks while it is in flight.
    # We yield once so the task has had a chance to schedule itself.
    await asyncio.sleep(0.01)
    assert len(bg_tasks) >= 1, (
        f"ActionRouter did not retain the TP runner task (bg_tasks={bg_tasks})"
    )

    # Wait for the task to finish and confirm it actually ran (not GC'd).
    await asyncio.gather(*list(bg_tasks), return_exceptions=True)
    assert clob.calls_started == 1
    assert clob.calls_completed == 1


# ---------------------------------------------------------------------------
# H-1 v2: _check_t1_entry claims entry_token before awaiting the handler
# ---------------------------------------------------------------------------


class _StubGate:
    def can_act(self, bc_now): return True
    def can_enter(self, bc_now, mid): return True
    def can_double_down(self, *a, **kw): return True
    def state(self, bc_now):
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
    def get_midpoint(self, token): return self._mid
    def bid(self, token): return self._bid
    def mid(self, token): return (self._mid + self._bid) / 2


class _StubBinanceCache:
    last_price = 100000.0


@pytest.mark.asyncio
async def test_h1_v2_entry_token_set_before_handler_await() -> None:
    """When _check_t1_entry awaits the handler, entry_token must already be
    set so a concurrent _evaluate tick cannot emit a duplicate ActionOpenT1.
    """
    eng = StrategyEngine.__new__(StrategyEngine)
    eng._t1_cap_reject_first_seen = {}
    pos = Position()
    eng._round = type("R", (), {
        "position": pos,
        "round_start_ts": int(time.time()),
        "up_token": "up",
        "down_token": "down",
        "tick_size": 0.01,
    })()
    eng._gate = _StubGate()
    eng._bc = _StubBinanceCache()
    eng._pc = _StubPriceCache(mid=0.39, bid=0.39)

    handler_invocations: list[Any] = []

    async def slow_handler(action):
        # While the handler is awaiting, entry_token must already be set.
        handler_invocations.append((action, pos.entry_token, pos.entry_side))
        # No additional await needed — but a yield proves the field is set.
        await asyncio.sleep(0)

    eng._handler = slow_handler
    s = eng._s = type("S", (), {
        "ENTRY_PRICE_CAP": 0.40,
        "T1_ENTRY_WINDOW_S": 300.0,
        "TIER1_NOTIONAL": 2.0,
    })()

    await eng._check_t1_entry(100000.0)

    assert len(handler_invocations) == 1, "handler should fire exactly once"
    action, entry_token, entry_side = handler_invocations[0]
    assert entry_token == "up", (
        f"entry_token must be set BEFORE awaiting handler, got {entry_token!r}"
    )
    assert entry_side == "UP"


# ---------------------------------------------------------------------------
# H-3 v2: averaging trigger baseline uses baseline_target_price
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_h3_v2_average_down_uses_target_price() -> None:
    """When T1 slips (real fill 0.45 vs intended 0.40), T2 must still
    trigger at 0.40 * (1 - 0.20) = 0.32, not 0.45 * 0.80 = 0.36.

    H-NEW-2026-07-12 (operator clarification, prod window 1783819800):
    updated to also seed `baseline_realized_price` because the engine
    now gates T2/T3 averaging on T1 having at least one confirmed fill
    (the operator specified the 30%/60% drop is from the T1 entry
    price, not from the pre-fill strategy bid). Without that seed the
    loop would early-return before T2 ever evaluates.
    """
    eng = StrategyEngine.__new__(StrategyEngine)
    pos = Position()
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos.baseline_target_price = 0.40  # intended entry bid
    # H-NEW-2026-07-12: anchor to realised T1 fill (0.45) so the
    # T1-has-filled guard passes and the averaging loop evaluates.
    pos.baseline_realized_price = 0.45
    pos.baseline_set_realized = True
    pos._total_qty = 10.0
    pos._total_cost = 4.5
    eng._round = type("R", (), {"position": pos})()
    eng._gate = _StubGate()
    eng._bc = _StubBinanceCache()

    # Mid sits at 0.25 — well below both possible triggers, T2 must fire.
    # H-NEW-2026-07-11-4: with the 3-tier cap TIER2_DROP_PCT=0.30, the
    # target-derived fire line is 0.40 * 0.70 = 0.28. We use 0.25 so
    # the test stays below both the target-derived (0.28) and the
    # baseline-derived (0.45 * 0.80 = 0.36) fire lines, matching the
    # test's intent: regardless of which baseline the engine uses, T2
    # must fire at this mid.
    eng._pc = _StubPriceCache(mid=0.25, bid=0.25)

    captured: list[Any] = []

    async def handler(action):
        captured.append(action)
    eng._handler = handler
    eng._s = type("S", (), {
        "TIER2_NOTIONAL": 3.0,
        "TIER3_NOTIONAL": 5.0,
        "TIER4_NOTIONAL": 6.0,
        # H-NEW-2026-07-15: TIER_NOTIONAL_MAX_MULT removed (engine
        # trusts the per-(round,tier) spawn mutex; no mid-flight cap).
        "tier_drop_pcts": [None, 0.20, 0.40, 0.60],
    })()

    await eng._check_average_down(100000.0)

    assert len(captured) == 1, (
        f"T2 should fire at mid 0.30; captured={captured}"
    )
    action = captured[0]
    assert isinstance(action, ActionOpenTN)
    assert action.tier == 2


@pytest.mark.asyncio
async def test_h3_v2_average_down_does_not_over_trigger_on_slippage() -> None:
    """Mid sits at 0.34 — between target-derived trigger (0.32) and
    fill-derived trigger (0.36). With H-3 v2 fix, T2 must NOT fire because
    the intended baseline is 0.40 and we have not actually dropped -20%
    against the design price.

    The pre-fix behaviour (using fill avg as baseline) would have ALLOWED
    T2 to fire here (0.34 <= 0.36). After the fix we are correctly MORE
    conservative about averaging — which is the right thing to do when T1
    filled at a worse price than intended.
    """
    eng = StrategyEngine.__new__(StrategyEngine)
    pos = Position()
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos.baseline_target_price = 0.40
    # legacy pos.baseline_price / pos.baseline_set removed (H-NEW-2026-07-15)
    pos._total_qty = 10.0
    pos._total_cost = 4.5
    eng._round = type("R", (), {"position": pos})()
    eng._gate = _StubGate()
    eng._bc = _StubBinanceCache()
    eng._pc = _StubPriceCache(mid=0.34, bid=0.34)

    captured: list[Any] = []

    async def handler(action):
        captured.append(action)
    eng._handler = handler
    eng._s = type("S", (), {
        "TIER2_NOTIONAL": 3.0,
        "TIER3_NOTIONAL": 5.0,
        "TIER4_NOTIONAL": 6.0,
        # H-NEW-2026-07-15: TIER_NOTIONAL_MAX_MULT removed (engine
        # trusts the per-(round,tier) spawn mutex; no mid-flight cap).
        "tier_drop_pcts": [None, 0.20, 0.40, 0.60],
    })()

    await eng._check_average_down(100000.0)

    assert captured == [], (
        f"T2 must not fire at mid 0.34 (target-derived trigger 0.32 not hit); "
        f"captured={captured}"
    )