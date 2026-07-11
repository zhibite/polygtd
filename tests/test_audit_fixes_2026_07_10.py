"""Audit-fix regression tests (B-1 / B-4 / H-1 / H-3 / H-4 / M-1).

Each test focuses on one audit item and verifies the fix path. Tests are
run via `pytest tests/test_audit_fixes_2026_07_10.py`.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from src.execution.clob_client import CLOBClient, FillResult
from src.execution.order_manager import (
    OrderManager,
    TP_FALLBACK_BUFFER_S,
    TP_GTD_LIFETIME_S,
    TrackedOrder,
)
from src.strategy.engine import (
    ActionOpenT1,
    ActionTakeProfit,
    StrategyEngine,
)
from src.strategy.round_state import Position, TierSnapshot


# ---------------------------------------------------------------------------
# M-1: size_expected uses placed_size, not notional/expected_price
# ---------------------------------------------------------------------------


def test_m1_size_expected_uses_placed_size_when_provided() -> None:
    """placed_size=5.128 must NOT be replaced by notional/expected_price."""
    eng = StrategyEngine.__new__(StrategyEngine)
    # Minimal stubbing
    eng._round = type("R", (), {"position": Position()})()
    # notional for T1 = TIER1_NOTIONAL
    eng.bind_server_order_id(
        tier=1,
        server_order_id="x",
        token_id="tok",
        expires_at=time.time() + 180,
        expected_price=0.391,
        placed_size=5.1282,
    )
    t = eng._round.position.tier1
    assert abs(t.size_expected - 5.1282) < 1e-9


def test_m1_size_expected_falls_back_when_no_placed_size() -> None:
    """If placed_size is None, fall back to notional/expected_price."""
    eng = StrategyEngine.__new__(StrategyEngine)
    eng._round = type("R", (), {"position": Position()})()
    eng.bind_server_order_id(
        tier=1,
        server_order_id="x",
        token_id="tok",
        expires_at=time.time() + 180,
        expected_price=0.40,
        placed_size=None,
    )
    t = eng._round.position.tier1
    # TIER1_NOTIONAL default ≈ 2.0; 2.0 / 0.40 = 5.0
    assert abs(t.size_expected - 5.0) < 1e-9


def test_m1_record_fill_triggers_is_complete_exactly() -> None:
    """Fill == placed_size must flip is_complete in the same record_fill call."""
    eng = StrategyEngine.__new__(StrategyEngine)
    pos = Position()
    eng._round = type("R", (), {"position": pos})()
    eng.bind_server_order_id(
        tier=1,
        server_order_id="x",
        token_id="tok",
        expires_at=time.time() + 180,
        expected_price=0.391,
        placed_size=5.1282,
    )
    t = pos.tier1
    # 99.5% of 5.1282 = 5.1026; a 5.1282 fill exceeds by 0.0256
    pos.record_fill(t, price=0.391, size=5.1282)
    assert t.is_complete is True


# ---------------------------------------------------------------------------
# H-3: TrackedOrder.expires_at mirrors CLOB server expiration_ts
# ---------------------------------------------------------------------------


class _FakeCLOBForH3:
    """Returns a FillResult with a known expiration_ts."""

    def __init__(self, expected_expiration_ts: int) -> None:
        self._expected = expected_expiration_ts

    async def place_gtd_buy(self, **kw: Any) -> FillResult:
        return FillResult(
            order_id="0xsid",
            tokens=5.0, usdc=5.0 * 0.40,
            fill_price=0.40, success=True,
            expiration_ts=self._expected,
        )

    async def _run(self, fn, *args, **kwargs):
        return None


@pytest.mark.asyncio
async def test_h3_tracked_order_expires_at_uses_server_value() -> None:
    """TrackedOrder.expires_at must equal result.expiration_ts (server)."""
    expected_ts = 1_700_000_000
    fake = _FakeCLOBForH3(expected_ts)
    mgr = OrderManager.__new__(OrderManager)
    mgr._clob = fake
    mgr._lock = asyncio.Lock()
    mgr._orders = {}
    mgr._by_server = {}
    mgr._pc = None
    mgr._pm = None
    mgr._s = type("S", (), {"GTD_LIFETIME_S": 120, "SELL_SLIPPAGE_PCT": 1.0})()

    order = await mgr.place_buy(
        token_id="tok", price=0.40, size=5.0,
        round_start_ts=int(time.time()),
        tier_name="T1", tick_size=0.01,
    )
    assert order is not None
    assert order.expires_at == float(expected_ts)


@pytest.mark.asyncio
async def test_h3_place_gtd_sell_tracked_order_uses_server_value() -> None:
    expected_ts = 1_700_000_500
    fake = _FakeCLOBForH3(expected_ts)

    # Override place_gtd_sell path
    class C:
        def __init__(self, ts):
            self.ts = ts
        async def place_gtd_sell(self, **kw):
            return FillResult(
                order_id="0xsid", tokens=5.0,
                usdc=2.5, fill_price=0.50, success=True,
                expiration_ts=self.ts,
            )
    fake.place_gtd_sell = C(expected_ts).place_gtd_sell
    # Make _adjust_sell_price a no-op so the test stays simple
    mgr = OrderManager.__new__(OrderManager)
    mgr._clob = fake
    mgr._lock = asyncio.Lock()
    mgr._orders = {}
    mgr._by_server = {}
    mgr._pc = None
    mgr._pm = None
    mgr._s = type("S", (), {"GTD_LIFETIME_S": 120, "SELL_SLIPPAGE_PCT": 1.0})()
    # Avoid network call inside _adjust_sell_price by having no _pm
    mgr._adjust_sell_price = AsyncNoop()  # type: ignore[assignment]

    order = await mgr.place_gtd_sell(
        token_id="tok", size=5.0, price=0.50,
        round_start_ts=int(time.time()),
        tier_name="TP", tick_size=0.01,
    )
    assert order is not None
    assert order.expires_at == float(expected_ts)


class AsyncNoop:
    async def __call__(self, *a, **kw):
        return a[1] if len(a) > 1 else 0.0


# ---------------------------------------------------------------------------
# H-1: _tp_runner does not stall the router and releases mutex on crash
# ---------------------------------------------------------------------------


class _CrashyCLOB:
    # H-NEW-2026-07-11: replaced place_tp_with_fallback with
    # place_gtd_sell_at_price on the TP path.
    async def place_gtd_sell_at_price(self, *a, **kw):
        raise RuntimeError("simulated CLOB crash")


class _DummyRound:
    def __init__(self, pos):
        self.position = pos
        self.tick_size = 0.01


class _DummyEngine:
    def __init__(self, pos):
        self._round = _DummyRound(pos)


@pytest.mark.asyncio
async def test_h1_tp_runner_crash_releases_inflight_mutex() -> None:
    from src.main import ActionRouter

    pos = Position()
    pos._tp_inflight = True

    state = type(
        "S",
        (),
        {
            "engine": _DummyEngine(pos),
            "order_manager": _CrashyCLOB(),
            "clob": type("C", (), {"fill_callback": None})(),
            "round_start_ts": int(time.time()),
            # B-3 v2 fix: ActionRouter now retains a strong reference to
            # the spawned TP runner task via `state._bg_tasks`; supply an
            # empty set here so the test stub mirrors the real AppState.
            "_bg_tasks": set(),
        },
    )()
    router = ActionRouter(state)  # type: ignore[arg-type]

    start = time.monotonic()
    await router(ActionTakeProfit(token="t", price=0.5, qty=10.0))
    elapsed = time.monotonic() - start

    # router returns immediately (no awaiting the TP fallback)
    assert elapsed < 0.1
    # let the spawned task drain
    await asyncio.sleep(0.05)
    # the spawned task should release _tp_inflight on the simulated crash
    assert pos._tp_inflight is False


# ---------------------------------------------------------------------------
# H-4: stage error tags returned by place_tp_with_fallback
# ---------------------------------------------------------------------------


class _FakeClobH4:
    """Two-stage fake: GTD post fails, then FAK fills."""

    def __init__(self) -> None:
        self.place_gtd_sell_calls = 0
        self.place_fak_sell_calls = 0

    async def place_gtd_sell(self, **kw):
        self.place_gtd_sell_calls += 1
        return FillResult("", 0, 0, 0.50, False, "gtd_post_failed")

    async def place_fak_sell(self, **kw):
        self.place_fak_sell_calls += 1
        return FillResult(
            order_id="0xFak",
            tokens=10.0, usdc=5.0, fill_price=0.50, success=True,
        )

    async def _run(self, fn, *a, **kw):
        return None


@pytest.mark.asyncio
async def test_h4_stage_error_tags_propagate() -> None:
    fake = _FakeClobH4()
    mgr = OrderManager.__new__(OrderManager)
    mgr._clob = fake
    mgr._lock = asyncio.Lock()
    mgr._orders = {}
    mgr._by_server = {}
    mgr._pc = None
    mgr._pm = None
    mgr._s = type("S", (), {"GTD_LIFETIME_S": 120, "SELL_SLIPPAGE_PCT": 1.0})()
    mgr._adjust_sell_price = AsyncNoop()  # type: ignore[assignment]

    gtd, fak, gtd_err, fak_err = await mgr.place_tp_with_fallback(
        token_id="t", size=10.0, price=0.50,
        round_start_ts=int(time.time()),
        tier_name="TP", tick_size=0.01,
    )

    assert gtd is None and fak is not None
    assert gtd_err == "gtd_post_failed"
    assert fak_err == ""  # success on FAK
    assert fake.place_gtd_sell_calls == 1
    assert fake.place_fak_sell_calls == 1


# ---------------------------------------------------------------------------
# B-1: DecisionLoop drives _evaluate, midpoint poll is cache-only
# ---------------------------------------------------------------------------


class _StubEngine:
    def __init__(self) -> None:
        self.calls = 0

    async def _evaluate(self) -> None:
        self.calls += 1


@pytest.mark.asyncio
async def test_b1_decision_loop_drives_evaluate() -> None:
    from src.strategy.loop import DecisionLoop

    eng = _StubEngine()
    loop = DecisionLoop(eng, tick_interval_s=0.05)  # type: ignore[arg-type]
    loop.start()
    await asyncio.sleep(0.25)
    await loop.stop()
    # ~3-5 ticks in 250ms
    assert eng.calls >= 2


@pytest.mark.asyncio
async def test_b1_inflight_lock_drops_re_entry() -> None:
    from src.strategy.loop import DecisionLoop

    eng = _StubEngine()
    # Pre-set inflight by patching the lock-held check
    loop = DecisionLoop(eng, tick_interval_s=0.05)  # type: ignore[arg-type]
    # Pre-acquire the lock so the next tick is dropped
    await loop._inflight.acquire()
    try:
        await loop._tick()  # should drop, not increment
        assert eng.calls == 0
    finally:
        loop._inflight.release()


# ---------------------------------------------------------------------------
# B-4: TP trigger uses bid, not mid
# ---------------------------------------------------------------------------


class _StubGate:
    def can_act(self, bc_now): return True


class _StubPriceCache:
    def __init__(self, mid: float, bid: float) -> None:
        self._mid = mid
        self._bid = bid
    def get_midpoint(self, token): return self._mid
    def bid(self, token): return self._bid


class _StubBinanceCache:
    last_price = 100000.0


@pytest.mark.asyncio
async def test_b4_tp_trigger_uses_bid_not_mid() -> None:
    """mid=0.48 (above tp_target=0.47) but bid=0.45 (below) → no fire."""
    eng = StrategyEngine.__new__(StrategyEngine)
    pos = Position()
    pos.entry_token = "tok"
    pos._total_qty = 10.0
    pos._total_cost = 4.0  # avg_cost = 0.40 → tp_target = 0.40 * 1.20 = 0.48 default
    pos._tp_inflight = False
    eng._round = type("R", (), {"position": pos})()
    eng._gate = _StubGate()
    eng._bc = _StubBinanceCache()
    eng._pc = _StubPriceCache(mid=0.48, bid=0.45)  # mid above, bid below tp_target

    captured = []
    async def handler(action):
        captured.append(action)
    eng._handler = handler

    await eng._check_take_profit(100000.0)
    # bid=0.45 < tp_target=0.48 → no TP action
    assert captured == []
    # And: mutex was NOT set (we never armed)
    assert pos._tp_inflight is False


@pytest.mark.asyncio
async def test_b4_tp_trigger_fires_when_bid_meets_target() -> None:
    """bid=0.49 >= tp_target=0.48 → TP action fires with that bid."""
    eng = StrategyEngine.__new__(StrategyEngine)
    pos = Position()
    pos.entry_token = "tok"
    pos._tp_inflight = False
    # Use record_fill so _avg_cost is set correctly via the property
    # maintenance in record_fill (avg = total_cost / total_qty = 4/10 = 0.40)
    pos.record_fill(pos.tier1, price=0.40, size=10.0)
    eng._round = type("R", (), {"position": pos})()
    eng._gate = _StubGate()
    eng._bc = _StubBinanceCache()
    eng._pc = _StubPriceCache(mid=0.50, bid=0.49)

    captured = []
    async def handler(action):
        captured.append(action)
    eng._handler = handler

    await eng._check_take_profit(100000.0)
    assert len(captured) == 1
    assert isinstance(captured[0], ActionTakeProfit)
    # The price sent to the action is the TP target (not mid)
    assert captured[0].price == 0.48
    assert pos._tp_inflight is True  # mutex armed


# ---------------------------------------------------------------------------
# combined TrackedOrder.placed_size passed through end-to-end
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_m1_tracked_order_has_placed_size(monkeypatch) -> None:
    """After place_buy, TrackedOrder.placed_size must equal result.tokens."""
    expected_size = 5.1282  # The size CLOB round-down accepted

    class _C:
        async def place_gtd_buy(self, **kw):
            return FillResult(
                order_id="0xsid", tokens=expected_size,
                usdc=expected_size * 0.40, fill_price=0.40, success=True,
                expiration_ts=int(time.time()) + 200,
            )
        async def _run(self, fn, *a, **kw):
            return None

    mgr = OrderManager.__new__(OrderManager)
    mgr._clob = _C()
    mgr._lock = asyncio.Lock()
    mgr._orders = {}
    mgr._by_server = {}
    mgr._pc = None
    mgr._pm = None
    mgr._s = type("S", (), {"GTD_LIFETIME_S": 120, "SELL_SLIPPAGE_PCT": 1.0})()

    order = await mgr.place_buy(
        token_id="tok", price=0.40, size=5.0,
        round_start_ts=int(time.time()),
        tier_name="T1", tick_size=0.01,
    )
    assert order is not None
    assert order.placed_size == expected_size
