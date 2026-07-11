"""Regression tests for the v4 audit fixes (2026-07-11).

Covers:
  B-V4-1  — place_buy / place_gtd_sell / place_sell drop sentinel + pending_fills on failure
  B-V4-2  — ActionRouter rolls back entry_token / entry_side / tier state on place_buy=None
  H-V4-1  — place_buy_by_notional sizes the order AFTER the orderbook price snap
  H-V4-2  — stake_cap_remaining uses total_notional_committed (in-flight counts)
  M-V4-1  — purge_round drops in-memory state for finished rounds
  M-V4-2  — _check_average_down resets a tier whose `placed=True` has no `server_order_id`
  M-V4-3  — cancel / cancel_market drain pending_fills for the affected server_id

Run via `pytest tests/test_audit_fixes_2026_07_11_v4.py`.
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
from src.strategy.round_state import Position


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
        # Pre-populate so the fake can return a stable order_id.
        self._next_id = 0

    def _new_id(self) -> str:
        self._next_id += 1
        return f"0xstub{self._next_id:064x}"


class _FailingBuyClob(_StubClob):
    """place_gtd_buy that registers sentinel + queues a fill, then returns
    success=False. Used to reproduce the B-V4-1 race.
    """

    def __init__(self) -> None:
        super().__init__(success=False)
        self._register_pending = None
        self._callback = None
        self._applied: list[tuple[str, float]] = []

    async def place_gtd_buy(self, **kw: Any) -> FillResult:
        server_id = self._new_id()
        if self._register_pending:
            self._register_pending(server_id)
        # Simulate: fill callback fires before post returns.
        if self._callback:
            self._callback("tok", server_id, 0.40, 5.0)
        return FillResult(
            order_id=server_id,
            tokens=0,
            usdc=0,
            fill_price=kw.get("price", 0.40),
            success=self.success,
            error="gtd_poll_timeout",
            expiration_ts=int(time.time()) + 200,
        )


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
# B-V4-1: place_buy failure path drops sentinel + pending fills
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_b_v4_1_place_buy_failure_drops_sentinel() -> None:
    """If CLOBClient pre-registers a sentinel AND fires a fill callback
    inside `place_gtd_buy` before returning success=False, the OM must
    not leak the sentinel / pending fill — the TrackedOrder will never
    be inserted.
    """
    clob = _FailingBuyClob()
    mgr = _build_om(clob)
    clob._register_pending = mgr.register_pending_server_id

    # Simulate the fill arriving at the sentinel path.
    pending_during_callback: list[str] = []
    def cb(token_id, order_id, price, size):
        mgr.apply_fill_event(order_id, size_matched=size, fill_price=price)
        pending_during_callback.append(order_id)
    clob._callback = cb

    # Stub _adjust_buy_price so we don't hit the network.
    async def _stub_adjust(token_id, price, tick_size=0.01):
        return price
    mgr._adjust_buy_price = _stub_adjust

    order = await mgr.place_buy(
        token_id="tok", price=0.40, size=5.0,
        round_start_ts=int(time.time()),
        tier_name="T1", tick_size=0.01,
    )

    assert order is None
    # The fill callback landed — confirm the pending fill was queued.
    assert len(pending_during_callback) == 1
    sid = pending_during_callback[0]
    # After failure, both the sentinel and the queued fill must be gone.
    assert sid not in mgr._by_server, (
        "B-V4-1 violation: sentinel `_pending_<sid>` leaked into "
        "_by_server after place_buy returned None."
    )
    assert sid not in mgr._pending_fills, (
        "B-V4-1 violation: pending fill leaked into _pending_fills "
        "after place_buy returned None."
    )


@pytest.mark.asyncio
async def test_b_v4_1_place_gtd_sell_failure_drops_sentinel() -> None:
    """Same as above for the GTD SELL path used by TP."""

    class _FailingSellClob(_StubClob):
        async def place_gtd_sell(self, **kw: Any) -> FillResult:
            sid = self._new_id()
            if self._register_pending:
                self._register_pending(sid)
            return FillResult(
                order_id=sid,
                tokens=0,
                usdc=0,
                fill_price=kw.get("price", 0.50),
                success=False,
                error="gtd_post_failed",
                expiration_ts=int(time.time()) + 200,
            )

    clob = _FailingSellClob()
    mgr = _build_om(clob)
    clob._register_pending = mgr.register_pending_server_id

    async def _stub_adjust(token_id, price, tick_size=0.01):
        return price
    mgr._adjust_sell_price = _stub_adjust

    order = await mgr.place_gtd_sell(
        token_id="tok", size=10.0, price=0.50,
        round_start_ts=int(time.time()),
        tier_name="TP", tick_size=0.01,
    )
    assert order is None
    # No sentinel should remain.
    assert not any(
        cid.startswith("_pending_") for cid in mgr._by_server.values()
    ), "place_gtd_sell failure left sentinel behind"


@pytest.mark.asyncio
async def test_b_v4_1_place_sell_failure_drops_sentinel() -> None:
    """Same as above for the FAK SELL path."""

    class _FakingSellClob(_StubClob):
        async def place_fak_sell(self, **kw: Any) -> FillResult:
            sid = self._new_id()
            if self._register_pending:
                self._register_pending(sid)
            return FillResult(
                order_id=sid,
                tokens=0,
                usdc=0,
                fill_price=kw.get("price", 0.50),
                success=False,
                error="FAK_no_fills",
            )

    clob = _FakingSellClob()
    mgr = _build_om(clob)
    clob._register_pending = mgr.register_pending_server_id

    async def _stub_adjust(token_id, price, tick_size=0.01):
        return price
    mgr._adjust_sell_price = _stub_adjust

    order = await mgr.place_sell(
        token_id="tok", size=10.0, price=0.50,
        round_start_ts=int(time.time()),
        tier_name="TP", tick_size=0.01,
    )
    assert order is None
    assert not any(
        cid.startswith("_pending_") for cid in mgr._by_server.values()
    ), "place_sell failure left sentinel behind"


@pytest.mark.asyncio
async def test_b_v4_1_cleanup_does_not_clobber_real_mapping() -> None:
    """`_cleanup_failed_post` must only drop sentinels. A real cid
    already mapped to the same server_id (theoretical edge case where
    a post succeeded but later a second post with the same server_id
    fails) must not be clobbered.
    """
    mgr = _build_om(_StubClob())
    mgr._by_server["0xreal"] = "real-cid"
    mgr._cleanup_failed_post("0xreal")
    assert mgr._by_server["0xreal"] == "real-cid"


# ---------------------------------------------------------------------------
# B-V4-2: router rolls back engine state on place_buy=None
# ---------------------------------------------------------------------------


class _StubEngine:
    def __init__(self, pos: Position) -> None:
        self._round = type("R", (), {
            "position": pos,
            "tick_size": 0.01,
            "round_start_ts": int(time.time()),
            "up_token": "up",
            "down_token": "down",
        })()
        self._gate = type("G", (), {"can_act": lambda *a: True})()


class _FailingOM:
    """place_buy / place_gtd_sell / place_sell / place_buy_by_strategy_price all return None."""

    async def place_buy_by_notional(self, **kw):
        return None

    async def place_buy_by_strategy_price(self, **kw):
        return None

    async def place_buy(self, **kw):
        return None

    async def place_gtd_sell(self, **kw):
        return None

    async def place_sell(self, **kw):
        return None


@pytest.mark.asyncio
async def test_b_v4_2_router_t1_failure_rolls_back_entry_token() -> None:
    """When OM.place_buy returns None for a T1 action, the router must
    roll back `entry_token` / `entry_side` so a future _evaluate can
    re-attempt or the next round starts cleanly.
    """
    from src.main import ActionRouter

    pos = Position()
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    eng = _StubEngine(pos)

    state = type("S", (), {
        "engine": eng,
        "order_manager": _FailingOM(),
        "clob": type("C", (), {"fill_callback": None})(),
        "round_start_ts": int(time.time()),
        "_bg_tasks": set(),
    })()
    router = ActionRouter(state)  # type: ignore[arg-type]

    await router(ActionOpenT1(side="UP", token="tok", price=0.40, notional=2.0))

    assert pos.entry_token is None, (
        "B-V4-2 violation: entry_token not rolled back after T1 failure."
    )
    assert pos.entry_side is None, (
        "B-V4-2 violation: entry_side not rolled back after T1 failure."
    )


@pytest.mark.asyncio
async def test_b_v4_2_router_tn_failure_resets_tier_state() -> None:
    """When OM.place_buy returns None for a T_N action, the router must
    reset `tier.placed=False` and clear `server_order_id` so a future
    `_check_average_down` can re-fire the same tier instead of skipping
    it forever.
    """
    from src.main import ActionRouter

    pos = Position()
    pos.tier2.placed = True
    pos.tier2.server_order_id = "0xstale"
    pos.tier2.expires_at = time.time() + 60
    eng = _StubEngine(pos)

    state = type("S", (), {
        "engine": eng,
        "order_manager": _FailingOM(),
        "clob": type("C", (), {"fill_callback": None})(),
        "round_start_ts": int(time.time()),
        "_bg_tasks": set(),
    })()
    router = ActionRouter(state)  # type: ignore[arg-type]

    await router(ActionOpenTN(tier=2, token="tok", price=0.32, notional=2.0, trigger_price=0.32))

    assert pos.tier2.placed is False
    assert pos.tier2.server_order_id is None
    assert pos.tier2.expires_at == 0.0


# ---------------------------------------------------------------------------
# H-V4-1: place_buy_by_notional sizes AFTER the price snap
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_h_v4_1_place_buy_by_notional_uses_adjusted_price() -> None:
    """`place_buy_by_notional(notional=2.0)` with ref_price=0.40 must
    end up calling the underlying place_gtd_buy with size = 2.0 /
    adjusted_price, where adjusted_price is the orderbook bid + BUY_STEP
    (i.e. 0.405, not 0.40).

    This closes the H-V4-1 audit finding: previously router computed
    size with the engine's decision-time bid, while CLOB accepted the
    order at adjusted_price, so actual cost was 1-3% over planned
    notional.
    """
    captured: list[dict[str, Any]] = []

    class _RecordingClob:
        async def place_gtd_buy(self, **kw: Any) -> FillResult:
            captured.append(kw)
            return FillResult(
                order_id="0xT1",
                tokens=kw["size"],
                usdc=kw["size"] * kw["price"],
                fill_price=kw["price"],
                success=True,
                expiration_ts=int(time.time()) + 200,
            )

    clob = _RecordingClob()
    mgr = _build_om(clob)

    # Force _adjust_buy_price to return 0.405 (bid 0.40 + half tick 0.005).
    async def _stub_adjust(token_id, price, tick_size=0.01):
        return 0.405
    mgr._adjust_buy_price = _stub_adjust

    order = await mgr.place_buy_by_notional(
        token_id="tok",
        notional=2.0,
        ref_price=0.40,
        round_start_ts=int(time.time()),
        tier_name="T1",
        tick_size=0.01,
    )

    assert order is not None
    assert len(captured) == 1
    sent = captured[0]
    assert sent["price"] == pytest.approx(0.405)
    # size = notional / adjusted_price = 2.0 / 0.405 ≈ 4.9383
    assert sent["size"] == pytest.approx(2.0 / 0.405)
    # And critically: NOT 2.0 / 0.40 = 5.0
    assert sent["size"] != pytest.approx(5.0)


@pytest.mark.asyncio
async def test_h_v4_1_place_buy_by_notional_no_price_returns_none() -> None:
    """If both orderbook and ref_price are unusable, return None cleanly
    (no negative size, no division by zero)."""
    clob = _StubClob()
    mgr = _build_om(clob)

    async def _bad_adjust(token_id, price, tick_size=0.01):
        return 0.0
    mgr._adjust_buy_price = _bad_adjust

    order = await mgr.place_buy_by_notional(
        token_id="tok",
        notional=2.0,
        ref_price=0.0,
        round_start_ts=int(time.time()),
        tier_name="T1",
        tick_size=0.01,
    )
    assert order is None


# ---------------------------------------------------------------------------
# H-V4-2: stake_cap_remaining uses total_notional_committed
# ---------------------------------------------------------------------------


def test_h_v4_2_stake_cap_counts_inflight_notional() -> None:
    """With T1 filled (cost=2.0) and T2 placed but not filled,
    stake_cap_remaining must reflect T2's notional as committed (so the
    cap is enforced even before T2's fill lands).
    """
    pos = Position()
    # T1 fills at 0.40 for 5 shares = $2 cost.
    pos.tier1.size_expected = 5.0
    pos.record_fill(pos.tier1, price=0.40, size=5.0)

    assert pos._total_cost == pytest.approx(2.0)
    assert pos.stake_cap_remaining == pytest.approx(14.0)  # 16 - 2

    # Mark T2 as placed (in-flight) without any fill yet.
    pos.tier2.placed = True
    pos.tier2.server_order_id = "0xt2"

    # H-V4-2: stake_cap_remaining must use total_notional_committed,
    # which now includes T2's notional (2.0). 16 - (2 + 2) = 12.
    assert pos.total_notional_committed == pytest.approx(4.0)
    assert pos.stake_cap_remaining == pytest.approx(12.0), (
        "H-V4-2 violation: stake_cap_remaining did not account for "
        "T2's in-flight notional. Use total_notional_committed, not "
        "_total_cost."
    )


def test_h_v4_2_stake_cap_no_inflight_unchanged() -> None:
    """Sanity check: with no in-flight tiers, stake_cap_remaining uses
    filled cost only (which equals committed since placed=False).
    """
    pos = Position()
    pos.tier1.size_expected = 5.0
    pos.record_fill(pos.tier1, price=0.40, size=5.0)
    # No in-flight.
    assert pos.stake_cap_remaining == pytest.approx(14.0)


# ---------------------------------------------------------------------------
# M-V4-1: purge_round drops in-memory state for finished rounds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_m_v4_1_purge_round_clears_orders_and_mappings() -> None:
    """`purge_round(round_ts)` must drop all TrackedOrders / server_id
    mappings / pending_fills for that round, leaving unrelated rounds
    untouched.
    """
    clob = _StubClob(success=True)
    mgr = _build_om(clob)
    # Insert one order for round A and one for round B.
    mgr._orders["A-1"] = TrackedOrder(
        client_order_id="A-1", server_order_id="0xA1",
        round_start_ts=1000, side="BUY", token_id="t",
        tier_name="T1", price=0.40, size=5.0,
        lifetime_s=200, expires_at=time.time() + 200, placed_at=time.time(),
        status="filled",
    )
    mgr._orders["B-1"] = TrackedOrder(
        client_order_id="B-1", server_order_id="0xB1",
        round_start_ts=2000, side="BUY", token_id="t",
        tier_name="T1", price=0.40, size=5.0,
        lifetime_s=200, expires_at=time.time() + 200, placed_at=time.time(),
        status="filled",
    )
    mgr._by_server["0xA1"] = "A-1"
    mgr._by_server["0xB1"] = "B-1"
    mgr._pending_fills["0xA1"] = [(0.40, 5.0)]
    mgr._pending_fills["0xB1"] = [(0.40, 5.0)]

    purged = await mgr.purge_round(1000)
    assert purged == 1
    assert "A-1" not in mgr._orders
    assert "B-1" in mgr._orders
    assert "0xA1" not in mgr._by_server
    assert "0xB1" in mgr._by_server
    assert "0xA1" not in mgr._pending_fills
    assert "0xB1" in mgr._pending_fills

    # Idempotent.
    purged_again = await mgr.purge_round(1000)
    assert purged_again == 0


# ---------------------------------------------------------------------------
# M-V4-2: stale `placed=True` with no server_order_id is reset, not skipped
# ---------------------------------------------------------------------------


class _StubGate2:
    def can_act(self, bc_now): return True
    def can_enter(self, bc_now, mid): return True
    def can_double_down(self, *a, **kw): return True
    def state(self, bc_now):
        class S:
            drift: float = 0.0
            within_limit: bool = True
        return S()


class _StubPriceCache2:
    def __init__(self, mid: float, bid: float) -> None:
        self._mid = mid
        self._bid = bid
    def get_midpoint(self, token): return self._mid
    def bid(self, token): return self._bid


class _StubBinanceCache2:
    last_price = 100000.0


@pytest.mark.asyncio
async def test_m_v4_2_stale_tier_resets_and_refires() -> None:
    """When `tier.placed=True` but `server_order_id is None` (a stale
    state from a previous router-level failure), `_check_average_down`
    must reset the tier instead of skipping it forever.
    """
    eng = StrategyEngine.__new__(StrategyEngine)
    pos = Position()
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos.baseline_target_price = 0.40
    pos.baseline_set = True
    pos._total_qty = 5.0
    pos._total_cost = 2.0
    # Simulate the stale state.
    pos.tier2.placed = True
    pos.tier2.server_order_id = None
    pos.tier2.expires_at = 0.0
    eng._round = type("R", (), {"position": pos})()
    eng._gate = _StubGate2()
    eng._bc = _StubBinanceCache2()
    eng._pc = _StubPriceCache2(mid=0.30, bid=0.30)  # below T2 trigger

    captured: list[Any] = []
    async def handler(action):
        captured.append(action)
    eng._handler = handler
    eng._s = type("S", (), {
        "TIER2_NOTIONAL": 2.0,
        "TIER3_NOTIONAL": 4.0,
        "TIER4_NOTIONAL": 8.0,
        "TIER_NOTIONAL_MAX_MULT": 1.1,
    })()

    await eng._check_average_down(100000.0)

    # After the reset, T2 should fire normally.
    assert len(captured) == 1, (
        "M-V4-2 violation: stale `placed=True`/`server_order_id=None` "
        "tier was not reset, so the re-fire was suppressed."
    )
    assert isinstance(captured[0], ActionOpenTN)
    assert captured[0].tier == 2
    # And the tier's state should now reflect a re-arm.
    assert pos.tier2.placed is False
    assert pos.tier2.server_order_id is None


@pytest.mark.asyncio
async def test_m_v4_2_valid_inflight_tier_still_skipped() -> None:
    """Sanity: a tier with `placed=True` AND a valid `server_order_id`
    is still skipped (no regression on the existing in-flight mutex).
    """
    eng = StrategyEngine.__new__(StrategyEngine)
    pos = Position()
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos.baseline_target_price = 0.40
    pos.baseline_set = True
    pos._total_qty = 5.0
    pos._total_cost = 2.0
    pos.tier2.placed = True
    pos.tier2.watching = True
    pos.tier2.server_order_id = "0xt2-live"  # valid mapping
    eng._round = type("R", (), {"position": pos})()
    eng._gate = _StubGate2()
    eng._bc = _StubBinanceCache2()
    eng._pc = _StubPriceCache2(mid=0.30, bid=0.30)

    captured: list[Any] = []
    async def handler(action):
        captured.append(action)
    eng._handler = handler
    eng._s = type("S", (), {
        "TIER2_NOTIONAL": 2.0,
        "TIER3_NOTIONAL": 4.0,
        "TIER4_NOTIONAL": 8.0,
        "TIER_NOTIONAL_MAX_MULT": 1.1,
    })()

    await eng._check_average_down(100000.0)

    assert captured == [], "T2 must still be skipped while in-flight"


# ---------------------------------------------------------------------------
# M-V4-3: cancel / cancel_market drain pending_fills
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_m_v4_3_cancel_drains_pending_fills() -> None:
    """`cancel(client_id)` must drop any queued pending_fills for the
    cancelled server_id, so the dead-letter queue doesn't accumulate.
    """
    clob = _StubClob(success=True)

    class _StubCancellingClob:
        def __init__(self):
            self.cancelled: list[str] = []

        async def cancel_order(self, server_id: str) -> bool:
            self.cancelled.append(server_id)
            return True

        async def cancel_orders(self, server_ids):
            self.cancelled.extend(s for s in server_ids)
            return {"canceled": list(server_ids), "not_canceled": []}

    cancelling = _StubCancellingClob()
    mgr = _build_om(clob)
    mgr._clob = cancelling
    mgr._orders["T1-x"] = TrackedOrder(
        client_order_id="T1-x", server_order_id="0xT1x",
        round_start_ts=int(time.time()), side="BUY", token_id="t",
        tier_name="T1", price=0.40, size=5.0,
        lifetime_s=200, expires_at=time.time() + 200, placed_at=time.time(),
        status="live",
    )
    mgr._by_server["0xT1x"] = "T1-x"
    mgr._pending_fills["0xT1x"] = [(0.40, 2.0)]  # partial fill before cancel

    ok = await mgr.cancel("T1-x", reason="manual")
    assert ok is True
    assert "0xT1x" not in mgr._pending_fills, (
        "M-V4-3 violation: cancel did not drain pending_fills."
    )


@pytest.mark.asyncio
async def test_m_v4_3_cancel_market_drains_pending_fills() -> None:
    """`cancel_market(round_ts)` must drain pending_fills for every
    server_id it cancels.
    """
    cancelling_clob = type("C", (), {
        "cancel_orders": lambda self, ids: asyncio.sleep(0, result={"canceled": list(ids), "not_canceled": []}),
    })()

    mgr = _build_om(_StubClob())
    mgr._clob = cancelling_clob
    # Two orders for round 1000, both with pending fills.
    for i in range(2):
        cid = f"r1000-{i}"
        sid = f"0xr1000{i}"
        mgr._orders[cid] = TrackedOrder(
            client_order_id=cid, server_order_id=sid,
            round_start_ts=1000, side="BUY", token_id="t",
            tier_name="T1", price=0.40, size=5.0,
            lifetime_s=200, expires_at=time.time() + 200, placed_at=time.time(),
            status="live",
        )
        mgr._by_server[sid] = cid
        mgr._pending_fills[sid] = [(0.40, 1.0)]
    # Plus an unrelated round that must be untouched.
    mgr._orders["r2000-0"] = TrackedOrder(
        client_order_id="r2000-0", server_order_id="0xr2000",
        round_start_ts=2000, side="BUY", token_id="t",
        tier_name="T1", price=0.40, size=5.0,
        lifetime_s=200, expires_at=time.time() + 200, placed_at=time.time(),
        status="live",
    )
    mgr._by_server["0xr2000"] = "r2000-0"
    mgr._pending_fills["0xr2000"] = [(0.40, 1.0)]

    cancelled = await mgr.cancel_market(1000)
    assert len(cancelled) == 2
    assert "0xr10000" not in mgr._pending_fills
    assert "0xr10001" not in mgr._pending_fills
    # Unrelated round preserved.
    assert "0xr2000" in mgr._pending_fills


import pytest  # noqa: E402  (placed at bottom for readability)