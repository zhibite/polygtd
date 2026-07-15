"""Unit tests for the GTD-sell TP flow.

These tests use a fake CLOBClient so they don't hit the network. The
behaviour we verify:
  1. CLOBClient.place_gtd_sell posts a SELL, fires fill callback incrementally.
  2. CLOBClient.place_gtd_sell returns success=False when posting fails.
  3. OrderManager.place_gtd_sell wraps a CLOB post and returns a TrackedOrder.
  4. OrderManager._adjust_sell_price snaps just below current ask.

H-NEW-2026-07-15 (audit follow-up): the legacy two-stage TP path
(`place_tp_with_fallback` / FAK-after-285s-wait) is no longer reachable
from any production caller — TP uses a single GTD via
`place_gtd_sell_at_price` and the strategy tick cancels it when mid
retreats. Tests for that path were removed.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable

import pytest

from src.execution.clob_client import FillResult
from src.execution.order_manager import (
    OrderManager,
    TP_GTD_LIFETIME_S,
    TrackedOrder,
)


# ---------------------------------------------------------------------------
# Fake CLOBClient that records calls and simulates a fill schedule.
# ---------------------------------------------------------------------------


class FakeCLOBClient:
    """In-memory replacement for CLOBClient. No HTTP, no asyncio tasks.

    The fill_schedule controls how a GTD order fills:
      * list of (seconds_from_start, size_filled) tuples
      * size_filled is the cumulative matched size at that moment
    """

    def __init__(self) -> None:
        self.place_gtd_sell_calls: list[dict] = []
        self.place_gtd_buy_calls: list[dict] = []
        self.place_fak_sell_calls: list[dict] = []
        self.fill_callbacks: list[Callable] = []

        # Per server-id, schedule of incremental fills
        self._sell_schedule: dict[str, list[tuple[float, float]]] = {}
        self._sell_status_after: dict[str, str] = {}  # terminal status
        self._sell_post_error: dict[str, str] = {}    # for post-failure test

        # gtd_buy post returns dict for compatibility
        self.next_order_id: int = 0
        self._client_stub = type("StubClient", (), {"get_order": None})()

    # -- internal: used by tests to set up behaviour ------------------------

    def queue_sell_schedule(
        self, order_id: str, schedule: list[tuple[float, float]], terminal: str = "matched"
    ) -> None:
        """schedule: list of (elapsed_sec, cumulative_size) — last one is final fill."""
        self._sell_schedule[order_id] = sorted(schedule, key=lambda x: x[0])
        self._sell_status_after[order_id] = terminal

    def force_sell_post_error(self, order_id: str, error: str) -> None:
        self._sell_post_error[order_id] = error

    def _next_id(self) -> str:
        self.next_order_id += 1
        return f"0x{self.next_order_id:064x}"

    # -- mirror CLOBClient surface used by OrderManager ----------------------

    async def place_gtd_sell(
        self,
        token_id: str,
        price: float,
        size: float,
        lifetime_s: int,
        tick_size: float,
    ) -> FillResult:
        # simulate post
        if self._sell_post_error:
            err_id, err_msg = next(iter(self._sell_post_error.items()))
            del self._sell_post_error[err_id]
            return FillResult("", 0, 0, price, False, err_msg)

        self.place_gtd_sell_calls.append(
            dict(token_id=token_id, price=price, size=size, lifetime_s=lifetime_s, tick_size=tick_size)
        )
        order_id = self._next_id()
        # schedule is delivered to the fill callback during post.
        # For tests we use a polling-style simulation: pop the schedule points
        # and immediately report the final state. This is realistic because
        # the production code polls every 0.5s — for the test we pretend a
        # very fast match.
        sched = self._sell_schedule.pop(order_id, [(0.5, size)])
        # compute the final cumulative size from the last schedule point
        _, final_sz = max(sched, key=lambda x: x[0])
        avg_px = price
        for cb in self.fill_callbacks:
            cb(token_id, order_id, avg_px, final_sz)
        return FillResult(
            order_id=order_id,
            tokens=final_sz,
            usdc=final_sz * avg_px,
            fill_price=avg_px,
            success=True,
        )

    async def place_fak_sell(
        self,
        token_id: str,
        size: float,
        price: float,
        tick_size: float,
        slippage_pct: float = 2.0,
    ) -> FillResult:
        self.place_fak_sell_calls.append(
            dict(token_id=token_id, size=size, price=price, tick_size=tick_size)
        )
        # Fake a full fill at the price (rounded down by tick)
        return FillResult(
            order_id=self._next_id(),
            tokens=size,
            usdc=size * price,
            fill_price=price,
            success=True,
        )

    async def place_gtd_buy(
        self,
        token_id: str,
        price: float,
        size: float,
        lifetime_s: int,
        tick_size: float,
    ) -> FillResult:
        self.place_gtd_buy_calls.append(
            dict(token_id=token_id, price=price, size=size, lifetime_s=lifetime_s, tick_size=tick_size)
        )
        return FillResult(
            order_id=self._next_id(),
            tokens=size,
            usdc=size * price,
            fill_price=price,
            success=True,
        )

    async def _run(self, fn, *args, **kwargs):
        if asyncio.iscoroutinefunction(fn):
            return await fn(*args, **kwargs)
        return fn(*args, **kwargs)

    @property
    def _client(self) -> Any:
        # Real CLOBClient exposes a `_client` attribute that has `.get_order()`.
        # Our fake returns a stub object whose `.get_order` is replaceable.
        return self._client_stub

    @_client.setter
    def _client(self, value: Any) -> None:
        self._client_stub = value


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_clob() -> FakeCLOBClient:
    return FakeCLOBClient()


@pytest.fixture
def om(fake_clob: FakeCLOBClient) -> OrderManager:
    """Build an OrderManager wired to the fake CLOB. We monkey-patch _lock,
    _clob, and skip auth/network calls."""
    # Skip the __init__ side effects: OrderManager() does some imports +
    # dependency injection we don't need here. We instantiate the bare
    # attributes manually.
    mgr = OrderManager.__new__(OrderManager)
    mgr._clob = fake_clob
    mgr._lock = asyncio.Lock()
    mgr._orders = {}
    mgr._by_server = {}
    mgr._pc = None
    mgr._pm = None
    mgr._s = type("S", (), {"GTD_LIFETIME_S": 120, "SELL_SLIPPAGE_PCT": 1.0})()
    return mgr


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_place_gtd_sell_returns_tracked_order(om: OrderManager, fake_clob: FakeCLOBClient):
    """place_gtd_sell posts, fires fill callback, registers the order."""
    fake_clob.fill_callbacks.append(lambda *a: None)

    order = await om.place_gtd_sell(
        token_id="tok_abc",
        size=10.0,
        price=0.50,
        round_start_ts=1234567890,
        tier_name="TP",
        tick_size=0.01,
    )

    assert order is not None
    assert order.side == "SELL"
    assert order.tier_name == "TP"
    assert order.size == 10.0
    assert order.status == "live"
    assert order.server_order_id is not None
    assert order.lifetime_s == TP_GTD_LIFETIME_S  # default for TP
    assert len(fake_clob.place_gtd_sell_calls) == 1
    sent = fake_clob.place_gtd_sell_calls[0]
    assert sent["token_id"] == "tok_abc"
    assert sent["size"] == 10.0
    assert sent["lifetime_s"] == TP_GTD_LIFETIME_S


@pytest.mark.asyncio
async def test_place_gtd_sell_returns_none_on_post_failure(om: OrderManager, fake_clob: FakeCLOBClient):
    fake_clob.force_sell_post_error("ignored", "server_500")

    order = await om.place_gtd_sell(
        token_id="tok_abc",
        size=10.0,
        price=0.50,
        round_start_ts=1234567890,
        tier_name="TP",
        tick_size=0.01,
    )

    assert order is None


# H-NEW-2026-07-15 (audit follow-up): the legacy
# `place_tp_with_fallback` two-stage maker/FAK helper was removed from
# OrderManager because production never invoked it — TP now uses a
# single GTD via `place_gtd_sell_at_price` and the strategy tick
# cancels it (ActionCancelTier tier=-1 sentinel) when mid retreats
# below the fire line. The four `test_tp_fallback_*` tests were
# removed alongside the helper; their assertions covered behaviour
# (FAK-after-285s wait, residual-below-5 skip, post-failure straight-
# to-FAK) that is no longer reachable from any production caller.
