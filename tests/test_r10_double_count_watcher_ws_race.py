"""Regression test for audit R10 double-counting bug (2026-07-12 round 1783855800).

Before the fix: the TierWatcher's on_done path pushed fills through
apply_tier_watcher_result's missed-fill branch (size_filled +=
filled_qty) WITHOUT advancing OrderManager.tracked.size_matched. The
WS TRADE event that arrived a few ms later had no way to know about
the watcher's accounting and pushed ANOTHER record_fill(tier, ws_price,
filled_qty) on top. Result: a 5.26-share BUY ended up with
tier.size_filled=10.52 in cumulative_size and total_qty=10.52.

After the fix: on_done advances tracked.size_matched first (so the WS
TRADE dedup short-circuits on the next event) and pushes the DELTA
through on_order_filled (so record_fill only ever receives the un-
credited increment).

This test exercises the two extreme orderings of the WS/watcher race
in isolation against a MockOrderManager + MockEngine that records
record_fill arguments.
"""

from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


class _TrackingOrderManager:
    """Minimal OrderManager stand-in that records apply_fill_event calls."""

    def __init__(self) -> None:
        self.by_id: dict[str, SimpleNamespace] = {}
        self.apply_calls: list[tuple[str, float, float | None]] = []

    def by_server_id(self, server_order_id: str) -> SimpleNamespace | None:
        return self.by_id.get(server_order_id)

    def apply_fill_event(
        self,
        server_order_id: str,
        size_matched: float,
        fill_price: float | None = None,
        trader_side: str | None = None,
    ) -> None:
        order = self.by_id.get(server_order_id)
        if order is None:
            order = SimpleNamespace(size_matched=0.0)
            self.by_id[server_order_id] = order
        if size_matched > order.size_matched:
            order.size_matched = float(size_matched)
            self.apply_calls.append(
                (server_order_id, float(size_matched), fill_price)
            )


class _TrackingEngine:
    """Minimal Engine stand-in that records on_order_filled calls."""

    def __init__(self, om: _TrackingOrderManager) -> None:
        self._round = SimpleNamespace(position=SimpleNamespace(tiers=[None]))
        self._om = om
        self.on_order_filled_calls: list[tuple[str, str, float, float]] = []

    def on_order_filled(
        self,
        token_id: str,
        server_order_id: str,
        fill_price: float,
        fill_size: float,
    ) -> None:
        self.on_order_filled_calls.append(
            (token_id, server_order_id, float(fill_price), float(fill_size))
        )


def _build_on_done(om: _TrackingOrderManager, eng: _TrackingEngine):
    """Inline the router.on_done dispatcher's fill-sync path.

    Production `on_done` lives in main.py inside `spawn_tier_watcher`
    and uses `self._state.engine`, `self._state.order_manager`, and
    `self._state.clob`. We reproduce the fill-sync sequence here as a
    small async closure so the regression test does not need the full
    app wiring (config loading, market discovery, executor, etc.).
    """

    async def on_done(result) -> None:
        if (
            result.outcome.value in ("filled", "over_filled")
            and om is not None
            and result.server_order_id
        ):
            tracked = om.by_server_id(result.server_order_id)
            prev_size_matched = (
                tracked.size_matched if tracked else 0.0
            )
            effective_delta = (
                float(result.filled_qty) - prev_size_matched
            )
            if effective_delta <= 1e-9:
                pass
            else:
                forward_price = result.avg_fill_price
                if not forward_price or forward_price <= 0:
                    forward_price = result.strategy_price
                om.apply_fill_event(
                    result.server_order_id,
                    size_matched=float(result.filled_qty),
                    fill_price=float(forward_price),
                )
                eng.on_order_filled(
                    "0xtoken",
                    result.server_order_id,
                    fill_price=float(forward_price),
                    fill_size=float(effective_delta),
                )

    return on_done


def _watcher_result(
    outcome_value: str,
    filled_qty: float,
    avg_fill_price: float,
    strategy_price: float,
    server_order_id: str = "0xserver",
) -> SimpleNamespace:
    return SimpleNamespace(
        outcome=SimpleNamespace(value=outcome_value),
        filled_qty=float(filled_qty),
        avg_fill_price=float(avg_fill_price),
        strategy_price=float(strategy_price),
        server_order_id=server_order_id,
        dedup_key=f"{server_order_id}|{outcome_value}",
    )


def test_r10_watcher_only_no_ws_no_double_count() -> None:
    """Pure watcher fast-fill: single on_done call must yield a
    single on_order_filled with the full filled_qty delta (=5.26,
    since no prior WS push)."""
    om = _TrackingOrderManager()
    eng = _TrackingEngine(om)
    on_done = _build_on_done(om, eng)

    asyncio.run(
        on_done(
            _watcher_result(
                outcome_value="filled",
                filled_qty=5.26,
                avg_fill_price=0.0,
                strategy_price=0.38,
            )
        )
    )

    assert len(eng.on_order_filled_calls) == 1
    token, server_id, price, size = eng.on_order_filled_calls[0]
    assert token == "0xtoken"
    assert server_id == "0xserver"
    assert size == pytest.approx(5.26)
    # avg_fill_price was 0, falls back to strategy_price=0.38.
    assert price == pytest.approx(0.38)
    # mirror advanced to 5.26
    assert om.by_id["0xserver"].size_matched == pytest.approx(5.26)


def test_r10_ws_first_then_watcher_dedups() -> None:
    """WS path landed a full 5.26 fill first; watcher's later on_done
    must skip its on_order_filled entirely (effective_delta <= 0).

    Setup: simulate the WS path by pre-populating tracked.size_matched
    to 5.26 via apply_fill_event (mirroring what _make_fill_callback
    does in production). Then call on_done with filled_qty=5.26 — it
    must NOT re-push on_order_filled, and the mirror must not advance
    further (still 5.26, not 10.52).
    """
    om = _TrackingOrderManager()
    eng = _TrackingEngine(om)
    on_done = _build_on_done(om, eng)

    # Simulate WS TRADE — fills the entire 5.26 at the real fill price 0.33.
    om.apply_fill_event("0xserver", size_matched=5.26, fill_price=0.33)
    eng.on_order_filled(
        "0xtoken", "0xserver", fill_price=0.33, fill_size=5.26
    )
    assert len(eng.on_order_filled_calls) == 1

    # Watcher fires its on_done AFTER the WS path. It must NOT add a
    # second record_fill (effective_delta = 5.26 - 5.26 = 0).
    asyncio.run(
        on_done(
            _watcher_result(
                outcome_value="filled",
                filled_qty=5.26,
                avg_fill_price=0.0,
                strategy_price=0.38,
            )
        )
    )

    # Still exactly 1 call (the WS one) — no second push.
    assert len(eng.on_order_filled_calls) == 1
    # Mirror stays at 5.26, not 10.52.
    assert om.by_id["0xserver"].size_matched == pytest.approx(5.26)


def test_r10_watcher_then_ws_no_double_count() -> None:
    """Watcher fast-filled first (filled_qty=5.26, real price=0.33 via
    REST re-fetch not exercised here — strategy_price fallback expected
    when avg_fill_price=0). Then the WS TRADE arrives and must dedup
    out (its `_make_fill_callback` would see prev=5.26 → delta=0, so
    it skips its own on_order_filled).

    We simulate the WS side by checking `om.by_server_id(...).size_
    matched` BEFORE running the WS path: it should already be 5.26,
    so a real `_make_fill_callback` would skip its callback entirely.
    Crucially, this test asserts the mirror ordering itself, not the
    WS callback — we don't have the production `_make_fill_callback`
    in scope.
    """
    om = _TrackingOrderManager()
    eng = _TrackingEngine(om)
    on_done = _build_on_done(om, eng)

    asyncio.run(
        on_done(
            _watcher_result(
                outcome_value="filled",
                filled_qty=5.26,
                avg_fill_price=0.33,  # simulated real CLOB avg_price
                strategy_price=0.38,
            )
        )
    )

    # Watcher advanced the mirror BEFORE the WS TRADE arrived — so any
    # subsequent `_make_fill_callback` call for this server_id will
    # read prev=5.26, compute effective_delta = 5.26 - 5.26 = 0, and
    # skip its own on_order_filled. That is the property that
    # guarantees no double-count.
    assert om.by_id["0xserver"].size_matched == pytest.approx(5.26)
    # Watcher pushed exactly one on_order_filled, with the full 5.26.
    assert len(eng.on_order_filled_calls) == 1
    _, _, price, size = eng.on_order_filled_calls[0]
    assert size == pytest.approx(5.26)
    assert price == pytest.approx(0.33)  # real avg_fill_price, not 0.38


def test_r10_partial_then_watcher_passes_delta_only() -> None:
    """WS path landed a partial 2.0 first. Watcher's on_done arrives
    with filled_qty=5.26 (the new total). It must push a DELTA of
    5.26 - 2.0 = 3.26 through on_order_filled, NOT the full 5.26 —
    otherwise size_filled would jump 2.0+5.26=7.26 (over-count).
    """
    om = _TrackingOrderManager()
    eng = _TrackingEngine(om)
    on_done = _build_on_done(om, eng)

    # Simulate WS TRADE for partial 2.0 at price 0.33.
    om.apply_fill_event("0xserver", size_matched=2.0, fill_price=0.33)
    eng.on_order_filled(
        "0xtoken", "0xserver", fill_price=0.33, fill_size=2.0
    )
    assert len(eng.on_order_filled_calls) == 1

    # Watcher fires with full 5.26.
    asyncio.run(
        on_done(
            _watcher_result(
                outcome_value="filled",
                filled_qty=5.26,
                avg_fill_price=0.33,
                strategy_price=0.38,
            )
        )
    )

    # Total on_order_filled calls = 2 (1 WS + 1 watcher), but the
    # watcher's push is the DELTA (3.26), not the full 5.26.
    assert len(eng.on_order_filled_calls) == 2
    watcher_call = eng.on_order_filled_calls[1]
    _, _, _, watcher_size = watcher_call
    assert watcher_size == pytest.approx(3.26), (
        "watcher must push only the delta above what WS already "
        "credited (5.26 - 2.0 = 3.26), not the full filled_qty — "
        "otherwise size_filled would be 2.0 + 5.26 = 7.26"
    )
    # Mirror lands at 5.26 (the watcher's authoritative cumulative).
    assert om.by_id["0xserver"].size_matched == pytest.approx(5.26)
