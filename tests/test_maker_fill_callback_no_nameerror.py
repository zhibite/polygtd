"""
Regression: WS TRADE event -> _handle_trade -> on_maker_fill must NOT raise.

H-NEW-2026-07-12 bug:
  The 2026-07-12 00:45:19Z prod push surfaced a NameError on
  src/main.py:_make_maker_fill_callback's on_maker_fill closure:

    File "/app/src/main.py", line 1611, in on_maker_fill
        eng.on_order_filled(asset_id, order_id, fill_price, delta)
                                                    ^^^^^
    NameError: name 'delta' is not defined

  Root cause: the closure used `fill_size_inc` (the per-event delta after
  monotonic-max) but the engine call referred to `delta`. The variable
  was never defined in that scope.

  Effect:
    * user_ws.on_maker_fill callback raised on EVERY trade event the
      bot received. WS events for trades where the bot was a maker
      caused exception in `_handle_trade`.
    * The OrderManager.fill_record line above (which happens BEFORE
      the engine call) succeeded, so fill_record_count >= expected
      in db but engine.on_order_filled never ran.
    * The position totals stayed at whatever was last successfully
      applied (often stale by minutes) while downstream stuff
      (TP watcher, tier cascade) thought no fill happened.
    * Exception was caught by user_ws wrapper that re-routes the
      callback, creating a tight loop on the SAME trade event every
      time the WS re-pushed the MATCHED state. That's where the
      "feels like a tight loop" came from in prod logs:
        00:45:19.928 TRADE 5818f993.. status=MATCHED
        00:45:22.079 same trade, status=MINED  -> same TRADE ID
        00:45:27.365 same trade, status=CONFIRMED -> same TRADE ID
        Each re-fire routes to on_maker_fill -> NameError -> catch ->
        next ws frame for same TRADE re-routes -> NameError again.

The test below feeds the closure a synthetic fill event that hits the
code path *before* the buggy line (so we exercise the monotonic-max
path), and asserts that no exception leaks out. The assertion catches
name errors, attribute errors, type errors, anything that propagates.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, List, Optional

import pytest

from src.execution.order_manager import OrderManager, TrackedOrder
from src.main import _make_maker_fill_callback


# ---------------------------------------------------------------------------
# Stubs. The wiring the closure actually needs:
#   state.order_manager      (om.by_server_id, om.apply_fill_event, om.mark_filled)
#   state.engine             (eng.on_order_filled)
# Trade object: duck-typed (we read .asset_id, .price, .size, .status).
# candidates tuple:
#   (oid, asset_id, mo_amount, maker_price, rest_delta)  per user_ws contract.
# ---------------------------------------------------------------------------


@dataclass
class _FakeOrderManager:
    tracked: dict = field(default_factory=dict)
    apply_log: List[Any] = field(default_factory=list)
    mark_filled_log: List[str] = field(default_factory=list)

    def by_server_id(self, server_id: str) -> Optional[TrackedOrder]:
        return self.tracked.get(server_id)

    def apply_fill_event(
        self, server_id: str, size_matched: float, fill_price: float
    ) -> None:
        self.apply_log.append((server_id, size_matched, fill_price))
        t = self.tracked.get(server_id)
        if t is not None:
            t.size_matched = size_matched

    def mark_filled(self, server_id: str) -> None:
        self.mark_filled_log.append(server_id)


class _FakeEngine:
    on_order_filled_log: List[Any] = []

    def on_order_filled(
        self, asset_id: str, order_id: str, fill_price: float, delta: float
    ) -> None:
        self.on_order_filled_log.append((asset_id, order_id, fill_price, delta))


class _FakeState:
    def __init__(self, om, eng):
        self.order_manager = om
        self.engine = eng


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _build_callback_and_state(matched_qty: float = 5.0, rest_delta: float = 0.0):
    om = _FakeOrderManager()
    tracked = TrackedOrder(
        client_order_id="c1",
        server_order_id="0xabc",
        round_start_ts=1783817100,
        side="BUY",
        token_id="2479945053069227",
        tier_name="T1",
        price=0.36,
        size=5.0,
        lifetime_s=180,
        placed_at=1783817100.0,
        expires_at=1783817280.0,
        placed_size=5.0,
    )
    tracked.size_matched = 0.0
    om.tracked["0xabc"] = tracked

    _FakeEngine.on_order_filled_log = []
    eng = _FakeEngine()
    state = _FakeState(om, eng)
    cb = _make_maker_fill_callback(state)
    return cb, om, eng, tracked


def test_maker_fill_callback_does_not_raise_name_error() -> None:
    """
    The H-NEW-2026-07-12 production regression. Whatever the closure
    does internally, propagating a NameError to user_ws is unacceptable
    because user_ws's outer try/except then re-routes the SAME TRADE
    on every ws frame re-fire (MATCHED -> MINED -> CONFIRMED), and the
    loop is the symptom the operator sees.
    """
    cb, om, eng, tracked = _build_callback_and_state(matched_qty=5.0)

    # Per-trade candidates: WS reported matched_amount=5.0 (TRADE 5818f993..
    # in the prod log), no REST fallback delta needed.
    fill = SimpleNamespace(
        asset_id="2479945053069227",
        price=0.31,
        size=5.55,
        status="MATCHED",
    )
    # (oid, asset_id, mo_amount, maker_price, rest_filled_size_delta)
    candidates = [("0xabc", "2479945053069227", 5.0, 0.31, 0.0)]

    # Just this call. If it raises, the test fails.
    cb(fill, candidates=candidates)  # type: ignore[call-arg]

    # And the engine MUST have been told about the fill.
    assert _FakeEngine.on_order_filled_log, (
        "Engine should have been called with the per-event delta; "
        "the bug was that on_order_filled was unreachable because of "
        "the NameError on the previous line."
    )
    asset_id, order_id, fill_price, delta = _FakeEngine.on_order_filled_log[0]
    assert asset_id == "2479945053069227"
    assert order_id == "0xabc"
    assert fill_price == pytest.approx(0.31)
    # Delta is the per-trade contribution, not the cumulative.
    assert delta == pytest.approx(5.0)


def test_maker_fill_callback_with_rest_delta_does_not_raise() -> None:
    """
    The H-NEW-2026-07-12 second leg (00:45:48Z). After T1 fired, T2
    took partial fills via REST fallback. Same closure. Same NameError
    would re-fire.
    """
    cb, om, eng, tracked = _build_callback_and_state(matched_qty=1.333332)

    # Tracks some prior progress so the delta math is non-trivial.
    tracked.size_matched = 1.333332

    fill = SimpleNamespace(
        asset_id="9858694723781260",  # the UP token in the prod log
        price=0.75,
        size=1.333332,
        status="MATCHED",
    )
    # chosen[4] = rest_delta for THIS trade alone
    candidates = [("0xabc", "9858694723781260", 0.0, 0.75, 6.76)]

    cb(fill, candidates=candidates)  # type: ignore[call-arg]

    assert _FakeEngine.on_order_filled_log
    _, _, _, delta = _FakeEngine.on_order_filled_log[0]
    # The bug would NOT reach this assertion because the NameError fires
    # before the engine call. With the fix, delta is the REST delta.
    assert delta == pytest.approx(6.76)
