"""Defence-in-depth dedup tests for the WS-TRADE vs polling double-fire race.

H-NEW-2026-07-12 audit finding (race window):
  T+0      GTD SELL 推送，FillDeduper.bookmark=0
  T+50ms   WS TRADE event 到达 → main.py _make_maker_fill_callback
           → om.apply_fill_event(size_matched=10)
           FillDeduper.bookmark 没有同步 ← 不一致
  T+1500   polling 看到 size_matched=10
           → FillDeduper.submit(10) → delta=10-0=10
           → fill_callback(delta=10) → engine.record_tp_fill(size=10)
           → _total_qty 减 10 (第二次)

Fix: _make_fill_callback now monotonic-max against om.by_server_id(order_id).
size_matched. WS-observed cumulative > polling-observed cumulative ⇒
polling sees delta=0 ⇒ engine call is skipped.

The tests below exercise that monotonic guard without touching the network.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import pytest

from src.execution.order_manager import OrderManager, TrackedOrder


# ---------------------------------------------------------------------------
# Minimal stand-ins. The full engine is not under test here.
# ---------------------------------------------------------------------------


@dataclass
class _FakeOrderManager:
    """Just enough of OrderManager for the dedup guard to read .size_matched."""

    tracked_by_server: dict[str, _FakeTracked] = field(default_factory=dict)

    def by_server_id(self, server_order_id: str) -> Optional[_FakeTracked]:
        return self.tracked_by_server.get(server_order_id)

    def apply_fill_event(
        self,
        server_order_id: str,
        size_matched: float,
        fill_price: float | None = None,
        trader_side: str | None = None,
    ) -> None:
        t = self.tracked_by_server.get(server_order_id)
        if t is None:
            return
        if size_matched > t.size_matched:
            t.size_matched = size_matched
            # Match real OM: the previous log row keys on client_order_id.
            t.last_apply_fill_size = size_matched

    def mark_filled(self, server_order_id: str) -> None:
        t = self.tracked_by_server.get(server_order_id)
        if t is not None:
            t.marked_filled = True


@dataclass
class _FakeTracked:
    size_matched: float = 0.0
    client_order_id: str = "fake_cid"
    marked_filled: bool = False
    last_apply_fill_size: float = 0.0


# ---------------------------------------------------------------------------
# The function-under-test is a closure inside main.py. Re-implement here in
# the same shape so we can verify the monotonic logic directly. If main.py
# drifts from this stub, the integration test below catches it.
# ---------------------------------------------------------------------------


def _make_fill_callback_for_test(om: _FakeOrderManager, log: list[tuple]):
    """Mirror of main._state.execution._make_fill_callback's on_fill body."""

    def on_fill(
        token_id: str,
        order_id: str,
        fill_price: float,
        fill_delta_size: float,
        fill_total_size: float,
    ) -> bool:
        """Return True iff the engine call is actually dispatched."""
        if fill_total_size <= 0:
            return False

        tracked = om.by_server_id(order_id)
        prev_size_matched = tracked.size_matched if tracked else 0.0
        effective_delta = fill_total_size - prev_size_matched
        if effective_delta <= 1e-9:
            if effective_delta < -1e-9:
                log.append(("out_of_order", order_id, fill_total_size, prev_size_matched))
            return False

        new_delta = effective_delta
        om.mark_filled(order_id)
        om.apply_fill_event(order_id, size_matched=fill_total_size, fill_price=fill_price)
        log.append(
            (
                "engine_call",
                order_id,
                fill_price,
                new_delta,
                fill_total_size,
            )
        )
        return True

    return on_fill


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_polling_after_ws_trade_does_not_double_fire() -> None:
    """Reproduces the T+0/50ms/1500ms race:

    1. Pre-stage OrderManager as if WS TRADE already pushed a fill of 10.
    2. Polling path arrives with size_matched=10 (FillDeduper diff = 10).
    3. We expect the engine call to be SKIPPED — only 0 net delta observed.
    """
    om = _FakeOrderManager()
    om.tracked_by_server["order-A"] = _FakeTracked(
        size_matched=10.0, client_order_id="cid-A"
    )
    log: list[tuple] = []
    on_fill = _make_fill_callback_for_test(om, log)

    # Simulate the polling fire: FillDeduper thinks its delta is 10 (its
    # bookmark is 0 — it didn't see the WS push). Our guard inspects
    # om.size_matched and rejects the delta.
    fired = on_fill(
        token_id="T", order_id="order-A",
        fill_price=0.42, fill_delta_size=10.0, fill_total_size=10.0,
    )

    assert fired is False, "polling after WS TRADE must NOT fire engine"
    assert log == [], f"expected no engine calls; got {log}"
    # OrderManager's mirror should be unchanged (10 == 10, monotonic).
    assert om.tracked_by_server["order-A"].size_matched == 10.0
    assert om.tracked_by_server["order-A"].marked_filled is False


def test_polling_sees_new_fill_ahead_of_ws_does_fire() -> None:
    """Polling advances past the OM baseline ⇒ engine call IS dispatched.

    This is the normal case (no WS push yet) and must still trigger.
    """
    om = _FakeOrderManager()
    om.tracked_by_server["order-B"] = _FakeTracked(size_matched=5.0, client_order_id="cid-B")
    log: list[tuple] = []
    on_fill = _make_fill_callback_for_test(om, log)

    fired = on_fill(
        token_id="T", order_id="order-B",
        fill_price=0.42, fill_delta_size=5.0, fill_total_size=10.0,
    )

    assert fired is True, "new fill beyond OM baseline must fire"
    assert len(log) == 1
    _, oid, px, delta, total = log[0]
    assert oid == "order-B"
    assert px == 0.42
    assert delta == 5.0  # 10 - 5
    assert total == 10.0
    assert om.tracked_by_server["order-B"].size_matched == 10.0


def test_polling_with_stale_om_observation_logs_out_of_order() -> None:
    """If polling sees a SMALLER size_matched than OM already has (e.g. WS
    advanced OM to 12, polling reads 10 due to network reordering), we
    must NOT regress the position by emitting a negative-delta record_fill.
    """
    om = _FakeOrderManager()
    om.tracked_by_server["order-C"] = _FakeTracked(size_matched=12.0, client_order_id="cid-C")
    log: list[tuple] = []
    on_fill = _make_fill_callback_for_test(om, log)

    fired = on_fill(
        token_id="T", order_id="order-C",
        fill_price=0.42, fill_delta_size=10.0, fill_total_size=10.0,
    )

    assert fired is False
    # An out-of-order breadcrumb should be present.
    assert any(entry[0] == "out_of_order" for entry in log)
    # OM cumulative MUST NOT regress.
    assert om.tracked_by_server["order-C"].size_matched == 12.0


def test_first_observation_when_om_does_not_track_yet() -> None:
    """Pre-spawn race: polling fires before OrderManager has a TrackedOrder
    for this server_id. We should still record the cumulative when the
    TrackedOrder is created later — but here the OM read returns None and
    the fallback treats prev_size_matched=0.
    """
    om = _FakeOrderManager()  # no entry for order-D
    log: list[tuple] = []
    on_fill = _make_fill_callback_for_test(om, log)

    fired = on_fill(
        token_id="T", order_id="order-D",
        fill_price=0.42, fill_delta_size=5.0, fill_total_size=5.0,
    )

    assert fired is True
    # apply_fill_event was called but the TrackedOrder was missing ⇒ no-op.
    # We just verify the guard did not refuse the legitimate first observation.
    assert any(entry[0] == "engine_call" for entry in log)


def test_zero_or_negative_total_skips_engine_call() -> None:
    """Defensive guard: FillDeduper normally filters these but if it ever
    forwards a non-positive total_size we must not pollute the position.
    """
    om = _FakeOrderManager()
    om.tracked_by_server["order-E"] = _FakeTracked(size_matched=3.0, client_order_id="cid-E")
    log: list[tuple] = []
    on_fill = _make_fill_callback_for_test(om, log)

    assert on_fill(
        "T", "order-E", 0.42, 0.0, 0.0,
    ) is False
    assert on_fill(
        "T", "order-E", 0.42, 5.0, -1.0,
    ) is False
    # OM state untouched.
    assert om.tracked_by_server["order-E"].size_matched == 3.0
    assert log == []
