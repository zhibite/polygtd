"""Regression tests for H-NEW-2026-07-15 audit follow-up batch.

Pins the four state-machine fixes that the comprehensive code review
(2026-07-15) identified and that the engineering team committed in this
batch:

  1. `OrderManager.mark_filled` must compare `size_matched` against
     `placed_size` (the CLOB round-down accepted count, M-1 fix) rather
     than the naive `size`. Otherwise a clean fill at `placed_size`
     can leave `TrackedOrder.status` stuck at `live` when the two values
     differ by a single tick.

  2. `Position.mark_expired` must clear `cancel_pending` so a stale
     dispatch latch can never block a future legitimate cancel.

  3. `OrderManager.place_buy` must respect a caller-supplied `lifetime_s`
     instead of unconditionally reading `self._s.GTD_LIFETIME_S`.

  4. `StrategyEngine._check_take_profit` mid-retreated path must release
     `pos._tp_inflight` in lockstep with the backoff reset.

These tests are intentionally minimal — they target the single state-
machine transition each patch touches.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from src.execution.order_manager import OrderManager, TrackedOrder
from src.strategy.round_state import Position


# ---------------------------------------------------------------------------
# Test 1: mark_filled uses placed_size, not size
# ---------------------------------------------------------------------------


def test_mark_filled_uses_placed_size_for_threshold() -> None:
    """When `placed_size < size` (CLOB round-down M-1), `mark_filled` must
    compare `size_matched` against `placed_size * 0.995`, not
    `size * 0.995`. Otherwise a fully-filled order at `placed_size`
    would never flip `status` to `filled`.
    """
    om = OrderManager.__new__(OrderManager)
    om._orders = {
        "cid-T1": TrackedOrder(
            client_order_id="cid-T1",
            server_order_id="0xsrv",
            round_start_ts=100,
            side="BUY",
            token_id="tok",
            tier_name="T1",
            price=0.40,
            size=5.26,
            placed_size=5.0,
            lifetime_s=300,
            expires_at=10**12,
            placed_at=0.0,
            status="live",
            size_matched=5.0,
        )
    }
    om._by_server = {"0xsrv": "cid-T1"}

    filled = om.mark_filled("0xsrv")
    assert filled is True, (
        "size_matched=5.0 must satisfy placed_size*0.995=4.975"
    )


def test_mark_filled_still_partial_when_below_placed_size() -> None:
    """When `size_matched < placed_size * 0.995`, `mark_filled` must still
    correctly return False (partial fill, not done).
    """
    om = OrderManager.__new__(OrderManager)
    om._orders = {
        "cid-T1": TrackedOrder(
            client_order_id="cid-T1",
            server_order_id="0xsrv",
            round_start_ts=100,
            side="BUY",
            token_id="tok",
            tier_name="T1",
            price=0.40,
            size=5.26,
            placed_size=5.0,
            lifetime_s=300,
            expires_at=10**12,
            placed_at=0.0,
            status="live",
            size_matched=3.0,
        )
    }
    om._by_server = {"0xsrv": "cid-T1"}

    filled = om.mark_filled("0xsrv")
    assert filled is False, "Partial fill (3.0/5.0) must NOT flip status"


def test_mark_filled_falls_back_to_size_when_placed_size_zero() -> None:
    """When `placed_size == 0` (legacy code path didn't wire M-1), the
    fallback to `size` must still work.
    """
    om = OrderManager.__new__(OrderManager)
    om._orders = {
        "cid-T1": TrackedOrder(
            client_order_id="cid-T1",
            server_order_id="0xsrv",
            round_start_ts=100,
            side="BUY",
            token_id="tok",
            tier_name="T1",
            price=0.40,
            size=5.0,
            placed_size=0.0,
            lifetime_s=300,
            expires_at=10**12,
            placed_at=0.0,
            status="live",
            size_matched=5.0,
        )
    }
    om._by_server = {"0xsrv": "cid-T1"}

    filled = om.mark_filled("0xsrv")
    assert filled is True, "Without placed_size, fall back to size=5.0"


# ---------------------------------------------------------------------------
# Test 2: mark_expired clears cancel_pending
# ---------------------------------------------------------------------------


def test_mark_expired_clears_cancel_pending() -> None:
    """If a cancel dispatch was queued before the watcher's EXPIRED
    terminal state arrived (race), the cancel-pending latch must come
    down. Otherwise a future legitimate cancel would be permanently
    blocked by the stale latch.
    """
    pos = Position()
    t1 = pos.tiers[0]
    t1.placed = True
    t1.watching = True
    t1.server_order_id = "0xrace"
    t1.size_filled = 3.0
    t1.cancel_pending = True

    pos.mark_expired(t1, reason="gtd_expired_race")

    assert t1.placed is False
    assert t1.watching is False
    assert t1.server_order_id is None
    assert t1.cancel_pending is False, (
        "mark_expired must clear cancel_pending — a stale latch from a "
        "race-with-cancel would otherwise block all future cancels."
    )


# ---------------------------------------------------------------------------
# Test 3: TP mid-retreated resets _tp_inflight (lockstep with backoff)
# ---------------------------------------------------------------------------


def test_tp_mid_retreated_clears_inflight_in_lockstep_with_backoff() -> None:
    """When `_check_take_profit` resets the backoff because the mid
    retreated below the fire line, any previously-held `_tp_inflight`
    must be released too. Without this, the next tick would short-
    circuit on `tp_inflight=True` and never re-evaluate.
    """
    import asyncio

    from src.strategy.engine import StrategyEngine

    eng = StrategyEngine.__new__(StrategyEngine)
    eng._s = MagicMock()
    eng._s.PRICE_PRE_MARGIN = 0.10

    eng._gate = MagicMock()
    eng._gate.can_act.return_value = True

    pos = MagicMock()
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos._tp_inflight = True
    pos.total_qty = 5.0
    pos.avg_cost = 0.30
    pos.stake_cap_remaining = 100.0

    class _FakeSnap:
        entry_token = "tok"
        entry_side = "UP"
        tp_target = 0.40
        tp_inflight = True
        # H-NEW-2026-07-15: the in-flight TP GTD's server_order_id is
        # surfaced via snapshot so the engine can cancel it when mid
        # retreats below the fire line. Without surfacing this, the
        # GTD would sit on the CLOB for the full GTD_LIFETIME_S while
        # uPnL bleeds.
        tp_server_order_id = "0xtp_gtd_xyz"
        has_inflight_tier = False
        total_qty = 5.0
        avg_cost = 0.30
        tier_size_filled = (0.0,) * 4
        baseline_realized_price = None
        baseline_set_realized = False
        baseline_target_price = None

    class _FakeRound:
        position = pos

    eng._round = _FakeRound()
    eng._tp_last_post_error_ts = 999.0
    eng._tp_post_failure_count = 2
    eng._tp_backoff_s = 20.0

    class _FakePC:
        def get_midpoint(self, _tok):
            return 0.33  # below fire_line=0.36

    eng._pc = _FakePC()

    # H-NEW-2026-07-15 (audit follow-up): mid-retreated must emit a
    # cancel for the in-flight TP GTD so it doesn't sit on the CLOB
    # for the full lifetime. Capture the cancel action.
    captured_actions: list = []

    async def _fake_handler(action) -> None:
        captured_actions.append(action)

    eng._handler = _fake_handler

    async def _run() -> None:
        pos.snapshot = lambda: _FakeSnap()
        await eng._check_take_profit(bc_now=100_000.0)

    asyncio.run(_run())

    assert eng._tp_last_post_error_ts == 0.0
    assert eng._tp_post_failure_count == 0
    assert eng._tp_backoff_s == 5.0
    assert pos._tp_inflight is False, (
        "_tp_inflight must be released in lockstep with backoff reset"
    )
    # H-NEW-2026-07-15 (audit follow-up): the engine must emit a
    # cancel for the in-flight TP GTD so the residual sell doesn't sit
    # on the book. Use `tier=-1` as a TP-sentinel so the router can
    # dispatch to the TP path instead of the BUY-tier path.
    assert len(captured_actions) == 1, (
        f"expected exactly 1 cancel action emitted, got {len(captured_actions)}"
    )
    action = captured_actions[0]
    assert action.tier == -1, f"TP sentinel must be -1, got {action.tier}"
    assert action.server_order_id == "0xtp_gtd_xyz"
    assert action.reason == "tp_mid_retreated"


# ---------------------------------------------------------------------------
# Test 4: place_buy respects caller-supplied lifetime_s
# ---------------------------------------------------------------------------


def test_place_buy_respects_caller_supplied_lifetime() -> None:
    """`place_buy` must forward a caller-supplied `lifetime_s` to CLOB,
    not unconditionally overwrite it with `self._s.GTD_LIFETIME_S`.
    """
    import asyncio

    om = OrderManager.__new__(OrderManager)
    om._s = MagicMock()
    om._s.GTD_LIFETIME_S = 300
    om._lock = asyncio.Lock()
    om._orders = {}
    om._by_server = {}
    om._pending_fills = {}

    captured: dict = {}

    class _StubClob:
        async def place_gtd_buy(self, **_kw):
            captured.update(_kw)
            from src.execution.clob_client import FillResult

            return FillResult(
                success=True,
                order_id="0xtest",
                tokens=5.0,
                usdc=2.0,
                fill_price=0.40,
                expiration_ts=0,
            )

    om._clob = _StubClob()

    async def _run() -> None:
        await om.place_buy(
            token_id="tok",
            price=0.40,
            size=5.0,
            round_start_ts=100,
            tier_name="T1",
            tick_size=0.01,
            lifetime_s=180,
        )

    asyncio.run(_run())
    assert captured["lifetime_s"] == 180, (
        "place_buy must forward the caller's lifetime_s=180, not 300"
    )


def test_place_buy_falls_back_to_config_when_lifetime_unspecified() -> None:
    """When `lifetime_s=None`, `place_buy` must fall back to the configured
    `GTD_LIFETIME_S` and forward that value to the CLOB.
    """
    import asyncio

    om = OrderManager.__new__(OrderManager)
    om._s = MagicMock()
    om._s.GTD_LIFETIME_S = 300
    om._lock = asyncio.Lock()
    om._orders = {}
    om._by_server = {}
    om._pending_fills = {}

    captured: dict = {}

    class _StubClob:
        async def place_gtd_buy(self, **_kw):
            captured.update(_kw)
            from src.execution.clob_client import FillResult

            return FillResult(
                success=True,
                order_id="0xtest",
                tokens=5.0,
                usdc=2.0,
                fill_price=0.40,
                expiration_ts=0,
            )

    om._clob = _StubClob()

    async def _run() -> None:
        await om.place_buy(
            token_id="tok",
            price=0.40,
            size=5.0,
            round_start_ts=100,
            tier_name="T1",
            tick_size=0.01,
        )

    asyncio.run(_run())
    assert captured["lifetime_s"] == 300
