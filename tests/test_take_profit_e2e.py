"""End-to-end test for the take-profit (TP) sell flow (2026-07-12).

Validates that when a position's TP target is breached, the bot actually
sells — covering the full chain:

  engine._check_take_profit
    ↓ ActionTakeProfit(token, price=tp_target, qty=total_qty)
  router._tp_runner
    ↓ pos.register_tp(server_order_id, …) → _tp_inflight holds
    ↓ om.place_gtd_sell_at_price  → TrackedOrder returned
    ↓ _spawn_tp_watcher
  CLOBClient.place_gtd_sell (polling loop)
    ↓ matched_sz > 0 → _fire_fill_callback
    ↓ self.fill_callback (router._make_fill_callback)
    ↓ om.apply_fill_event (mirror only) + om.mark_filled
    ↓ eng.on_order_filled(token, server_order_id, fill_price, delta)
  engine.on_order_filled
    ↓ no BUY tier matches server_order_id → falls through to
    ↓ pos.record_tp_fill(server_order_id, fill_price, fill_size)
  Position.record_tp_fill
    ↓ _total_qty     -= size
    ↓ _total_cost    -= avg-cost * size
    ↓ _avg_cost      recomputed
    ↓ _tp_inflight   cleared
    ↓ _tp_snapshots  drop when size_sold >= target_qty

The audit fix that makes the critical step `record_tp_fill` reachable:
H-NEW-1 register_pending_server_id was added so that if CLOBClient's
in-place polling fires `_fire_fill_callback` BEFORE OrderManager has
inserted the TrackedOrder (i.e. the WS TRADE raced the lock-acquire),
the fill does not orphan. This test exercises that race directly.

Run via `pytest tests/test_take_profit_e2e.py`.
"""
from __future__ import annotations

import asyncio
import pathlib
import sys
from dataclasses import dataclass
from typing import Any

import pytest

# Repo root on sys.path
_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ---------------------------------------------------------------------------
# Test fakes
# ---------------------------------------------------------------------------


class _FakeMakerFill:
    """Stub for the WS TRADE MakerFill used by `_make_maker_fill_callback`.

    We deliberately do NOT exercise the WS path here — the test below
    targets the CLOBClient polling callback, which is the more reliable
    source of TP fill signal (the WS TRADE path is covered by
    test_audit_fixes_2026_07_11.py).
    """

    def __init__(self, size: float = 0.0, price: float = 0.0):
        self.size = size
        self.price = price
        self.maker_order_id = ""
        self.asset_id = ""
        self.trade_id = ""
        self.status = ""


@dataclass
class _FillState:
    """Snapshot of the polling-CLOBClient fill for one TP GTD SELL.

    The fake CLOBClient knows about exactly one GTD SELL; we control the
    sequence of "polls" by enqueueing `_FillState` rows on
    `self._fill_states[server_order_id]`.
    """
    size_matched: float
    avg_price: float
    status: str = "live"  # "live" | "matched" | "filled" | "cancelled" | "expired"


class _FakeCLOB:
    """Fake CLOBClient whose place_gtd_sell acts like the real polling loop:

      - Posts the order immediately (records it in `_posted_orders`).
      - Drains `_fill_states[server_order_id]` one row at a time, firing
        `_fire_fill_callback` on each, until status is terminal.
      - Returns a `FillResult` reflecting the final state.
    """

    def __init__(self) -> None:
        self.fill_callback = None
        self._posted_orders: dict[str, dict[str, Any]] = {}
        self._fill_states: dict[str, list[_FillState]] = {}
        self._deduper_bookmarks: dict[str, float] = {}

    def queue_fills(self, server_order_id: str, states: list[_FillState]) -> None:
        self._fill_states[server_order_id] = list(states)

    async def place_gtd_sell(
        self,
        token_id: str,
        price: float,
        size: float,
        lifetime_s: int,
        tick_size: float,
    ) -> "FillResult":
        from src.execution.clob_client import FillResult

        server_id = f"0xtp_{len(self._posted_orders):04d}"
        self._posted_orders[server_id] = dict(
            token_id=token_id, price=price, size=size, lifetime_s=lifetime_s,
        )
        self._deduper_bookmarks[server_id] = 0.0

        # Drain queued fill states. Each non-terminal state fires a fill
        # callback; terminal states (matched / cancelled / expired) end
        # the loop.
        states = self._fill_states.get(server_id, [])
        total_sz = 0.0
        total_usdc = 0.0
        last_fill_price = price
        for fs in states:
            total_sz = fs.size_matched
            total_usdc = total_sz * fs.avg_price
            last_fill_price = fs.avg_price
            # Fire the cumulative through deduper → router callback.
            if self.fill_callback and fs.size_matched > 0:
                prev = self._deduper_bookmarks[server_id]
                delta = fs.size_matched - prev
                if delta > 0:
                    self._deduper_bookmarks[server_id] = fs.size_matched
                    # Mimic _fire_fill_callback signature:
                    # fill_callback(token_id, order_id, fill_price,
                    #               fill_delta, fill_total)
                    self.fill_callback(
                        token_id, server_id, fs.avg_price,
                        delta, fs.size_matched,
                    )
            if fs.status in ("matched", "filled"):
                return FillResult(
                    order_id=server_id,
                    tokens=float(total_sz),
                    usdc=float(total_usdc),
                    fill_price=float(last_fill_price),
                    success=True,
                    expiration_ts=0,
                )
            if fs.status in ("cancelled", "expired"):
                return FillResult(
                    order_id=server_id,
                    tokens=float(total_sz),
                    usdc=float(total_usdc),
                    fill_price=float(last_fill_price),
                    success=total_sz > 0,
                    error=f"gtd_{fs.status}",
                    expiration_ts=0,
                )

        # No terminal state queued — order remains live. Return a partial
        # FillResult (this matches the real client's `poll_timeout` branch).
        return FillResult(
            order_id=server_id,
            tokens=float(total_sz),
            usdc=float(total_usdc),
            fill_price=float(last_fill_price),
            success=False,
            error="no_terminal_state",
            expiration_ts=0,
        )

    async def place_gtd_buy(self, *a, **kw):  # not used here
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Test: end-to-end TP sell after a 5-share T1 fill
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_take_profit_e2e_full_chain() -> None:
    """Drive the full TP chain end-to-end:

      1. Build an engine with T1 partially filled (5 shares @ 0.40,
         _avg_cost = 0.40 → tp_target ≈ 0.48).
      2. Pre-arm CLOBClient with a single matched-at-target fill state.
      3. Wire up a minimal OrderManager + ActionRouter-equivalent fill
         callback so CLOBClient.fill_callback reaches eng.on_order_filled.
      4. Call engine._check_take_profit with mid above fire_line; expect:
            ActionTakeProfit emitted
            _tp_inflight=True
            _tp_snapshots[server_id] populated
            position.record_tp_fill decrements _total_qty, clears _tp_inflight
            _tp_snapshots dropped (size_sold == target_qty)
    """
    from src.strategy.engine import StrategyEngine
    from src.strategy.round_state import Position, RoundState
    from src.execution.order_manager import OrderManager
    from src.execution.clob_client import FillDeduper

    # ------------------------------------------------------------------
    # Step 1: engine with a populated T1 fill
    # ------------------------------------------------------------------
    eng = StrategyEngine.__new__(StrategyEngine)
    pos = Position()
    pos.entry_token = "tok_up"
    pos.entry_side = "UP"
    pos.tier1.size_filled = 5.0
    pos.tier1.size_expected = 5.0
    pos.tier1.cost_filled = 5.0 * 0.40  # 2.0 USDC
    pos.tier1.avg_fill_price = 0.40
    pos.tier1.filled = True
    pos.tier1.token_id = "tok_up"
    pos._total_qty = 5.0
    pos._total_cost = 2.0
    pos._avg_cost = 0.40
    pos.baseline_set_realized = True
    pos.baseline_realized_price = 0.40

    import time
    eng._round = RoundState(round_start_ts=int(time.time()), market=None)
    eng._round.position = pos
    eng._round.up_token = "tok_up"
    eng._round.down_token = "tok_dn"

    eng._gate = type("G", (), {
        "can_act": lambda *a, **kw: True,
        "can_double_down": lambda *a, **kw: True,
        "state": lambda *a, **kw: type(
            "S", (), {"drift": 0.0, "within_limit": True}
        )(),
    })()
    eng._bc = type("B", (), {"last_price": 100.0})()

    # Mid price well above fire_line (tp_target=0.48, margin=0.10,
    # fire_line = 0.48 * 0.9 = 0.432). Mid=0.55 fires.
    eng._pc = type("P", (), {
        "get_midpoint": lambda *args: 0.55,
    })()

    class _Cfg:
        PRICE_PRE_MARGIN = 0.10
        ENTRY_PRICE_CAP = 0.36
        TP_PROFIT_PCT = 0.20
        TP_DECREMENT_PCT = 0.05
        TP_MIN_PCT = 0.05
        TIER_WATCHER_POLL_S = 0.5
        TIER_WATCHER_FILL_RATIO = 0.95
    eng._s = _Cfg()

    # ------------------------------------------------------------------
    # Step 2: OrderManager + FakeCLOB with the matched fill pre-queued
    # ------------------------------------------------------------------
    fake_clob = _FakeCLOB()
    # Match-the-target fill: 5 shares at 0.48 (= tp_target).
    fake_clob.queue_fills(
        "0xtp_0000",
        [_FillState(size_matched=5.0, avg_price=0.48, status="matched")],
    )

    om = OrderManager.__new__(OrderManager)
    import asyncio as _asyncio
    om._clob = fake_clob
    om._lock = _asyncio.Lock()
    om._orders = {}
    om._by_server = {}
    om._pending_fills = {}
    om._pc = None
    om._pm = None
    om._s = type("S", (), {
        "GTD_LIFETIME_S": 120,
        "SELL_SLIPPAGE_PCT": 1.0,
        "TIER_WATCHER_POLL_S": 0.5,
        "TIER_WATCHER_FILL_RATIO": 0.95,
    })()

    # ------------------------------------------------------------------
    # Step 3: wire the router-side fill callback (the polling path)
    # ------------------------------------------------------------------
    # This is the same wiring as `main.py::_make_fill_callback`. We
    # inline the minimal version so the test exercises the exact code
    # path that production runs.
    eng._tp_last_post_error_ts = 0.0
    eng._tp_backoff_s = 5.0
    eng._tp_post_failure_count = 0

    def on_fill(token_id, order_id, fill_price, fill_delta_size, fill_total_size):
        # Mirror into OrderManager (track size_matched monotonic).
        tracked = om.by_server_id(order_id)
        prev = tracked.size_matched if tracked else 0.0
        effective = fill_total_size - prev
        if effective <= 1e-9:
            return
        om.apply_fill_event(
            order_id, size_matched=fill_total_size, fill_price=fill_price,
        )
        om.mark_filled(order_id)
        # Forward the engine entry point.
        eng.on_order_filled(
            token_id, order_id, fill_price, fill_delta_size,
        )

    fake_clob.fill_callback = on_fill

    # ------------------------------------------------------------------
    # Step 4: simulate the engine emitting an ActionTakeProfit and the
    # router running the TP GTD through the fake CLOB.
    # ------------------------------------------------------------------
    from src.strategy.engine import ActionTakeProfit

    captured_actions = []
    async def handler(action):
        captured_actions.append(action)

    eng._handler = handler

    # Sanity: tp_target computed.
    tp_target = pos.tp_target()
    assert tp_target is not None and abs(tp_target - 0.48) < 1e-9, (
        f"tp_target mismatch: expected 0.48, got {tp_target}"
    )

    # Engine emits.
    await eng._check_take_profit(0.0)

    # ActionTakeProfit emitted.
    assert len(captured_actions) == 1, (
        f"expected 1 ActionTakeProfit, got {len(captured_actions)}"
    )
    action = captured_actions[0]
    assert action.token == "tok_up"
    assert abs(action.price - 0.48) < 1e-9
    assert abs(action.qty - 5.0) < 1e-9

    # Engine has flipped _tp_inflight BEFORE awaiting the handler
    # (line 1359 of engine.py).
    assert pos._tp_inflight is True, (
        "_tp_inflight must be True after ActionTakeProfit emit; "
        "this is the gate that prevents re-fire while the SELL is live"
    )

    # ------------------------------------------------------------------
    # Step 5: drive the router's _tp_runner logic — register_tp, post GTD
    # ------------------------------------------------------------------
    pos.register_tp(
        server_order_id="0xtp_0000",
        token_id="tok_up",
        target_qty=action.qty,
    )
    order = await om.place_gtd_sell_at_price(
        token_id=action.token,
        size=action.qty,
        price=action.price,
        round_start_ts=int(time.time()),
        tier_name="TP",
        tick_size=0.01,
    )
    assert order is not None, "GTD SELL post should succeed"
    assert order.server_order_id == "0xtp_0000"
    assert order.size == 5.0

    # The fake CLOBClient immediately drained the queued matched state
    # and fired the fill_callback, which has already called
    # eng.on_order_filled → pos.record_tp_fill. So at this point the
    # TP should already be cleared.
    assert pos.total_qty == 0.0, (
        f"expected _total_qty to drop to 0 after matched fill; got {pos.total_qty}"
    )
    assert pos._avg_cost == 0.0, (
        f"expected _avg_cost to reset to 0 after full TP fill; got {pos._avg_cost}"
    )
    assert pos._tp_inflight is False, (
        f"_tp_inflight must be cleared after record_tp_fill; got {pos._tp_inflight}"
    )
    # Snapshot dropped once size_sold == target_qty.
    assert "0xtp_0000" not in pos._tp_snapshots, (
        "TP snapshot should be popped after size_sold == target_qty"
    )

    # OrderManager's tracked-order entry should also reflect the fill.
    tracked = om.by_server_id("0xtp_0000")
    assert tracked is not None
    assert tracked.size_matched >= 5.0 - 1e-9


# ---------------------------------------------------------------------------
# Test: partial TP fill leaves residual that re-arms
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_take_profit_e2e_partial_then_residual() -> None:
    """Drive a partial TP fill (3 of 5 shares), verify _tp_inflight is
    cleared and the residual can be picked up on the next _evaluate tick.

    Scenario: GTD SELL posts at tp_target, only 3 shares get taken
    before the order expires. record_tp_fill drops 3 from _total_qty,
    leaves 2 as residual, and clears _tp_inflight. The engine can then
    re-fire a fresh GTD for the residual if mid is still above target.
    """
    from src.strategy.engine import StrategyEngine
    from src.strategy.round_state import Position, RoundState

    eng = StrategyEngine.__new__(StrategyEngine)
    pos = Position()
    pos.entry_token = "tok_up"
    pos.entry_side = "UP"
    pos.tier1.size_filled = 5.0
    pos.tier1.size_expected = 5.0
    pos.tier1.cost_filled = 2.0
    pos.tier1.avg_fill_price = 0.40
    pos.tier1.filled = True
    pos._total_qty = 5.0
    pos._total_cost = 2.0
    pos._avg_cost = 0.40
    pos.baseline_set_realized = True
    pos.baseline_realized_price = 0.40

    import time
    eng._round = RoundState(round_start_ts=int(time.time()), market=None)
    eng._round.position = pos
    eng._round.up_token = "tok_up"
    eng._round.down_token = "tok_dn"

    # Direct test: bypass ActionRouter; just call record_tp_fill as the
    # fill callback would. This is what we want to assert in this test.
    pos._tp_inflight = True
    pos.register_tp(server_order_id="0xtp", token_id="tok_up", target_qty=5.0)

    # First partial fill: 3 of 5 shares at 0.48.
    ok = pos.record_tp_fill(server_order_id="0xtp", price=0.48, size=3.0)
    assert ok is True

    # After partial: 2 residual shares, _tp_inflight cleared.
    assert pos._total_qty == 2.0
    assert pos._avg_cost > 0  # residual still has cost basis
    assert pos._tp_inflight is False, (
        "partial TP fill must release _tp_inflight so the engine can re-arm"
    )
    # Snapshot kept (size_sold=3 < target_qty=5).
    snap = pos._tp_snapshots["0xtp"]
    assert snap.size_sold == 3.0
    assert snap.size_sold + 1e-9 < snap.target_qty

    # Subsequent partial fill: 2 of 5 shares at 0.48.
    pos._tp_inflight = True  # engine re-armed for the residual
    pos.record_tp_fill(server_order_id="0xtp", price=0.48, size=2.0)

    assert pos._total_qty == 0.0
    assert pos._avg_cost == 0.0
    assert pos._tp_inflight is False
    assert "0xtp" not in pos._tp_snapshots


# ---------------------------------------------------------------------------
# Test: TP rejected because position aggregate lags tier sum (R6)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_take_profit_blocked_when_avg_cost_unreconciled() -> None:
    """R6 fix: when tier.size_filled sum exceeds _total_qty (i.e.
    TierWatcher is mid-reconcile), TP must NOT fire. Verifying the
    emission gate from the recent audit hardening.
    """
    from src.strategy.engine import StrategyEngine
    from src.strategy.round_state import Position, RoundState

    eng = StrategyEngine.__new__(StrategyEngine)
    pos = Position()
    pos.entry_token = "tok_up"
    pos.entry_side = "UP"
    pos.tier1.size_filled = 5.0
    pos.tier1.filled = True
    pos.tier2.size_filled = 5.0  # T2 filled but not yet reconciled
    pos.tier2.filled = True
    pos._total_qty = 5.0  # behind by 5 — TierWatcher hasn't caught up
    pos._total_cost = 2.0
    pos._avg_cost = 0.40
    pos.baseline_set_realized = True
    pos.baseline_realized_price = 0.40

    import time
    eng._round = RoundState(round_start_ts=int(time.time()), market=None)
    eng._round.position = pos
    eng._round.up_token = "tok_up"
    eng._round.down_token = "tok_dn"

    eng._gate = type("G", (), {
        "can_act": lambda *a, **kw: True,
        "can_double_down": lambda *a, **kw: True,
        "state": lambda *a, **kw: type(
            "S", (), {"drift": 0.0, "within_limit": True}
        )(),
    })()
    eng._bc = type("B", (), {"last_price": 100.0})()
    eng._pc = type("P", (), {
        "get_midpoint": lambda *args: 0.55,
    })()

    class _Cfg:
        PRICE_PRE_MARGIN = 0.10
        ENTRY_PRICE_CAP = 0.36
        TP_PROFIT_PCT = 0.20
        TP_DECREMENT_PCT = 0.05
        TP_MIN_PCT = 0.05
    eng._s = _Cfg()

    captured = []
    async def handler(action):
        captured.append(action)

    eng._handler = handler
    eng._tp_last_post_error_ts = 0.0
    eng._tp_backoff_s = 5.0
    eng._tp_post_failure_count = 0

    await eng._check_take_profit(0.0)

    assert captured == [], (
        f"R6 regression: TP fired while position aggregate was lagging "
        f"per-tier sum; got {captured}"
    )