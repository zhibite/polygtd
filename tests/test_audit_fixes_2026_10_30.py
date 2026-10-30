"""Regression tests for the 2026-10-30 full-project audit fixes.

Covered defects (all previously live in the production path):

  1. `OrderManager.cancel_by_server_id` acquired the non-reentrant
     asyncio.Lock and then called `cancel()`, which re-acquired it →
     permanent deadlock of the DecisionLoop and round rollover.
  2. TierWatcher used the WALLET TOKEN BALANCE as per-order fill
     accounting. For a SELL the balance decreases as the order fills, so
     an unfilled TP was reported as FILLED; for BUY T2+ the balance is
     cumulative across tiers, over-crediting later tiers.
  3. CLOBClient's balance helpers called SDK methods that do not exist
     (`get_balance` / `get_pusd_balance`), and swallowed the error into
     0.0 — which made `_maybe_realize_closed` wipe live positions.
  4. `place_gtd_sell` polled for up to 60 s while holding OrderManager's
     global lock, then reported a still-live GTD as failed.
  5. `place_fak_sell` passed `size=` / `slippage=` to MarketOrderArgsV2,
     which raises TypeError (the fields are `amount` / `price`).
  6. The WS maker-fill callback referenced `tracked` before assignment
     (UnboundLocalError), and treated per-trade amounts as cumulative
     values (dropping every partial fill after the first).
  7. The TP watcher's on_done referenced the removed
     `TierOutcome.CROSSED`, so every TP terminal raised and leaked
     `_tp_inflight`.
"""
from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest

from src.execution.clob_client import CLOBClient, FillDeduper, FillResult
from src.execution.order_manager import OrderManager, TrackedOrder
from src.execution.tier_watcher import TierWatcher, TierOutcome
from src.main import ActionRouter, _make_maker_fill_callback
from src.strategy.round_state import Position


# ---------------------------------------------------------------------------
# 1) cancel_by_server_id must not deadlock
# ---------------------------------------------------------------------------


class _CancelClob:
    def __init__(self) -> None:
        self.cancelled: list[str] = []

    async def cancel_order(self, server_id: str) -> bool:
        self.cancelled.append(server_id)
        return True


@pytest.mark.asyncio
async def test_cancel_by_server_id_does_not_deadlock() -> None:
    clob = _CancelClob()
    om = OrderManager(clob)
    order = TrackedOrder(
        client_order_id="1-T1-abc",
        server_order_id="0xserver",
        round_start_ts=1,
        side="BUY",
        token_id="tok",
        tier_name="T1",
        price=0.40,
        size=5.0,
        lifetime_s=181,
        expires_at=time.time() + 181,
        placed_at=time.time(),
        status="live",
    )
    om._orders[order.client_order_id] = order
    om._by_server[order.server_order_id] = order.client_order_id

    ok = await asyncio.wait_for(
        om.cancel_by_server_id("0xserver"), timeout=1.0
    )
    assert ok is True
    assert order.status == "cancelled"
    assert clob.cancelled == ["0xserver"]


# ---------------------------------------------------------------------------
# 2) TierWatcher terminal decisions use per-order size_matched
# ---------------------------------------------------------------------------


class _FillStateClob:
    def __init__(self, size_matched: float, wallet_balance: float) -> None:
        self.size_matched = size_matched
        self.wallet_balance = wallet_balance
        self.cancel_count = 0

    async def get_order_fill_state(self, order_id: str) -> dict:
        return {
            "size_matched": self.size_matched,
            "avg_price": 0.48,
            "status": "live",
        }

    async def get_token_balance(self, token_id: str) -> float:
        return self.wallet_balance

    async def cancel_order(self, order_id: str) -> bool:
        self.cancel_count += 1
        return True


async def _run_watcher(
    side: str,
    size_matched: float,
    wallet_balance: float,
    expected: float,
) -> object:
    results: list[object] = []

    def on_complete(result):  # noqa: ANN001
        results.append(result)
        return asyncio.sleep(0)

    watcher = TierWatcher(
        clob=_FillStateClob(size_matched, wallet_balance),
        server_order_id="0xorder",
        token_id="tok",
        side=side,
        strategy_price=0.48,
        expected_size=expected,
        expires_at=time.time() + 0.2,
        poll_s=0.5,
        on_complete=on_complete,
        is_still_active=lambda: True,
    )
    watcher.start()
    for _ in range(100):
        if results:
            break
        await asyncio.sleep(0.02)
    await watcher.stop()
    assert results, "watcher did not reach a terminal state"
    return results[0]


@pytest.mark.asyncio
async def test_watcher_sell_ignores_remaining_wallet_balance() -> None:
    """Full wallet balance + zero order fills must NOT be FILLED.

    Before the fix, `balance >= expected_size` fired immediately and the
    watcher also reported `filled_qty=balance`, booking a sell that never
    happened.
    """
    result = await _run_watcher("SELL", size_matched=0.0, wallet_balance=5.0, expected=5.0)
    assert result.outcome == TierOutcome.EXPIRED
    assert result.filled_qty == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_watcher_sell_filled_uses_order_matched_size() -> None:
    result = await _run_watcher("SELL", size_matched=5.0, wallet_balance=0.0, expected=5.0)
    assert result.outcome == TierOutcome.FILLED
    assert result.filled_qty == pytest.approx(5.0)


@pytest.mark.asyncio
async def test_watcher_buy_does_not_count_other_tiers_balance() -> None:
    """T2 watcher: wallet holds 14.4 shares (T1+T2), this order matched 0.

    The old balance-based check reported FILLED/14.4. The per-order
    `size_matched` is authoritative → EXPIRED/0.
    """
    result = await _run_watcher("BUY", size_matched=0.0, wallet_balance=14.4, expected=9.4)
    assert result.outcome == TierOutcome.EXPIRED
    assert result.filled_qty == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# 3) CLOBClient balance helpers use get_balance_allowance + 6-decimals
# ---------------------------------------------------------------------------


class _BalanceSdk:
    def __init__(self, response: dict | None) -> None:
        self.response = response
        self.calls: list[object] = []

    def get_balance_allowance(self, params):  # noqa: ANN001
        self.calls.append(params)
        if self.response is None:
            raise RuntimeError("network down")
        return self.response


@pytest.mark.asyncio
async def test_token_balance_uses_balance_allowance_and_returns_none_on_error() -> None:
    client = CLOBClient.__new__(CLOBClient)

    client._client = _BalanceSdk({"balance": "5000000", "allowance": "10000000"})
    assert await client.get_token_balance("tok") == pytest.approx(5.0)
    assert await client.get_balance() == pytest.approx(5.0)

    client._client = _BalanceSdk(None)
    assert await client.get_token_balance("tok") is None
    assert await client.get_balance() is None

    client._client = _BalanceSdk({})
    assert await client.get_token_balance("tok") is None


# ---------------------------------------------------------------------------
# 4/5) GTD sell is non-blocking; FAK uses the V2 MarketOrderArgs fields
# ---------------------------------------------------------------------------


class _OrderSdk:
    def __init__(self) -> None:
        self.market_args = None
        self.get_order_calls = 0

    def create_and_post_order(self, **kwargs):  # noqa: ANN003
        return {"orderID": "0xgtd"}

    def create_and_post_market_order(self, args):  # noqa: ANN001
        self.market_args = args
        return {"orderID": "0xfak", "fills": [{"size": "5.0", "price": "0.40"}]}

    def get_order(self, order_id: str):
        self.get_order_calls += 1
        raise AssertionError("place_gtd_sell must not poll get_order")


@pytest.mark.asyncio
async def test_gtd_sell_returns_immediately_without_polling() -> None:
    sdk = _OrderSdk()
    client = CLOBClient.__new__(CLOBClient)
    client._client = sdk
    client.register_pending = None
    client.fill_callback = None
    client._deduper = FillDeduper()

    started = time.monotonic()
    result = await client.place_gtd_sell(
        token_id="tok", price=0.50, size=5.0, lifetime_s=300, tick_size=0.01
    )
    elapsed = time.monotonic() - started

    assert result.success is True
    assert result.tokens == 0.0
    assert elapsed < 1.0
    assert sdk.get_order_calls == 0


@pytest.mark.asyncio
async def test_fak_sell_uses_v2_amount_and_price_fields() -> None:
    sdk = _OrderSdk()
    client = CLOBClient.__new__(CLOBClient)
    client._client = sdk
    client.register_pending = None
    client.fill_callback = None
    client._deduper = FillDeduper()

    result = await client.place_fak_sell(
        token_id="tok", size=5.0, price=0.40, tick_size=0.01
    )
    assert result.success is True
    assert result.tokens == pytest.approx(5.0)
    assert sdk.market_args.amount == pytest.approx(5.0)
    assert sdk.market_args.price == pytest.approx(0.40)


# ---------------------------------------------------------------------------
# 6) Maker-fill callback: no UnboundLocalError; per-trade amounts ADD
# ---------------------------------------------------------------------------


class _FillOM:
    def __init__(self, size: float = 10.0) -> None:
        self.tracked = TrackedOrder(
            client_order_id="c",
            server_order_id="0xabc",
            round_start_ts=1,
            side="BUY",
            token_id="tok",
            tier_name="T1",
            price=0.40,
            size=size,
            lifetime_s=181,
            expires_at=time.time() + 181,
            placed_at=time.time(),
            placed_size=size,
        )

    def by_server_id(self, server_id: str):
        return self.tracked if server_id == "0xabc" else None

    def apply_fill_event(self, server_id: str, size_matched: float, fill_price: float | None = None) -> None:
        self.tracked.size_matched = max(self.tracked.size_matched, size_matched)

    def mark_filled(self, server_id: str) -> bool:
        return False


class _FillEngine:
    def __init__(self) -> None:
        self.deltas: list[float] = []

    def on_order_filled(self, token_id, server_order_id, fill_price, delta):  # noqa: ANN001
        self.deltas.append(delta)


def _fill(trade_id: str):  # noqa: ANN202
    return SimpleNamespace(
        asset_id="tok", price=0.0, size=10.0, status="MATCHED",
        trade_id=trade_id,
    )


def test_maker_fill_missing_price_does_not_raise() -> None:
    om, eng = _FillOM(), _FillEngine()
    cb = _make_maker_fill_callback(SimpleNamespace(order_manager=om, engine=eng))
    cb(_fill("t1"), candidates=[("0xabc", "tok", 3.0, 0.0, 0.0)])
    assert eng.deltas == pytest.approx([3.0])


def test_maker_fill_partial_trades_accumulate() -> None:
    om, eng = _FillOM(size=10.0), _FillEngine()
    cb = _make_maker_fill_callback(SimpleNamespace(order_manager=om, engine=eng))

    cb(_fill("t1"), candidates=[("0xabc", "tok", 3.0, 0.40, 0.0)])
    cb(_fill("t2"), candidates=[("0xabc", "tok", 4.0, 0.40, 0.0)])
    cb(_fill("t3"), candidates=[("0xabc", "tok", 3.0, 0.40, 0.0)])

    # 3 + 4 + 3 = 10. The old subtraction logic produced 3 + 1 + 0.
    assert eng.deltas == pytest.approx([3.0, 4.0, 3.0])
    assert om.tracked.size_matched == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# 7) TP watcher terminal callback books the fill and closes the snapshot
# ---------------------------------------------------------------------------


class _TpClob:
    async def get_order_fill_state(self, order_id: str) -> dict:
        return {"size_matched": 5.0, "avg_price": 0.48, "status": "matched"}

    async def get_token_balance(self, token_id: str) -> float:
        return 0.0

    async def cancel_order(self, order_id: str) -> bool:
        return True


@pytest.mark.asyncio
async def test_tp_watcher_terminal_books_fill_and_closes_snapshot() -> None:
    round_ts = int(time.time())
    pos = Position()
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos._total_qty = 5.0
    pos._total_cost = 2.0
    pos._avg_cost = 0.40
    pos.register_tp("0xtp", "tok", 5.0)

    engine = SimpleNamespace(
        _round=SimpleNamespace(position=pos, round_start_ts=round_ts)
    )
    state = SimpleNamespace(
        engine=engine,
        clob=_TpClob(),
        round_start_ts=round_ts,
        _bg_tasks=set(),
    )
    router = ActionRouter(state)
    router._spawn_tp_watcher(
        round_ts, "0xtp", "tok", 0.48, 5.0, time.time() + 300
    )

    for _ in range(100):
        watcher = router._tp_watchers.get(round_ts)
        if watcher is not None and watcher.result is not None:
            break
        await asyncio.sleep(0.02)
    # Let the on_done callback run.
    await asyncio.sleep(0.05)

    assert watcher is not None and watcher.result is not None
    assert watcher.result.outcome == TierOutcome.FILLED
    assert pos.total_qty == pytest.approx(0.0)
    assert pos._tp_inflight is False
    assert "0xtp" not in pos._tp_snapshots


# ===========================================================================
# GTD invariants: one live order per tier, never over the tier budget
# ===========================================================================


class _VerifyClob:
    """Fake CLOB with an open-orders endpoint for the orphan check."""

    def __init__(self, open_orders=None, query_error: bool = False) -> None:
        self.open_orders = open_orders or []
        self.query_error = query_error
        self.place_calls: list[dict] = []

    async def get_open_orders(self, asset_id=None):  # noqa: ANN001
        if self.query_error:
            raise RuntimeError("open orders unavailable")
        if asset_id is None:
            return list(self.open_orders)
        return [
            o for o in self.open_orders
            if o.get("asset_id") in (None, asset_id)
        ]

    async def place_gtd_buy(self, **kwargs):  # noqa: ANN003
        self.place_calls.append(kwargs)
        return FillResult(
            order_id="0xnew",
            tokens=0.0,          # real client returns 0 for a non-polling post
            usdc=0.0,
            fill_price=kwargs["price"],
            success=True,
            expiration_ts=int(time.time()) + 300,
        )


def _make_om_for_placement(clob) -> OrderManager:  # noqa: ANN001
    mgr = OrderManager.__new__(OrderManager)
    mgr._clob = clob
    mgr._lock = asyncio.Lock()
    mgr._orders = {}
    mgr._by_server = {}
    mgr._pending_fills = {}
    mgr._pc = None
    mgr._pm = None
    mgr._startup_reconciled = False
    mgr._s = SimpleNamespace(
        GTD_LIFETIME_S=300,
        VERIFY_NO_EXISTING_ORDER=True,
        STARTUP_RECONCILE_CANCEL_ALL=True,
    )
    return mgr


@pytest.mark.asyncio
async def test_pre_post_check_refuses_orphan_duplicate() -> None:
    """A live order at the same asset/side/price blocks a second POST."""
    clob = _VerifyClob(
        open_orders=[{
            "id": "0xorphan",
            "asset_id": "tok",
            "side": "BUY",
            "price": "0.40",
            "status": "LIVE",
        }]
    )
    om = _make_om_for_placement(clob)
    order = await om.place_buy(
        token_id="tok", price=0.40, size=5.0,
        round_start_ts=int(time.time()), tier_name="T1", tick_size=0.01,
    )
    assert order is None
    assert clob.place_calls == [], "must not POST while an orphan exists"


@pytest.mark.asyncio
async def test_pre_post_check_fails_closed_when_query_fails() -> None:
    clob = _VerifyClob(query_error=True)
    om = _make_om_for_placement(clob)
    order = await om.place_buy(
        token_id="tok", price=0.40, size=5.0,
        round_start_ts=int(time.time()), tier_name="T1", tick_size=0.01,
    )
    assert order is None
    assert clob.place_calls == [], "must not POST when verification failed"


@pytest.mark.asyncio
async def test_placed_size_is_server_accepted_quantized_size() -> None:
    clob = _VerifyClob(open_orders=[])
    om = _make_om_for_placement(clob)
    order = await om.place_buy(
        token_id="tok", price=0.40, size=5.126,
        round_start_ts=int(time.time()), tier_name="T1", tick_size=0.01,
    )
    assert order is not None
    # 5.126 → 5.12 at tick 0.01; stored as both size and placed_size so
    # the engine's size_expected matches the server.
    assert order.size == pytest.approx(5.12)
    assert order.placed_size == pytest.approx(5.12)
    assert clob.place_calls[0]["size"] == pytest.approx(5.12)


@pytest.mark.asyncio
async def test_average_down_rearm_uses_remaining_tier_budget() -> None:
    """After a partial fill, a re-armed tier may only spend the remainder.

    This is the invariant that prevents a tier from spending 1.5-2x its
    configured notional when a partially-filled GTD is cancelled and later
    re-triggered.
    """
    from src.strategy.engine import ActionOpenTN, StrategyEngine
    from src.strategy.round_state import Position, RoundState

    eng = StrategyEngine.__new__(StrategyEngine)
    pos = Position()
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos.baseline_set_realized = True
    pos.baseline_realized_price = 0.40
    pos.tier1.size_filled = 5.0
    pos.tier1.cost_filled = 2.0
    pos.tier1.filled = True
    pos._total_qty = 8.0
    pos._total_cost = 3.0
    pos._avg_cost = 3.0 / 8.0
    # T2: 1 USD already spent of its 3 USD budget.
    pos.tier2.size_filled = 3.0
    pos.tier2.cost_filled = 1.0
    pos.tier2.notional = 3.0
    pos.tier2.placed = False
    pos.tier2.watching = False

    eng._round = RoundState(round_start_ts=int(time.time()), market=None)
    eng._round.position = pos
    eng._round.up_token = "tok"
    eng._round.down_token = "dn"
    eng._gate = SimpleNamespace(
        can_act=lambda *a, **kw: True,
        can_enter=lambda *a, **kw: True,
        can_double_down=lambda *a, **kw: True,
        state=lambda *a, **kw: SimpleNamespace(drift=0.0, within_limit=True),
    )
    eng._bc = SimpleNamespace(last_price=100000.0)
    eng._pc = SimpleNamespace(get_midpoint=lambda token: 0.30)
    eng._s = SimpleNamespace(
        PRICE_PRE_MARGIN=0.10,
        tier_drop_pcts=[None, 0.20, 0.40, 0.60],
        TP_DECREMENT_PCT=0.03,
        TP_PROFIT_PCT=0.20,
        TP_MIN_PCT=0.05,
    )
    eng._tN_skip_last_log_ts = {}

    captured = []

    async def handler(action):  # noqa: ANN001
        captured.append(action)

    eng._handler = handler
    await eng._check_average_down(100000.0)

    assert len(captured) == 1 and isinstance(captured[0], ActionOpenTN)
    assert captured[0].tier == 2
    # 3.0 planned − 1.0 already filled = 2.0 remaining (NOT the full 3.0).
    assert captured[0].notional == pytest.approx(2.0)


@pytest.mark.asyncio
async def test_failed_cancel_restores_tier_reservation() -> None:
    """If the CLOB does not confirm a cancel, the slot must stay reserved.

    Otherwise `total_notional_committed` stops counting a still-live order
    and the engine can place another tier on top of it (over-exposure).
    """
    from src.main import ActionRouter
    from src.strategy.engine import ActionCancelTier
    from src.strategy.round_state import Position

    live_expiry = time.time() + 300

    class _OM:
        async def cancel_by_server_id(self, server_id: str, reason: str = "x") -> bool:
            return False

        def by_server_id(self, server_id: str):
            return SimpleNamespace(status="live", expires_at=live_expiry)

    round_ts = int(time.time())
    pos = Position()
    pos.tier2.placed = True
    pos.tier2.watching = True
    pos.tier2.server_order_id = "0xold"
    eng = SimpleNamespace(
        _round=SimpleNamespace(
            position=pos, round_start_ts=round_ts, tick_size=0.01
        )
    )
    state = SimpleNamespace(
        engine=eng,
        order_manager=_OM(),
        clob=None,
        round_start_ts=round_ts,
        _bg_tasks=set(),
    )
    router = ActionRouter(state)

    await router(ActionCancelTier(tier=2, server_order_id="0xold", reason="test"))

    assert pos.tier2.placed is True, (
        "a live order whose cancel failed must keep consuming its tier budget"
    )
    assert pos.tier2.watching is True
