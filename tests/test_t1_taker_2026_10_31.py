"""T1 taker-entry path (1/2/4/8 ladder).

A 1-USD T1 at ~0.40 is only ~2.5 shares, below the CLOB's 5-share
minimum for resting GTD orders. T1 therefore enters with a FAK market
BUY sized in USDC, while T2-T4 stay maker GTDs.

Covered:
  * `CLOBClient.place_fak_buy` parses executed fills / aggregate amounts;
  * `OrderManager.place_market_buy` returns an already-filled order;
  * the router binds tier 1 and feeds that fill to the engine without
    spawning a TierWatcher.
"""
from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest
from py_clob_client_v2 import OrderType, Side

from src.execution.clob_client import CLOBClient, FillResult
from src.execution.order_manager import OrderManager, TrackedOrder
from src.main import ActionRouter
from src.strategy.engine import ActionOpenT1, ActionTakeProfit
from src.strategy.round_state import Position


# ---------------------------------------------------------------------------
# CLOBClient.place_fak_buy
# ---------------------------------------------------------------------------


class _FakeSdk:
    def __init__(self, response) -> None:
        self.response = response
        self.calls: list[tuple[object, object]] = []

    def create_and_post_market_order(self, order_args, order_type=None):
        self.calls.append((order_args, order_type))
        return self.response


def _clob_with(response):
    clob = CLOBClient(private_key="")
    sdk = _FakeSdk(response)
    clob._client = sdk  # type: ignore[assignment]
    return clob, sdk


@pytest.mark.asyncio
async def test_fak_buy_sends_a_usdc_market_order() -> None:
    clob, sdk = _clob_with({
        "orderID": "0xabc",
        "fills": [{"size": 2.5, "price": 0.40}],
    })

    out = await clob.place_fak_buy(
        token_id="tok", amount=1.0, price=0.40, tick_size=0.01
    )

    assert out.success is True
    assert out.tokens == pytest.approx(2.5)
    assert out.usdc == pytest.approx(1.0)
    assert out.fill_price == pytest.approx(0.40)

    args, order_type = sdk.calls[0]
    assert order_type == OrderType.FAK
    assert args.side == Side.BUY
    assert args.amount == pytest.approx(1.0)
    assert args.price == pytest.approx(0.40)


@pytest.mark.asyncio
async def test_fak_buy_prefers_aggregate_amounts_over_fills() -> None:
    """The aggregate amounts match the CLOB trade ledger; fills[] can be a
    tick off (production 0.36 vs ledger 0.37 on a 1-USDC buy)."""
    clob, _ = _clob_with({
        "orderID": "0xabc",
        "fills": [{"size": 2.5, "price": 0.36}],
        "takingAmount": "2.7027",   # shares
        "makingAmount": "1.0",      # USDC
    })
    out = await clob.place_fak_buy(
        token_id="tok", amount=1.0, price=0.40, tick_size=0.01
    )
    assert out.tokens == pytest.approx(2.7027)
    assert out.fill_price == pytest.approx(0.37)


@pytest.mark.asyncio
async def test_fak_buy_falls_back_to_aggregate_amounts() -> None:
    """Some CLOB responses only carry taking/makingAmount."""
    clob, _ = _clob_with({
        "orderID": "0xdef",
        "takingAmount": "2.5",
        "makingAmount": "1.0",
    })

    out = await clob.place_fak_buy(
        token_id="tok", amount=1.0, price=0.40, tick_size=0.01
    )

    assert out.success is True
    assert out.tokens == pytest.approx(2.5)
    assert out.fill_price == pytest.approx(0.40)


@pytest.mark.asyncio
async def test_fak_buy_without_fills_fails() -> None:
    clob, _ = _clob_with({"orderID": "0x0"})
    out = await clob.place_fak_buy(
        token_id="tok", amount=1.0, price=0.40, tick_size=0.01
    )
    assert out.success is False
    assert out.error == "FAK_no_fills"


@pytest.mark.asyncio
async def test_fak_buy_no_match_is_a_normal_no_fill() -> None:
    """The CLOB's 400 "no orders found to match" is a no-fill, not a
    POST/network error — the T1 router uses it to skip the window."""
    class _NoMatchSdk:
        def create_and_post_market_order(self, *args, **kwargs):
            raise RuntimeError(
                "PolyApiException[status_code=400, error_message={'error': "
                "'no orders found to match with FAK order. FAK orders are "
                "partially filled or killed if no match is found.'}]"
            )

    clob = CLOBClient(private_key="")
    clob._client = _NoMatchSdk()  # type: ignore[assignment]

    out = await clob.place_fak_buy(
        token_id="tok", amount=1.0, price=0.40, tick_size=0.01
    )

    assert out.success is False
    assert out.error == "FAK_no_fills"


@pytest.mark.asyncio
async def test_fak_sell_marketable_parses_aggregate_amounts() -> None:
    """A matched FAK sell may omit fills[]; makingAmount/takingAmount are
    authoritative (production 2026-10-31 14:31 UTC: 2.7 @0.41 sold but
    misread as FAK_no_fills)."""
    clob, sdk = _clob_with({
        "orderID": "0xsell",
        "makingAmount": "2.7",     # shares sold
        "takingAmount": "1.107",   # USDC received
    })
    out = await clob.place_fak_sell_marketable(
        token_id="tok", size=2.7, reference_price=0.41,
        tick_size=0.01, slippage_pct=0.0,
    )
    assert out.success is True
    assert out.tokens == pytest.approx(2.7)
    assert out.fill_price == pytest.approx(0.41)
    args, _ = sdk.calls[0]
    assert args.amount == pytest.approx(2.7)


@pytest.mark.asyncio
async def test_fak_sell_marketable_subtracts_slippage_and_skips_callback() -> None:
    """The compact-TP sell must cross BELOW the mid, and the router owns
    the fill (the callback must not race the TP snapshot)."""
    clob, sdk = _clob_with({
        "orderID": "0xsell",
        "fills": [{"size": 2.5, "price": 0.49}],
    })
    seen: list = []
    clob.fill_callback = lambda *args, **kwargs: seen.append(args)

    out = await clob.place_fak_sell_marketable(
        token_id="tok", size=2.5, reference_price=0.50,
        tick_size=0.01, slippage_pct=2.0,
    )

    assert out.success is True
    args, _order_type = sdk.calls[0]
    assert args.side == Side.SELL
    assert args.amount == pytest.approx(2.5)
    assert args.price == pytest.approx(0.49)  # 0.50 × (1 − 2%)
    assert seen == [], "taker TP records the fill in the router"


# ---------------------------------------------------------------------------
# OrderManager.place_market_buy
# ---------------------------------------------------------------------------


class _BuyClob:
    def __init__(self, result: FillResult) -> None:
        self.result = result
        self.kwargs: dict | None = None

    async def get_open_orders(self, *args, **kwargs):  # noqa: ANN002,ANN003
        return []

    async def get_balance(self):
        return 100.0

    async def place_fak_buy(self, **kwargs):
        self.kwargs = kwargs
        return self.result


@pytest.mark.asyncio
async def test_place_market_buy_returns_an_already_filled_order() -> None:
    clob = _BuyClob(FillResult(
        order_id="s1", tokens=2.5, usdc=1.0,
        fill_price=0.40, success=True,
    ))
    om = OrderManager(clob=clob)  # type: ignore[arg-type]

    order = await om.place_market_buy(
        token_id="tok", notional=1.0, limit_price=0.40,
        round_start_ts=1_000, tier_name="T1", tick_size=0.01,
    )

    assert order is not None
    assert order.status == "filled"
    assert order.size == pytest.approx(2.5)
    assert order.placed_size == pytest.approx(2.5)
    assert order.size_matched == pytest.approx(2.5)
    assert clob.kwargs is not None
    assert clob.kwargs["amount"] == pytest.approx(1.0)
    assert clob.kwargs["price"] == pytest.approx(0.40)


@pytest.mark.asyncio
async def test_place_market_buy_respects_the_balance_precheck() -> None:
    clob = _BuyClob(FillResult(
        order_id="s1", tokens=2.5, usdc=1.0,
        fill_price=0.40, success=True,
    ))
    clob.get_balance = lambda: _zero()  # type: ignore[assignment]
    om = OrderManager(clob=clob)  # type: ignore[arg-type]

    order = await om.place_market_buy(
        token_id="tok", notional=1.0, limit_price=0.40,
        round_start_ts=1_000, tier_name="T1", tick_size=0.01,
    )

    assert order is None
    assert om.last_buy_reject_reason == "insufficient_balance"


async def _zero() -> float:
    return 0.0


# ---------------------------------------------------------------------------
# Router wiring: bind tier 1 + feed the fill, no TierWatcher
# ---------------------------------------------------------------------------


class _RouterEngine:
    def __init__(self) -> None:
        self._round = SimpleNamespace(tick_size=0.01, position=Position())
        self.bound: list[dict] = []
        self.fills: list[tuple] = []
        self.tp_recovered = False
        self.tp_error: str | None = None

    def bind_server_order_id(self, **kwargs):  # noqa: ANN003
        self.bound.append(kwargs)

    def on_order_filled(self, token_id, server_id, price, size):
        self.fills.append((token_id, server_id, price, size))

    def notify_tp_recovered(self) -> None:
        self.tp_recovered = True

    def notify_tp_post_error(self, reason: str) -> None:
        self.tp_error = reason


class _RouterOM:
    def __init__(self) -> None:
        self.kwargs: dict | None = None

    async def place_market_buy(self, **kwargs):  # noqa: ANN003
        self.kwargs = kwargs
        return TrackedOrder(
            client_order_id="c1",
            server_order_id="s1",
            round_start_ts=1_000,
            side="BUY",
            token_id="tok",
            tier_name="T1",
            price=0.40,
            size=2.5,
            placed_size=2.5,
            size_matched=2.5,
            lifetime_s=0,
            expires_at=0.0,
            placed_at=time.time(),
            status="filled",
        )


class _SettlePosition:
    def __init__(self, age_s: float) -> None:
        self.total_qty = 2.8571
        self.cleared = False
        self.tiers = [SimpleNamespace(last_fill_ts=time.time() - age_s)]

    def realize_position_closed(self, **kwargs):  # noqa: ANN003
        self.cleared = True


class _ZeroBalanceClob:
    def __init__(self) -> None:
        self.calls = 0

    async def get_token_balance(self, token_id):  # noqa: ANN001
        self.calls += 1
        return 0.0


def test_settle_grace_keeps_a_just_bought_position() -> None:
    """The CLOB balance lags a fresh buy; a zero read must not wipe it.

    Production 2026-10-31 14:00 UTC: taker T1 bought 2.857 shares, the
    immediate taker TP read balance 0, and the reconcile cleared a real
    position from the engine.
    """
    from src.main import _maybe_realize_closed

    pos = _SettlePosition(age_s=1.0)
    clob = _ZeroBalanceClob()

    asyncio.run(_maybe_realize_closed("tok", clob, pos))

    assert pos.cleared is False
    assert clob.calls == 0, "the grace period must skip the balance read"


def test_drained_position_is_cleared_after_the_grace() -> None:
    from src.main import _maybe_realize_closed

    pos = _SettlePosition(age_s=60.0)
    clob = _ZeroBalanceClob()

    asyncio.run(_maybe_realize_closed("tok", clob, pos))

    assert pos.cleared is True
    assert clob.calls == 2, "two consecutive zero reads confirm a drain"


@pytest.mark.asyncio
async def test_router_t1_taker_binds_and_feeds_the_fill() -> None:
    eng = _RouterEngine()
    om = _RouterOM()
    state = SimpleNamespace(
        engine=eng,
        order_manager=om,
        clob=SimpleNamespace(fill_callback=None),
        round_start_ts=1_000,
        killswitch=None,
        _bg_tasks=set(),
    )
    router = ActionRouter(state)  # type: ignore[arg-type]

    await router(ActionOpenT1(side="UP", token="tok", price=0.40, notional=1.0))

    assert om.kwargs is not None
    assert om.kwargs["notional"] == pytest.approx(1.0)
    assert om.kwargs["limit_price"] == pytest.approx(0.40)
    assert eng.bound and eng.bound[0]["tier"] == 1
    assert eng.bound[0]["placed_size"] == pytest.approx(2.5)
    assert eng.fills == [("tok", "s1", 0.40, 2.5)]
    assert router._watchers == {}, "taker T1 must not spawn a TierWatcher"


class _NoFillOM:
    last_buy_reject_reason = "FAK_no_fills"

    async def place_market_buy(self, **kwargs):  # noqa: ANN003
        return None


@pytest.mark.asyncio
async def test_router_skips_the_window_after_a_fak_buy_no_fill() -> None:
    eng = _RouterEngine()
    pos = eng._round.position
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    eng._round.skip_reason = None
    state = SimpleNamespace(
        engine=eng,
        order_manager=_NoFillOM(),
        clob=SimpleNamespace(fill_callback=None),
        round_start_ts=1_000,
        killswitch=None,
        _bg_tasks=set(),
    )
    router = ActionRouter(state)  # type: ignore[arg-type]

    await router(ActionOpenT1(
        side="UP", token="tok", price=0.40, notional=1.0
    ))

    assert eng._round.skip_reason == "t1_fak_no_fill"
    assert pos.entry_token is None
    assert pos.entry_side is None


# ---------------------------------------------------------------------------
# Compact-position TP: taker sell instead of a GTD
# ---------------------------------------------------------------------------


class _MarketSellClob:
    def __init__(self, result: FillResult, balance: float = 2.5) -> None:
        self.result = result
        self.balance = balance
        self.kwargs: dict | None = None

    async def get_open_orders(self, *args, **kwargs):  # noqa: ANN002,ANN003
        return []

    async def get_balance(self):
        return 100.0

    async def get_token_balance(self, token_id):  # noqa: ANN001
        return self.balance

    async def place_fak_sell_marketable(self, **kwargs):
        self.kwargs = kwargs
        return self.result


@pytest.mark.asyncio
async def test_place_market_sell_returns_a_filled_sell_order() -> None:
    clob = _MarketSellClob(FillResult(
        order_id="s2", tokens=2.5, usdc=1.225,
        fill_price=0.49, success=True,
    ))
    om = OrderManager(clob=clob)  # type: ignore[arg-type]

    order = await om.place_market_sell(
        token_id="tok", size=2.5, reference_price=0.50,
        round_start_ts=1_000, tier_name="TP", tick_size=0.01,
    )

    assert order is not None
    assert order.side == "SELL"
    assert order.status == "filled"
    assert order.size_matched == pytest.approx(2.5)
    assert clob.kwargs is not None
    assert clob.kwargs["reference_price"] == pytest.approx(0.50)


class _RouterSellOM:
    def __init__(self) -> None:
        self.kwargs: dict | None = None

    async def place_market_sell(self, **kwargs):  # noqa: ANN003
        self.kwargs = kwargs
        return TrackedOrder(
            client_order_id="c2",
            server_order_id="s2",
            round_start_ts=1_000,
            side="SELL",
            token_id="tok",
            tier_name="TP",
            price=0.49,
            size=2.5,
            placed_size=2.5,
            size_matched=2.5,
            lifetime_s=0,
            expires_at=0.0,
            placed_at=time.time(),
            status="filled",
        )


@pytest.mark.asyncio
async def test_router_compact_tp_uses_a_taker_sell_and_closes_the_position() -> None:
    eng = _RouterEngine()
    pos = eng._round.position
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos._total_qty = 2.5
    pos._total_cost = 1.0
    pos._avg_cost = 0.40
    pos.tier1.size_filled = 2.5
    pos.tier1.cost_filled = 1.0
    pos.tier1.filled = True
    om = _RouterSellOM()
    state = SimpleNamespace(
        engine=eng,
        order_manager=om,
        clob=SimpleNamespace(fill_callback=None),
        round_start_ts=1_000,
        killswitch=None,
        _bg_tasks=set(),
    )
    router = ActionRouter(state)  # type: ignore[arg-type]

    await router(ActionTakeProfit(
        token="tok", price=0.50, qty=2.5, taker=True
    ))
    for _ in range(5):
        pending = [t for t in state._bg_tasks if not t.done()]
        if not pending:
            break
        await asyncio.gather(*pending)

    assert om.kwargs is not None
    assert om.kwargs["reference_price"] == pytest.approx(0.50)
    assert pos.total_qty == 0.0
    assert pos._tp_inflight is False
    assert pos._position_closed is True
    assert pos.entry_token == "tok", "TP exit must block T1 re-entry"
    assert eng.tp_recovered is True
