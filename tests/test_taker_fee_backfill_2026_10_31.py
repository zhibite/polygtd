"""Taker fees are completed even when the first feed books the fill blind.

Production 2026-10-31 (09:30 round): all six fills were takers according to
Polymarket's trade history, yet the fills table showed no fee. The first
feed to observe a fill is usually the WS ORDER-UPDATE: it carries the
cumulative matched size but neither the real execution price (only the
order's LIMIT) nor the maker/taker role, so `apply_fill_event` books the
row at the limit with fee=0. The later authoritative observation — WS
TRADE, TierWatcher poll (role derived from the order's associate_trades) or
the cancel-time reconcile — hits the SAME cumulative and previously
short-circuited, leaving the fee at 0 forever.

`OrderManager.reconcile_fill_terms` now backfills that row's price / role /
fee, and `apply_fill_event` calls it whenever a repeated observation
carries better terms.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.execution.clob_client import CLOBClient, FillDeduper
from src.execution.order_manager import OrderManager, TrackedOrder
from src.execution.tier_watcher import TierOutcome, TierWatcher, WatcherResult
from src.main import _make_maker_fill_callback
from src.persistence.db import DB, init_db


def _db(tmp_path) -> DB:
    return DB(init_db(tmp_path / "fills.db"))


def _om(db: DB) -> OrderManager:
    om = OrderManager(clob=object(), db=db)  # type: ignore[arg-type]
    om._s = SimpleNamespace(TAKER_FEE_RATE=0.07)
    return om


def _track(order_id: str = "0xorder", *, limit: float = 0.40,
           client_id: str = "1000-T1-abc") -> TrackedOrder:
    return TrackedOrder(
        client_order_id=client_id,
        server_order_id=order_id,
        round_start_ts=1_000,
        side="BUY",
        token_id="tok",
        tier_name="T1",
        price=limit,
        size=10.0,
        lifetime_s=120,
        expires_at=1_000_120.0,
        placed_at=1_000_000.0,
    )


def _register(om: OrderManager, order: TrackedOrder) -> None:
    om._orders[order.client_order_id] = order
    om._by_server[order.server_order_id] = order.client_order_id


def _fill_id(order: TrackedOrder, cumulative: float) -> str:
    return f"{order.client_order_id}:{cumulative:.6f}"


def _taker_fee(size: float, price: float, rate: float = 0.07) -> float:
    return max(round(size * rate * price * (1.0 - price), 5), 0.00001)


def test_update_first_fill_is_backfilled_with_the_taker_fee(tmp_path) -> None:
    """WS UPDATE books at the LIMIT with no role; the TRADE completes it."""
    db = _db(tmp_path)
    om = _om(db)
    order = _track()
    _register(om, order)

    # 1) WS ORDER-UPDATE first: size only, no price, no role.
    om.apply_fill_event("0xorder", size_matched=10.0)

    price, size, fee, role = db.get_fill_terms(_fill_id(order, 10.0))
    assert price == pytest.approx(0.40)   # the LIMIT, not the fill
    assert size == pytest.approx(10.0)
    assert fee == 0.0
    assert role is None

    # 2) Authoritative observation for the SAME cumulative: real taker
    #    execution at 0.39. Must rewrite the row instead of being dropped.
    assert om.reconcile_fill_terms(
        "0xorder", cumulative=10.0, fill_price=0.39, trader_side="TAKER"
    ) is True

    price, size, fee, role = db.get_fill_terms(_fill_id(order, 10.0))
    assert price == pytest.approx(0.39)
    assert role == "TAKER"
    assert fee == pytest.approx(_taker_fee(10.0, 0.39))


def test_apply_fill_event_reconciles_a_repeated_cumulative(tmp_path) -> None:
    """The watcher / FAK paths call apply_fill_event directly."""
    db = _db(tmp_path)
    om = _om(db)
    order = _track()
    _register(om, order)

    om.apply_fill_event("0xorder", size_matched=10.0)
    om.apply_fill_event(
        "0xorder", size_matched=10.0, fill_price=0.39, trader_side="TAKER"
    )

    _, _, fee, role = db.get_fill_terms(_fill_id(order, 10.0))
    assert role == "TAKER"
    assert fee == pytest.approx(_taker_fee(10.0, 0.39))


def test_backfill_is_idempotent_and_never_double_charges(tmp_path) -> None:
    db = _db(tmp_path)
    om = _om(db)
    order = _track()
    _register(om, order)

    om.apply_fill_event("0xorder", size_matched=10.0)
    for _ in range(3):
        om.reconcile_fill_terms(
            "0xorder", cumulative=10.0, fill_price=0.39, trader_side="TAKER"
        )

    _, _, fee, _ = db.get_fill_terms(_fill_id(order, 10.0))
    assert fee == pytest.approx(_taker_fee(10.0, 0.39))


def test_maker_backfill_keeps_the_fee_at_zero(tmp_path) -> None:
    db = _db(tmp_path)
    om = _om(db)
    order = _track()
    _register(om, order)

    om.apply_fill_event("0xorder", size_matched=10.0)
    om.reconcile_fill_terms(
        "0xorder", cumulative=10.0, fill_price=0.40, trader_side="MAKER"
    )

    price, _, fee, role = db.get_fill_terms(_fill_id(order, 10.0))
    assert price == pytest.approx(0.40)
    assert role == "MAKER"
    assert fee == 0.0


def test_role_only_backfill_keeps_the_stored_price(tmp_path) -> None:
    """A role learned without an execution price must not wipe the price."""
    db = _db(tmp_path)
    om = _om(db)
    order = _track()
    _register(om, order)

    om.apply_fill_event("0xorder", size_matched=10.0, fill_price=0.385)
    om.reconcile_fill_terms("0xorder", cumulative=10.0, trader_side="TAKER")

    price, _, fee, role = db.get_fill_terms(_fill_id(order, 10.0))
    assert price == pytest.approx(0.385)
    assert role == "TAKER"
    assert fee == pytest.approx(_taker_fee(10.0, 0.385))


def test_unknown_cumulative_is_a_noop(tmp_path) -> None:
    db = _db(tmp_path)
    om = _om(db)
    order = _track()
    _register(om, order)

    om.apply_fill_event("0xorder", size_matched=10.0)
    assert om.reconcile_fill_terms(
        "0xorder", cumulative=99.0, fill_price=0.39, trader_side="TAKER"
    ) is False


def test_fak_sell_books_the_taker_role_without_the_row_existing_yet(
    tmp_path,
) -> None:
    """FAK fills fire from inside place_fak_sell, before the order exists.

    The fill is queued on the sentinel and replayed when the TrackedOrder
    lands — the role has to survive that round-trip or every FAK exit is
    booked fee-free.
    """
    db = _db(tmp_path)
    om = _om(db)
    om.register_pending_server_id("0xfak")

    # Callback fires before place_sell() inserts the TrackedOrder.
    om.apply_fill_event(
        "0xfak", size_matched=5.0, fill_price=0.49, trader_side="TAKER"
    )
    assert om._pending_fills["0xfak"] == [(0.49, 5.0, "TAKER")]

    order = _track(order_id="0xfak", limit=0.48, client_id="1000-TP-deadbeef")
    om._orders[order.client_order_id] = order
    om.resolve_pending_server_id(order.client_order_id, "0xfak")

    price, size, fee, role = db.get_fill_terms(_fill_id(order, 5.0))
    assert price == pytest.approx(0.49)
    assert size == pytest.approx(5.0)
    assert role == "TAKER"
    assert fee == pytest.approx(_taker_fee(5.0, 0.49))


class _FakSdk:
    def __init__(self) -> None:
        self.posted = 0

    def create_and_post_market_order(self, args):  # noqa: ANN001,ANN201
        self.posted += 1
        return {
            "orderID": "0xfak",
            "fills": [{"size": "5", "price": "0.49"}],
        }


@pytest.mark.asyncio
async def test_place_fak_sell_forwards_taker_to_the_fill_callback() -> None:
    sdk = _FakSdk()
    client = CLOBClient.__new__(CLOBClient)
    client._client = sdk
    client.register_pending = None
    client._deduper = FillDeduper()
    seen: list[tuple] = []
    client.fill_callback = (
        lambda token, oid, px, delta, total, role=None: seen.append(
            (token, oid, px, delta, total, role)
        )
    )

    result = await client.place_fak_sell(
        token_id="tok", size=5.0, price=0.48, tick_size=0.01
    )

    assert result.success is True
    assert sdk.posted == 1
    assert len(seen) == 1
    assert seen[0][5] == "TAKER"
    assert seen[0][2] == pytest.approx(0.49)
    assert seen[0][3] == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# WS ORDER-UPDATE vs WS TRADE ordering (review 2026-10-31)
# ---------------------------------------------------------------------------


class _Engine:
    def __init__(self) -> None:
        self.fills: list[tuple] = []

    def on_order_filled(
        self, asset_id, order_id, fill_price, delta  # noqa: ANN001
    ) -> None:
        self.fills.append((asset_id, order_id, fill_price, delta))


def _trade(trade_id: str, role: str = "TAKER", price: float = 0.39):
    return SimpleNamespace(
        asset_id="tok",
        price=price,
        size=4.0,
        status="MATCHED",
        trade_id=trade_id,
        trader_side=role,
    )


def test_update_first_then_trade_neither_double_books_nor_loses_the_fee(
    tmp_path,
) -> None:
    """Both channels report the same fill; only one may advance the order.

    Regression for the ordering hazard: UPDATE mirrors 4 shares, then the
    TRADE for those same 4 was ADDED to the mirrored cumulative (8), which
    double-booked the fill, promoted the order to `filled` and made the
    genuine second fill disappear under the expected-size cap.
    """
    db = _db(tmp_path)
    om = _om(db)
    order = _track(order_id="0xsrv", limit=0.40, client_id="1000-T1-abc")
    order.size = 8.0
    order.placed_size = 8.0
    _register(om, order)
    eng = _Engine()
    cb = _make_maker_fill_callback(
        SimpleNamespace(order_manager=om, engine=eng)
    )

    # WS ORDER-UPDATE arrives first (mirror only: limit price, no role).
    om.apply_fill_event("0xsrv", size_matched=4.0)

    # TRADE for the SAME 4 shares.
    cb(_trade("t1"), candidates=[("0xsrv", "tok", 4.0, 0.39, 0.0)])

    assert order.size_matched == pytest.approx(4.0), (
        "the UPDATE-mirrored fill was added a second time — phantom size"
    )
    assert eng.fills == [], "engine must not be fed an already-booked delta"

    price, size, fee, role = db.get_fill_terms(_fill_id(order, 4.0))
    assert price == pytest.approx(0.39), "the TRADE price must backfill"
    assert role == "TAKER"
    assert fee == pytest.approx(_taker_fee(4.0, 0.39))

    # The genuine second trade must still advance the order.
    cb(_trade("t2", price=0.38), candidates=[("0xsrv", "tok", 4.0, 0.38, 0.0)])

    assert order.size_matched == pytest.approx(8.0)
    assert eng.fills == [("tok", "0xsrv", 0.38, 4.0)]
    _, _, fee2, role2 = db.get_fill_terms(_fill_id(order, 8.0))
    assert role2 == "TAKER"
    assert fee2 == pytest.approx(_taker_fee(4.0, 0.38))


def test_duplicate_trade_without_a_row_price_cannot_clobber_the_real_price(
    tmp_path,
) -> None:
    """A collapsed maker row (price 0) must not fall back to the LIMIT.

    `_make_maker_fill_callback` falls back to `tracked.price` (the LIMIT)
    while logging; that fallback is not an observed execution price and
    must never be handed to the fill-terms backfill.
    """
    db = _db(tmp_path)
    om = _om(db)
    order = _track(order_id="0xsrv", limit=0.40, client_id="1000-T1-abc")
    order.size = 8.0
    order.placed_size = 8.0
    _register(om, order)
    eng = _Engine()
    cb = _make_maker_fill_callback(
        SimpleNamespace(order_manager=om, engine=eng)
    )

    om.apply_fill_event(
        "0xsrv", size_matched=4.0, fill_price=0.37, trader_side="TAKER"
    )

    # REST-fallback candidate shape: chosen[2] is CUMULATIVE, chosen[4] is
    # the per-trade delta; the maker row carries no price.
    duplicate = SimpleNamespace(
        asset_id="tok", price=0.42, size=4.0, status="MINED",
        trade_id="t-dup", trader_side="TAKER",
    )
    cb(duplicate, candidates=[("0xsrv", "tok", 4.0, 0.0, 1.0)])

    price, _, fee, role = db.get_fill_terms(_fill_id(order, 4.0))
    assert price == pytest.approx(0.37), "limit price clobbered the fill"
    assert role == "TAKER"
    assert fee == pytest.approx(_taker_fee(4.0, 0.37))
    assert order.size_matched == pytest.approx(4.0)


def test_reconcile_never_overrides_a_known_role(tmp_path) -> None:
    """Order-wide `is_taker` is sticky; per-trade MAKER must win.

    `get_order_fill_state` derives is_taker as "ANY associated trade was a
    taker", so for a mixed maker/taker order a later reconcile must not
    re-label an already-recorded MAKER row and charge it a taker fee.
    """
    db = _db(tmp_path)
    om = _om(db)
    order = _track()
    _register(om, order)

    om.apply_fill_event(
        "0xorder", size_matched=10.0, fill_price=0.40, trader_side="MAKER"
    )
    om.reconcile_fill_terms(
        "0xorder", cumulative=10.0, fill_price=0.39, trader_side="TAKER"
    )

    price, _, fee, role = db.get_fill_terms(_fill_id(order, 10.0))
    assert role == "MAKER"
    assert fee == 0.0
    # A real observed price may still be refined.
    assert price == pytest.approx(0.39)


@pytest.mark.asyncio
async def test_watcher_reports_zero_instead_of_the_limit_for_unknown_avg() -> None:
    """The watcher must not pass the strategy LIMIT off as a fill average.

    Pretending the limit was observed used to (a) make the router treat it
    as an authoritative execution price when backfilling fees and (b) make
    `_ensure_avg_price_for_ws_fast_fill` a no-op (`> 0` looked real).
    """
    watcher = TierWatcher.__new__(TierWatcher)
    watcher._server_order_id = "0xsrv"
    watcher._token_id = "tok"
    watcher._side = "BUY"
    watcher._strategy_price = 0.40
    watcher._expected_size = 8.0
    watcher._avg_price_running = 0.0
    watcher._is_taker = False
    watcher._last_observed_size_matched = 0.0
    watcher._last_size_matched = 0.0
    watcher._partial_filling_initialised = False
    watcher._last_pushed_size_matched = 0.0

    states = [
        {"size_matched": 4.0, "avg_price": 0.0, "is_taker": False},
        {"size_matched": 8.0, "avg_price": 0.0, "is_taker": True},
    ]

    async def read_state():  # noqa: ANN202
        return states.pop(0)

    pushed: list[tuple] = []

    async def on_partial(cumulative, price, hint):  # noqa: ANN001
        pushed.append((cumulative, price, hint))

    watcher._read_fill_state = read_state  # type: ignore[assignment]
    watcher._on_partial_fill = on_partial  # type: ignore[assignment]

    await watcher._maybe_push_partial_fill(0.0)  # prime at 4
    assert watcher._avg_price_running == 0.0, (
        "priming substituted the strategy price for an unobserved average"
    )
    assert pushed == []

    await watcher._maybe_push_partial_fill(0.0)  # increment 4 → 8
    assert len(pushed) == 1
    cumulative, price, hint = pushed[0]
    assert cumulative == pytest.approx(8.0)
    assert price == 0.0, "unknown avg must be 0, not the 0.40 limit"
    assert hint.avg_fill_price == 0.0
    assert hint.is_taker is True


def test_watcher_learns_the_tri_state_role_from_fill_state() -> None:
    """REST can positively identify MAKER too, not just "not taker".

    A maker fill first booked by the watcher (WS TRADE missed) used to
    stay role-less forever, so the dashboard counted it as unknown and a
    taker fill in that position would have kept fee=0.
    """
    watcher = TierWatcher.__new__(TierWatcher)
    watcher._is_taker = False
    watcher._trader_side = None

    watcher._learn_trade_role({"trader_side": "MAKER"})
    assert watcher._trader_side == "MAKER"
    assert watcher._is_taker is False

    watcher._learn_trade_role({"trader_side": "TAKER"})
    assert watcher._trader_side == "TAKER"
    assert watcher._is_taker is True

    # Older fill-state shape: is_taker=True still maps to TAKER.
    fresh = TierWatcher.__new__(TierWatcher)
    fresh._is_taker = False
    fresh._trader_side = None
    fresh._learn_trade_role({"is_taker": True})
    assert fresh._trader_side == "TAKER"
    assert fresh._is_taker is True
