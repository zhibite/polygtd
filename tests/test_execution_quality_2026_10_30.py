"""Tests for the order-execution improvements (2026-10-30).

Covers:
  * SELL prices round UP to the tick (TP target 0.2698 must not post 0.26);
  * the balance pre-check refuses doomed BUYs without hitting the CLOB and
    fails open when the balance query is unavailable;
  * the fills table stores the MAKER/TAKER role and the DB migrates older
    schemas that lack the column;
  * `execution_quality` derives maker ratio, price improvement, cancel
    rate and average time-to-first-fill.
"""
from __future__ import annotations

import sqlite3

import pytest

from src.execution.clob_client import CLOBClient, FillDeduper, FillResult
from src.execution.order_manager import OrderManager
from src.persistence.db import DB, init_db
from src.web import status as status_mod


# ---------------------------------------------------------------------------
# 1) SELL price rounding
# ---------------------------------------------------------------------------


class _OrderSdk:
    def __init__(self) -> None:
        self.last_args = None

    def create_and_post_order(self, **kwargs):  # noqa: ANN003
        self.last_args = kwargs.get("order_args")
        return {"orderID": "0xsell"}


@pytest.mark.asyncio
async def test_gtd_sell_floors_price_to_tick_for_fill_priority() -> None:
    """0.2698 must post at 0.26 (floor), not 0.27.

    Fill-priority policy (2026-10-30): the limit sits at the nearest tick
    AT OR BELOW the target so the order crosses and fills at the
    prevailing bid once the market reaches it. Posting the ceiling can
    miss the exit by one full tick.
    """
    sdk = _OrderSdk()
    client = CLOBClient.__new__(CLOBClient)
    client._client = sdk
    client.register_pending = None
    client.fill_callback = None
    client._deduper = FillDeduper()

    result = await client.place_gtd_sell(
        token_id="tok", price=0.2698, size=5.0, lifetime_s=300, tick_size=0.01,
    )
    assert result.success is True
    assert sdk.last_args is not None
    assert float(sdk.last_args.price) == pytest.approx(0.26)


class _SellClob:
    def __init__(self, token_balance=None) -> None:
        self.kwargs: dict | None = None
        self.token_balance = token_balance

    async def get_open_orders(self, *args, **kwargs):  # noqa: ANN002,ANN003
        return []

    async def get_token_balance(self, token_id):  # noqa: ANN001
        return self.token_balance

    async def place_gtd_sell(self, **kwargs):  # noqa: ANN003
        self.kwargs = kwargs
        return FillResult(
            order_id="s1", tokens=0.0, usdc=0.0,
            fill_price=kwargs["price"], success=True, expiration_ts=1_000_300,
        )


@pytest.mark.asyncio
async def test_order_manager_tracks_floored_sell_price() -> None:
    clob = _SellClob()
    om = OrderManager(clob=clob)  # type: ignore[arg-type]

    order = await om.place_gtd_sell_at_price(
        token_id="tok", size=10.0, price=0.2698,
        round_start_ts=1_000, tier_name="TP", tick_size=0.01,
    )

    assert order is not None
    assert clob.kwargs is not None
    assert clob.kwargs["price"] == pytest.approx(0.26)
    assert order.price == pytest.approx(0.26)


@pytest.mark.asyncio
async def test_sell_size_is_capped_to_wallet_token_balance() -> None:
    """A drifted position must not make the TP SELL unfillable.

    Production 2026-10-30: position 20.38 shares while the wallet held
    14.36 — every TP POST was rejected by the CLOB and the bot could not
    exit until the operator sold manually.
    """
    clob = _SellClob(token_balance=14.36)
    om = OrderManager(clob=clob)  # type: ignore[arg-type]

    order = await om.place_gtd_sell_at_price(
        token_id="tok", size=20.38, price=0.27,
        round_start_ts=1_000, tier_name="TP", tick_size=0.01,
    )

    assert order is not None
    assert clob.kwargs is not None
    assert clob.kwargs["size"] == pytest.approx(14.36)
    assert order.size == pytest.approx(14.36)


@pytest.mark.asyncio
async def test_sell_skipped_when_wallet_has_nothing_sellable() -> None:
    clob = _SellClob(token_balance=2.0)  # below the 5-share CLOB minimum
    om = OrderManager(clob=clob)  # type: ignore[arg-type]

    order = await om.place_gtd_sell_at_price(
        token_id="tok", size=20.38, price=0.27,
        round_start_ts=1_000, tier_name="TP", tick_size=0.01,
    )

    assert order is None
    assert clob.kwargs is None


@pytest.mark.asyncio
async def test_sell_fails_open_when_token_balance_unknown() -> None:
    clob = _SellClob(token_balance=None)
    om = OrderManager(clob=clob)  # type: ignore[arg-type]

    order = await om.place_gtd_sell_at_price(
        token_id="tok", size=20.38, price=0.27,
        round_start_ts=1_000, tier_name="TP", tick_size=0.01,
    )

    assert order is not None
    assert clob.kwargs is not None
    assert clob.kwargs["size"] == pytest.approx(20.38)


# ---------------------------------------------------------------------------
# 2) Balance pre-check
# ---------------------------------------------------------------------------


class _PoorClob:
    def __init__(self, balance=0.0) -> None:
        self.balance = balance
        self.balance_calls = 0
        self.posted = 0

    async def get_open_orders(self, *args, **kwargs):  # noqa: ANN002,ANN003
        return []

    async def get_balance(self):
        self.balance_calls += 1
        return self.balance

    async def place_gtd_buy(self, **kwargs):  # noqa: ANN003
        self.posted += 1
        return FillResult(
            order_id="s1", tokens=kwargs["size"], usdc=0.0,
            fill_price=kwargs["price"], success=True,
            expiration_ts=1_000_300,
        )


@pytest.mark.asyncio
async def test_empty_wallet_skips_buy_without_posting() -> None:
    clob = _PoorClob(balance=0.0)
    om = OrderManager(clob=clob)  # type: ignore[arg-type]

    order = await om.place_buy_by_strategy_price(
        token_id="tok", strategy_price=0.40, notional=2.0,
        round_start_ts=1_000, tier_name="T1", tick_size=0.01,
    )

    assert order is None
    assert clob.posted == 0, "a zero-balance BUY must never reach the CLOB"
    assert om.last_buy_reject_reason == "insufficient_balance"
    # The balance is cached: a second attempt within the TTL reuses it.
    assert clob.balance_calls == 1
    await om.place_buy_by_strategy_price(
        token_id="tok", strategy_price=0.40, notional=2.0,
        round_start_ts=1_000, tier_name="T1", tick_size=0.01,
    )
    assert clob.balance_calls == 1


@pytest.mark.asyncio
async def test_balance_query_failure_fails_open() -> None:
    clob = _PoorClob(balance=None)
    om = OrderManager(clob=clob)  # type: ignore[arg-type]

    order = await om.place_buy_by_strategy_price(
        token_id="tok", strategy_price=0.40, notional=2.0,
        round_start_ts=1_000, tier_name="T1", tick_size=0.01,
    )

    assert order is not None, "an unknown balance must not block trading"
    assert clob.posted == 1
    assert om.last_buy_reject_reason == ""


@pytest.mark.asyncio
async def test_sufficient_balance_posts_normally() -> None:
    clob = _PoorClob(balance=10.0)
    om = OrderManager(clob=clob)  # type: ignore[arg-type]

    order = await om.place_buy_by_strategy_price(
        token_id="tok", strategy_price=0.40, notional=2.0,
        round_start_ts=1_000, tier_name="T1", tick_size=0.01,
    )

    assert order is not None
    assert clob.posted == 1


# ---------------------------------------------------------------------------
# 3) fills.trader_side persistence + legacy-schema migration
# ---------------------------------------------------------------------------


def test_legacy_fills_table_gets_new_columns(tmp_path) -> None:
    path = tmp_path / "legacy.db"
    raw = sqlite3.connect(path)
    raw.execute(
        "CREATE TABLE fills (fill_id TEXT PRIMARY KEY, order_id TEXT, "
        "round_start_ts INTEGER, token_id TEXT, side TEXT, price REAL, "
        "size REAL, ts_ms INTEGER, confirmed INTEGER DEFAULT 0, "
        "recorded_at TEXT)"
    )
    raw.commit()
    raw.close()

    conn = init_db(path)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(fills)")}
    assert "trader_side" in cols
    assert "fee" in cols
    conn.close()


# ---------------------------------------------------------------------------
# 4) execution_quality metrics
# ---------------------------------------------------------------------------


@pytest.fixture()
def conn(tmp_path):
    c = init_db(tmp_path / "test.db")
    yield c
    c.close()


def test_execution_quality_metrics(conn) -> None:
    db = DB(conn)
    db.upsert_round(round_start_ts=1_000, slug="r1")
    # Order c1: BUY limit 0.40, filled 0.38 (taker improvement) then sold
    # at 0.50 (taker upside vs the 0.40 limit recorded on the order).
    db.insert_order(
        client_order_id="c1", server_order_id="s1", round_start_ts=1_000,
        side="BUY", token_id="tok", tier_name="T1", price=0.40, size=10.0,
        lifetime_s=300, expires_at=1_300, placed_at=1_000,
    )
    # Order c2: never filled, cancelled -> counts toward the cancel rate.
    db.insert_order(
        client_order_id="c2", server_order_id="s2", round_start_ts=1_000,
        side="BUY", token_id="tok", tier_name="T2", price=0.30, size=5.0,
        lifetime_s=300, expires_at=1_300, placed_at=1_000,
    )
    db.update_order_status("c2", "cancelled")

    db.insert_fill(
        "f1", "c1", 1_000, "tok", "BUY", 0.38, 10.0,
        ts_ms=1_000_500, trader_side="MAKER",
    )
    db.insert_fill(
        "f2", "c1", 1_000, "tok", "SELL", 0.50, 10.0,
        ts_ms=1_002_000, trader_side="TAKER",
    )

    q = status_mod.execution_quality(conn)
    assert q["maker_fills"] == 1
    assert q["taker_fills"] == 1
    assert q["unknown_fills"] == 0
    assert q["maker_ratio"] == pytest.approx(0.5)
    # BUY: (0.40 - 0.38) / 0.40 = +500 bps
    assert q["buy_improvement_bps"] == pytest.approx(500.0)
    # SELL: (0.50 - 0.40) / 0.40 = +2500 bps
    assert q["sell_improvement_bps"] == pytest.approx(2500.0)
    assert q["total_orders"] == 2
    assert q["cancelled_orders"] == 1
    assert q["cancel_ratio"] == pytest.approx(0.5)
    # First fill 0.5 s after placement (c2 never filled, excluded).
    assert q["avg_time_to_fill_s"] == pytest.approx(0.5)

    fills = status_mod.recent_fills(conn)
    by_id = {f["fill_id"]: f for f in fills}
    assert by_id["f1"]["trader_side"] == "MAKER"
    assert by_id["f1"]["limit_price"] == pytest.approx(0.40)
    assert by_id["f1"]["improvement_bps"] == pytest.approx(500.0)
    assert by_id["f2"]["improvement_bps"] == pytest.approx(2500.0)
