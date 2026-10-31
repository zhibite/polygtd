"""Tests for the web dashboard (status builder, history queries, wiring).

Covers:
  * `build_status` renders a safe snapshot with no engine, and a full
    position/tier payload when the engine is live.
  * `rounds_with_pnl` / `compute_stats` derive realised PnL from fills.
  * `OrderManager` mirrors orders and fills into the DB when `db=` is set
    (and stays a no-op when it is omitted).
  * FastAPI routes serve the pages/API.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.execution.order_manager import OrderManager, TrackedOrder
from src.market.market_ws import PriceCache
from src.persistence.db import DB, init_db
from src.web import status as status_mod


# ---------------------------------------------------------------------------
# Live status builder
# ---------------------------------------------------------------------------


class _FakeWM:
    def __init__(self, mkt) -> None:
        self._ctx = SimpleNamespace(market=mkt)

    def get(self, asset: str):  # noqa: ARG002
        return self._ctx

    def window_period_s(self) -> int:
        return 900

    def seconds_until_next(self) -> float:
        return 500.0


class _FakePosition:
    entry_side = "DOWN"
    entry_token = "DOWNTOKEN"
    total_qty = 5.0
    total_cost = 1.75
    avg_cost = 0.35
    tiers = [
        SimpleNamespace(
            tier=1, notional=2.0, placed=True, watching=False, is_complete=True,
            size_filled=5.0, size_expected=5.0, avg_fill_price=0.35,
            server_order_id="0xabc",
        )
    ]

    def tp_target(self):
        return 0.42


def test_build_status_without_engine() -> None:
    state = SimpleNamespace(
        wm=None,
        engine=None,
        price_cache=PriceCache(),
        binance_cache=SimpleNamespace(last_price=0.0),
        order_manager=None,
    )
    out = status_mod.build_status(state)
    assert out["asset"] == "BTC"
    assert out["gate"]["state"] == "pending"
    assert out["position"] is None
    assert out["orders"] == []
    assert out["window"]["phase"] in ("IN_T1", "PAST_T1", "END")


def test_build_status_with_position() -> None:
    mkt = SimpleNamespace(
        up_token="UPTOKEN", down_token="DOWNTOKEN",
        slug="btc-updown-15m-1", condition_id="0xdead",
        window_start=None, window_end=None,
    )
    pc = PriceCache()
    pc.midpoint["UPTOKEN"] = 0.60
    pc.midpoint["DOWNTOKEN"] = 0.40

    state = SimpleNamespace(
        wm=_FakeWM(mkt),
        engine=SimpleNamespace(
            _round=SimpleNamespace(
                round_start_ts=1_000, slug=mkt.slug,
                position=_FakePosition(), skip_reason=None,
            ),
            _gate=None,
        ),
        price_cache=pc,
        binance_cache=SimpleNamespace(last_price=50_000.0),
        order_manager=None,
    )
    out = status_mod.build_status(state)

    assert out["prices"]["down_mid"] == pytest.approx(0.40)
    pos = out["position"]
    assert pos["side"] == "DOWN"
    assert pos["qty"] == pytest.approx(5.0)
    # (0.40 - 0.35) * 5 = 0.25
    assert pos["upnl_usd"] == pytest.approx(0.25)
    assert pos["upnl_pct"] == pytest.approx(0.25 / 1.75)
    assert pos["tp_target"] == pytest.approx(0.42)
    assert pos["tiers"][0]["status"] == "filled"


# ---------------------------------------------------------------------------
# History queries
# ---------------------------------------------------------------------------


@pytest.fixture()
def conn(tmp_path):
    c = init_db(tmp_path / "test.db")
    yield c
    c.close()


def test_rounds_with_pnl_and_stats(conn) -> None:
    db = DB(conn)
    db.upsert_round(round_start_ts=1_000, slug="r1")
    db.insert_order(
        client_order_id="c1", server_order_id="s1", round_start_ts=1_000,
        side="BUY", token_id="tok", tier_name="T1", price=0.40, size=5.0,
        lifetime_s=300, expires_at=1_300, placed_at=1_000,
    )
    db.insert_fill("f1", "c1", 1_000, "tok", "BUY", 0.40, 5.0, 1_000_000)
    db.insert_fill("f2", "c1", 1_000, "tok", "SELL", 0.48, 5.0, 2_000_000)

    rounds = status_mod.rounds_with_pnl(conn)
    assert len(rounds) == 1
    r = rounds[0]
    assert r["closed"] is True
    assert r["buy_cost"] == pytest.approx(2.0)
    assert r["sell_proceeds"] == pytest.approx(2.4)
    assert r["pnl_usd"] == pytest.approx(0.4)
    assert r["pnl_pct"] == pytest.approx(0.2)

    stats = status_mod.compute_stats(rounds)
    assert stats["closed_rounds"] == 1
    assert stats["wins"] == 1
    assert stats["win_rate"] == pytest.approx(1.0)
    assert stats["total_pnl_usd"] == pytest.approx(0.4)

    fills = status_mod.recent_fills(conn)
    assert len(fills) == 2
    assert fills[0]["side"] == "SELL"
    assert fills[0]["tier"] == "T1"


def test_open_round_is_not_counted_as_loss(conn) -> None:
    db = DB(conn)
    db.upsert_round(round_start_ts=2_000, slug="r2")
    db.insert_order(
        client_order_id="c2", server_order_id="s2", round_start_ts=2_000,
        side="BUY", token_id="tok", tier_name="T1", price=0.40, size=5.0,
        lifetime_s=300, expires_at=2_300, placed_at=2_000,
    )
    db.insert_fill("f3", "c2", 2_000, "tok", "BUY", 0.40, 5.0, 3_000_000)

    rounds = status_mod.rounds_with_pnl(conn)
    assert rounds[0]["closed"] is False
    assert rounds[0]["status"] == "open"
    assert rounds[0]["pnl_usd"] is None
    stats = status_mod.compute_stats(rounds)
    assert stats["closed_rounds"] == 0
    assert stats["open_rounds"] == 1
    assert stats["total_pnl_usd"] == 0.0


def test_round_without_activity_is_not_listed(conn) -> None:
    """A window where nothing was ever bought must not appear in history.

    Legacy builds inserted a `rounds` row at every window boundary, before
    any order. With an empty wallet every BUY is rejected by the CLOB, yet
    those windows rendered as "未平仓" open positions (production
    2026-10-30: two phantom rounds with $0 cost and 0 fills).
    """
    db = DB(conn)
    db.upsert_round(round_start_ts=3_000, slug="r3")

    assert status_mod.rounds_with_pnl(conn) == []
    stats = status_mod.compute_stats([])
    assert stats["open_rounds"] == 0
    assert stats["closed_rounds"] == 0


def test_round_with_order_but_no_fill_is_unfilled(conn) -> None:
    """Orders posted but never filled are 'unfilled', not open positions."""
    db = DB(conn)
    db.upsert_round(round_start_ts=4_000, slug="r4")
    db.insert_order(
        client_order_id="c4", server_order_id="s4", round_start_ts=4_000,
        side="BUY", token_id="tok", tier_name="T1", price=0.40, size=5.0,
        lifetime_s=300, expires_at=4_300, placed_at=4_000,
    )
    db.update_order_status("c4", "cancelled")

    rounds = status_mod.rounds_with_pnl(conn)
    assert len(rounds) == 1
    assert rounds[0]["status"] == "unfilled"
    assert rounds[0]["order_count"] == 1
    assert rounds[0]["cancel_count"] == 1
    assert rounds[0]["fill_count"] == 0
    assert rounds[0]["closed"] is False
    stats = status_mod.compute_stats(rounds)
    assert stats["open_rounds"] == 0
    assert stats["unfilled_rounds"] == 1


# ---------------------------------------------------------------------------
# OrderManager persistence
# ---------------------------------------------------------------------------


class _FakeDB:
    def __init__(self) -> None:
        self.rounds: list[dict] = []
        self.orders: list[dict] = []
        self.statuses: list[tuple] = []
        self.fills: list[dict] = []

    def upsert_round(self, **kw) -> None:
        self.rounds.append(kw)

    def insert_order(self, **kw) -> None:
        self.orders.append(kw)

    def update_order_status(self, cid, status, size_matched=0.0) -> None:
        self.statuses.append((cid, status, size_matched))

    def insert_fill(self, **kw) -> None:
        self.fills.append(kw)


def _tracked_order() -> TrackedOrder:
    return TrackedOrder(
        client_order_id="c1",
        server_order_id="s1",
        round_start_ts=1_000,
        side="BUY",
        token_id="tok",
        tier_name="T1",
        price=0.40,
        size=5.0,
        placed_size=5.0,
        lifetime_s=300,
        expires_at=1_300,
        placed_at=1_000,
        status="live",
    )


def test_order_manager_mirrors_order_and_fills() -> None:
    fake = _FakeDB()
    om = OrderManager(clob=object(), db=fake)  # type: ignore[arg-type]
    order = _tracked_order()
    om._orders[order.client_order_id] = order
    om._by_server[order.server_order_id] = order.client_order_id

    om._persist_order(order)
    assert fake.orders and fake.orders[0]["client_order_id"] == "c1"
    assert fake.orders[0]["size"] == 5.0

    om.apply_fill_event("s1", 2.0, 0.40)
    om.apply_fill_event("s1", 5.0, 0.40)
    # Duplicate observation of the same cumulative size must not double-insert.
    om.apply_fill_event("s1", 5.0, 0.40)

    assert [f["size"] for f in fake.fills] == [2.0, 3.0]
    assert fake.fills[0]["order_id"] == "c1"
    assert fake.fills[0]["side"] == "BUY"


def test_round_row_written_lazily_on_first_fill() -> None:
    """Window open and order placement create no history row; a fill does."""
    fake = _FakeDB()
    om = OrderManager(clob=object(), db=fake)  # type: ignore[arg-type]
    om.set_round_meta(
        1_000,
        slug="btc-updown-15m-1000",
        condition_id="0xabc",
        up_token="up",
        down_token="down",
        tick_size=0.01,
    )

    # Registering metadata (window open) must not create a history row.
    assert fake.rounds == []

    order = _tracked_order()
    om._orders[order.client_order_id] = order
    om._by_server[order.server_order_id] = order.client_order_id
    om._persist_order(order)
    # An order alone (e.g. a GTD that never crossed) is not a traded round.
    assert fake.rounds == []
    assert fake.orders and fake.orders[0]["client_order_id"] == "c1"

    om.apply_fill_event("s1", 2.0, 0.40)
    assert len(fake.rounds) == 1
    assert fake.rounds[0]["round_start_ts"] == 1_000
    assert fake.rounds[0]["slug"] == "btc-updown-15m-1000"
    assert fake.rounds[0]["condition_id"] == "0xabc"
    assert fake.rounds[0]["up_token"] == "up"
    assert fake.rounds[0]["down_token"] == "down"
    assert fake.rounds[0]["tick_size"] == 0.01
    assert fake.fills and fake.fills[0]["side"] == "BUY"


def test_order_manager_without_db_is_noop() -> None:
    om = OrderManager(clob=object())  # type: ignore[arg-type]
    order = _tracked_order()
    om._orders[order.client_order_id] = order
    om._by_server[order.server_order_id] = order.client_order_id

    # None of these may raise when no DB is configured.
    om._persist_order(order)
    om._persist_status(order)
    om._persist_fill(order, 1.0, 0.40)
    om.apply_fill_event("s1", 3.0, 0.40)


# ---------------------------------------------------------------------------
# HTTP routes
# ---------------------------------------------------------------------------


def test_static_assets_are_cache_busted() -> None:
    """Templates must pin a content-hash version on CSS/JS.

    Browsers heuristically cache StaticFiles responses (no Cache-Control),
    which produced new HTML + stale CSS after a redeploy (production
    2026-10-30: the wallet card rendered unstyled until a hard refresh).
    """
    from fastapi.testclient import TestClient

    from src.web import app as app_mod

    client = TestClient(app_mod.app)

    r = client.get("/login")
    assert r.status_code == 200
    assert "__ASSET_VERSION__" not in r.text
    assert f"style.css?v={app_mod.ASSET_VERSION}" in r.text

    # Static responses must ask the browser to revalidate.
    r = client.get("/static/style.css")
    assert r.status_code == 200
    assert r.headers["cache-control"] == "no-cache, must-revalidate"


def test_http_routes_serve_pages_and_health() -> None:
    from fastapi.testclient import TestClient

    from src.web.app import app

    client = TestClient(app)

    r = client.get("/api/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

    r = client.get("/")
    assert r.status_code == 200
    assert "PolyGTD" in r.text
    # Emergency-stop prompts use the in-page <dialog>, not window.confirm.
    assert 'id="confirm-dialog"' in r.text
    assert 'id="toast-wrap"' in r.text
    assert 'class="modal"' in r.text

    r = client.get("/trades")
    assert r.status_code == 200
    assert "交易记录" in r.text

    r = client.get("/static/style.css")
    assert r.status_code == 200


def test_build_status_flat_position_shows_idle_tiers() -> None:
    """A closed position must not render leftover filled tier rows.

    Production 2026-10-31 09:02: the TP sold the whole position but the
    dashboard's 分层状态 kept showing T1 filled 5/5 with a server order id,
    while the actual position was empty.
    """
    mkt = SimpleNamespace(
        up_token="UPTOKEN", down_token="DOWNTOKEN",
        slug="btc-updown-15m-1", condition_id="0xdead",
        window_start=None, window_end=None,
    )

    class _FlatPos:
        entry_side = "DOWN"
        entry_token = "DOWNTOKEN"
        total_qty = 0.0
        total_cost = 0.0
        avg_cost = 0.0
        tiers = [
            SimpleNamespace(
                tier=1, notional=2.0, placed=False, watching=False,
                is_complete=True, size_filled=5.0, size_expected=5.0,
                avg_fill_price=0.40, server_order_id="0xabc",
            )
        ]

        def tp_target(self):
            return None

    state = SimpleNamespace(
        wm=_FakeWM(mkt),
        engine=SimpleNamespace(
            _round=SimpleNamespace(
                round_start_ts=1_000, slug=mkt.slug,
                position=_FlatPos(), skip_reason=None,
            ),
            _gate=None,
        ),
        price_cache=PriceCache(),
        binance_cache=SimpleNamespace(last_price=50_000.0),
        order_manager=None,
    )

    out = status_mod.build_status(state)
    tier = out["position"]["tiers"][0]
    assert tier["status"] == "idle"
    assert tier["filled"] is False
    assert tier["size_filled"] == 0.0
    assert tier["size_expected"] == 0.0
    assert tier["server_order_id"] is None
    assert tier["avg_fill_price"] is None
