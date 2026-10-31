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

import asyncio
import time
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


def test_build_status_treats_rounding_dust_as_flat() -> None:
    """0.004925 shares left after a TP must render as a flat position."""
    class _DustPosition(_FakePosition):
        total_qty = 0.004925
        total_cost = 0.0014
        avg_cost = 0.283

    mkt = SimpleNamespace(
        up_token="UPTOKEN", down_token="DOWNTOKEN",
        slug="btc-updown-15m-1", condition_id="0xdead",
        window_start=None, window_end=None,
    )
    state = SimpleNamespace(
        wm=_FakeWM(mkt),
        engine=SimpleNamespace(
            _round=SimpleNamespace(
                round_start_ts=1_000, slug=mkt.slug,
                position=_DustPosition(), skip_reason=None,
            ),
            _gate=None,
        ),
        price_cache=PriceCache(),
        binance_cache=SimpleNamespace(last_price=50_000.0),
        order_manager=None,
    )
    pos = status_mod.build_status(state)["position"]
    assert pos["qty"] == 0.0
    assert pos["cost"] == 0.0
    assert pos["avg_cost"] == 0.0
    assert pos["upnl_usd"] == 0.0
    # Flat position: no leftover filled tier rows.
    assert pos["tiers"][0]["status"] == "idle"


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


def test_fills_pagination_is_stable_and_counted(conn) -> None:
    """成交明细 pages must not overlap or skip when ts_ms ties.

    Fills are ordered by (ts_ms DESC, fill_id DESC); the secondary key is
    what keeps LIMIT/OFFSET paging deterministic when several fills share
    the same millisecond.
    """
    db = DB(conn)
    db.upsert_round(round_start_ts=1_000, slug="r1")
    for i in range(45):
        db.insert_fill(
            fill_id=f"f{i:03d}", order_id="c1", round_start_ts=1_000,
            token_id="tok", side="BUY", price=0.40, size=1.0,
            # 5 fills share each millisecond — exercises the tiebreaker.
            ts_ms=1_000_000 + (i // 5),
        )

    assert status_mod.count_fills(conn) == 45

    page1 = status_mod.recent_fills(conn, limit=20, offset=0)
    page2 = status_mod.recent_fills(conn, limit=20, offset=20)
    page3 = status_mod.recent_fills(conn, limit=20, offset=40)

    ids = [f["fill_id"] for f in page1 + page2 + page3]
    assert len(ids) == 45
    assert len(set(ids)) == 45, "pages overlap or skip a fill"
    # Newest first; ties broken by fill_id DESC.
    assert ids[0] == "f044"
    assert ids[-1] == "f000"
    assert len(page3) == 5


def test_trades_page_exposes_the_fill_pager() -> None:
    from fastapi.testclient import TestClient

    from src.web.app import app

    r = TestClient(app).get("/trades")
    assert r.status_code == 200
    assert 'id="fills-pager"' in r.text
    assert 'id="fill-prev"' in r.text
    assert 'id="fill-next"' in r.text
    assert "/api/fills?limit=" in r.text
    assert "FILL_PAGE_SIZE = 20" in r.text
    # Settlement at expiry is shown separately from SELL proceeds.
    assert "到期结算" in r.text
    assert "settlement_usd" in r.text


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


def test_round_with_only_rounding_dust_is_closed(conn) -> None:
    """A sub-0.01-share residual is CLOB dust, not an open position.

    Production 2026-10-31 03:00: the TP was placed for 29.6049 shares but
    the CLOB matched 29.6, leaving 0.004925 behind. The engine already
    closes the position at < 0.01 shares, yet the history derived from
    fills kept the round "未平仓" forever.
    """
    db = DB(conn)
    db.upsert_round(round_start_ts=4_000, slug="r4")
    db.insert_order(
        client_order_id="c4", server_order_id="s4", round_start_ts=4_000,
        side="BUY", token_id="tok", tier_name="T1", price=0.40, size=10.0,
        lifetime_s=300, expires_at=4_300, placed_at=4_000,
    )
    db.insert_fill("f4a", "c4", 4_000, "tok", "BUY", 0.40, 10.004925, 4_000_100)
    db.insert_fill("f4b", "c4", 4_000, "tok", "SELL", 0.50, 10.0, 4_000_200)

    r = status_mod.rounds_with_pnl(conn)[0]
    assert r["status"] == "closed"
    assert r["closed"] is True
    assert r["open_qty"] == 0.0
    stats = status_mod.compute_stats([r])
    assert stats["open_rounds"] == 0
    assert stats["closed_rounds"] == 1


def test_round_with_a_real_partial_residual_is_still_holding(conn) -> None:
    """The dust cutoff must not hide a genuine leftover position."""
    db = DB(conn)
    db.upsert_round(round_start_ts=5_000, slug="r5")
    db.insert_order(
        client_order_id="c5", server_order_id="s5", round_start_ts=5_000,
        side="BUY", token_id="tok", tier_name="T1", price=0.40, size=10.0,
        lifetime_s=300, expires_at=5_300, placed_at=5_000,
    )
    db.insert_fill("f5a", "c5", 5_000, "tok", "BUY", 0.40, 10.0, 5_000_100)
    db.insert_fill("f5b", "c5", 5_000, "tok", "SELL", 0.50, 5.0, 5_000_200)

    r = status_mod.rounds_with_pnl(conn)[0]
    assert r["status"] == "holding"
    assert r["closed"] is False
    assert r["open_qty"] == pytest.approx(5.0)


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


def test_realized_pnl_books_each_sell_and_ignores_open_reentry(conn) -> None:
    """A same-window re-entry must not turn a realised win into a fake loss.

    Cycle 1: buy 5 @0.40, sell 5 @0.48  -> +$0.40 realised.
    Cycle 2: buy 5 @0.40, still open.
    Old logic: pnl = sell_proceeds - all_buys = 2.40 - 4.00 = -1.60 and the
    dashboard showed a loss right after the re-entry buy.
    New logic: each SELL books its realised PnL immediately; the open buy
    is ignored.
    """
    db = DB(conn)
    now = int(time.time())
    now_ms = now * 1000
    round_ts = now - 60
    db.upsert_round(round_start_ts=round_ts, slug="r-reentry")
    db.insert_order(
        client_order_id="c1", server_order_id="s1", round_start_ts=round_ts,
        side="BUY", token_id="tok", tier_name="T1", price=0.40, size=5.0,
        lifetime_s=300, expires_at=round_ts + 300, placed_at=round_ts,
    )
    db.insert_fill("f1", "c1", round_ts, "tok", "BUY", 0.40, 5.0,
                   ts_ms=now_ms - 3000)
    db.insert_fill("f2", "c1", round_ts, "tok", "SELL", 0.48, 5.0,
                   ts_ms=now_ms - 2000)
    db.insert_order(
        client_order_id="c2", server_order_id="s2", round_start_ts=round_ts,
        side="BUY", token_id="tok", tier_name="T1", price=0.40, size=5.0,
        lifetime_s=300, expires_at=round_ts + 300, placed_at=round_ts,
    )
    db.insert_fill("f3", "c2", round_ts, "tok", "BUY", 0.40, 5.0,
                   ts_ms=now_ms - 1000)

    rounds = status_mod.rounds_with_pnl(conn)
    r = rounds[0]
    assert r["status"] == "holding"
    assert r["closed"] is False
    assert r["buy_cost"] == pytest.approx(4.0)
    assert r["sell_proceeds"] == pytest.approx(2.4)
    assert r["pnl_usd"] == pytest.approx(0.40)
    assert r["pnl_pct"] == pytest.approx(0.20)
    assert r["open_qty"] == pytest.approx(5.0)

    stats = status_mod.compute_stats(rounds)
    assert stats["today_pnl_usd"] == pytest.approx(0.40)
    assert stats["total_pnl_usd"] == pytest.approx(0.40)
    assert stats["wins"] == 1
    assert stats["losses"] == 0
    assert stats["open_rounds"] == 1
    assert stats["closed_rounds"] == 0
    assert stats["today_rounds"] == 1


def test_settled_losing_round_is_booked_as_loss(conn) -> None:
    """A T4 position that expires worthless must count as a loss.

    No SELL fill exists for a losing binary outcome, so before settlement
    accounting the round stayed "未平仓" forever and the loss was missing
    from the dashboard totals (operator report 2026-10-31).
    """
    db = DB(conn)
    round_ts = int(time.time()) - 900
    db.upsert_round(
        round_start_ts=round_ts, slug="r-loss", condition_id="0xdead",
        up_token="UP", down_token="DOWN",
    )
    db.insert_order(
        client_order_id="c-loss", server_order_id="s-loss",
        round_start_ts=round_ts, side="BUY", token_id="UP", tier_name="T4",
        price=0.40, size=10.0, lifetime_s=300,
        expires_at=round_ts + 300, placed_at=round_ts,
    )
    db.insert_fill("f-loss", "c-loss", round_ts, "UP", "BUY", 0.40, 10.0,
                   ts_ms=round_ts * 1000)
    db.set_round_resolution(round_ts, up_won=False)

    r = status_mod.rounds_with_pnl(conn)[0]
    assert r["resolved"] is True
    assert r["up_won"] is False
    assert r["status"] == "closed"
    assert r["closed"] is True
    assert r["open_qty"] == 0.0
    assert r["pnl_usd"] == pytest.approx(-4.0)
    assert r["settlement_usd"] == pytest.approx(-4.0)
    assert r["pnl_pct"] == pytest.approx(-1.0)

    stats = status_mod.compute_stats([r])
    assert stats["closed_rounds"] == 1
    assert stats["open_rounds"] == 0
    assert stats["wins"] == 0
    assert stats["losses"] == 1
    assert stats["win_rate"] == pytest.approx(0.0)
    assert stats["total_pnl_usd"] == pytest.approx(-4.0)
    assert stats["today_pnl_usd"] == pytest.approx(-4.0)
    assert stats["today_rounds"] == 1


def test_settled_loss_includes_buy_fees(conn) -> None:
    """Buy-side taker fees are part of the basis and the return denominator."""
    db = DB(conn)
    round_ts = int(time.time()) - 900
    db.upsert_round(
        round_start_ts=round_ts, slug="r-fee", condition_id="0xfee",
        up_token="UP", down_token="DOWN",
    )
    db.insert_fill("f-fee", "c-fee", round_ts, "UP", "BUY", 0.40, 10.0,
                   ts_ms=round_ts * 1000, fee=0.10)
    db.set_round_resolution(round_ts, up_won=False)

    r = status_mod.rounds_with_pnl(conn)[0]
    assert r["pnl_usd"] == pytest.approx(-4.10)
    assert r["pnl_pct"] == pytest.approx(-1.0)


def test_settled_winning_round_books_payout(conn) -> None:
    """Winning shares pay $1 at expiry; book that as realised profit."""
    db = DB(conn)
    round_ts = int(time.time()) - 900
    db.upsert_round(
        round_start_ts=round_ts, slug="r-win", condition_id="0xbeef",
        up_token="UP", down_token="DOWN",
    )
    db.insert_fill("f-win", "c-win", round_ts, "DOWN", "BUY", 0.40, 10.0,
                   ts_ms=round_ts * 1000)
    db.set_round_resolution(round_ts, up_won=False)  # DOWN wins

    r = status_mod.rounds_with_pnl(conn)[0]
    assert r["status"] == "closed"
    assert r["pnl_usd"] == pytest.approx(6.0)  # 10 × $1 − $4 cost
    assert r["settlement_usd"] == pytest.approx(6.0)
    assert r["pnl_pct"] == pytest.approx(1.5)

    stats = status_mod.compute_stats([r])
    assert stats["wins"] == 1
    assert stats["losses"] == 0
    assert stats["total_pnl_usd"] == pytest.approx(6.0)


def test_partial_tp_then_settlement_books_remaining_loss(conn) -> None:
    """A half-sold position that loses the rest combines both PnL legs."""
    db = DB(conn)
    round_ts = int(time.time()) - 900
    db.upsert_round(
        round_start_ts=round_ts, slug="r-mixed", condition_id="0xcafe",
        up_token="UP", down_token="DOWN",
    )
    db.insert_fill("f-buy", "c-mixed", round_ts, "UP", "BUY", 0.40, 10.0,
                   ts_ms=round_ts * 1000 + 1_000)
    db.insert_fill("f-sell", "c-mixed", round_ts, "UP", "SELL", 0.50, 5.0,
                   ts_ms=round_ts * 1000 + 2_000)
    db.set_round_resolution(round_ts, up_won=False)

    r = status_mod.rounds_with_pnl(conn)[0]
    assert r["status"] == "closed"
    assert r["sell_proceeds"] == pytest.approx(2.5)
    # +$0.50 on the first 5 shares, −$2.00 on the 5 left at expiry.
    assert r["pnl_usd"] == pytest.approx(-1.5)
    assert r["settlement_usd"] == pytest.approx(-2.0)
    assert r["open_qty"] == 0.0


def test_unresolved_round_stays_open_even_if_old(conn) -> None:
    """Without a Gamma outcome the position must not be guessed closed."""
    db = DB(conn)
    round_ts = int(time.time()) - 3_600
    db.upsert_round(
        round_start_ts=round_ts, slug="r-unknown", condition_id="0xfeed",
        up_token="UP", down_token="DOWN",
    )
    db.insert_fill("f-unknown", "c-unknown", round_ts, "UP", "BUY", 0.40, 10.0,
                   ts_ms=round_ts * 1000)

    r = status_mod.rounds_with_pnl(conn)[0]
    assert r["resolved"] is False
    assert r["status"] == "open"
    assert r["closed"] is False
    assert r["pnl_usd"] is None
    assert r["open_qty"] == pytest.approx(10.0)

    assert len(db.unresolved_rounds(before_ts=round_ts + 900)) == 1


def test_unresolved_rounds_excludes_recorded_outcomes(conn) -> None:
    db = DB(conn)
    db.upsert_round(
        round_start_ts=1_000, condition_id="0xa",
        up_token="UP", down_token="DOWN",
    )
    db.upsert_round(
        round_start_ts=2_000, condition_id="0xb",
        up_token="UP", down_token="DOWN",
    )
    db.set_round_resolution(1_000, up_won=True)

    rows = db.unresolved_rounds(before_ts=5_000, lookback_s=10_000)
    assert [r[0] for r in rows] == [2_000]
    assert rows[0][1] == "0xb"


class _FakeResolutionPM:
    def __init__(self, result) -> None:
        self.result = result
        self.calls: list[tuple[str, str | None]] = []

    async def get_market_resolution(
        self, condition_id: str, slug: str | None = None
    ):
        self.calls.append((condition_id, slug))
        return self.result


class _FakeResolutionDB:
    def __init__(self, rows) -> None:
        self.rows = rows
        self.recorded: list[tuple[int, bool]] = []

    def unresolved_rounds(self, **_kw):
        return self.rows

    def set_round_resolution(self, round_ts: int, up_won: bool) -> None:
        self.recorded.append((round_ts, up_won))


def test_resolve_rounds_once_records_the_outcome() -> None:
    from src.main import _resolve_rounds_once

    pm = _FakeResolutionPM({"resolved": True, "up_wins": False})
    db = _FakeResolutionDB(
        [(1_000, "0xabc", "slug", "UP", "DOWN")]
    )
    state = SimpleNamespace(pm_client=pm, db=db)
    attempts: dict[int, int] = {}

    resolved = asyncio.run(_resolve_rounds_once(state, attempts))

    assert resolved == 1
    assert pm.calls == [("0xabc", "slug")]
    assert db.recorded == [(1_000, False)]
    assert attempts == {}


def test_resolve_rounds_once_retries_until_resolved() -> None:
    from src.main import _resolve_rounds_once

    pm = _FakeResolutionPM({"resolved": False, "up_wins": None})
    db = _FakeResolutionDB(
        [(1_000, "0xabc", "slug", "UP", "DOWN")]
    )
    state = SimpleNamespace(pm_client=pm, db=db)
    attempts: dict[int, int] = {}

    assert asyncio.run(_resolve_rounds_once(state, attempts)) == 0
    assert db.recorded == []
    assert attempts == {1_000: 1}


def test_resolve_rounds_once_skips_rows_without_token_ids() -> None:
    """Without both tokens the held side cannot be attributed."""
    from src.main import _resolve_rounds_once

    pm = _FakeResolutionPM({"resolved": True, "up_wins": True})
    db = _FakeResolutionDB([(1_000, "0xabc", "slug", None, "DOWN")])
    state = SimpleNamespace(pm_client=pm, db=db)

    assert asyncio.run(_resolve_rounds_once(state, {})) == 0
    assert pm.calls == []


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
    # Tables are scroll containers so the mobile layout never squashes them.
    assert 'class="table-wrap"' in r.text
    # Window-remaining is rendered as "14m 19s", not raw seconds.
    assert "const dur = (s) =>" in r.text

    r = client.get("/trades")
    assert r.status_code == 200
    assert "交易记录" in r.text
    assert 'class="table-wrap"' in r.text

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


def test_fees_are_recorded_and_reduce_realized_pnl(conn) -> None:
    """Official Polymarket taker fee: C × rate × p × (1-p).

    Buy fees join the cost basis; sell fees reduce the proceeds.
    """
    db = DB(conn)
    round_ts = 5_000
    db.upsert_round(round_start_ts=round_ts, slug="fee-round")
    db.insert_order(
        client_order_id="c1", server_order_id="s1", round_start_ts=round_ts,
        side="BUY", token_id="tok", tier_name="T1", price=0.40, size=5.0,
        lifetime_s=300, expires_at=round_ts + 300, placed_at=round_ts,
    )
    buy_fee = 5.0 * 0.07 * 0.40 * (1 - 0.40)      # 0.084
    sell_fee = 5.0 * 0.07 * 0.48 * (1 - 0.48)     # 0.08736
    db.insert_fill(
        "f1", "c1", round_ts, "tok", "BUY", 0.40, 5.0,
        ts_ms=1_000_000, fee=buy_fee,
    )
    db.insert_fill(
        "f2", "c1", round_ts, "tok", "SELL", 0.48, 5.0,
        ts_ms=2_000_000, fee=sell_fee,
    )

    rounds = status_mod.rounds_with_pnl(conn)
    r = rounds[0]
    assert r["fee_total"] == pytest.approx(buy_fee + sell_fee)
    assert r["pnl_usd"] == pytest.approx(
        (0.48 * 5 - sell_fee) - (0.40 * 5 + buy_fee)
    )

    fills = status_mod.recent_fills(conn)
    assert fills[0]["fee"] == pytest.approx(sell_fee)
    assert fills[1]["fee"] == pytest.approx(buy_fee)


def test_order_manager_computes_taker_fee_maker_zero() -> None:
    """Makers pay no fee; takers pay C × rate × p × (1-p)."""
    fake = _FakeDB()
    om = OrderManager(clob=object(), db=fake)
    order = _tracked_order()
    om._orders[order.client_order_id] = order
    om._by_server[order.server_order_id] = order.client_order_id

    om.apply_fill_event("s1", 5.0, 0.40, trader_side="TAKER")
    assert fake.fills[0]["fee"] == pytest.approx(5.0 * 0.07 * 0.40 * 0.60)

    maker = _tracked_order()
    maker.client_order_id = "c2"
    maker.server_order_id = "s2"
    om._orders["c2"] = maker
    om._by_server["s2"] = "c2"
    om.apply_fill_event("s2", 5.0, 0.40, trader_side="MAKER")
    assert fake.fills[-1]["fee"] == 0.0
