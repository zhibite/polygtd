"""Emergency-stop switch (dashboard control bar).

Covers:
  * engage/release persistence (survives a restart)
  * engine emits no actions while stopped
  * OrderManager refuses every place_* call while stopped
  * the control API requires login and cancels open orders on stop
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.config import strategy
from src.execution.order_manager import OrderManager
from src.strategy.killswitch import KillSwitch, engage, release
from src.strategy.round_state import RoundState


@pytest.fixture()
def ks(tmp_path):
    return KillSwitch(path=tmp_path / "emergency_stop.json")


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def test_engage_release_persists(ks) -> None:
    ks.engage("test reason")
    assert ks.active is True
    assert ks.path.exists()

    reloaded = KillSwitch.load(ks.path)
    assert reloaded.active is True
    assert reloaded.reason == "test reason"
    assert reloaded.since > 0

    ks.release()
    assert ks.active is False
    assert not ks.path.exists()
    assert KillSwitch.load(ks.path).active is False


def test_corrupt_state_file_is_inactive(tmp_path) -> None:
    path = tmp_path / "emergency_stop.json"
    path.write_text("{oops", encoding="utf-8")
    assert KillSwitch.load(path).active is False


# ---------------------------------------------------------------------------
# Engine / OrderManager guards
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_engine_emits_nothing_while_stopped(ks) -> None:
    from src.feeds.binance_spot import BinanceCache
    from src.market.market_ws import PriceCache
    from src.strategy.engine import StrategyEngine
    from src.strategy.gatekeeper import GateKeeper

    calls: list[object] = []

    async def handler(action) -> None:  # pragma: no cover
        calls.append(action)

    eng = StrategyEngine(BinanceCache(), PriceCache(), handler, killswitch=ks)
    eng._round = RoundState(round_start_ts=1)
    eng._round.position.entry_token = "tok"
    eng._gate = GateKeeper(100.0)
    eng._bc.last_price = 100.0

    ks.engage("test")
    await eng._evaluate()

    assert calls == []


@pytest.mark.asyncio
async def test_order_manager_blocks_place_calls(ks) -> None:
    om = OrderManager(clob=object(), killswitch=ks)  # type: ignore[arg-type]

    ks.engage("test")

    assert await om.place_buy_by_strategy_price(
        token_id="tok", strategy_price=0.4, notional=2.0,
        round_start_ts=1, tier_name="T1", tick_size=0.01,
    ) is None
    assert await om.place_gtd_sell_at_price(
        token_id="tok", size=5.0, price=0.5,
        round_start_ts=1, tier_name="TP", tick_size=0.01,
    ) is None
    assert await om.place_sell(
        token_id="tok", size=5.0, price=0.5,
        round_start_ts=1, tier_name="TP", tick_size=0.01,
    ) is None

    ks.release()
    # Guard lifted: the fake clob has no _client, so the call now proceeds
    # far enough to fail on its own (returns None with a different reason).
    assert om._blocked("place_buy") is False


# ---------------------------------------------------------------------------
# engage()/release() helpers
# ---------------------------------------------------------------------------

class _FakeRouter:
    def __init__(self) -> None:
        self.stopped = 0

    async def stop_all_watchers(self) -> None:
        self.stopped += 1


class _FakeOM:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def cancel_all_open(self, reason: str = "killswitch") -> int:
        self.calls.append(reason)
        return 3


@pytest.mark.asyncio
async def test_engage_cancels_orders_and_stops_watchers(ks) -> None:
    router = _FakeRouter()
    om = _FakeOM()
    state = SimpleNamespace(killswitch=ks, router=router, order_manager=om)

    result = await engage(state, reason="test")

    assert ks.active is True
    assert router.stopped == 1
    assert om.calls == ["killswitch"]
    assert result["cancelled_orders"] == 3
    assert result["killswitch"]["active"] is True


def test_release_clears_state(ks) -> None:
    state = SimpleNamespace(killswitch=ks, router=None, order_manager=None)
    ks.engage("test")
    result = release(state)
    assert result["killswitch"]["active"] is False
    assert ks.active is False


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------

@pytest.fixture()
def web_auth():
    from src.web import auth

    live = strategy()
    old_user, old_pass = live.WEB_USERNAME, live.WEB_PASSWORD
    live.WEB_USERNAME = "admin"
    live.WEB_PASSWORD = "test-pass"
    auth._sessions.clear()
    auth._failures.clear()
    yield
    live.WEB_USERNAME = old_user
    live.WEB_PASSWORD = old_pass
    auth._sessions.clear()
    auth._failures.clear()


def _logged_in_client(state):
    from fastapi.testclient import TestClient

    import src.web.app as app_mod

    app_mod.set_state(state)
    client = TestClient(app_mod.app)
    r = client.post(
        "/api/auth/login",
        json={"username": "admin", "password": "test-pass"},
    )
    assert r.status_code == 200
    return client


def test_control_api_requires_login(ks) -> None:
    from fastapi.testclient import TestClient

    import src.web.app as app_mod

    app_mod.set_state(SimpleNamespace(killswitch=ks, router=None, order_manager=None))
    try:
        client = TestClient(app_mod.app)
        assert client.post("/api/control/stop").status_code == 401
        assert client.post("/api/control/start").status_code == 401
        assert client.get("/api/control").status_code == 401
    finally:
        app_mod.set_state(None)


def test_control_api_stop_then_start(ks, web_auth) -> None:
    om = _FakeOM()
    router = _FakeRouter()
    state = SimpleNamespace(killswitch=ks, router=router, order_manager=om)
    client = _logged_in_client(state)
    try:
        r = client.post("/api/control/stop", json={"reason": "unit-test"})
        assert r.status_code == 200
        body = r.json()
        assert body["cancelled_orders"] == 3
        assert body["killswitch"]["active"] is True
        assert "unit-test" in body["killswitch"]["reason"]
        assert ks.active is True
        assert router.stopped == 1

        r = client.get("/api/control")
        assert r.json()["killswitch"]["active"] is True

        r = client.post("/api/control/start")
        assert r.status_code == 200
        assert r.json()["killswitch"]["active"] is False
        assert ks.active is False
    finally:
        import src.web.app as app_mod

        app_mod.set_state(None)
