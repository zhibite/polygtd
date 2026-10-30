"""Wallet balance snapshot used by the config page."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.config import strategy
from src.web import wallet


class _FakeClob:
    def __init__(
        self,
        collateral=None,
        token_balance=None,
        configured: bool = True,
    ) -> None:
        self._collateral = collateral
        self._token_balance = token_balance
        self._configured = configured
        self.collateral_calls = 0
        self.token_calls = 0

    def is_configured(self) -> bool:
        return self._configured

    @property
    def funder_address(self) -> str:
        return "0xF82653E6D4d3496e263e6395dFD6E64E0911bf34"

    @property
    def signature_type(self) -> int:
        return 2

    async def get_collateral(self):
        self.collateral_calls += 1
        return self._collateral

    async def get_token_balance(self, token_id: str):  # noqa: ARG002
        self.token_calls += 1
        return self._token_balance


def _state(clob, entry_token="tok") -> SimpleNamespace:
    return SimpleNamespace(
        clob=clob,
        engine=SimpleNamespace(
            _round=SimpleNamespace(
                position=SimpleNamespace(entry_token=entry_token),
            )
        ),
    )


@pytest.fixture(autouse=True)
def _clear_cache():
    wallet.invalidate()
    yield
    wallet.invalidate()


@pytest.mark.asyncio
async def test_snapshot_reads_balance_allowance_and_shares() -> None:
    clob = _FakeClob(
        collateral={"balance": 12.34, "allowance": 1e15},
        token_balance=5.0,
    )

    snap = await wallet.wallet_snapshot(_state(clob))

    assert snap["configured"] is True
    assert snap["balance_usd"] == pytest.approx(12.34)
    assert snap["allowance_usd"] == pytest.approx(1e15)
    assert snap["position_shares"] == pytest.approx(5.0)
    assert snap["funder"].startswith("0xF826")
    assert snap["signature_type"] == 2
    assert snap["error"] is None


@pytest.mark.asyncio
async def test_snapshot_without_position_skips_token_query() -> None:
    clob = _FakeClob(collateral={"balance": 3.0, "allowance": 0.0})

    snap = await wallet.wallet_snapshot(_state(clob, entry_token=None))

    assert snap["position_shares"] is None
    assert clob.token_calls == 0


@pytest.mark.asyncio
async def test_snapshot_not_configured() -> None:
    clob = _FakeClob(configured=False)

    snap = await wallet.wallet_snapshot(_state(clob))

    assert snap["configured"] is False
    assert "未配置" in snap["error"]
    assert snap["balance_usd"] is None


@pytest.mark.asyncio
async def test_snapshot_surfaces_query_failure() -> None:
    clob = _FakeClob(collateral=None)

    snap = await wallet.wallet_snapshot(_state(clob))

    # Unknown must never be rendered as zero.
    assert snap["balance_usd"] is None
    assert "查询失败" in snap["error"]


@pytest.mark.asyncio
async def test_snapshot_is_cached_and_force_bypasses() -> None:
    clob = _FakeClob(collateral={"balance": 1.0, "allowance": 0.0})
    state = _state(clob, entry_token=None)

    await wallet.wallet_snapshot(state)
    await wallet.wallet_snapshot(state)
    assert clob.collateral_calls == 1

    await wallet.wallet_snapshot(state, force=True)
    assert clob.collateral_calls == 2


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------

@pytest.fixture()
def web_auth():
    live = strategy()
    old_user, old_pass = live.WEB_USERNAME, live.WEB_PASSWORD
    live.WEB_USERNAME = "admin"
    live.WEB_PASSWORD = "test-pass"
    from src.web import auth

    auth._sessions.clear()
    auth._failures.clear()
    yield
    live.WEB_USERNAME = old_user
    live.WEB_PASSWORD = old_pass
    auth._sessions.clear()
    auth._failures.clear()


def test_wallet_api_requires_login(web_auth) -> None:
    from fastapi.testclient import TestClient

    from src.web.app import app

    client = TestClient(app)
    assert client.get("/api/wallet").status_code == 401


def test_wallet_api_returns_snapshot(web_auth) -> None:
    from fastapi.testclient import TestClient

    import src.web.app as app_mod

    clob = _FakeClob(collateral={"balance": 7.5, "allowance": 1e15})
    app_mod.set_state(_state(clob, entry_token=None))
    try:
        client = TestClient(app_mod.app)
        r = client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "test-pass"},
        )
        assert r.status_code == 200

        r = client.get("/api/wallet")
        assert r.status_code == 200
        body = r.json()
        assert body["balance_usd"] == pytest.approx(7.5)
        assert body["configured"] is True
    finally:
        app_mod.set_state(None)
