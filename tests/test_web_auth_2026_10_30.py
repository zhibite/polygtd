"""Cookie-session login for the dashboard.

Policy under test:
  * WEB_PASSWORD set   → every page/API (except /login, /api/auth/* and
    /api/health) requires a session.
  * WEB_PASSWORD empty → read-only dashboard stays public, but /config and
    /api/config are always locked; login is impossible until the operator
    configures a password.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.config import strategy

PASSWORD = "test-pass"


@pytest.fixture(autouse=True)
def auth_env():
    from src.web import auth

    live = strategy()
    old_user, old_pass = live.WEB_USERNAME, live.WEB_PASSWORD
    live.WEB_USERNAME = "admin"
    live.WEB_PASSWORD = ""

    auth._sessions.clear()
    auth._failures.clear()
    yield auth
    live.WEB_USERNAME = old_user
    live.WEB_PASSWORD = old_pass
    auth._sessions.clear()
    auth._failures.clear()


@pytest.fixture()
def client():
    from src.web.app import app

    return TestClient(app)


def _enable_login(auth_env) -> None:
    strategy().WEB_PASSWORD = PASSWORD


def _login(client, password: str = PASSWORD):
    return client.post(
        "/api/auth/login",
        json={"username": "admin", "password": password},
    )


# ---------------------------------------------------------------------------
# No password configured
# ---------------------------------------------------------------------------

def test_dashboard_public_without_password(client) -> None:
    assert client.get("/").status_code == 200
    assert client.get("/trades").status_code == 200
    assert client.get("/api/health").status_code == 200


def test_config_locked_without_password(client) -> None:
    r = client.get("/config", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login?next=/config"

    assert client.get("/api/config").status_code == 401
    assert client.put("/api/config", json={"ENTRY_PRICE_CAP": 0.3}).status_code == 401
    assert client.post("/api/config/reset").status_code == 401


def test_login_impossible_without_password(client) -> None:
    r = _login(client, "anything")
    assert r.status_code == 503
    assert "WEB_PASSWORD" in r.json()["detail"]


def test_login_page_is_public(client) -> None:
    r = client.get("/login")
    assert r.status_code == 200
    assert "登录" in r.text


# ---------------------------------------------------------------------------
# Password configured
# ---------------------------------------------------------------------------

def test_login_flow_and_protected_pages(client, auth_env) -> None:
    _enable_login(auth_env)

    # Unauthenticated: pages redirect, APIs 401.
    r = client.get("/", follow_redirects=False)
    assert r.status_code == 302 and r.headers["location"].startswith("/login")
    assert client.get("/trades", follow_redirects=False).status_code == 302
    assert client.get("/config", follow_redirects=False).status_code == 302
    assert client.get("/api/status").status_code == 401
    assert client.get("/api/config").status_code == 401

    me = client.get("/api/auth/me").json()
    assert me["authenticated"] is False
    assert me["auth_required"] is True
    assert me["credentials_configured"] is True

    # Wrong password.
    assert _login(client, "wrong").status_code == 401
    assert client.get("/api/auth/me").json()["authenticated"] is False

    # Correct password sets the session cookie.
    r = _login(client)
    assert r.status_code == 200
    assert r.json()["user"] == "admin"
    assert client.get("/api/auth/me").json()["authenticated"] is True

    assert client.get("/").status_code == 200
    assert client.get("/config").status_code == 200
    assert client.get("/api/config").status_code == 200


def test_wrong_username_rejected(client, auth_env) -> None:
    _enable_login(auth_env)
    r = client.post(
        "/api/auth/login",
        json={"username": "root", "password": PASSWORD},
    )
    assert r.status_code == 401


def test_logout_invalidates_session(client, auth_env) -> None:
    _enable_login(auth_env)
    assert _login(client).status_code == 200
    assert client.get("/api/config").status_code == 200

    assert client.post("/api/auth/logout").status_code == 200
    assert client.get("/api/config").status_code == 401
    assert client.get("/api/auth/me").json()["authenticated"] is False


def test_login_rate_limited(client, auth_env) -> None:
    _enable_login(auth_env)
    for _ in range(10):
        assert _login(client, "wrong").status_code == 401
    # Even the correct password is refused while the IP is blocked.
    r = _login(client)
    assert r.status_code == 429


# ---------------------------------------------------------------------------
# Session / redirect internals
# ---------------------------------------------------------------------------

def test_session_expiry(auth_env) -> None:
    token = auth_env.create_session("admin")
    assert auth_env.get_session(token) is not None

    auth_env._sessions[token].expires_at = 0
    assert auth_env.get_session(token) is None


def test_safe_next_rejects_open_redirect() -> None:
    from src.web.app import _safe_next

    assert _safe_next("/config") == "/config"
    assert _safe_next("/trades?x=1") == "/trades?x=1"
    assert _safe_next("//evil.example") == "/"
    assert _safe_next("https://evil.example") == "/"
    assert _safe_next(None) == "/"


def test_https_detection_from_forwarded_proto() -> None:
    from starlette.requests import Request

    from src.web.auth import is_https

    scope = {
        "type": "http", "method": "GET", "path": "/", "scheme": "http",
        "headers": [(b"x-forwarded-proto", b"https")],
        "query_string": b"", "server": ("test", 80), "client": ("1.1.1.1", 1),
    }
    assert is_https(Request(scope)) is True
