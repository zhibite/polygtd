"""Cookie-session authentication for the dashboard.

Design:

  * Credentials come from the existing ``WEB_USERNAME`` / ``WEB_PASSWORD``
    settings (Coolify env panel / process env). There is no user table —
    this dashboard is single-operator.
  * A successful login mints a random 256-bit token held in an in-memory
    session store; the browser gets it as an ``HttpOnly``, ``SameSite=Lax``
    cookie (``Secure`` when the request arrived over HTTPS). No tokens are
    kept in JavaScript, so an XSS cannot exfiltrate the session.
  * Sessions live in memory only: a restart/redeploy invalidates every
    session and the operator logs in again. That is intentional — there is
    no session table to maintain.

Access policy (see `src.web.app`):

  * ``WEB_PASSWORD`` set   → every page/API except ``/login``,
    ``/api/auth/*`` and ``/api/health`` requires a login.
  * ``WEB_PASSWORD`` empty → the read-only dashboard stays public (the
    previous behaviour), but the **config surface is always locked**: its
    routes require a session, and without a configured password nobody can
    obtain one. The login page explains how to fix that.
"""
from __future__ import annotations

import secrets
import time
from dataclasses import dataclass

from fastapi import Request, Response

from src.config import strategy as _strategy
from src.infra.logging import get_logger

log = get_logger(__name__)

SESSION_COOKIE = "gtd_session"
DEFAULT_TTL_S = 7 * 24 * 3600

# Brute-force guard: N failures per client IP inside the window.
_MAX_FAILURES = 10
_FAILURE_WINDOW_S = 300.0


@dataclass
class Session:
    user: str
    expires_at: float


_sessions: dict[str, Session] = {}
_failures: dict[str, list[float]] = {}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def session_ttl_s() -> int:
    try:
        return max(60, int(_strategy().WEB_SESSION_TTL_S))
    except Exception:  # noqa: BLE001
        return DEFAULT_TTL_S


def credentials_configured() -> bool:
    return bool(_strategy().WEB_PASSWORD)


def auth_required() -> bool:
    """True when the whole dashboard (not just /config) needs a login."""
    return credentials_configured()


def verify_credentials(username: str, password: str) -> bool:
    cfg = _strategy()
    if not cfg.WEB_PASSWORD:
        return False
    expected_user = cfg.WEB_USERNAME or "admin"
    return (
        secrets.compare_digest(username or "", expected_user)
        and secrets.compare_digest(password or "", cfg.WEB_PASSWORD)
    )


# ---------------------------------------------------------------------------
# Session store
# ---------------------------------------------------------------------------

def create_session(user: str) -> str:
    now = time.time()
    _prune(now)
    token = secrets.token_urlsafe(32)
    _sessions[token] = Session(user=user, expires_at=now + session_ttl_s())
    return token


def get_session(token: str | None) -> Session | None:
    if not token:
        return None
    session = _sessions.get(token)
    if session is None:
        return None
    if session.expires_at <= time.time():
        _sessions.pop(token, None)
        return None
    return session


def delete_session(token: str | None) -> None:
    if token:
        _sessions.pop(token, None)


def _prune(now: float) -> None:
    for token in [t for t, s in _sessions.items() if s.expires_at <= now]:
        _sessions.pop(token, None)


# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------

def current_session(request: Request) -> Session | None:
    return get_session(request.cookies.get(SESSION_COOKIE))


def current_user(request: Request) -> str | None:
    session = current_session(request)
    return session.user if session else None


def is_https(request: Request) -> bool:
    """Trust the proxy's X-Forwarded-Proto (Coolify/Traefik terminates TLS).

    uvicorn only honours that header from `forwarded_allow_ips` (default
    127.0.0.1), which does not include the Docker network peer, so read it
    directly here.
    """
    forwarded = request.headers.get("x-forwarded-proto", "")
    first = forwarded.split(",")[0].strip().lower()
    return request.url.scheme == "https" or first == "https"


def set_session_cookie(response: Response, token: str, request: Request) -> None:
    response.set_cookie(
        SESSION_COOKIE,
        token,
        max_age=session_ttl_s(),
        httponly=True,
        samesite="lax",
        secure=is_https(request),
        path="/",
    )


def clear_session_cookie(response: Response) -> None:
    response.delete_cookie(SESSION_COOKIE, path="/")


# ---------------------------------------------------------------------------
# Login throttling
# ---------------------------------------------------------------------------

def client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    first = forwarded.split(",")[0].strip()
    if first:
        return first
    return request.client.host if request.client else "unknown"


def login_blocked(ip: str) -> bool:
    now = time.time()
    stamps = [t for t in _failures.get(ip, []) if now - t < _FAILURE_WINDOW_S]
    _failures[ip] = stamps
    return len(stamps) >= _MAX_FAILURES


def record_login_failure(ip: str) -> None:
    _failures.setdefault(ip, []).append(time.time())


def reset_login_failures(ip: str) -> None:
    _failures.pop(ip, None)
