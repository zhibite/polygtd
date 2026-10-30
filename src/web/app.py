"""FastAPI dashboard for the polygtd bot.

Runs inside the bot process (see `src/main.py`), on the same asyncio event
loop, so `/api/status` reads live engine state without locks. History comes
from the SQLite file the bot already writes.

Start-up:  `uvicorn.Server(...).serve()` is scheduled from `main()`.
Auth:      cookie sessions (see `src/web/auth.py`). Set WEB_PASSWORD (and
           optionally WEB_USERNAME) in the environment / Coolify panel.

           * WEB_PASSWORD set   → every page/API except /login, /api/auth/*
             and /api/health requires a login.
           * WEB_PASSWORD empty → the read-only dashboard stays public, but
             /config and /api/config are always locked and the login page
             explains that WEB_PASSWORD must be configured.

Routes:
           GET  /                    dashboard page
           GET  /trades              trade history page
           GET  /config              strategy config page (login only)
           GET  /login               login page
           POST /api/auth/login      authenticate, sets session cookie
           POST /api/auth/logout     clear session cookie
           GET  /api/auth/me         current session state
           GET  /api/health          liveness probe (never authenticated)
           GET  /api/status          live snapshot
           GET  /api/rounds          rounds + derived PnL
           GET  /api/fills           recent fills
           GET  /api/trades          rounds + fills + aggregate stats
           GET  /api/config          strategy config (login only)
           PUT  /api/config          update strategy config (login only)
           POST /api/config/reset    reset strategy config (login only)
           GET  /api/wallet          funder balance snapshot (login only)
"""
from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.infra.logging import get_logger
from src.web import auth
from src.web import status as status_mod

log = get_logger(__name__)

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
STATIC_DIR = Path(__file__).resolve().parent / "static"

app = FastAPI(
    title="PolyGTD",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _asset_version() -> str:
    """Short content hash of the static assets.

    Browsers cache CSS/JS heuristically (StaticFiles sends no
    Cache-Control), so a redeploy could pair new HTML with a stale
    stylesheet — production 2026-10-30: the wallet card rendered
    unstyled until a hard refresh. Templates link
    ``/static/style.css?v=__ASSET_VERSION__`` and this hash changes
    whenever the files do.
    """
    digest = hashlib.md5()
    try:
        for name in ("style.css", "auth-nav.js"):
            path = STATIC_DIR / name
            if path.exists():
                digest.update(path.read_bytes())
    except Exception:  # noqa: BLE001
        return "1"
    return digest.hexdigest()[:10] or "1"


ASSET_VERSION = _asset_version()


@app.middleware("http")
async def _static_revalidate(request: Request, call_next):  # type: ignore[no-untyped-def]
    """Make browsers revalidate static files instead of guessing."""
    response = await call_next(request)
    if request.url.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-cache, must-revalidate"
    return response


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class LoginPayload(BaseModel):
    username: str = ""
    password: str = ""


def _safe_next(value: str | None) -> str:
    """Only allow same-site relative redirects (no open redirect)."""
    if value and value.startswith("/") and not value.startswith("//"):
        return value
    return "/"


def _login_redirect(request: Request) -> RedirectResponse | None:
    if auth.auth_required() and auth.current_session(request) is None:
        return RedirectResponse(
            f"/login?next={request.url.path}", status_code=302
        )
    return None


async def _require_auth(request: Request) -> None:
    """Protect read APIs when a password is configured."""
    if not auth.auth_required():
        return
    if auth.current_session(request) is None:
        raise HTTPException(status_code=401, detail="需要登录")


async def _require_config_auth(request: Request) -> str:
    """The config surface ALWAYS needs a session, password or not."""
    user = auth.current_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="需要登录后才能访问策略配置")
    return user


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Bound by `src.main._web_app()` at start-up. Do NOT import `src.main` here:
# the bot runs as `python -m src.main`, i.e. as the `__main__` module, so
# `from src.main import STATE` would import a SECOND copy of the module and
# return a fresh, empty AppState instead of the live one.
_bot_state: Any = None


def set_state(state: Any) -> None:
    global _bot_state
    _bot_state = state


def _state() -> Any:
    if _bot_state is None:
        raise HTTPException(status_code=503, detail="bot state not attached")
    return _bot_state


def _conn() -> Any:
    conn = getattr(_state(), "db_conn", None)
    if conn is None:
        raise HTTPException(status_code=503, detail="database not initialised")
    return conn


def _read_template(name: str) -> str:
    path = TEMPLATES_DIR / name
    if path.exists():
        content = path.read_text(encoding="utf-8")
        return content.replace("__ASSET_VERSION__", ASSET_VERSION)
    return f"<h1>{name} not found</h1>"


def _render(name: str) -> HTMLResponse:
    return HTMLResponse(content=_read_template(name))


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> Any:
    if auth.current_session(request) is not None:
        return RedirectResponse(
            _safe_next(request.query_params.get("next")), status_code=302
        )
    return _render("login.html")


@app.get("/api/auth/me")
async def api_auth_me(request: Request) -> dict[str, Any]:
    session = auth.current_session(request)
    return {
        "authenticated": session is not None,
        "user": session.user if session else None,
        "auth_required": auth.auth_required(),
        "credentials_configured": auth.credentials_configured(),
    }


@app.post("/api/auth/login")
async def api_auth_login(
    payload: LoginPayload, request: Request, response: Response
) -> dict[str, Any]:
    ip = auth.client_ip(request)
    if not auth.credentials_configured():
        raise HTTPException(
            status_code=503,
            detail="管理员尚未设置 WEB_PASSWORD，无法登录。请在部署环境变量中设置后重启。",
        )
    if auth.login_blocked(ip):
        raise HTTPException(
            status_code=429, detail="登录失败次数过多，请 5 分钟后再试"
        )
    if not auth.verify_credentials(payload.username, payload.password):
        auth.record_login_failure(ip)
        log.warning("web.login_failed", user=payload.username, ip=ip)
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    token = auth.create_session(payload.username)
    auth.set_session_cookie(response, token, request)
    auth.reset_login_failures(ip)
    log.info("web.login_ok", user=payload.username, ip=ip)
    return {"ok": True, "user": payload.username}


@app.post("/api/auth/logout")
async def api_auth_logout(
    request: Request, response: Response
) -> dict[str, Any]:
    auth.delete_session(request.cookies.get(auth.SESSION_COOKIE))
    auth.clear_session_cookie(response)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def dashboard_page(request: Request) -> Any:
    redirect = _login_redirect(request)
    if redirect is not None:
        return redirect
    return _render("dashboard.html")


@app.get("/trades", response_class=HTMLResponse)
async def trades_page(request: Request) -> Any:
    redirect = _login_redirect(request)
    if redirect is not None:
        return redirect
    return _render("trades.html")


@app.get("/config", response_class=HTMLResponse)
async def config_page(request: Request) -> Any:
    # Never public — even when the rest of the dashboard is.
    if auth.current_session(request) is None:
        return RedirectResponse("/login?next=/config", status_code=302)
    return _render("config.html")


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

@app.get("/api/health")
async def api_health() -> dict[str, Any]:
    """Liveness probe — intentionally unauthenticated for Coolify."""
    return {"status": "ok", "ts": int(time.time())}


@app.get("/api/status")
async def api_status(_: None = Depends(_require_auth)) -> dict[str, Any]:
    return status_mod.build_status(_state())


@app.get("/api/rounds")
async def api_rounds(
    limit: int = Query(50, ge=1, le=500),
    _: None = Depends(_require_auth),
) -> dict[str, Any]:
    rounds = status_mod.rounds_with_pnl(_conn(), limit)
    return {"rounds": rounds, "stats": status_mod.compute_stats(rounds)}


@app.get("/api/fills")
async def api_fills(
    limit: int = Query(200, ge=1, le=2000),
    _: None = Depends(_require_auth),
) -> dict[str, Any]:
    return {"fills": status_mod.recent_fills(_conn(), limit)}


@app.get("/api/trades")
async def api_trades(
    limit: int = Query(100, ge=1, le=500),
    _: None = Depends(_require_auth),
) -> dict[str, Any]:
    rounds = status_mod.rounds_with_pnl(_conn(), limit)
    return {
        "rounds": rounds,
        "fills": status_mod.recent_fills(_conn(), limit * 2),
        "stats": status_mod.compute_stats(rounds),
    }


@app.get("/api/rounds/{round_start_ts}/fills")
async def api_round_fills(
    round_start_ts: int,
    _: None = Depends(_require_auth),
) -> dict[str, Any]:
    return {
        "round_start_ts": round_start_ts,
        "fills": status_mod.fills_for_round(_conn(), round_start_ts),
    }


# ---------------------------------------------------------------------------
# Runtime strategy config (login required, always)
# ---------------------------------------------------------------------------

@app.get("/api/config")
async def api_get_config(
    _: str = Depends(_require_config_auth),
) -> dict[str, Any]:
    from src.web import runtime_config as rc

    return {
        "values": rc.current_values(),
        "overrides": rc.load_overrides(),
        "spec": rc.spec_payload(),
        "auth_enabled": bool(auth.credentials_configured()),
    }


@app.put("/api/config")
async def api_update_config(
    patch: dict[str, Any],
    _: str = Depends(_require_config_auth),
) -> dict[str, Any]:
    from src.web import runtime_config as rc

    try:
        values = rc.update(patch)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    return {"ok": True, "values": values, "overrides": rc.load_overrides()}


@app.post("/api/config/reset")
async def api_reset_config(
    _: str = Depends(_require_config_auth),
) -> dict[str, Any]:
    from src.web import runtime_config as rc

    return {"ok": True, "values": rc.reset(), "overrides": {}}


@app.get("/api/wallet")
async def api_wallet(
    refresh: bool = False,
    _: str = Depends(_require_config_auth),
) -> dict[str, Any]:
    """Funder wallet balance for the config page (login required)."""
    from src.web import wallet as wallet_mod

    return await wallet_mod.wallet_snapshot(_state(), force=refresh)
