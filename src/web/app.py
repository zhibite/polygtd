"""FastAPI dashboard for the polygtd bot.

Runs inside the bot process (see `src/main.py`), on the same asyncio event
loop, so `/api/status` reads live engine state without locks. History comes
from the SQLite file the bot already writes.

Start-up:  `uvicorn.Server(...).serve()` is scheduled from `main()`.
Auth:      optional HTTP Basic. Set WEB_PASSWORD (and optionally WEB_USERNAME)
           in the environment / Coolify panel to lock the dashboard. When
           WEB_PASSWORD is empty the dashboard is public — the `web.auth_disabled`
           warning is logged at start-up.
Routes:
           GET /               dashboard page
           GET /trades         trade history page
           GET /api/health     liveness probe (never authenticated)
           GET /api/status     live snapshot
           GET /api/rounds     rounds + derived PnL
           GET /api/fills      recent fills
           GET /api/trades     rounds + fills + aggregate stats
"""
from __future__ import annotations

import secrets
import time
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles

from src.config import strategy as _strategy
from src.infra.logging import get_logger
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
_security = HTTPBasic(auto_error=False)


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _authorized(creds: HTTPBasicCredentials | None) -> bool:
    cfg = _strategy()
    if not cfg.WEB_PASSWORD:
        return True
    if creds is None:
        return False
    expected_user = cfg.WEB_USERNAME or "admin"
    return (
        secrets.compare_digest(creds.username, expected_user)
        and secrets.compare_digest(creds.password, cfg.WEB_PASSWORD)
    )


async def _require_auth(
    creds: HTTPBasicCredentials | None = Depends(_security),
) -> None:
    if not _authorized(creds):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": 'Basic realm="polygtd"'},
        )


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
        return path.read_text(encoding="utf-8")
    return f"<h1>{name} not found</h1>"


def _render(name: str) -> HTMLResponse:
    return HTMLResponse(content=_read_template(name))


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def dashboard_page(_: None = Depends(_require_auth)) -> HTMLResponse:
    return _render("dashboard.html")


@app.get("/trades", response_class=HTMLResponse)
async def trades_page(_: None = Depends(_require_auth)) -> HTMLResponse:
    return _render("trades.html")


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
