"""Wallet balance snapshot for the dashboard.

Reads the funder wallet's pUSD collateral (`get_balance_allowance`) and,
when a position is open, the outcome-token share balance. The CLOB REST
call is cached briefly because the config page may be refreshed often and
the endpoint is rate-limited.

Everything is best-effort: a failed query returns ``None`` fields plus an
``error`` string. It must never be rendered as a zero balance — the bot
itself treats a failed balance query as "unknown", not "empty".
"""
from __future__ import annotations

import time
from typing import Any

from src.infra.logging import get_logger

log = get_logger(__name__)

CACHE_TTL_S = 15.0

_cache: dict[str, Any] = {"ts": 0.0, "data": None}


def _funder(clob: Any) -> str:
    try:
        return str(getattr(clob, "funder_address", "") or "")
    except Exception:  # noqa: BLE001
        return ""


def _signature_type(clob: Any) -> int:
    try:
        return int(getattr(clob, "signature_type", 0) or 0)
    except Exception:  # noqa: BLE001
        return 0


async def wallet_snapshot(state: Any, force: bool = False) -> dict[str, Any]:
    now = time.time()
    if (
        not force
        and _cache["data"] is not None
        and now - _cache["ts"] < CACHE_TTL_S
    ):
        return _cache["data"]

    clob = getattr(state, "clob", None)
    configured = bool(
        clob is not None
        and getattr(clob, "is_configured", lambda: False)()
    )

    data: dict[str, Any] = {
        "configured": configured,
        "funder": _funder(clob) if clob is not None else "",
        "signature_type": _signature_type(clob) if clob is not None else 0,
        "balance_usd": None,
        "allowance_usd": None,
        "position_token": None,
        "position_shares": None,
        "error": None,
        "updated_at": int(now),
    }

    if not configured:
        data["error"] = "CLOB 未配置（缺少 API 凭证）"
        _cache.update(ts=now, data=data)
        return data

    try:
        collateral = await clob.get_collateral()
        if collateral is None:
            data["error"] = "余额查询失败（CLOB REST 无响应）"
        else:
            data["balance_usd"] = collateral.get("balance")
            data["allowance_usd"] = collateral.get("allowance")

        # Shares held for the current round (if any). Uses the position's
        # entry token, so it is empty outside an open position.
        engine = getattr(state, "engine", None)
        round_state = getattr(engine, "_round", None) if engine else None
        position = getattr(round_state, "position", None) if round_state else None
        token = getattr(position, "entry_token", None) if position else None
        if token:
            data["position_token"] = token
            shares = await clob.get_token_balance(token)
            if shares is not None:
                data["position_shares"] = shares
    except Exception as exc:  # noqa: BLE001
        data["error"] = f"余额查询异常: {exc}"
        log.warning("wallet.snapshot_failed", error=str(exc))

    _cache.update(ts=now, data=data)
    return data


def invalidate() -> None:
    """Drop the cache (e.g. after an order fill)."""
    _cache.update(ts=0.0, data=None)
