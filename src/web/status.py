"""Read-only status/history builders for the web dashboard.

Two data sources:

  * live bot state (`src.main.STATE`) — current round, gate drift, position,
    tier state and open orders. Read in-process, no locking needed: the
    FastAPI server runs on the bot's event loop and these builders are
    synchronous, so they can never interleave with a mutation.
  * SQLite (`polymarket.db`) — rounds / orders / fills history, mirrored by
    `OrderManager` (`db=` argument).

Nothing here mutates bot state.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from src.config import strategy as _strategy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iso(ts: float | int | None) -> str | None:
    if not ts:
        return None
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()


def _tier_status(t: Any) -> str:
    if getattr(t, "is_complete", False):
        return "filled"
    if getattr(t, "watching", False):
        return "watching"
    if getattr(t, "placed", False):
        return "placed"
    return "idle"


def _tier_payload(t: Any) -> dict[str, Any]:
    return {
        "tier": int(getattr(t, "tier", 0)),
        "notional": float(getattr(t, "notional", 0.0) or 0.0),
        "status": _tier_status(t),
        "placed": bool(getattr(t, "placed", False)),
        "watching": bool(getattr(t, "watching", False)),
        "filled": bool(getattr(t, "is_complete", False)),
        "size_filled": float(getattr(t, "size_filled", 0.0) or 0.0),
        "size_expected": float(getattr(t, "size_expected", 0.0) or 0.0),
        "avg_fill_price": getattr(t, "avg_fill_price", None),
        "server_order_id": getattr(t, "server_order_id", None),
    }


# ---------------------------------------------------------------------------
# Live status
# ---------------------------------------------------------------------------

def build_status(state: Any) -> dict[str, Any]:
    """Snapshot of the running bot, safe to serialise to JSON."""
    s = _strategy()
    wm = getattr(state, "wm", None)
    ctx = wm.get("BTC") if wm else None
    mkt = getattr(ctx, "market", None) if ctx else None
    eng = getattr(state, "engine", None)
    rs = getattr(eng, "_round", None) if eng else None
    pos = getattr(rs, "position", None) if rs else None
    pc = state.price_cache
    bc = float(getattr(state.binance_cache, "last_price", 0.0) or 0.0)

    up_token = (mkt.up_token if mkt else "") or ""
    down_token = (mkt.down_token if mkt else "") or ""
    up_mid = float(pc.get_midpoint(up_token)) if up_token else 0.0
    down_mid = float(pc.get_midpoint(down_token)) if down_token else 0.0

    # ── Window / phase ──────────────────────────────────────────────────
    period_s = int(wm.window_period_s()) if wm else int(s.T1_ENTRY_WINDOW_S)
    try:
        remain_s = max(0.0, min(float(period_s), float(wm.seconds_until_next())))
    except Exception:
        remain_s = 0.0
    elapsed_s = max(0.0, period_s - remain_s)
    if remain_s < 1.0:
        phase = "END"
    elif elapsed_s <= s.T1_ENTRY_WINDOW_S:
        phase = "IN_T1"
    else:
        phase = "PAST_T1"

    # ── Gate ────────────────────────────────────────────────────────────
    gate: dict[str, Any] = {
        "state": "pending",
        "baseline": None,
        "drift": None,
        "limit": float(s.BASELINE_DRIFT_LIMIT),
        "binance": bc,
    }
    if eng and getattr(eng, "_gate", None) and bc > 0:
        gs = eng._gate.state(bc)
        gate.update(
            state="PASS" if gs.within_limit else "FAIL",
            baseline=float(gs.binance_baseline),
            drift=float(gs.drift),
        )

    # ── Position ────────────────────────────────────────────────────────
    position = None
    if pos is not None:
        qty = float(pos.total_qty)
        cost = float(pos.total_cost)
        avg = float(pos.avg_cost)
        held_mid = 0.0
        if pos.entry_side == "UP":
            held_mid = up_mid
        elif pos.entry_side == "DOWN":
            held_mid = down_mid
        upnl = (held_mid - avg) * qty if qty > 0 and held_mid > 0 else 0.0
        position = {
            "side": pos.entry_side,
            "token": pos.entry_token,
            "qty": qty,
            "avg_cost": avg,
            "cost": cost,
            "mid": held_mid,
            "upnl_usd": upnl,
            "upnl_pct": (upnl / cost) if cost > 0 else 0.0,
            "tp_target": pos.tp_target(),
            "tiers": [_tier_payload(t) for t in getattr(pos, "tiers", [])],
        }

    # ── Open orders (live GTDs this round) ──────────────────────────────
    orders: list[dict[str, Any]] = []
    om = getattr(state, "order_manager", None)
    if om is not None and rs is not None:
        try:
            for o in om.open_orders_for_round(rs.round_start_ts):
                orders.append({
                    "client_order_id": o.client_order_id,
                    "server_order_id": o.server_order_id,
                    "side": o.side,
                    "tier": o.tier_name,
                    "price": o.price,
                    "size": o.placed_size or o.size,
                    "size_matched": o.size_matched,
                    "status": o.status,
                    "expires_at": o.expires_at,
                    "age_s": max(0.0, time.time() - o.placed_at),
                })
        except Exception:
            orders = []

    ks = getattr(state, "killswitch", None)
    now_ts = int(time.time())
    return {
        "server_time": now_ts,
        "asset": "BTC",
        "killswitch": {
            "active": bool(getattr(ks, "active", False)),
            "since": getattr(ks, "since", 0.0) or None,
            "reason": getattr(ks, "reason", "") or None,
        },
        "round_start_ts": getattr(rs, "round_start_ts", None),
        "slug": getattr(rs, "slug", None) or getattr(mkt, "slug", None),
        "condition_id": getattr(mkt, "condition_id", None),
        "skip_reason": getattr(rs, "skip_reason", None),
        "window": {
            "period_s": period_s,
            "remain_s": remain_s,
            "elapsed_s": elapsed_s,
            "phase": phase,
            "start": _iso(getattr(mkt, "window_start", None) and mkt.window_start.timestamp()),
            "end": _iso(getattr(mkt, "window_end", None) and mkt.window_end.timestamp()),
        },
        "gate": gate,
        "prices": {
            "up_token": up_token,
            "down_token": down_token,
            "up_mid": up_mid,
            "down_mid": down_mid,
            "entry_price_cap": float(s.ENTRY_PRICE_CAP),
        },
        "position": position,
        "orders": orders,
        "config": {
            "t1_entry_window_s": float(s.T1_ENTRY_WINDOW_S),
            "drift_limit": float(s.BASELINE_DRIFT_LIMIT),
        },
    }


# ---------------------------------------------------------------------------
# History (SQLite)
# ---------------------------------------------------------------------------

_ROUNDS_SQL = """
SELECT r.round_start_ts,
       r.slug,
       r.started_at,
       r.finished_at,
       COALESCE(SUM(CASE WHEN f.side = 'BUY'  THEN f.price * f.size END), 0.0) AS buy_cost,
       COALESCE(SUM(CASE WHEN f.side = 'SELL' THEN f.price * f.size END), 0.0) AS sell_proceeds,
       COUNT(f.fill_id) AS fill_count
FROM rounds r
LEFT JOIN fills f ON f.round_start_ts = r.round_start_ts
GROUP BY r.round_start_ts
ORDER BY r.round_start_ts DESC
LIMIT ?
"""


def rounds_with_pnl(conn: Any, limit: int = 100) -> list[dict[str, Any]]:
    """Rounds with realised PnL derived from fills.

    A round is `closed` once it has at least one SELL fill. Rounds with only
    BUY fills are still open (or were abandoned) and report `pnl_usd=None`
    so they are excluded from win-rate / realised-PnL stats.
    """
    rows = conn.execute(_ROUNDS_SQL, (int(limit),)).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        (round_ts, slug, started_at, finished_at,
         buy_cost, sell_proceeds, fill_count) = row
        buy_cost = float(buy_cost or 0.0)
        sell_proceeds = float(sell_proceeds or 0.0)
        closed = sell_proceeds > 0
        pnl = (sell_proceeds - buy_cost) if closed else None
        out.append({
            "round_start_ts": round_ts,
            "slug": slug,
            "started_at": started_at,
            "finished_at": finished_at,
            "buy_cost": buy_cost,
            "sell_proceeds": sell_proceeds,
            "fill_count": int(fill_count or 0),
            "closed": closed,
            "pnl_usd": pnl,
            "pnl_pct": (pnl / buy_cost) if (closed and buy_cost > 0) else None,
        })
    return out


def compute_stats(rounds: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [r for r in rounds if r["closed"]]
    wins = [r for r in closed if (r["pnl_usd"] or 0.0) > 0]
    losses = [r for r in closed if (r["pnl_usd"] or 0.0) <= 0]
    total_pnl = sum(r["pnl_usd"] or 0.0 for r in closed)
    gross_win = sum(r["pnl_usd"] for r in wins)
    gross_loss = abs(sum(r["pnl_usd"] for r in losses))
    today_start = int(
        datetime.now(timezone.utc)
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .timestamp()
    )
    today = [
        r for r in closed
        if int(r["round_start_ts"] or 0) >= today_start
    ]
    return {
        "closed_rounds": len(closed),
        "open_rounds": len(rounds) - len(closed),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": (len(wins) / len(closed)) if closed else None,
        "total_pnl_usd": total_pnl,
        "today_pnl_usd": sum(r["pnl_usd"] or 0.0 for r in today),
        "today_rounds": len(today),
        "avg_win_usd": (gross_win / len(wins)) if wins else 0.0,
        "avg_loss_usd": (-gross_loss / len(losses)) if losses else 0.0,
        "profit_factor": (gross_win / gross_loss) if gross_loss > 0 else None,
    }


_FILLS_SQL = """
SELECT f.fill_id,
       f.round_start_ts,
       f.side,
       f.price,
       f.size,
       f.ts_ms,
       COALESCE(o.tier_name, '') AS tier_name,
       COALESCE(r.slug, '') AS slug
FROM fills f
LEFT JOIN orders o ON o.client_order_id = f.order_id
LEFT JOIN rounds r ON r.round_start_ts = f.round_start_ts
ORDER BY f.ts_ms DESC
LIMIT ?
"""


def recent_fills(conn: Any, limit: int = 200) -> list[dict[str, Any]]:
    rows = conn.execute(_FILLS_SQL, (int(limit),)).fetchall()
    out = []
    for fill_id, round_ts, side, price, size, ts_ms, tier_name, slug in rows:
        notional = float(price or 0.0) * float(size or 0.0)
        out.append({
            "fill_id": fill_id,
            "round_start_ts": round_ts,
            "side": side,
            "price": float(price or 0.0),
            "size": float(size or 0.0),
            "notional": notional,
            "ts_ms": ts_ms,
            "tier": tier_name,
            "slug": slug,
        })
    return out


def fills_for_round(conn: Any, round_start_ts: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT f.fill_id, f.side, f.price, f.size, f.ts_ms,
               COALESCE(o.tier_name, '') AS tier_name
        FROM fills f
        LEFT JOIN orders o ON o.client_order_id = f.order_id
        WHERE f.round_start_ts = ?
        ORDER BY f.ts_ms ASC
        """,
        (int(round_start_ts),),
    ).fetchall()
    return [
        {
            "fill_id": r[0], "side": r[1], "price": float(r[2] or 0.0),
            "size": float(r[3] or 0.0), "ts_ms": r[4], "tier": r[5],
        }
        for r in rows
    ]
