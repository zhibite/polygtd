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
        tiers = [_tier_payload(t) for t in getattr(pos, "tiers", [])]
        if qty <= 1e-9:
            # Flat position: never show leftover filled/placed tier rows.
            # The engine clears them on its next tick, but the dashboard may
            # poll inside that window — and this also covers legacy state
            # (production 2026-10-31 09:02: T1 displayed filled 5/5 after a
            # TP had closed the whole position).
            for payload in tiers:
                payload.update(
                    status="idle",
                    placed=False,
                    watching=False,
                    filled=False,
                    size_filled=0.0,
                    size_expected=0.0,
                    avg_fill_price=None,
                    server_order_id=None,
                )
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
            "tiers": tiers,
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
       COUNT(f.fill_id) AS fill_count,
       (SELECT COUNT(*) FROM orders o
         WHERE o.round_start_ts = r.round_start_ts) AS order_count,
       (SELECT COUNT(*) FROM orders o3
         WHERE o3.round_start_ts = r.round_start_ts
           AND o3.status = 'cancelled') AS cancel_count
FROM rounds r
LEFT JOIN fills f ON f.round_start_ts = r.round_start_ts
WHERE EXISTS (SELECT 1 FROM orders o2 WHERE o2.round_start_ts = r.round_start_ts)
   OR EXISTS (SELECT 1 FROM fills  f2 WHERE f2.round_start_ts = r.round_start_ts)
GROUP BY r.round_start_ts
ORDER BY r.round_start_ts DESC
LIMIT ?
"""


def _round_realized_pnl(
    conn: Any, wanted: set[int]
) -> tuple[
    dict[int, float], dict[int, float], dict[int, float],
    dict[int, float], dict[int, int], dict[int, float],
]:
    """Walk each round's fills in time order and return REALISED PnL.

    Uses the same average-cost accounting as the engine: BUY fills grow the
    position and its cost basis, SELL fills realise
    `(sell_price - avg_cost) * sold_qty` and reduce the basis pro-rata.

    Why this exists (operator 2026-10-31): the old round PnL was
    `sell_proceeds - buy_cost`, so a same-window re-entry after a TP made
    the round look like a loss — the fresh BUY's cost was subtracted even
    though that position had not been sold yet (first TP +0.5 then a 2u
    re-entry displayed -1.5). Realised PnL counts each SELL when it fills;
    an open re-entry does not touch it.

    Fees (Polymarket taker fee, stored per fill) are included: a BUY fee
    is added to the cost basis, a SELL fee is deducted from the proceeds.

    Returns dicts keyed by round_start_ts:
      realized, cost_of_sold, open_qty, pnl_today, sells_today, fees_total
    """
    realized: dict[int, float] = {}
    cost_sold: dict[int, float] = {}
    open_qty: dict[int, float] = {}
    pnl_today: dict[int, float] = {}
    sells_today: dict[int, int] = {}
    fees_total: dict[int, float] = {}
    today_start_ms = int(
        datetime.now(timezone.utc)
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .timestamp() * 1000
    )
    state: dict[int, list[float]] = {}  # round -> [qty, cost basis]
    for round_ts, side, price, size, ts_ms, fee in conn.execute(
        "SELECT round_start_ts, side, price, size, ts_ms, COALESCE(fee, 0) "
        "FROM fills "
        "ORDER BY round_start_ts ASC, ts_ms ASC, fill_id ASC"
    ):
        if round_ts not in wanted:
            continue
        try:
            px = float(price or 0.0)
            sz = float(size or 0.0)
            fee_f = float(fee or 0.0)
        except (TypeError, ValueError):
            continue
        if px <= 0 or sz <= 0:
            continue
        st = state.setdefault(round_ts, [0.0, 0.0])
        fees_total[round_ts] = fees_total.get(round_ts, 0.0) + fee_f
        if str(side or "").upper() == "BUY":
            st[0] += sz
            st[1] += px * sz + fee_f
            continue
        # SELL: realise only against shares actually held.
        sold = min(sz, st[0])
        if sold <= 1e-12:
            continue
        avg = st[1] / st[0]
        fee_sold = fee_f * (sold / sz)
        proceeds = px * sold - fee_sold
        pnl = proceeds - avg * sold
        realized[round_ts] = realized.get(round_ts, 0.0) + pnl
        cost_sold[round_ts] = cost_sold.get(round_ts, 0.0) + avg * sold
        try:
            ts_ms_i = int(ts_ms or 0)
        except (TypeError, ValueError):
            ts_ms_i = 0
        if ts_ms_i >= today_start_ms:
            pnl_today[round_ts] = pnl_today.get(round_ts, 0.0) + pnl
            sells_today[round_ts] = sells_today.get(round_ts, 0) + 1
        st[0] -= sold
        st[1] -= avg * sold
        if st[0] <= 1e-9:
            st[0] = 0.0
            st[1] = 0.0
    open_qty = {ts: st[0] for ts, st in state.items()}
    return realized, cost_sold, open_qty, pnl_today, sells_today, fees_total


def rounds_with_pnl(conn: Any, limit: int = 100) -> list[dict[str, Any]]:
    """Rounds with realised PnL derived from fills.

    Realised PnL is computed per SELL fill against the round's running
    average cost, so each take-profit is booked the moment it fills — a
    same-window re-entry does not subtract its (still open) buy cost.

    Status:
      * `closed`  — at least one SELL and the position is flat
      * `holding` — realised some sells but still holds inventory
      * `open`    — holds inventory, no sells yet (pnl_usd is None)
      * `unfilled`— orders but no fills at all

    Rounds with neither orders nor fills are excluded entirely. Older
    builds wrote a `rounds` row at every window boundary, so windows where
    nothing was ever bought (e.g. every BUY rejected for insufficient
    balance) lingered in the history as phantom open positions.
    """
    rows = conn.execute(_ROUNDS_SQL, (int(limit),)).fetchall()
    wanted = {int(row[0]) for row in rows}
    if wanted:
        (realized, cost_sold, open_qty, pnl_today, sells_today,
         fees_total) = _round_realized_pnl(conn, wanted)
    else:
        realized, cost_sold, open_qty, pnl_today, sells_today, fees_total = (
            {}, {}, {}, {}, {}, {}
        )
    out: list[dict[str, Any]] = []
    for row in rows:
        (round_ts, slug, started_at, finished_at,
         buy_cost, sell_proceeds, fill_count, order_count,
         cancel_count) = row
        buy_cost = float(buy_cost or 0.0)
        sell_proceeds = float(sell_proceeds or 0.0)
        fill_count = int(fill_count or 0)
        has_sells = sell_proceeds > 0
        qty_open = float(open_qty.get(round_ts, 0.0))
        if qty_open > 1e-9:
            round_status = "holding" if has_sells else "open"
        elif has_sells:
            round_status = "closed"
        elif fill_count > 0:
            round_status = "open"
        else:
            round_status = "unfilled"
        pnl = float(realized.get(round_ts, 0.0)) if has_sells else None
        cost_sold_round = float(cost_sold.get(round_ts, 0.0))
        out.append({
            "round_start_ts": round_ts,
            "slug": slug,
            "started_at": started_at,
            "finished_at": finished_at,
            "buy_cost": buy_cost,
            "sell_proceeds": sell_proceeds,
            "fill_count": fill_count,
            "order_count": int(order_count or 0),
            "cancel_count": int(cancel_count or 0),
            # Taker fees recorded on this round's fills (USDC).
            "fee_total": float(fees_total.get(round_ts, 0.0)),
            "status": round_status,
            "closed": round_status == "closed",
            "pnl_usd": pnl,
            "pnl_pct": (
                (pnl / cost_sold_round)
                if (pnl is not None and cost_sold_round > 0)
                else None
            ),
            # Realised PnL booked TODAY (by sell fill timestamp), so the
            # dashboard reflects a TP immediately, not at the round's end.
            "pnl_today_usd": (
                float(pnl_today.get(round_ts, 0.0)) if has_sells else 0.0
            ),
            "sells_today": int(sells_today.get(round_ts, 0)),
            "open_qty": qty_open,
        })
    return out


def compute_stats(rounds: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [r for r in rounds if r["status"] == "closed"]
    # "未平" includes rounds still holding inventory, whether or not they
    # already realised some sells (same-window re-entry).
    open_rounds = [r for r in rounds if r["status"] in ("open", "holding")]
    unfilled = [r for r in rounds if r["status"] == "unfilled"]
    # Win/loss is judged on REALISED PnL per round (a holding round with a
    # profitable TP already counts).
    traded = [r for r in rounds if r["sell_proceeds"] > 0]
    wins = [r for r in traded if (r["pnl_usd"] or 0.0) > 0]
    losses = [r for r in traded if (r["pnl_usd"] or 0.0) <= 0]
    total_pnl = sum(r["pnl_usd"] or 0.0 for r in rounds)
    gross_win = sum(r["pnl_usd"] for r in wins)
    gross_loss = abs(sum(r["pnl_usd"] for r in losses))
    today_pnl = sum(float(r.get("pnl_today_usd") or 0.0) for r in rounds)
    today_rounds = sum(
        1 for r in rounds if int(r.get("sells_today") or 0) > 0
    )
    return {
        "closed_rounds": len(closed),
        "open_rounds": len(open_rounds),
        "unfilled_rounds": len(unfilled),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": (len(wins) / len(traded)) if traded else None,
        "total_pnl_usd": total_pnl,
        "today_pnl_usd": today_pnl,
        "today_rounds": today_rounds,
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
       COALESCE(r.slug, '') AS slug,
       COALESCE(o.price, 0.0) AS limit_price,
       COALESCE(f.trader_side, '') AS trader_side,
       COALESCE(f.fee, 0.0) AS fee
FROM fills f
LEFT JOIN orders o ON o.client_order_id = f.order_id
LEFT JOIN rounds r ON r.round_start_ts = f.round_start_ts
ORDER BY f.ts_ms DESC, f.fill_id DESC
LIMIT ? OFFSET ?
"""


def _improvement_bps(side: str, price: float, limit_price: float) -> float | None:
    """Signed price improvement vs the posted limit, in basis points.

    Positive means the fill beat the limit: a BUY filled below its limit
    price, or a SELL above it. Maker fills land exactly on the limit
    (0 bps); crossing (taker) fills usually show positive improvement.
    """
    if price <= 0 or limit_price <= 0:
        return None
    side_u = str(side or "").upper()
    if side_u == "BUY":
        return (limit_price - price) / limit_price * 10000.0
    if side_u == "SELL":
        return (price - limit_price) / limit_price * 10000.0
    return None


def count_fills(conn: Any) -> int:
    """Total number of fill rows (pagination denominator)."""
    row = conn.execute("SELECT COUNT(*) FROM fills").fetchone()
    return int(row[0] or 0) if row else 0


def recent_fills(
    conn: Any, limit: int = 200, offset: int = 0
) -> list[dict[str, Any]]:
    """Fill rows, newest first.

    `offset` powers the 成交明细 pager: the query is deterministically
    ordered by (ts_ms, fill_id) so consecutive pages neither overlap nor
    skip a row when several fills share a millisecond.
    """
    rows = conn.execute(
        _FILLS_SQL, (int(limit), max(0, int(offset)))
    ).fetchall()
    out = []
    for (fill_id, round_ts, side, price, size, ts_ms,
         tier_name, slug, limit_price, trader_side, fee) in rows:
        price = float(price or 0.0)
        size = float(size or 0.0)
        limit_price = float(limit_price or 0.0)
        fee = float(fee or 0.0)
        notional = price * size
        out.append({
            "fill_id": fill_id,
            "round_start_ts": round_ts,
            "side": side,
            "price": price,
            "size": size,
            "notional": notional,
            "ts_ms": ts_ms,
            "tier": tier_name,
            "slug": slug,
            "limit_price": limit_price,
            "trader_side": str(trader_side or "").upper(),
            "fee": fee,
            "improvement_bps": _improvement_bps(side, price, limit_price),
        })
    return out


def execution_quality(conn: Any, limit: int = 500) -> dict[str, Any]:
    """Execution-quality stats for the dashboard.

    Covers the four levers we tune when adjusting the order logic:
      * maker vs taker mix (WS `trader_side`; REST-only observations are
        counted as unknown),
      * realised price improvement vs the posted limit (SELL premium and
        BUY discount, in bps),
      * cancel rate (cancelled orders / all orders),
      * average time from placement to first fill.
    """
    rows = conn.execute(
        """
        SELECT f.side, f.price, COALESCE(o.price, 0.0), f.trader_side
        FROM fills f
        LEFT JOIN orders o ON o.client_order_id = f.order_id
        ORDER BY f.ts_ms DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()

    maker = taker = unknown = 0
    buy_improvements: list[float] = []
    sell_improvements: list[float] = []
    for side, price, limit_price, trader_side in rows:
        role = str(trader_side or "").upper()
        if role == "MAKER":
            maker += 1
        elif role == "TAKER":
            taker += 1
        else:
            unknown += 1
        bps = _improvement_bps(side, float(price or 0.0), float(limit_price or 0.0))
        if bps is None:
            continue
        if str(side or "").upper() == "BUY":
            buy_improvements.append(bps)
        elif str(side or "").upper() == "SELL":
            sell_improvements.append(bps)

    order_row = conn.execute(
        "SELECT COUNT(*), "
        "COALESCE(SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END), 0) "
        "FROM orders"
    ).fetchone()
    total_orders = int(order_row[0] or 0) if order_row else 0
    cancelled_orders = int(order_row[1] or 0) if order_row else 0

    latency_row = conn.execute(
        """
        SELECT AVG((ff.first_ts - o.placed_at * 1000.0) / 1000.0)
        FROM orders o
        JOIN (
            SELECT order_id, MIN(ts_ms) AS first_ts
            FROM fills GROUP BY order_id
        ) ff ON ff.order_id = o.client_order_id
        WHERE o.placed_at > 0
          AND ff.first_ts >= o.placed_at * 1000.0
          AND ff.first_ts - o.placed_at * 1000.0 < 900000.0
        """
    ).fetchone()
    avg_fill_time = (
        float(latency_row[0])
        if latency_row and latency_row[0] is not None
        else None
    )

    known = maker + taker
    return {
        "maker_fills": maker,
        "taker_fills": taker,
        "unknown_fills": unknown,
        "maker_ratio": (maker / known) if known else None,
        "buy_improvement_bps": (
            sum(buy_improvements) / len(buy_improvements)
            if buy_improvements else None
        ),
        "sell_improvement_bps": (
            sum(sell_improvements) / len(sell_improvements)
            if sell_improvements else None
        ),
        "total_orders": total_orders,
        "cancelled_orders": cancelled_orders,
        "cancel_ratio": (cancelled_orders / total_orders) if total_orders else None,
        "avg_time_to_fill_s": avg_fill_time,
    }


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
