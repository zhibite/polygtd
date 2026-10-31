"""Replay model for the polygtd BTC 15m Up/Down strategy.

This module is deliberately independent of ``src``: it receives a
:class:`WindowData` (both token klines in ~5 s bars + Binance 1 m bars)
and replays the live engine's decision rules. Keeping it standalone lets
it run on a host that only has the research package.

Replicated rules (see ``src/strategy/engine.py`` and ``src/config.py``):
  * T1: 1 USD taker buy of the side whose mid is <= ENTRY_PRICE_CAP
    (0.40), within the first T1_ENTRY_WINDOW_S seconds, BTC drift pass.
  * T2-T4: pre-arm when mid <= trigger * (1 + PRICE_PRE_MARGIN); maker
    fill at the trigger unless the order is marketable (mid <= trigger at
    arm time, in which case it fills at the mid as taker).
  * T4 extra gates: remaining time, BTC distance-to-strike z-score, and
    the "no gap far through the trigger" rule.
  * TP: sell the whole position at avg * (1 + max(TP_MIN_PCT,
    TP_PROFIT_PCT - N * TP_DECREMENT_PCT)); a <5-share position exits as
    a taker at the mid, larger ones as a maker at the target.
  * T4 post-entry two-stage stop plus the time/final exits.
  * Expiry settlement at 1/0 based on the resolved winner.

Known approximations (documented in README):
  * 15 m klines are 5 s bars; there is no bid/ask or order book. Fills are
    modelled from the bar price; maker fills are optimistic.
  * BTC is 1 m bars; the price at a 5 s tick is the open of the containing
    minute (up to 60 s stale, no lookahead).
  * A tier's fill is assumed to happen when a bar price crosses its
    trigger; partial fills / queue position are ignored.
"""
from __future__ import annotations

import bisect
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Parameters (defaults mirror .env.docker / src/config.py on 2026-10-31)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Params:
    entry_price_cap: float = 0.40
    price_pre_margin: float = 0.10

    tier_notionals: tuple[float, float, float, float] = (1.0, 2.0, 4.0, 8.0)
    tier_drop_pcts: tuple[float, float, float, float] = (0.0, 0.20, 0.40, 0.60)

    tp_profit_pct: float = 0.20
    tp_decrement_pct: float = 0.03
    tp_min_pct: float = 0.05

    t1_entry_window_s: float = 300.0
    baseline_drift_limit: float = 0.0015

    t4_min_remaining_s: float = 240.0
    t4_strike_gate_enabled: bool = True
    t4_max_strike_z: float = 2.0
    t4_strike_sigma_pct_per_min: float = 0.05
    t4_max_gap_below_trigger_pct: float = 0.05
    t4_drift_bypass_min_remaining_s: float = 540.0

    stop_loss_enabled: bool = True
    stop_loss_t4_drop_pct: float = 0.45
    stop_loss_confirm_s: float = 20.0
    stop_loss_stage1_sell_pct: float = 0.60
    stop_loss_stage2_extra_drop_pct: float = 0.30
    stop_loss_time_exit_s: float = 120.0
    stop_loss_floor: float = 0.05
    stop_loss_final_exit_s: float = 60.0
    stop_loss_final_mid: float = 0.50

    taker_fee_rate: float = 0.07

    # BTC flash-move guard: (max-min)/min over the previous 1 m bar > this
    # percentage blocks new entries/averaging. Mirrors BTC_VOL_* (1 m proxy).
    btc_vol_max_move_pct: float = 0.3


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PricePoint:
    time_ms: int
    price: float
    volume: float = 0.0


@dataclass(frozen=True)
class BtcBar:
    time_ms: int  # open time, ms
    open: float
    high: float
    low: float
    close: float


@dataclass
class WindowData:
    start_ts: int
    slug: str
    up_won: bool | None
    up: list[PricePoint]
    down: list[PricePoint]
    btc: list[BtcBar]

    @property
    def start_ms(self) -> int:
        return int(self.start_ts) * 1000

    @property
    def end_ms(self) -> int:
        return self.start_ms + 900_000


@dataclass
class Fill:
    time_ms: int
    tier: str
    price: float
    shares: float
    role: str  # TAKER | MAKER
    fee: float


@dataclass
class RoundResult:
    start_ts: int
    slug: str
    side: str | None
    entry_ms: int | None
    entry_price: float | None
    exit: str  # NO_ENTRY | TP | SL | SETTLE | UNRESOLVED
    pnl: float
    fills: list[Fill] = field(default_factory=list)
    up_won: bool | None = None
    t4_filled: bool = False
    max_shares: float = 0.0

    @property
    def cost(self) -> float:
        return sum(f.price * f.shares + f.fee for f in self.fills if f.shares > 0)

    def tier_filled(self, tier: int) -> bool:
        return any(f.tier == f"T{tier}" and f.shares > 0 for f in self.fills)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def taker_fee(shares: float, price: float, rate: float) -> float:
    """Official Polymarket taker fee: C * rate * p * (1 - p)."""
    return max(0.0, shares) * rate * price * (1.0 - price)


def tp_pct(n_average_tiers: int, p: Params) -> float:
    return max(
        p.tp_min_pct,
        p.tp_profit_pct - n_average_tiers * p.tp_decrement_pct,
    )


def _btc_at(bars: list[BtcBar], t_ms: int) -> float | None:
    """BTC price at t: the open of the containing 1 m bar (no lookahead)."""
    if not bars:
        return None
    times = [b.time_ms for b in bars]
    idx = bisect.bisect_right(times, t_ms) - 1
    if idx < 0:
        idx = 0
    return bars[idx].open


def _btc_baseline(bars: list[BtcBar], start_ms: int) -> float | None:
    if not bars:
        return None
    times = [b.time_ms for b in bars]
    idx = bisect.bisect_right(times, start_ms) - 1
    if idx < 0:
        idx = 0
    return bars[idx].open


def _btc_prev_bar(bars: list[BtcBar], t_ms: int) -> BtcBar | None:
    """Last *completed* 1 m bar before t (no lookahead)."""
    if not bars:
        return None
    times = [b.time_ms for b in bars]
    idx = bisect.bisect_right(times, t_ms - 60_000) - 1
    if idx < 0:
        return None
    return bars[idx]


def _btc_vol_blocked(bars: list[BtcBar], t_ms: int, p: Params) -> bool:
    bar = _btc_prev_bar(bars, t_ms)
    if bar is None or bar.low <= 0:
        return False
    move_pct = (bar.high - bar.low) / bar.low * 100.0
    return move_pct > p.btc_vol_max_move_pct


def _btc_sigma_1m(bars: list[BtcBar], t_ms: int, p: Params) -> float:
    """1-minute sigma estimate: previous 1 m bar range / 2, with a floor."""
    sigma = max(p.t4_strike_sigma_pct_per_min, 0.0) / 100.0
    bar = _btc_prev_bar(bars, t_ms)
    if bar is not None and bar.low > 0:
        sigma = max(sigma, (bar.high - bar.low) / bar.low / 2.0)
    return sigma


class _Series:
    """Forward-filled price view over a sorted PricePoint list."""

    def __init__(self, points: list[PricePoint]) -> None:
        self.points = sorted(points, key=lambda x: x.time_ms)
        self.times = [p.time_ms for p in self.points]

    def at(self, t_ms: int) -> float | None:
        if not self.points:
            return None
        idx = bisect.bisect_right(self.times, t_ms) - 1
        if idx < 0:
            return None
        return self.points[idx].price

    def points_from(self, t_ms: int) -> list[PricePoint]:
        idx = bisect.bisect_left(self.times, t_ms)
        return self.points[idx:]


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------


def _pick_entry(
    w: WindowData, up: _Series, down: _Series, p: Params
) -> tuple[str, int, float] | None:
    """First side with mid <= cap, inside the T1 window and drift pass.

    The live engine checks UP first, then DOWN; UP+DOWN ~ 1 means at most
    one side can be <= 0.40. Fill price = the bar mid (taker).

    Blind-spot correction: the future.news kline starts ~75 s after the
    window opens. If the *first observable* sample is already below the
    cap, the bot must have crossed the cap during those 75 s; assume it
    entered at the cap rather than at the (possibly much lower) first
    sample. Without this, every early entry would look 5-15 cents cheaper
    than it was in production.
    """
    baseline = _btc_baseline(w.btc, w.start_ms)
    if not baseline or baseline <= 0:
        return None
    times = sorted(set(up.times) | set(down.times))
    first_valid: int | None = None
    for t in times:
        if t < w.start_ms or t > w.start_ms + int(p.t1_entry_window_s * 1000):
            continue
        up_p = up.at(t)
        down_p = down.at(t)
        if up_p is None or down_p is None:
            continue
        if first_valid is None:
            first_valid = t
        if up_p <= p.entry_price_cap:
            side, price = "UP", up_p
        elif down_p <= p.entry_price_cap:
            side, price = "DOWN", down_p
        else:
            continue
        btc = _btc_at(w.btc, t)
        if btc is None or btc <= 0:
            continue
        if abs(btc - baseline) / baseline > p.baseline_drift_limit:
            continue
        if _btc_vol_blocked(w.btc, t, p):
            continue
        if t == first_valid:
            price = p.entry_price_cap
        return side, t, price
    return None


def _t4_gates_ok(
    w: WindowData, t_ms: int, price: float, side: str, trigger: float, p: Params
) -> tuple[bool, str]:
    remaining = (w.end_ms - t_ms) / 1000.0
    if p.t4_min_remaining_s > 0 and remaining < p.t4_min_remaining_s:
        return False, "late_window"
    if p.t4_max_gap_below_trigger_pct > 0:
        floor_price = trigger * (1.0 - p.t4_max_gap_below_trigger_pct)
        if price < floor_price:
            return False, "gap"
    if p.t4_strike_gate_enabled:
        baseline = _btc_baseline(w.btc, w.start_ms)
        btc = _btc_at(w.btc, t_ms)
        sigma = _btc_sigma_1m(w.btc, t_ms, p)
        if baseline and btc and sigma > 0:
            minutes = max(remaining, 1.0) / 60.0
            z = (btc - baseline) / baseline / (sigma * minutes**0.5)
            adverse = z if side == "DOWN" else -z
            if adverse > p.t4_max_strike_z:
                return False, "strike"
    return True, ""


def _drift_allows(
    w: WindowData,
    t_ms: int,
    price: float,
    t1_entry: float,
    tier_idx: int,
    p: Params,
) -> bool:
    """GateKeeper.can_double_down + the T4 time-based bypass."""
    baseline = _btc_baseline(w.btc, w.start_ms)
    btc = _btc_at(w.btc, t_ms)
    drift_ok = (
        baseline is not None
        and btc is not None
        and baseline > 0
        and abs(btc - baseline) / baseline <= p.baseline_drift_limit
    )
    if drift_ok:
        return True
    # Momentum bypass: price already at the NEXT tier's trigger.
    if tier_idx + 1 < len(p.tier_drop_pcts):
        next_trigger = t1_entry * (1.0 - p.tier_drop_pcts[tier_idx + 1])
        if price <= next_trigger:
            return True
    # T4-only time-based bypass (the z/gap gates run before this).
    if tier_idx == 3 and p.t4_drift_bypass_min_remaining_s > 0:
        remaining = (w.end_ms - t_ms) / 1000.0
        if remaining >= p.t4_drift_bypass_min_remaining_s:
            return True
    return False


def replay(w: WindowData, p: Params = Params()) -> RoundResult:
    """Replay one 15-minute window and return its simulated round."""
    up = _Series(w.up)
    down = _Series(w.down)
    result = RoundResult(
        start_ts=w.start_ts, slug=w.slug, side=None, entry_ms=None,
        entry_price=None, exit="NO_ENTRY", pnl=0.0, up_won=w.up_won,
    )

    if p.tier_notionals[0] <= 0:
        return result  # ladder disabled entirely (e.g. research variant)

    picked = _pick_entry(w, up, down, p)
    if picked is None:
        return result
    side, entry_ms, entry_price = picked
    series = up if side == "UP" else down
    result.side = side
    result.entry_ms = entry_ms
    result.entry_price = entry_price

    t1_notional = p.tier_notionals[0]
    # T1 is a FAK sized in USDC (1 USD), so shares = notional / fill price.
    t1_shares = t1_notional / entry_price
    t1_fee = taker_fee(t1_shares, entry_price, p.taker_fee_rate)
    result.fills.append(Fill(entry_ms, "T1", entry_price, t1_shares, "TAKER", t1_fee))

    shares = t1_shares
    gross_cost = t1_notional
    buy_fees = t1_fee
    proceeds = 0.0
    avg = entry_price
    t1_entry = entry_price
    result.max_shares = shares

    tier_filled = [True, False, False, False]
    tier_armed = [False, False, False, False]
    tier_pending = [False, False, False, False]
    tier_trigger = [0.0, 0.0, 0.0, 0.0]
    n_avg = 0
    t4_avg = 0.0
    t4_filled = False
    stage = 0
    below_since: int | None = None
    exit_reason = "UNRESOLVED"

    def _sell(t_ms: int, price: float, qty: float, role: str, tier: str) -> None:
        nonlocal shares, proceeds
        qty = min(qty, shares)
        if qty <= 1e-9:
            return
        fee = taker_fee(qty, price, p.taker_fee_rate) if role == "TAKER" else 0.0
        proceeds += qty * price - fee
        shares -= qty
        result.fills.append(Fill(t_ms, tier, price, -qty, role, fee))

    for pt in series.points_from(entry_ms):
        t = pt.time_ms
        if t <= entry_ms:
            continue
        if t > w.end_ms:
            break  # the live engine stops at round end
        price = pt.price
        if price <= 0:
            continue
        remaining = (w.end_ms - t) / 1000.0

        # 1) Resting BUY GTDs fill when the bar crosses their trigger.
        for idx in (1, 2, 3):
            if p.tier_notionals[idx] <= 0:
                continue
            if tier_pending[idx] and price <= tier_trigger[idx]:
                # The live GTD is sized in SHARES at the strategy price:
                # qty = notional / trigger. A maker fill lands on the
                # trigger; a marketable fill at arm time lands lower.
                fill_price = tier_trigger[idx]
                qty = p.tier_notionals[idx] / fill_price
                shares += qty
                gross_cost += qty * fill_price
                avg = gross_cost / shares
                tier_filled[idx] = True
                tier_pending[idx] = False
                n_avg += 1
                result.fills.append(
                    Fill(t, f"T{idx + 1}", fill_price, qty, "MAKER", 0.0)
                )
                if idx == 3:
                    t4_filled = True
                    t4_avg = fill_price
                result.max_shares = max(result.max_shares, shares)
                break  # one fill at a time, like the live engine

        # 2) Arm / fill the next averaging tier.
        for idx in (1, 2, 3):
            if tier_filled[idx] or p.tier_notionals[idx] <= 0:
                continue
            trigger = t1_entry * (1.0 - p.tier_drop_pcts[idx])
            fire = trigger * (1.0 + p.price_pre_margin)
            if price > fire:
                continue
            if not tier_armed[idx]:
                if idx == 3:
                    ok, _reason = _t4_gates_ok(w, t, price, side, trigger, p)
                    if not ok:
                        continue
                if not _drift_allows(w, t, price, t1_entry, idx, p):
                    continue
                if _btc_vol_blocked(w.btc, t, p):
                    continue
                tier_armed[idx] = True
                tier_trigger[idx] = trigger
                if price <= trigger:
                    # Marketable at arm time. The order is still sized in
                    # shares at the trigger (notional / trigger); the taker
                    # fill happens at the bar price, which can be lower.
                    qty = p.tier_notionals[idx] / trigger
                    fee = taker_fee(qty, price, p.taker_fee_rate)
                    shares += qty
                    gross_cost += qty * price
                    buy_fees += fee
                    avg = gross_cost / shares
                    tier_filled[idx] = True
                    n_avg += 1
                    result.fills.append(
                        Fill(t, f"T{idx + 1}", price, qty, "TAKER", fee)
                    )
                    if idx == 3:
                        t4_filled = True
                        t4_avg = price
                    result.max_shares = max(result.max_shares, shares)
                else:
                    tier_pending[idx] = True
            break  # one tier decision at a time

        # 3) Take-profit: sell the whole position at the target.
        target = avg * (1.0 + tp_pct(n_avg, p))
        if price >= target:
            if shares < 5.0:
                _sell(t, price, shares, "TAKER", "TP")
            else:
                _sell(t, target, shares, "MAKER", "TP")
            exit_reason = "TP"
            break

        # 4) Two-stage stop after any T4 fill.
        if t4_filled and p.stop_loss_enabled and shares > 0:
            line = t4_avg * (1.0 - p.stop_loss_t4_drop_pct)
            if stage == 0:
                if price <= line:
                    if below_since is None:
                        below_since = t
                    elif t - below_since >= p.stop_loss_confirm_s * 1000:
                        qty = int(shares * p.stop_loss_stage1_sell_pct * 10000) / 10000
                        _sell(t, price, qty, "TAKER", "SL1")
                        stage = 1
                        below_since = None
                else:
                    below_since = None
            else:
                stage2_line = line * (1.0 - p.stop_loss_stage2_extra_drop_pct)
                if (
                    price <= stage2_line
                    or (remaining <= p.stop_loss_time_exit_s and price <= p.stop_loss_floor)
                    or (remaining <= p.stop_loss_final_exit_s and price < p.stop_loss_final_mid)
                ):
                    _sell(t, price, shares, "TAKER", "SL2")
                    exit_reason = "SL"
                    break

    result.t4_filled = t4_filled
    if exit_reason == "UNRESOLVED":
        if shares > 1e-9:
            if w.up_won is None:
                exit_reason = "UNRESOLVED"
            else:
                won = (side == "UP") == bool(w.up_won)
                proceeds += shares * (1.0 if won else 0.0)
                shares = 0.0
                exit_reason = "SETTLE"
        else:
            exit_reason = "TP" if any(f.tier == "TP" for f in result.fills) else "SL"

    result.exit = exit_reason
    result.pnl = proceeds - gross_cost - buy_fees
    return result
