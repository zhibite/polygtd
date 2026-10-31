"""Unit tests for the research backtest replay model (synthetic series)."""
from __future__ import annotations

import pytest

from src.backtest.model import (
    BtcBar,
    Params,
    PricePoint,
    WindowData,
    replay,
    taker_fee,
)

START = 1_800_000_000  # 2027-01-15 08:00:00 UTC, divisible by 900


def _points(start_ts: int, up_path: dict[int, float]) -> tuple[list[PricePoint], list[PricePoint]]:
    """Build 5 s UP/DOWN series from {offset_s: up_price}, step-filled."""
    offsets = sorted(up_path)
    up, down = [], []
    last = None
    for off in range(-60, 960, 5):
        for o in offsets:
            if o <= off:
                last = up_path[o]
        if last is None:
            continue
        p = last
        up.append(PricePoint((start_ts + off) * 1000, p))
        down.append(PricePoint((start_ts + off) * 1000, 1.0 - p))
    return up, down


def _flat_btc(start_ts: int, price: float = 100_000.0,
              overrides: dict[int, float] | None = None) -> list[BtcBar]:
    overrides = overrides or {}
    bars = []
    for off in range(-180, 1080, 60):
        p = overrides.get(off, price)
        bars.append(BtcBar((start_ts + off) * 1000, p, p * 1.0001, p * 0.9999, p))
    return bars


def _window(up_path: dict[int, float], up_won: bool | None = True,
            btc: list[BtcBar] | None = None) -> WindowData:
    up, down = _points(START, up_path)
    return WindowData(
        start_ts=START, slug=f"btc-updown-15m-{START}", up_won=up_won,
        up=up, down=down, btc=btc or _flat_btc(START),
    )


def test_taker_fee_formula() -> None:
    assert taker_fee(10.0, 0.40, 0.07) == pytest.approx(10 * 0.07 * 0.4 * 0.6)


def test_no_entry_when_price_never_below_cap() -> None:
    r = replay(_window({0: 0.50, 900: 0.50}))
    assert r.exit == "NO_ENTRY"
    assert r.pnl == pytest.approx(0.0)


def test_t1_only_take_profit() -> None:
    # Enter UP at 0.39, then rally through the 20% T1 target (0.468).
    r = replay(_window({0: 0.50, 10: 0.39, 60: 0.39, 120: 0.48, 900: 0.48}))
    assert r.side == "UP"
    assert r.exit == "TP"
    assert r.entry_price == pytest.approx(0.39)
    assert not r.t4_filled
    # A T1-only position is <5 shares, so the live TP is a taker FAK at
    # the mid (0.48 here), not a maker order at the target.
    shares = 1 / 0.39
    expected = (
        shares * 0.48
        - taker_fee(shares, 0.48, 0.07)
        - 1.0
        - taker_fee(shares, 0.39, 0.07)
    )
    assert r.pnl == pytest.approx(expected, abs=1e-6)


def test_t4_full_stop() -> None:
    # T1 0.39 -> T2 0.31 -> T3 0.23 -> T4 0.15 -> stop at 0.08/0.05.
    r = replay(_window(
        {
            0: 0.50, 10: 0.39,
            100: 0.31, 200: 0.23, 300: 0.15,
            500: 0.08, 530: 0.08, 560: 0.05, 900: 0.05,
        }
    ))
    tiers = [f.tier for f in r.fills if f.shares > 0]
    assert tiers[:4] == ["T1", "T2", "T3", "T4"]
    assert r.t4_filled
    assert "SL1" in {f.tier for f in r.fills}
    assert "SL2" in {f.tier for f in r.fills}
    assert r.exit == "SL"
    assert r.pnl < 0


def test_t4_late_window_gate_blocks() -> None:
    # Same path but T4 triggers at +700 s (200 s left < 240 s floor).
    r = replay(_window(
        {
            0: 0.50, 10: 0.39,
            100: 0.31, 200: 0.23, 700: 0.15, 900: 0.15,
        }
    ))
    assert not r.t4_filled
    assert r.exit in ("SETTLE", "SL", "TP")


def test_t4_strike_gate_blocks_adverse_trend() -> None:
    # The underdog is UP (0.39), so an adverse BTC move is *below* the
    # strike: -0.4% with 10 min left -> z ~ 2.5 > 2.0.
    btc = _flat_btc(START, overrides={
        off: 99_600.0 for off in range(300, 900, 60)
    })
    r = replay(_window(
        {0: 0.50, 10: 0.39, 100: 0.31, 200: 0.23, 300: 0.15, 900: 0.15},
        btc=btc,
    ))
    assert not r.t4_filled


def test_gap_gate_blocks_arming_after_a_gap() -> None:
    # T4 trigger 0.156; jump straight to 0.10 (below the 5% gap floor
    # 0.148) without ever arming in the pre-arm zone.
    r = replay(_window(
        {0: 0.50, 10: 0.39, 100: 0.31, 200: 0.23, 300: 0.10, 900: 0.10}
    ))
    assert not r.t4_filled


def test_settlement_at_expiry() -> None:
    # Enter UP at 0.39, never reach the TP, resolve UP = 1.
    r = replay(_window({0: 0.50, 10: 0.39, 900: 0.39}, up_won=True))
    assert r.exit == "SETTLE"
    assert r.pnl > 0
    # 2.563 shares settle at 1.0: 2.563 - 1.0 - fee.
    assert r.pnl == pytest.approx(1 / 0.39 - 1.0 - taker_fee(1 / 0.39, 0.39, 0.07))
