"""Tests for the in-process backtest runner + the PAUSE_NEW_ENTRIES gate."""
from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from src.backtest.model import Fill, RoundResult
from src.backtest.runner import build_conclusion, live_params, summarize
from src.config import strategy
from src.strategy.engine import ActionTakeProfit, StrategyEngine
from src.strategy.round_state import Position


# ---------------------------------------------------------------------------
# Summary / conclusion
# ---------------------------------------------------------------------------


def _round(ts: int, exit_: str, pnl: float, fills: list[Fill]) -> RoundResult:
    return RoundResult(
        start_ts=ts, slug=f"w{ts}", side="UP", entry_ms=ts * 1000 + 1000,
        entry_price=0.40, exit=exit_, pnl=pnl, fills=fills, up_won=True,
        t4_filled=any(f.tier == "T4" for f in fills), max_shares=10.0,
    )


def test_summarize_splits_by_highest_tier() -> None:
    results = [
        _round(1, "TP", 0.2, [Fill(0, "T1", 0.40, 2.5, "TAKER", 0.04)]),
        _round(2, "SL", -10.0, [
            Fill(0, "T1", 0.40, 2.5, "TAKER", 0.04),
            Fill(0, "T2", 0.32, 6.25, "MAKER", 0.0),
            Fill(0, "T3", 0.24, 16.67, "MAKER", 0.0),
            Fill(0, "T4", 0.16, 50.0, "MAKER", 0.0),
        ]),
        _round(3, "NO_ENTRY", 0.0, []),
    ]
    s = summarize(results, live_params(), 1)
    assert s["windows_replayed"] == 3
    assert s["entries"] == 2
    assert s["no_entry"] == 1
    assert s["by_highest_tier"]["T1"]["n"] == 1
    assert s["by_highest_tier"]["T4"]["n"] == 1
    assert s["t4_rounds"] == 1
    assert s["t4_pnl"] == pytest.approx(-10.0)
    assert s["pnl_total"] == pytest.approx(-9.8)
    assert s["max_drawdown"] <= 0


def test_conclusion_mentions_t4_and_negative_ev() -> None:
    s = summarize(
        [_round(1, "SL", -10.0, [Fill(0, "T4", 0.16, 50.0, "MAKER", 0.0)])],
        live_params(), 7,
    )
    text = build_conclusion(7, s, [{"name": "baseline", "summary": s}])
    assert "T4" in text
    assert "负期望" in text


def test_live_params_mirror_the_strategy_settings() -> None:
    p = live_params()
    s = strategy()
    assert list(p.tier_notionals) == [
        s.TIER1_NOTIONAL, s.TIER2_NOTIONAL,
        s.TIER3_NOTIONAL, s.TIER4_NOTIONAL,
    ]
    assert p.entry_price_cap == pytest.approx(s.ENTRY_PRICE_CAP)
    assert p.tp_profit_pct == pytest.approx(s.TP_PROFIT_PCT)


# ---------------------------------------------------------------------------
# PAUSE_NEW_ENTRIES
# ---------------------------------------------------------------------------


def _engine(*, paused: bool, mid: float = 0.39):
    eng = StrategyEngine.__new__(StrategyEngine)
    eng._bc = SimpleNamespace(
        last_price=100_000.0, ts_ms=int(time.time() * 1000)
    )
    eng._pc = SimpleNamespace(get_midpoint=lambda tok: mid)
    eng._gate = SimpleNamespace(
        can_enter=lambda bc, mid: True,
        can_act=lambda bc: True,
        can_double_down=lambda *a, **k: True,
        state=lambda bc: SimpleNamespace(drift=0.0, within_limit=True),
    )
    eng._s = SimpleNamespace(
        ENTRY_PRICE_CAP=0.40,
        PRICE_PRE_MARGIN=0.10,
        CANCEL_HYSTERESIS_PCT=0.02,
        TP_PROFIT_PCT=0.20,
        TP_DECREMENT_PCT=0.03,
        TP_MIN_PCT=0.05,
        T1_ENTRY_WINDOW_S=300.0,
        TIER1_NOTIONAL=1.0,
        BASELINE_DRIFT_LIMIT=0.0015,
        PAUSE_NEW_ENTRIES=paused,
    )
    eng._t1_skip_last_log_ts = {}
    eng._t1_cap_reject_first_seen = {}
    eng._t1_cap_reject_summary_logged = {}
    eng._tN_skip_last_log_ts = {}
    eng._tp_last_post_error_ts = 0.0
    eng._tp_post_failure_count = 0
    eng._tp_backoff_s = 5.0
    actions: list = []

    async def handler(action) -> None:  # noqa: ANN001
        actions.append(action)

    eng._handler = handler
    eng._actions = actions
    return eng


def _empty_round():
    return SimpleNamespace(
        position=Position(),
        round_start_ts=int(time.time() - 10),
        up_token="UP",
        down_token="DOWN",
    )


@pytest.mark.asyncio
async def test_pause_blocks_t1_entry() -> None:
    paused = _engine(paused=True)
    paused._round = _empty_round()
    await paused._check_t1_entry(bc_now=100_000.0)
    assert paused._actions == []

    live = _engine(paused=False)
    live._round = _empty_round()
    await live._check_t1_entry(bc_now=100_000.0)
    assert len(live._actions) == 1


def _t1_to_t3_position() -> Position:
    """T1-T3 filled; with mid 0.15 an unpaused engine would arm T4."""
    pos = Position()
    pos.entry_token = "UP"
    pos.entry_side = "UP"
    pos.baseline_realized_price = 0.40
    pos.baseline_set_realized = True
    pos._total_qty = 2.5 + 6.25 + 16.67
    pos._total_cost = 7.0
    pos._avg_cost = pos._total_cost / pos._total_qty
    for tier, filled, cost in (
        (pos.tier1, 2.5, 1.0),
        (pos.tier2, 6.25, 2.0),
        (pos.tier3, 16.67, 4.0),
    ):
        tier.size_filled = filled
        tier.cost_filled = cost
        tier.filled = True
    return pos


@pytest.mark.asyncio
async def test_pause_blocks_averaging() -> None:
    # 0.155 is inside the T4 pre-arm zone and above the 5% gap floor, so
    # an unpaused engine would place T4; the paused one must not.
    eng = _engine(paused=True, mid=0.155)
    eng._round = SimpleNamespace(
        position=_t1_to_t3_position(),
        round_start_ts=int(time.time() - 600),
        up_token="UP", down_token="DOWN",
    )
    await eng._check_average_down(bc_now=100_000.0)
    assert eng._actions == [], "paused engine must not add a tier"

    live = _engine(paused=False, mid=0.155)
    live._round = SimpleNamespace(
        position=_t1_to_t3_position(),
        round_start_ts=int(time.time() - 600),
        up_token="UP", down_token="DOWN",
    )
    await live._check_average_down(bc_now=100_000.0)
    assert len(live._actions) == 1, "sanity: the same setup fires when unpaused"


@pytest.mark.asyncio
async def test_pause_does_not_block_take_profit() -> None:
    eng = _engine(paused=True, mid=0.40)
    pos = Position()
    pos.entry_token = "UP"
    pos.entry_side = "UP"
    pos._total_qty = 10.0
    pos._total_cost = 3.0
    pos._avg_cost = 0.30
    pos.tier1.size_filled = 10.0
    pos.tier1.cost_filled = 3.0
    pos.baseline_realized_price = 0.30
    pos.baseline_set_realized = True
    eng._round = SimpleNamespace(
        position=pos, round_start_ts=int(time.time() - 300),
        up_token="UP", down_token="DOWN",
    )

    await eng._check_take_profit(bc_now=100_000.0)
    assert len(eng._actions) == 1
    assert isinstance(eng._actions[0], ActionTakeProfit)
