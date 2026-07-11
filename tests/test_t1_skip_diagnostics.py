"""H-V6-8 regression test: every T1 skip path must log (throttled).

Production symptom: 2026-07-11 01:02 round 01:00 — STATUS showed
UP=0.3850 < ENTRY_PRICE_CAP=0.40 yet `tiers=[T1=○]` and no
`engine.trigger_t1`. Without these logs the operator cannot tell which
condition was the blocker: empty midpoint cache, drift gate, empty
best_bid (MarketWS not received bba yet), stake cap, or something else.

This test enumerates each branch of `_check_t1_entry` and asserts the
diagnostic log line fired.
"""
from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.strategy.engine import StrategyEngine


class _StubBinance:
    last_price: float = 64_000.0


class _StubPC:
    """Each scenario sets midpoint / best_bid / ask per token."""

    def __init__(self) -> None:
        self.midpoint: dict[str, float] = {}
        self.best_bid: dict[str, float] = {}

    def get_midpoint(self, token: str) -> float:
        return self.midpoint.get(token, 0.0)

    def bid(self, token: str) -> float:
        return self.best_bid.get(token, 0.0)

    def ask(self, token: str) -> float:
        return self.best_bid.get(token, 0.0)


def _make_engine() -> tuple[StrategyEngine, _StubPC]:
    captured: list[Any] = []

    async def handler(action: Any) -> None:
        captured.append(action)

    eng = StrategyEngine(_StubBinance(), _StubPC(), handler)  # type: ignore[arg-type]
    return eng, eng._pc  # type: ignore[attr-defined]


async def _drive_one_evaluate(eng: StrategyEngine) -> None:
    """Run one _evaluate() and capture the log records it produced."""
    # Pre-bind a real RoundState so _check_t1_entry is reachable.
    import time
    from datetime import datetime, timezone

    from src.strategy.round_state import RoundState
    from src.market.models import MarketInfo

    now = datetime.now(timezone.utc)
    eng._round = RoundState(
        round_start_ts=int(time.time()) - 5,  # well inside T1_ENTRY_WINDOW_S
        market=MarketInfo(
            asset="BTC",
            slug="btc-updown-15m-0",
            condition_id="0xdeadbeef",
            token_up_id="UP_TOKEN",
            token_down_id="DOWN_TOKEN",
            tick_size=0.01,
            window_start=now,
            window_end=now,
        ),
    )
    from src.strategy.gatekeeper import GateKeeper
    eng._gate = GateKeeper(64_000.0)

    await eng._evaluate()


@pytest.mark.asyncio
async def test_t1_skip_logs_when_midpoint_empty(monkeypatch) -> None:
    eng, pc = _make_engine()
    pc.midpoint["UP_TOKEN"] = 0.0
    pc.best_bid["UP_TOKEN"] = 0.30  # would otherwise pass

    seen: dict[str, dict[str, Any]] = {}
    # Patch _t1_skip_log to capture calls without throttling

    def capture(reason: str, *, side: str, **fields: Any) -> None:
        seen[reason] = {"side": side, **fields}

    eng._t1_skip_log = capture  # type: ignore[method-assign]
    await _drive_one_evaluate(eng)

    assert "engine.t1_skip_no_mid" in seen, (
        "T1 skip path for empty midpoint cache did not log."
    )


@pytest.mark.asyncio
async def test_t1_skip_logs_when_above_price_cap(monkeypatch) -> None:
    eng, pc = _make_engine()
    pc.midpoint["UP_TOKEN"] = 0.45  # > 0.40 cap
    pc.best_bid["UP_TOKEN"] = 0.30

    seen: dict[str, dict[str, Any]] = {}

    def capture(reason: str, *, side: str, **fields: Any) -> None:
        seen[reason] = {"side": side, **fields}

    eng._t1_skip_log = capture  # type: ignore[method-assign]
    await _drive_one_evaluate(eng)

    assert "engine.t1_skip_too_expensive" in seen, (
        "T1 skip path for above-cap price did not log."
    )


@pytest.mark.asyncio
async def test_t1_does_not_skip_when_best_bid_empty() -> None:
    """After the pre-arm refactor, T1 entry is driven by midpoint, not
    best_bid. With a valid midpoint and empty best_bid, the engine should
    still emit ActionOpenT1 instead of silently skipping.
    """
    eng, pc = _make_engine()
    pc.midpoint["UP_TOKEN"] = 0.30  # below cap, so entry should fire
    pc.best_bid["UP_TOKEN"] = 0.0   # best_bid is no longer the entry gate

    seen: dict[str, dict[str, Any]] = {}

    def capture(reason: str, *, side: str, **fields: Any) -> None:
        seen[reason] = {"side": side, **fields}

    eng._t1_skip_log = capture  # type: ignore[method-assign]
    await _drive_one_evaluate(eng)

    assert "engine.t1_skip_no_bid" not in seen, (
        "T1 must not skip just because best_bid is empty; midpoint is the "
        "entry signal after the pre-arm refactor."
    )
    # The handler in _make_engine records actions in a closure list. Verify
    # the action was emitted by checking the round's claimed entry state.
    assert eng._round is not None
    assert eng._round.position.entry_token == "UP_TOKEN"
    assert eng._round.position.entry_side == "UP"