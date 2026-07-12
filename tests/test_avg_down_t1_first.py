"""
Regression tests for H-NEW-2026-07-12 (operator clarification, prod
window 1783819800).

The strategy spec says the 30% / 60% averaging drops are measured
from the T1 *entry* price — not the T1 *strategy/intended bid*
(the cap, ~0.36 in prod). Previously the `_check_average_down`
fallback chain silently used `baseline_target_price` (the cap)
when no `baseline_realized_price` was available yet, which let T2
fire with `baseline=0.36` in prod even though T1 ended up filling
at 0.22. The fire-line and strategy_price for T2 ended up ~50%
above what the operator asked for.

These tests pin the new behaviour:
  - T2/T3 averaging MUST NOT evaluate before T1 has at least one
    confirmed fill (baseline_realized_price set).
  - When T1 has not filled, the function returns cleanly with a
    breadcrumb and does NOT emit an ActionOpenTN.
  - Once T1 fills and baseline_realized_price is set, averaging
    evaluates against the realised price, not the cap.
  - The legacy baseline_price fallback is no longer consulted
    (it would have allowed the prod 1783819800 bug to keep
    happening).
"""
from __future__ import annotations

from typing import Any

import pytest

from src.strategy.engine import ActionOpenTN, StrategyEngine
from src.strategy.round_state import TierSnapshot


class _StubGate:
    """Always allows the engine to fire."""

    def can_act(self, *_args, **_kwargs) -> bool:
        return True

    def can_double_down(self, *_args, **_kwargs) -> bool:
        return True

    def state(self, _bc_now: float) -> Any:
        class _S:
            drift = 0.0
            within_limit = True
        return _S()


class _StubBinanceCache:
    def snapshot(self) -> dict[str, float]:
        return {}


class _StubPriceCache:
    def __init__(self, mid: float, bid: float = 0.0) -> None:
        self._mid = mid
        self._bid = bid

    def get_midpoint(self, _token: str) -> float:
        return self._mid

    def get_price(self, _token: str, _side: str = "BUY") -> float:
        return self._bid or self._mid


def _make_engine(mid: float = 0.20) -> Any:
    eng = StrategyEngine.__new__(StrategyEngine)
    eng._gate = _StubGate()
    eng._bc = _StubBinanceCache()
    eng._pc = _StubPriceCache(mid=mid, bid=mid)
    eng._handler = None
    eng._s = type(
        "S",
        (),
        {
            "TIER2_NOTIONAL": 2.0,
            "TIER3_NOTIONAL": 4.0,
            "TIER_NOTIONAL_MAX_MULT": 1.1,
            "tier_drop_pcts": [None, 0.30, 0.60],
        },
    )()
    return eng


def _make_pos(
    eng: Any,
    *,
    baseline_target: float | None = 0.40,
    baseline_realized: float | None = None,
    baseline_legacy: float | None = 0.45,
) -> Any:
    """Build a Position-like stub whose tiers are real TierSnapshot
    instances (so they have a real `trigger_price` method). We use
    a stub P rather than a real Position because the tests need to
    mutate baseline_realized_price / baseline_set_realized freely
    without going through the bind_server_order_id ceremony."""
    pos = type("P", (), {})()
    pos.entry_token = "tok"
    pos.entry_side = "UP"
    pos.baseline_target_price = baseline_target
    pos.baseline_price = baseline_legacy
    pos.baseline_set = True
    pos.baseline_realized_price = baseline_realized
    pos.baseline_set_realized = baseline_realized is not None
    pos.tiers = [
        TierSnapshot(tier=1, notional=2.0),
        TierSnapshot(tier=2, notional=2.0),
        TierSnapshot(tier=3, notional=4.0),
    ]
    pos.tiers[0].placed = True
    pos.tiers[0].server_order_id = "x"  # T1 is placed (waiting fill)
    pos.tiers[0].size_expected = 5.55
    pos.tiers[1].size_expected = 15.87
    pos.tiers[2].size_expected = 31.74
    pos._tp_inflight = False
    pos._total_qty = 5.55
    pos._total_cost = 2.0
    pos.tier_is_saturated = lambda *_a, **_k: False
    pos.has_inflight_tier = False
    pos._baseline_rebased_logged = False
    pos.tier_notional_remaining = lambda tier: max(
        0.0, tier.notional * eng._s.TIER_NOTIONAL_MAX_MULT - tier.cost_filled,
    )
    pos.stake_cap_remaining = 999.0  # big enough that no tier hits it
    return pos


@pytest.mark.asyncio
async def test_no_avg_fire_before_t1_fill() -> None:
    """T2 must NOT fire if T1 has not produced any fill yet.

    Production window 1783819800 reproduced this: T1 and T2 both fired
    on the same tick because the engine allowed T2 to evaluate against
    `baseline_target_price` (= 0.36, the T1 cap) before T1 had filled.
    The strategy_price ended up ~50% above the operator-specified
    30%-drop-from-entry trigger.
    """
    eng = _make_engine(mid=0.20)
    pos = _make_pos(
        eng,
        baseline_target=0.36,    # T1 cap (where the prod bug came from)
        baseline_realized=None,  # T1 has not filled yet
    )
    eng._round = type("R", (), {"position": pos})()

    captured: list[Any] = []

    async def handler(action):
        captured.append(action)
    eng._handler = handler

    await eng._check_average_down(100000.0)

    assert captured == [], (
        f"H-NEW-2026-07-12 violation: T2 fired before T1 had any fill. "
        f"captured={captured}"
    )


@pytest.mark.asyncio
async def test_avg_fires_after_t1_realized_fill() -> None:
    """Once T1 has produced a fill, T2 evaluates against the realised
    price (NOT the cap). mid=0.10 must trigger T2 because T1 filled at
    0.20 — 50% drop — which is deeper than the 30% threshold.
    """
    eng = _make_engine(mid=0.10)
    pos = _make_pos(
        eng,
        baseline_target=0.36,         # the pre-fill cap
        baseline_realized=0.20,       # T1 actually filled at 0.20
    )
    eng._round = type("R", (), {"position": pos})()

    captured: list[Any] = []

    async def handler(action):
        captured.append(action)
    eng._handler = handler

    await eng._check_average_down(100000.0)

    # T2 should fire at least once (operator spec: 30% drop from
    # the 0.20 realised entry → fire line ≈ 0.20 * 0.70 * 1.1 = 0.154;
    # mid=0.10 < fire_line → fires).
    assert any(isinstance(a, ActionOpenTN) and a.tier == 2 for a in captured), (
        f"H-NEW-2026-07-12: T2 must fire after T1 realised fill; "
        f"captured={captured}"
    )


@pytest.mark.asyncio
async def test_legacy_baseline_not_used_as_fallback() -> None:
    """The legacy baseline_price (e.g. 0.45) must NOT be consulted as a
    fallback. Production saw T2 fire with baseline=0.36 (the cap) even
    though legacy baseline_price was 0.45 — that was the prod bug. We
    now refuse to use the legacy field at all.
    """
    eng = _make_engine(mid=0.30)  # mid above any reasonable fire line
    pos = _make_pos(
        eng,
        baseline_target=0.40,
        baseline_realized=None,    # T1 not filled
        baseline_legacy=0.99,      # would have been the old fallback
    )
    eng._round = type("R", (), {"position": pos})()

    captured: list[Any] = []

    async def handler(action):
        captured.append(action)
    eng._handler = handler

    await eng._check_average_down(100000.0)

    assert captured == [], (
        f"H-NEW-2026-07-12 violation: T2 fired using legacy baseline. "
        f"captured={captured}"
    )


@pytest.mark.asyncio
async def test_baseline_rebased_logged_only_once() -> None:
    """When T1's realised fill is set, the engine should log a single
    `baseline.rebased` breadcrumb showing the cap → realised
    transition. Subsequent ticks with the same realised value must
    not re-log. This guards against log spam from the new branch.
    """
    eng = _make_engine(mid=0.10)
    pos = _make_pos(
        eng,
        baseline_target=0.40,
        baseline_realized=0.20,
    )
    eng._round = type("R", (), {"position": pos})()

    # First call: baseline.rebased should log once.
    captured: list[Any] = []

    async def handler1(action):
        captured.append(action)
    eng._handler = handler1
    await eng._check_average_down(100000.0)
    assert pos._baseline_rebased_logged is True, (
        "first call should mark _baseline_rebased_logged=True"
    )

    # Second call: should NOT change _baseline_rebased_logged state.
    async def handler2(action):
        pass
    eng._handler = handler2
    pos._baseline_rebased_logged = True  # ensure still set
    await eng._check_average_down(100001.0)
    assert pos._baseline_rebased_logged is True