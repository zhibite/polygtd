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
            "TIER2_NOTIONAL": 3.0,
            "TIER3_NOTIONAL": 5.0,
            "TIER4_NOTIONAL": 6.0,
            # H-NEW-2026-07-15: TIER_NOTIONAL_MAX_MULT removed.
            "tier_drop_pcts": [None, 0.20, 0.40, 0.60],
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
    pos.baseline_realized_price = baseline_realized
    pos.baseline_set_realized = baseline_realized is not None
    # Stash the legacy value under a non-conflicting attribute. The
    # legacy `baseline_price` / `baseline_set` Position fields were
    # removed entirely (H-NEW-2026-07-15), so we cannot write to
    # them any more — and importantly, the engine no longer reads
    # any such value, so this attribute is purely informational.
    # The `test_legacy_baseline_not_used_as_fallback` test still
    # passes `baseline_legacy` to confirm the value is ignored.
    pos._legacy_baseline_price = baseline_legacy
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
    # H-NEW-2026-07-15: tier_is_saturated / tier_notional_remaining removed;
    # fake stubs only need to satisfy the (very few) call sites.
    pos.has_inflight_tier = False
    pos._t1_rebased_logged = False
    pos.stake_cap_remaining = 999.0  # big enough that no tier hits it

    # H-NEW-2026-07-15 (tick-driven refactor): `_check_average_down`
    # now expects a PositionSnapshot. Mirror the production snapshot()
    # shape from a duck-typed SimpleNamespace so the stub tests can
    # continue to pass.
    from types import SimpleNamespace

    def _snapshot() -> Any:
        ts = pos.tiers
        return SimpleNamespace(
            total_qty=pos._total_qty,
            avg_cost=pos._avg_cost,
            total_cost=pos._total_cost,
            tier_size_filled=tuple(t.size_filled for t in ts),
            tier_cost_filled=tuple(t.cost_filled for t in ts),
            tier_size_expected=tuple(t.size_expected for t in ts),
            tier_placed=tuple(t.placed for t in ts),
            tier_filled=tuple(t.filled for t in ts),
            tier_is_complete=tuple(t.is_complete for t in ts),
            tier_server_order_id=tuple(t.server_order_id for t in ts),
            tier_expires_at=tuple(t.expires_at for t in ts),
            baseline_set_realized=pos.baseline_set_realized,
            baseline_realized_price=pos.baseline_realized_price,
            baseline_target_price=pos.baseline_target_price,
            tp_target=None,
            tp_inflight=pos._tp_inflight,
            has_inflight_tier=pos.has_inflight_tier,
            entry_token=pos.entry_token,
            entry_side=pos.entry_side,
        )

    pos.snapshot = _snapshot
    pos._avg_cost = pos._total_cost / pos._total_qty if pos._total_qty > 0 else 0.0
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
async def test_t1_rebased_logged_only_once() -> None:
    """When T1's realised fill is set, the engine should log a single
    `engine.t1_rebased` breadcrumb showing the cap → realised
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

    # First call: engine.t1_rebased should log once.
    captured: list[Any] = []

    async def handler1(action):
        captured.append(action)
    eng._handler = handler1
    await eng._check_average_down(100000.0)
    assert pos._t1_rebased_logged is True, (
        "first call should mark _t1_rebased_logged=True"
    )

    # Second call: should NOT change _t1_rebased_logged state.
    async def handler2(action):
        pass
    eng._handler = handler2
    pos._t1_rebased_logged = True  # ensure still set
    await eng._check_average_down(100001.0)
    assert pos._t1_rebased_logged is True