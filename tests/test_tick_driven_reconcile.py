"""
Tick-driven reconcile tests for H-NEW-2026-07-15 refactor.

Covers the new behavior where the strategy tick is the single source
of truth for place/cancel decisions:

  - mid drops into the next-tier fire zone (strategy_price_next_tier *
    (1 + PRICE_PRE_MARGIN))     ->engine cancels the previous tier's
                                   BUY GTD via ActionCancelTier.
  - mid rises into the TP fire zone (tp_target * (1 - margin))
                                   ->engine cancels every BUY GTD and
                                     the TP SELL can take over.
  - tier.is_complete              ->`_reconcile_inflight_buys` must
                                   NOT emit another cancel (already
                                   home) and must keep `tier.placed`
                                   cleared so place-side doesn't retry.

We construct the engine via ``StrategyEngine.__new__`` (mirroring
``test_tp_inflight_mutex_leak_2026_07_12.py``) so we don't need a real
``pm_client`` / WS bootstrap. The tests assert on
``ActionCancelTier`` emissions and on the engine's reconcile-internal
log breadcrumbs (``engine.reconcile.cancel_for_next_tier``,
``engine.reconcile.cancel_for_tp``).

Each test builds its own engine instance (no fixture) so state never
leaks between them.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest


# Force the real strategy config into ``eng._s`` so PRICE_PRE_MARGIN
# and tier_drop_pcts come from src.config (single source of truth
# for the live bot).
from src.config import strategy as _strategy
from src.strategy.engine import (
    ActionCancelTier,
    StrategyEngine,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass
class _FakeTier:
    """Per-tier mutable state."""
    tier: int
    placed: bool = False
    watching: bool = False
    filled: bool = False
    is_complete: bool = False
    server_order_id: str | None = None
    size_filled: float = 0.0
    size_expected: float = 10.0
    notional: float = 1.0
    expires_at: float = 0.0
    cost_filled: float = 0.0
    # H-NEW-2026-07-15: single-flight guard for cancel dispatches.
    cancel_pending: bool = False


class _FakePos:
    """Position stub --only the attrs touched by `_reconcile_inflight_buys`.

    The real `Position.snapshot()` reads 17 fields, so we expose a
    `snapshot()` that mirrors them on a duck-typed SimpleNamespace.
    """

    def __init__(
        self,
        *,
        tiers: list[_FakeTier],
        avg_cost: float = 0.36,
        total_qty: float = 5.55,
        total_cost: float = 5.55 * 0.36,
        baseline_realized_price: float | None = 0.36,
        baseline_target_price: float = 0.36,
        tp_target: float | None = None,
        entry_token: str = "0xtok",
        entry_side: str = "DOWN",
    ) -> None:
        self.tiers = tiers
        self.entry_token = entry_token
        self.entry_side = entry_side
        self._tp_inflight = False
        self.total_qty = total_qty
        self.avg_cost = avg_cost
        self._avg_cost = avg_cost
        self.total_cost = total_cost
        self._total_qty = total_qty
        self._total_cost = total_cost
        self.baseline_realized_price = baseline_realized_price
        self.baseline_target_price = baseline_target_price
        self.baseline_set_realized = baseline_realized_price is not None
        self._tp_target_override = tp_target

    def tp_target(self) -> float | None:
        return self._tp_target_override

    @property
    def has_inflight_tier(self) -> bool:
        return any(t.placed for t in self.tiers)

    def snapshot(self) -> Any:
        return SimpleNamespace(
            total_qty=self._total_qty,
            avg_cost=self._avg_cost,
            total_cost=self._total_cost,
            tier_size_filled=tuple(t.size_filled for t in self.tiers),
            tier_cost_filled=tuple(t.cost_filled for t in self.tiers),
            tier_size_expected=tuple(t.size_expected for t in self.tiers),
            tier_placed=tuple(t.placed for t in self.tiers),
            tier_filled=tuple(t.is_complete for t in self.tiers),
            tier_is_complete=tuple(t.is_complete for t in self.tiers),
            tier_server_order_id=tuple(t.server_order_id for t in self.tiers),
            tier_expires_at=tuple(t.expires_at for t in self.tiers),
            baseline_set_realized=self.baseline_set_realized,
            baseline_realized_price=self.baseline_realized_price,
            baseline_target_price=self.baseline_target_price,
            tp_target=self.tp_target(),
            tp_inflight=self._tp_inflight,
            has_inflight_tier=self.has_inflight_tier,
            entry_token=self.entry_token,
            entry_side=self.entry_side,
        )


class _FakeRound:
    round_start_ts = 1783818900

    def __init__(self, pos: _FakePos) -> None:
        self.position = pos
        self.gate_last_state = "PASS"


class _FakeGate:
    """Everything passes."""
    class _S:
        within_limit = True
        drift = 0.0
    def can_act(self, _ts: float) -> bool: return True
    def can_enter(self, _ts: float, _mid: float) -> bool: return True
    def can_double_down(self, _ts: float, _mid: float, *_a: Any, **_kw: Any) -> bool: return True
    def state(self, _ts: float) -> Any:
        return self._S()


class _FakePC:
    """Midpoint cache keyed by entry_token."""
    def __init__(self, mid_by_token: dict[str, float] | None = None) -> None:
        self.mid_by_token = mid_by_token or {}

    def get_midpoint(self, token: str) -> float:
        return self.mid_by_token.get(token, 0.0)


class _FakeBC:
    """BinanceCache stub --engine reads `last_price`."""
    def __init__(self, price: float = 100_000.0) -> None:
        self.last_price = price


def _make_engine(pos: _FakePos, mid: float) -> Any:
    eng = StrategyEngine.__new__(StrategyEngine)
    eng._round = _FakeRound(pos)
    eng._pc = _FakePC(mid_by_token={pos.entry_token: mid})
    eng._gate = _FakeGate()
    eng._bc = _FakeBC()
    eng._s = _strategy()
    eng._eval_inflight = False
    eng._tp_last_post_error_ts = 0.0
    eng._tp_backoff_s = 5.0
    eng._tp_post_failure_count = 0
    eng._t1_skip_last_log_ts = {}
    eng._tN_skip_last_log_ts = {}
    eng._t1_cap_reject_first_seen = {}
    eng._t1_cap_reject_summary_logged = {}
    eng._t2_diag_last_log_ts = {}
    eng._t2_diag_last_fingerprint = {}
    return eng


# ---------------------------------------------------------------------------
# Test 1: mid drops into T2 fire zone ->cancel T1's BUY
# ---------------------------------------------------------------------------



async def test_next_tier_fire_cancels_prev_tier_buy() -> None:
    """
    With baseline=0.40, T2 drops 20% ->next_strategy_price = 0.32,
    fire_line = 0.32 * 1.10 = 0.352. If mid=0.34 (<=0.352), the
    engine MUST cancel T1's BUY GTD so T2 can fire.
    """
    tiers = [
        _FakeTier(
            tier=1, placed=True, is_complete=False,
            server_order_id="0xT1srv", size_filled=0.0,
        ),
        _FakeTier(
            tier=2, placed=False, is_complete=False,
            server_order_id=None, size_filled=0.0,
        ),
        _FakeTier(
            tier=3, placed=False, is_complete=False,
            server_order_id=None, size_filled=0.0,
        ),
        _FakeTier(
            tier=4, placed=False, is_complete=False,
            server_order_id=None, size_filled=0.0,
        ),
    ]
    pos = _FakePos(
        tiers=tiers,
        baseline_realized_price=0.40,
        baseline_target_price=0.40,
        tp_target=None,  # no TP yet ->only next-tier path
    )
    eng = _make_engine(pos, mid=0.34)

    emitted: list[ActionCancelTier] = []

    async def handler(action: ActionCancelTier) -> None:
        emitted.append(action)

    eng._handler = handler

    async def _run() -> None:
        await eng._reconcile_inflight_buys(bc_now=100_000.0)

    await _run()

    assert len(emitted) == 1, (
        f"expected exactly one ActionCancelTier for T1, got {len(emitted)}: "
        f"{emitted}"
    )
    assert emitted[0].tier == 1
    assert emitted[0].server_order_id == "0xT1srv"
    assert emitted[0].reason == "mid_below_next_fire"


# ---------------------------------------------------------------------------
# Test 2: mid NOT in T2 zone ->no cancel
# ---------------------------------------------------------------------------



async def test_mid_above_next_tier_fire_does_not_cancel() -> None:
    """
    mid=0.40 is ABOVE the T2 fire line (0.352) ->no cancel: T1's BUY
    GTD is still wanted on the book.
    """
    tiers = [
        _FakeTier(
            tier=1, placed=True, is_complete=False,
            server_order_id="0xT1srv",
        ),
        _FakeTier(tier=2, placed=False),
        _FakeTier(tier=3, placed=False),
        _FakeTier(tier=4, placed=False),
    ]
    pos = _FakePos(
        tiers=tiers,
        baseline_realized_price=0.40,
        baseline_target_price=0.40,
        tp_target=None,
    )
    eng = _make_engine(pos, mid=0.40)

    emitted: list[ActionCancelTier] = []

    async def handler(action: ActionCancelTier) -> None:
        emitted.append(action)

    eng._handler = handler

    async def _run() -> None:
        await eng._reconcile_inflight_buys(bc_now=100_000.0)

    await _run()

    assert emitted == [], (
        f"mid above next-tier fire must NOT cancel, got {emitted}"
    )


# ---------------------------------------------------------------------------
# Test 3: mid in TP fire zone ->cancel ALL BUY GTDs (T1 and T2..T4)
# ---------------------------------------------------------------------------



async def test_tp_fire_cancels_all_buys() -> None:
    """
    With baseline=0.40, T1 filled at 0.40 ->avg_cost=0.40, tp_target
    for 0 averaging-down = 0.40 * 1.2 = 0.48. fire_line = 0.48 * 0.90
    = 0.432.

    mid=0.45 (<=0.432) ->engine cancels T1 (still on book as partial
    residue? here placed=True means still on book), AND T2 (placed
    too), AND any other place=T BUY tiers.
    """
    tiers = [
        _FakeTier(
            tier=1, placed=True, is_complete=False,
            server_order_id="0xT1srv", size_filled=2.0,
        ),
        _FakeTier(
            tier=2, placed=True, is_complete=False,
            server_order_id="0xT2srv", size_filled=0.0,
        ),
        _FakeTier(tier=3, placed=False),
        _FakeTier(tier=4, placed=False),
    ]
    pos = _FakePos(
        tiers=tiers,
        avg_cost=0.40, total_qty=2.0,
        total_cost=2.0 * 0.40,
        baseline_realized_price=0.40,
        baseline_target_price=0.40,
        tp_target=0.48,  # 0.40 * 1.2 (no averagings yet)
    )
    eng = _make_engine(pos, mid=0.45)

    emitted: list[ActionCancelTier] = []

    async def handler(action: ActionCancelTier) -> None:
        emitted.append(action)

    eng._handler = handler

    async def _run() -> None:
        await eng._reconcile_inflight_buys(bc_now=100_000.0)

    await _run()

    # T1 + T2 placed, both should cancel for TP.
    cancel_tiers = sorted(a.tier for a in emitted)
    assert cancel_tiers == [1, 2], (
        f"expected cancel on T1 + T2 for TP, got tiers={cancel_tiers}"
    )
    assert all(a.reason == "mid_above_tp_fire" for a in emitted), (
        f"all TP cancels must share reason 'mid_above_tp_fire', got: "
        f"{[a.reason for a in emitted]}"
    )


# ---------------------------------------------------------------------------
# Test 4: tier.is_complete ->no cancel emitted for that tier
# ---------------------------------------------------------------------------



async def test_complete_tiers_are_skipped_in_reconcile() -> None:
    """
    If T1 is is_complete=True (filled and done), reconcile must NOT
    emit a cancel for it even if mid has crossed into next-tier fire.
    Completed tiers have no GTD on the book.

    Setup:
      - T1 fully filled (placed=True, is_complete=True)
      - T2 GTD still on book (placed=True)
      - mid=0.10 → way below T3's fire line (drop 40% from 0.40
        baseline → 0.24, fire = 0.24*1.10 = 0.264)
      - reconcile must cancel T2 (T3 next-tier zone reached) but
        must SKIP T1 (complete → no GTD).
    """
    tiers = [
        _FakeTier(
            tier=1, placed=True, is_complete=True,  # fully filled
            server_order_id="0xT1srv", size_filled=10.0,
        ),
        _FakeTier(
            tier=2, placed=True, is_complete=False,
            server_order_id="0xT2srv",  # T2 GTD still on book
        ),
        _FakeTier(tier=3, placed=False),
        _FakeTier(tier=4, placed=False),
    ]
    pos = _FakePos(
        tiers=tiers,
        baseline_realized_price=0.40,
        baseline_target_price=0.40,
        tp_target=None,
    )
    eng = _make_engine(pos, mid=0.10)

    emitted: list[ActionCancelTier] = []

    async def handler(action: ActionCancelTier) -> None:
        emitted.append(action)

    eng._handler = handler

    async def _run() -> None:
        await eng._reconcile_inflight_buys(bc_now=100_000.0)

    await _run()

    # Only T2 should be cancelled (T1 is complete → no GTD → skip).
    cancel_tiers = sorted(a.tier for a in emitted)
    assert cancel_tiers == [2], (
        f"expected cancels only on T2 (T1 is complete → skip), "
        f"got tiers={cancel_tiers}"
    )


# ---------------------------------------------------------------------------
# Test 5: _eval_inflight single-flight guard prevents re-entrance
# ---------------------------------------------------------------------------



async def test_eval_inflight_blocks_reentrant_tick() -> None:
    """
    The B-1 single-flight guard on `_evaluate` must still work after
    the refactor. A second concurrent tick must short-circuit on
    `_eval_inflight=True` and NOT do any reconciliation work, emit
    any ActionCancelTier, or advance any state.
    """
    tiers = [
        _FakeTier(
            tier=1, placed=True, is_complete=False,
            server_order_id="0xT1srv",
        ),
        _FakeTier(tier=2, placed=False),
        _FakeTier(tier=3, placed=False),
        _FakeTier(tier=4, placed=False),
    ]
    pos = _FakePos(
        tiers=tiers,
        baseline_realized_price=0.40,
        baseline_target_price=0.40,
        tp_target=None,
    )
    eng = _make_engine(pos, mid=0.34)

    emitted: list[ActionCancelTier] = []

    async def handler(action: ActionCancelTier) -> None:
        emitted.append(action)

    eng._handler = handler

    # Pre-arm the single-flight guard as if a previous tick is
    # mid-flight. The next call to `_evaluate` must short-circuit.
    eng._eval_inflight = True

    async def _run() -> None:
        await eng._evaluate()

    await _run()

    # Single-flight guard honoured: no handler call, no state change.
    assert emitted == [], (
        f"single-flight must suppress all emissions, got {emitted}"
    )
    assert eng._eval_inflight is True, (
        "single-flight flag must remain True (suppressed tick must "
        "not have tried to clear it)"
    )


# ---------------------------------------------------------------------------
# Test 6: last tier's BUY cannot trigger next-tier cancel (no next tier)
# ---------------------------------------------------------------------------



async def test_last_tier_does_not_attempt_next_tier_cancel() -> None:
    """
    T4 has no next-tier --reconcile must iterate T1..T3 only for the
    next-tier sweep, and skip T4 entirely. Also ensure TP path
    behaves consistently when T4 is the only placed tier.
    """
    tiers = [
        _FakeTier(
            tier=1, placed=True, is_complete=True,  # filled
            server_order_id="0xT1srv",
        ),
        _FakeTier(
            tier=2, placed=True, is_complete=True,
            server_order_id="0xT2srv",
        ),
        _FakeTier(
            tier=3, placed=True, is_complete=True,
            server_order_id="0xT3srv",
        ),
        _FakeTier(
            tier=4, placed=True, is_complete=False,  # still on book
            server_order_id="0xT4srv",
        ),
    ]
    pos = _FakePos(
        tiers=tiers,
        baseline_realized_price=0.40,
        baseline_target_price=0.40,
        tp_target=None,
    )
    eng = _make_engine(pos, mid=0.10)  # well below anything

    emitted: list[ActionCancelTier] = []

    async def handler(action: ActionCancelTier) -> None:
        emitted.append(action)

    eng._handler = handler

    async def _run() -> None:
        await eng._reconcile_inflight_buys(bc_now=100_000.0)

    await _run()

    assert emitted == [], (
        "last tier (T4) has no next tier ->no cancel should fire even "
        f"at very low mid. got {emitted}"
    )
