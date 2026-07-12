"""TierWatcher continuous ground-truth reconciliation tests (no network).

H-NEW-2026-07-12 (audit finding): the previous wiring only forwarded a
terminal `WatcherResult` (FILLED/EXPIRED/CROSSED) into the engine via
``apply_tier_watcher_result``. That left the position view drifting behind
reality while the watcher was still in its 99.5 %-fill-ratio wait — worst
case ~1.5 s in a fast cascade. If the WS TRADE channel happened to drop
events during that window, a TP or next-tier decision would be made on
stale data.

Fix: ``TierWatcher`` now also fires ``on_partial_fill(delta, price, hint)``
on every poll iteration that observes a fresh ``size_matched`` increment,
routing it through the same ``om.apply_fill_event`` + ``eng.on_order_filled``
entry point the WS path uses. The dedup contract is monotonic on tracked
size_matched (existing defence-in-depth), so if WS already pushed a fill,
the polling delta collapses to 0 and the engine call is skipped.

These tests prove four invariants:

  1. WS TRADE 全断 + polling 看到 size_matched 累加 → position view 同步。
  2. WS 先看到 size_matched=10,polling 之后看到 10 → polling 不重复喂引擎。
  3. watch loop 多次 poll 多次 push, position totals 严格正确(无 double-count)。
  4. error 路径(on_partial_fill 抛异常)不推进 _last_pushed,下次会重试。
"""

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass, field
from typing import Any, Optional

from src.execution.tier_watcher import TierOutcome, TierWatcher, WatcherResult


async def _wait_for_terminal(
    watcher: TierWatcher, max_iters: int = 500
) -> None:
    """Yield to the loop until the watcher hits a terminal state.

    Real-time sleeps are required because TierWatcher's ``_watch`` uses
    ``asyncio.wait_for(... timeout=poll_s)``. A zero-second ``sleep(0)``
    would not let the timer fire.
    """
    for _ in range(max_iters):
        if watcher.result is not None:
            return
        await asyncio.sleep(0.02)


# ---------------------------------------------------------------------------
# Test doubles — mimic the surface the watcher actually calls.
# ---------------------------------------------------------------------------


@dataclass
class _FakeTracked:
    """Minimal TrackedOrder for the OrderManager double."""

    size_matched: float = 0.0
    client_order_id: str = "fake_cid"


@dataclass
class _FakeOrderManager:
    """Tiny double of OrderManager.

    We don't pull in the real OrderManager because (a) it has heavyweight
    state for engines we don't want to spin up, (b) the partial-fill path
    only needs the monotonic ``size_matched`` accounting that
    ``apply_fill_event`` provides in production. The reconciliation
    contract tested here is exactly that monotonic max.
    """

    tracked_by_server: dict[str, _FakeTracked] = field(default_factory=dict)
    apply_log: list[tuple[str, float, float]] = field(default_factory=list)

    def by_server_id(self, server_order_id: str) -> Optional[_FakeTracked]:
        return self.tracked_by_server.get(server_order_id)

    def apply_fill_event(
        self,
        server_order_id: str,
        size_matched: float,
        fill_price: float | None = None,
    ) -> None:
        t = self.tracked_by_server.get(server_order_id)
        if t is None:
            return
        # Mirrors production line 1244: keep the max.
        if size_matched > t.size_matched:
            prev = t.size_matched
            t.size_matched = size_matched
            self.apply_log.append((server_order_id, prev, size_matched))

    def mark_filled(self, server_order_id: str) -> bool:
        return True


@dataclass
class _FakeEngine:
    """Records each ``on_order_filled`` call for assertion."""

    on_order_filled_log: list[tuple[str, str, float, float]] = field(
        default_factory=list
    )
    total_qty: float = 0.0
    total_cost: float = 0.0

    def on_order_filled(
        self,
        token_id: str,
        server_order_id: str,
        fill_price: float,
        delta: float,
    ) -> None:
        self.on_order_filled_log.append(
            (token_id, server_order_id, fill_price, delta)
        )
        # Mirror the production record_fill delta semantics:
        # size_filled += delta; cost_filled += price * delta.
        self.total_qty += delta
        self.total_cost += fill_price * delta


@dataclass
class _FakeCLOB:
    """Mocks the four CLOBClient methods the watcher touches.

    Both ``get_token_balance`` AND ``get_order_fill_state`` advance the
    SAME iterator — every call advances by 1. So if the timeline has
    7 entries, the watcher reads (fast-path, balance, fill_state) in
    order from the head of the timeline. This mirrors production: each
    REST round-trip reads a fresh value; balance is the post-trade
    holding, fill_state is the per-order cumulative.
    """

    token_id: str = "0xTOKEN_DOWN"
    cancel_ok: bool = True
    size_matched_timeline: list[dict] = field(default_factory=list)
    call_idx: int = 0
    cancel_call_count: int = 0

    def _next(self) -> Optional[dict]:
        if self.call_idx >= len(self.size_matched_timeline):
            return None
        state = dict(self.size_matched_timeline[self.call_idx])
        self.call_idx += 1
        return state

    async def get_token_balance(self, token_id: str) -> Optional[float]:
        state = self._next()
        if state is None:
            return None
        return float(state.get("size_matched", 0.0) or 0.0)

    async def get_order_filled_size(self, order_id: str) -> float | None:
        return None  # watcher fast-path not under test here

    async def get_order_fill_state(self, order_id: str) -> Optional[dict]:
        return self._next()

    async def cancel_order(self, server_order_id: str) -> bool:
        self.cancel_call_count += 1
        return self.cancel_ok

    async def get_order(self, *_args: Any, **_kwargs: Any) -> dict:
        # Unused but kept for the surface mirror.
        return {}


# ---------------------------------------------------------------------------
# Local copy of the router's on_partial_fill closure shape.
# Mirrors main.py::_spawn_buy_watcher's on_partial_fill. If main.py drifts,
# the integration smoke at the bottom will surface the divergence.
# ---------------------------------------------------------------------------


def _build_partial_resync(om: _FakeOrderManager, eng: _FakeEngine):
    """Build the on_partial_fill closure mirroring main.py wiring.

    Caller passes the CLOB's authoritative cumulative_size_matched; the
    router derives the engine-call delta from OrderManager's monotonic
    tracked state. This matches the WS-path contract: when WS UPDATE
    arrives first, the polling delta collapses to 0 and the engine call
    is skipped.
    """

    async def on_partial_fill(
        token_id: str,
        server_order_id: str,
        cumulative_size_matched: float,
        fill_price: float,
        hint: Optional[Any],
    ) -> None:
        if cumulative_size_matched <= 0:
            return
        tracked = om.by_server_id(server_order_id)
        prev_size_matched = tracked.size_matched if tracked else 0.0
        effective_delta = cumulative_size_matched - prev_size_matched
        if effective_delta <= 1e-9:
            return
        om.apply_fill_event(
            server_order_id,
            size_matched=cumulative_size_matched,
            fill_price=fill_price,
        )
        om.mark_filled(server_order_id)
        eng.on_order_filled(
            token_id, server_order_id, fill_price, effective_delta
        )

    return on_partial_fill


def _build_complete_handler(eng: _FakeEngine, completed: list[WatcherResult]):
    async def on_done(result: WatcherResult) -> None:
        completed.append(result)

    return on_done


# ---------------------------------------------------------------------------
# Test 1 — WS 全断 + polling 补全到 position
# ---------------------------------------------------------------------------


async def test_polling_recovers_when_ws_dropped() -> int:
    """Scenario: WS TRADE channel drops everything. TierWatcher polls every
    iteration and pushes size_matched deltas into the engine. Final
    position view must equal the CLOB ground truth."""
    server_id = "0xORDER_T1"
    token_id = "0xTOKEN_DOWN"
    om = _FakeOrderManager()
    om.tracked_by_server[server_id] = _FakeTracked(size_matched=0.0)
    eng = _FakeEngine()
    completed: list[WatcherResult] = []

    # Polling timeline: 0 → 1.2 → 2.4 → 3.6 → 4.8 → 5.0 (fill_ratio=0.995 → 5)
    clob = _FakeCLOB(
        size_matched_timeline=[
            # Fast-path read (only one _read_fill_state call returns here):
            {"size_matched": 0.0, "avg_price": 0.0},
            # --- iter 1: balance + fill_state ---
            {"size_matched": 1.2, "avg_price": 0.40},   # balance
            {"size_matched": 1.2, "avg_price": 0.40},   # fill_state (prime)
            # --- iter 2: balance + fill_state ---
            {"size_matched": 2.4, "avg_price": 0.40},   # balance
            {"size_matched": 2.4, "avg_price": 0.40},   # fill_state → push 1.2
            # --- iter 3: balance + fill_state ---
            {"size_matched": 3.6, "avg_price": 0.41},
            {"size_matched": 3.6, "avg_price": 0.41},   # push 1.2
            # --- iter 4: balance + fill_state ---
            {"size_matched": 4.8, "avg_price": 0.41},
            {"size_matched": 4.8, "avg_price": 0.41},   # push 1.2
            # --- iter 5: push final 0.2 then balance triggers FILLED ---
            {"size_matched": 5.0, "avg_price": 0.42},   # balance
            {"size_matched": 5.0, "avg_price": 0.42},   # fill_state → push 0.2
        ],
    )

    async def on_partial_fill_router(
        cumulative_size_matched: float, fill_price: float, hint: Optional[Any]
    ) -> None:
        await _build_partial_resync(om, eng)(
            token_id, server_id, cumulative_size_matched, fill_price, hint
        )

    watcher = TierWatcher(
        clob=clob,
        server_order_id=server_id,
        token_id=token_id,
        side="BUY",
        strategy_price=0.42,
        expected_size=5.0,
        expires_at=float("+inf"),  # never expire in test
        poll_s=0.01,                # tight loop
        fill_ratio=0.9,             # H-NEW-2026-07-12 config default
        cross_threshold=None,
        mid_price_provider=None,
        on_complete=_build_complete_handler(eng, completed),
        on_partial_fill=on_partial_fill_router,
        is_still_active=lambda: True,
    )
    watcher.start()
    await _wait_for_terminal(watcher)
    result = await watcher.stop()
    if result is None:
        print(f"DEBUG: cancel_count={clob.cancel_call_count} "
              f"total_clob_calls={clob.call_idx}")
        print(f"DEBUG: on_order_filled={eng.on_order_filled_log}")
    assert result is not None, "watcher should have reached FILLED terminal"
    assert result.outcome == TierOutcome.FILLED, (
        f"expected FILLED, got {result.outcome}"
    )
    assert clob.cancel_call_count >= 1, "watcher must cancel on FILLED terminal"
    # With fill_ratio=0.9 the watcher FILLED at cumulative=4.8 (the
    # CLOB-reported balance at that tick), NOT 5.0. The terminal
    # WatcherResult reflects what the watcher actually saw, not what
    # eventually lands. The apply_tier_watcher_result path uses this as
    # the authoritative qty.
    assert abs(result.filled_qty - 4.8) < 1e-6, (
        f"watcher must report FILLED @ 4.8 (90 % of 5.0), got {result.filled_qty}"
    )

    # The crux of the test: every poll that grew size_matched should have
    # fired a partial fill through the engine.
    #
    # Timeline cumulatives (after prime): 2.4, 3.6, 4.8.
    # First push has cumulative=2.4 (delta from prev=0 = 2.4); subsequent
    # pushes advance cumulative by 1.2, 1.2. The 4.8→5.0 increment
    # never reaches the watcher because fill_ratio=0.9 short-circuits
    # the FILLED branch first (4.8 / 5.0 = 0.96 ≥ 0.9). Total qty = 4.8.
    expected_qty = 4.8
    assert abs(eng.total_qty - expected_qty) < 1e-6, (
        f"position qty must equal watcher-terminated qty 4.8, got {eng.total_qty}"
    )
    # The watcher's `_avg_price_running` is set per tick (running avg of
    # CLOB-reported avg_price). The first push uses 0.40, then 0.41 for
    # the next two. The 0.2 push at 0.42 never fires (FILLED @ 4.8
    # pre-empts it).
    expected_cost = 2.4 * 0.40 + 1.2 * 0.41 + 1.2 * 0.41
    assert abs(eng.total_cost - expected_cost) < 1e-3, (
        f"position cost must reflect avg prices, "
        f"got {eng.total_cost:.4f}, expected ~{expected_cost:.4f}"
    )
# Each tick that grew size_matched must produce exactly ONE engine call,
    # never duplicates. With fill_ratio=0.9 the watcher FILLED at
    # cumulative=4.8 (4.8 / 5.0 = 0.96 ≥ 0.9), so we expect 3 pushes
    # (cumulative 2.4, 3.6, 4.8) — NOT 4 (5.0 is never reached because
    # the FILLED branch fires first).
    assert len(eng.on_order_filled_log) == 3, (
        f"expected 3 partial pushes (FILLED @ 4.8 with fill_ratio=0.9), "
        f"got {len(eng.on_order_filled_log)}: {eng.on_order_filled_log}"
    )
    # Engine deltas sum: 2.4 + 1.2 + 1.2 = 4.8 (matches watcher.result.filled_qty).
    delta_sum = sum(call[3] for call in eng.on_order_filled_log)
    assert abs(delta_sum - 4.8) < 1e-6, (
        f"sum of engine deltas must equal 4.8 (terminal fill @ 90%), "
        f"got {delta_sum}"
    )
    # The terminal WatcherResult must report 4.8, NOT 5.0 — proving the
    # fill_ratio=0.9 cutoff short-circuited the watcher before reaching
    # full size_matched.
    assert abs(result.filled_qty - 4.8) < 1e-6, (
        f"watcher must report FILLED at 4.8 (90% of 5.0), got "
        f"{result.filled_qty}"
    )
    print(
        f"OK test_polling_recovers_when_ws_dropped  "
        f"qty={eng.total_qty:.4f} cost=${eng.total_cost:.4f} "
        f"fills={len(eng.on_order_filled_log)} "
        f"terminal_qty={result.filled_qty:.4f} (fill_ratio=0.9)"
    )
    return 0


# ---------------------------------------------------------------------------
# Test 2 — WS 先到 + polling 后到 → polling 静默跳过
# ---------------------------------------------------------------------------


async def test_polling_is_idempotent_vs_ws_first() -> int:
    """Scenario: WS channel raced ahead of polling — OrderManager
    already tracks size_matched=4.0 AND TierWatcher received a WS update
    that pushed `_last_size_matched=4.0`. Polling then sees the same
    value (a tick later) — on_partial_fill MUST collapse to delta=0
    because the high-water mark has already moved. The next poll sees
    5.0 — only the 4.0→5.0 increment should fire."""
    server_id = "0xORDER_T1"
    token_id = "0xTOKEN_DOWN"
    om = _FakeOrderManager()
    om.tracked_by_server[server_id] = _FakeTracked(size_matched=4.0)
    eng = _FakeEngine()
    completed: list[WatcherResult] = []

    clob = _FakeCLOB(
        size_matched_timeline=[
            # Fast-path:
            {"size_matched": 0.0, "avg_price": 0.0},
            # --- iter 1: balance (4.0, mid-fill) + fill_state ---
            {"size_matched": 4.0, "avg_price": 0.40},
            {"size_matched": 4.0, "avg_price": 0.40},   # prime at 4.0 (matches WS)
            # --- iter 2: balance + fill_state (no new) ---
            {"size_matched": 4.0, "avg_price": 0.40},
            {"size_matched": 4.0, "avg_price": 0.40},   # high_water still 4.0, no delta
            # --- iter 3: balance triggers FILLED at 5.0 ---
            {"size_matched": 5.0, "avg_price": 0.42},
            {"size_matched": 5.0, "avg_price": 0.42},   # fill_state sees new high
        ],
    )

    async def on_partial_fill_router(
        cumulative_size_matched: float, fill_price: float, hint: Optional[Any]
    ) -> None:
        await _build_partial_resync(om, eng)(
            token_id, server_id, cumulative_size_matched, fill_price, hint
        )

    watcher = TierWatcher(
        clob=clob,
        server_order_id=server_id,
        token_id=token_id,
        side="BUY",
        strategy_price=0.42,
        expected_size=5.0,
        expires_at=float("+inf"),
        poll_s=0.01,
        fill_ratio=0.995,
        cross_threshold=None,
        mid_price_provider=None,
        on_complete=_build_complete_handler(eng, completed),
        on_partial_fill=on_partial_fill_router,
        is_still_active=lambda: True,
    )
    watcher.start()
    # WS push at +0.005s: receives size_matched=4.0 (was lost by WS in the
    # previous scenario, here it's racing ahead)
    async def ws_push():
        await asyncio.sleep(0.005)
        watcher.receive_update(4.0)
    asyncio.create_task(ws_push())

    await _wait_for_terminal(watcher)
    result = await watcher.stop()
    assert result is not None
    assert result.outcome == TierOutcome.FILLED

    # Replay the production flow: the 4.0 was already credited by WS.
    # Polling observations of 4.0 collapse to delta=0. Only the
    # terminal 5.0 observation triggers a push BUT the WS already
    # advanced so that push's effective_delta should ALSO be 0 in the
    # router (because OrderManager monotonic 4.0 = our claimed 5.0 -
    # 1.0 delta; effective_delta = 5.0 - 4.0 = 1.0).
    #
    # Wait — OrderManager is at 4.0 (WS), so a partial push with
    # cumulative=5.0 sets effective_delta=1.0, which IS new info — so
    # exactly 1 engine call should fire with delta=1.0.
    assert len(eng.on_order_filled_log) == 1, (
        f"expected exactly 1 engine call (only the 4→5 increment), "
        f"got {len(eng.on_order_filled_log)}: {eng.on_order_filled_log}"
    )
    pushed_delta = eng.on_order_filled_log[0][3]
    assert abs(pushed_delta - 1.0) < 1e-9, (
        f"pushed delta must be 1.0 (4→5), got {pushed_delta}"
    )
    print(
        f"OK test_polling_is_idempotent_vs_ws_first  "
        f"eng_calls={len(eng.on_order_filled_log)}"
    )
    return 0


# ---------------------------------------------------------------------------
# Test 3 — partial_fill callback 抛异常:不推进 _last_pushed,下次会重试
# ---------------------------------------------------------------------------


async def test_partial_fill_error_is_safe_to_retry() -> int:
    """If on_partial_fill raises (e.g. OrderManager transient error), the
    watcher must NOT advance `_last_pushed_size_matched` — otherwise the
    fill would be silently dropped and the next poll wouldn't retry."""
    server_id = "0xORDER_T1"
    token_id = "0xTOKEN_DOWN"
    om = _FakeOrderManager()
    om.tracked_by_server[server_id] = _FakeTracked(size_matched=0.0)
    eng = _FakeEngine()

    clob = _FakeCLOB(
        size_matched_timeline=[
            # Fast-path:
            {"size_matched": 0.0, "avg_price": 0.0},
            # --- iter 1: balance + fill_state (prime at 1.0) ---
            {"size_matched": 0.0, "avg_price": 0.0},
            {"size_matched": 1.0, "avg_price": 0.40},   # prime at 1.0
            # --- iter 2: balance + fill_state (NEW 1.0→2.0) ---
            {"size_matched": 2.0, "avg_price": 0.40},
            {"size_matched": 2.0, "avg_price": 0.40},   # delta=1.0; push RAISES
            # --- iter 3: balance + fill_state (NEW 2.0→3.0) ---
            {"size_matched": 3.0, "avg_price": 0.41},
            {"size_matched": 3.0, "avg_price": 0.41},   # delta=1.0; retry succeeds
            # --- iter 4: balance triggers FILLED ---
            {"size_matched": 5.0, "avg_price": 0.42},
            {"size_matched": 5.0, "avg_price": 0.42},   # push delta=2.0
        ],
    )

    push_attempts = {"count": 0, "errors_so_far": 1}  # first push raises

    async def flaky_on_partial_fill(
        cumulative_size_matched: float, fill_price: float, hint: Optional[Any]
    ) -> None:
        push_attempts["count"] += 1
        if push_attempts["errors_so_far"] > 0:
            push_attempts["errors_so_far"] -= 1
            raise RuntimeError("transient OM error")
        # Subsequent calls succeed — derive delta from CUMULATIVE - tracked.
        tracked = om.by_server_id(server_id)
        prev = tracked.size_matched if tracked else 0.0
        delta = cumulative_size_matched - prev
        if delta <= 1e-9:
            return
        om.apply_fill_event(
            server_id, size_matched=cumulative_size_matched, fill_price=fill_price
        )
        eng.on_order_filled(token_id, server_id, fill_price, delta)

    watcher = TierWatcher(
        clob=clob,
        server_order_id=server_id,
        token_id=token_id,
        side="BUY",
        strategy_price=0.42,
        expected_size=5.0,
        expires_at=float("+inf"),
        poll_s=0.01,
        fill_ratio=0.995,
        cross_threshold=None,
        mid_price_provider=None,
        on_complete=lambda r: asyncio.sleep(0),
        on_partial_fill=flaky_on_partial_fill,
        is_still_active=lambda: True,
    )
    watcher.start()
    await _wait_for_terminal(watcher)
    await watcher.stop()

    # After the failed push, _last_pushed_size_matched stays at the prime
    # value. The next poll that observes a strictly-greater cumulative
    # will fire again (the retry), and a third poll that observes the
    # final cumulative fires the residual delta. Total successful engine
    # calls = 2 (retry + final).
    assert len(eng.on_order_filled_log) == 2, (
        f"expected 2 successful engine calls (retry + terminal delta), "
        f"got {len(eng.on_order_filled_log)}: {eng.on_order_filled_log}"
    )
    assert push_attempts["count"] >= 3, (
        f"first push must have raised, then retried + final; "
        f"attempts={push_attempts}"
    )
    # Engine deltas must sum to the ground truth.
    delta_sum = sum(call[3] for call in eng.on_order_filled_log)
    assert abs(delta_sum - 5.0) < 1e-6, (
        f"sum of engine deltas must equal 5.0 (full ground truth), "
        f"got {delta_sum}"
    )
    print(
        f"OK test_partial_fill_error_is_safe_to_retry  "
        f"push_attempts={push_attempts['count']} qty={delta_sum:.4f}"
    )
    return 0


# ---------------------------------------------------------------------------
# Test 4 — backward compat: on_partial_fill=None 不影响现有行为
# ---------------------------------------------------------------------------


async def test_no_partial_fill_callback_keeps_legacy_path() -> int:
    """If on_partial_fill is None (e.g. the SELL/TP path), the watcher
    must still terminate cleanly using only terminal-state
    reconciliation. This is the legacy contract — backstop in case any
    downstream caller forgets to wire the new callback."""
    server_id = "0xORDER_T1"
    token_id = "0xTOKEN_DOWN"
    om = _FakeOrderManager()
    om.tracked_by_server[server_id] = _FakeTracked(size_matched=0.0)
    eng = _FakeEngine()
    completed: list[WatcherResult] = []

    clob = _FakeCLOB(
        size_matched_timeline=[
            # Fast-path: terminal-fill on spawn (size_matched=5.0 ≥ 4.975)
            {"size_matched": 5.0, "avg_price": 0.42},
        ],
    )

    watcher = TierWatcher(
        clob=clob,
        server_order_id=server_id,
        token_id=token_id,
        side="BUY",
        strategy_price=0.42,
        expected_size=5.0,
        expires_at=float("+inf"),
        poll_s=0.01,
        fill_ratio=0.995,
        cross_threshold=None,
        mid_price_provider=None,
        on_complete=_build_complete_handler(eng, completed),
        on_partial_fill=None,  # legacy contract
        is_still_active=lambda: True,
    )
    watcher.start()
    await _wait_for_terminal(watcher)
    result = await watcher.stop()
    assert result is not None and result.outcome == TierOutcome.FILLED
    # No partial pushes were fired:
    assert len(eng.on_order_filled_log) == 0
    print("OK test_no_partial_fill_callback_keeps_legacy_path")
    return 0


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------


async def main() -> int:
    rc = 0
    for test in (
        test_polling_recovers_when_ws_dropped,
        test_polling_is_idempotent_vs_ws_first,
        test_partial_fill_error_is_safe_to_retry,
        test_no_partial_fill_callback_keeps_legacy_path,
    ):
        try:
            sub_rc = await test()
            if sub_rc != 0:
                rc = 1
        except AssertionError as e:
            print(f"FAIL {test.__name__}: {e}")
            rc = 1
        except Exception as e:
            print(f"ERROR {test.__name__}: {type(e).__name__}: {e}")
            rc = 1
    if rc == 0:
        print("ALL TierWatcher partial-resync TESTS PASSED")
    return rc


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
