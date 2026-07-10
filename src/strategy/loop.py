"""Decision loop — drives StrategyEngine._evaluate() on a periodic tick.

Why this exists:
    StrategyEngine._evaluate() is the entry point for all strategy decisions
    (T1 entry, averaging-down, take-profit). Until B-1 was fixed, nothing in
    the codebase ever invoked _evaluate, so the entire decision loop was dead
    code. DecisionLoop is a thin asyncio task that wakes up on a fixed
    interval and re-runs _evaluate() while a round is active.

Concurrency:
    A single in-flight asyncio.Lock guarantees that two ticks never overlap,
    so engine state is touched by exactly one task at a time. This also
    prevents the H-6 race where two ticks could both see a clean state and
    emit duplicate T1 actions.

Tick source:
    The interval is intentionally short (default 250ms) because decisions
    are driven off price-cache snapshots that are refreshed independently
    by the REST midpoint poller and the BinanceFeed. We do NOT need to
    sleep until a fresh tick arrives — re-evaluating on stale data is cheap
    (a single mid-price read + a few short-circuits) and avoids the
    complexity of subscribing to upstream price events.
"""
from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from src.infra.logging import get_logger

if TYPE_CHECKING:
    from src.strategy.engine import StrategyEngine

log = get_logger(__name__)


class DecisionLoop:
    """Periodic driver for StrategyEngine._evaluate()."""

    def __init__(
        self,
        engine: "StrategyEngine",
        tick_interval_s: float = 0.25,
    ) -> None:
        self._engine = engine
        self._tick_interval = max(0.05, float(tick_interval_s))
        self._running = False
        self._task: asyncio.Task[None] | None = None
        # Serializes _evaluate() calls — see module docstring.
        self._inflight = asyncio.Lock()

    def start(self) -> None:
        """Spawn the background task. Idempotent."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name="decision_loop")
        log.info("decision_loop.started", tick_interval_s=self._tick_interval)

    async def stop(self) -> None:
        """Cancel the task and wait for it to drain."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        log.info("decision_loop.stopped")

    async def _run(self) -> None:
        """Main loop. Each iteration is one evaluate tick."""
        try:
            while self._running:
                try:
                    await self._tick()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # Never let a single bad tick kill the loop.
                    log.exception("decision_loop.tick_error", error=str(exc))
                await asyncio.sleep(self._tick_interval)
        except asyncio.CancelledError:
            pass

    async def _tick(self) -> None:
        """Run a single evaluation under the inflight lock.

        If the previous tick is still running (slow handler, network blip),
        we skip this tick rather than queueing — strategy decisions are
        idempotent and the next tick will re-evaluate against fresh state.
        """
        if self._inflight.locked():
            # Previous tick still running. Skip rather than queue.
            return

        async with self._inflight:
            await self._engine._evaluate()