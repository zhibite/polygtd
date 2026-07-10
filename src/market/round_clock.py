"""Round clock — tracks the current 15-min window boundary.

Responsibilities:
  - Compute the current round start timestamp from system clock.
  - Detect round transitions so the bot knows when to switch markets.
  - Provide a simple event-style callback (on_new_round).
"""
from __future__ import annotations

import asyncio
import time
from typing import Callable, Awaitable

from src.config import general, round_clock as cfg
from src.infra.logging import get_logger

log = get_logger(__name__)

RoundCallback = Callable[[int], Awaitable[None]]
# int = round_start_ts (Unix seconds, UTC)


class RoundClock:
    """Fires on_new_round() every time the 15-min window rolls over."""

    def __init__(self, on_new_round: RoundCallback) -> None:
        self._on_new = on_new_round
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._period = general().ROUND_PERIOD_S
        self._current_start: int | None = None

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    @staticmethod
    def round_start_of(ts: float | int) -> int:
        """Floor ts to nearest 15-min boundary."""
        return int(ts // 900) * 900

    @property
    def current_start(self) -> int | None:
        return self._current_start

    def next_round_start(self) -> int:
        """Timestamp of the upcoming round boundary."""
        now = time.time()
        return self.round_start_of(now) + self._period

    def seconds_until_next(self) -> float:
        return self.next_round_start() - time.time()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._current_start = self.round_start_of(time.time())
        log.info("round_clock.started",
                 period_s=self._period,
                 current_start=self._current_start)
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("round_clock.stopped")

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        tick_interval = 0.5  # check every 500ms

        while self._running:
            await asyncio.sleep(tick_interval)

            now_start = self.round_start_of(time.time())

            if now_start != self._current_start:
                prev = self._current_start
                self._current_start = now_start
                log.info("round_clock.transition",
                         prev_start=prev,
                         new_start=now_start,
                         next_start=self.next_round_start())
                try:
                    await self._on_new(now_start)
                except Exception:
                    log.exception("round_clock.on_new_round raised")

    # ------------------------------------------------------------------
    # Human-readable helpers
    # ------------------------------------------------------------------

    @staticmethod
    def format_ts(ts: int) -> str:
        import datetime
        return datetime.datetime.fromtimestamp(ts, tz=None).strftime("%H:%M:%S")
