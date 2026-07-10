"""Heartbeat loop — keeps Polymarket aware we're alive.

Per Polymarket rules:
  - POST /heartbeat must be sent at least every 10 seconds.
  - If no heartbeat is received for 10s, all open GTD orders are cancelled.
  - This runs as an independent asyncio Task so it is not blocked by other loops.
"""
from __future__ import annotations

import asyncio

from src.execution.clob_client import CLOBClient
from src.infra.logging import get_logger

log = get_logger(__name__)


class HeartbeatLoop:
    """Sends POST /heartbeat every 5 seconds (safe margin under 10s limit)."""

    def __init__(self, clob: CLOBClient) -> None:
        self._clob = clob
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._heartbeat_id: str = ""

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run())
        log.info("heartbeat.started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("heartbeat.stopped")

    async def _run(self) -> None:
        interval = 5.0  # post every 5s — well within the 10s mandatory window

        while self._running:
            await asyncio.sleep(interval)

            if not self._clob.is_configured():
                continue

            try:
                resp = await self._clob.post_heartbeat(self._heartbeat_id)
                if resp and resp.get("heartbeat_id"):
                    self._heartbeat_id = resp["heartbeat_id"]
                elif not resp.get("success", True):
                    log.warning("heartbeat.error",
                                error=resp.get("error", "")[:100])
            except Exception as exc:
                log.warning("heartbeat.exception", error=str(exc))
