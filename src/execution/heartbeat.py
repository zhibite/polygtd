"""Heartbeat loop — keeps Polymarket aware we're alive.

Per Polymarket rules:
  - POST /heartbeat must be sent at least every 10 seconds.
  - If no heartbeat is received for 10s, all open GTD orders are cancelled.
  - This runs as an independent asyncio Task so it is not blocked by other loops.

Bootstrap: the first call sends a null id and the server answers 400
"Invalid Heartbeat ID" **with the session id in the body**. `CLOBClient`
surfaces that as `suggested_heartbeat_id`; this loop adopts it immediately so
the next tick (5 s later) is a valid heartbeat instead of another blind
bootstrap. Before this, production took 4 attempts (~20 s) to reach
`session_ready` — dangerous mid-round, because Polymarket cancels open GTDs
after 10 s without a heartbeat.

Logging policy: the happy path is silent (this loop runs every 5 seconds and
would otherwise flood logs). We only emit:
  - INFO on start / stop / first successful session id (one-shot each)
  - WARNING when we lose an established session (had a heartbeat_id, then error)
  - WARNING if the server keeps handing us new ids for 4+ ticks
  - DEBUG for the expected first-call bootstrap failure (empty id → 400)
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
        self._session_announced: bool = False
        self._consecutive_errors: int = 0
        # Post cadence — overridable in tests.
        self._interval: float = 5.0
        # Consecutive "server handed us a new id" responses. Normally 1,
        # because the next tick succeeds; a long streak means the server
        # keeps rejecting our id and deserves a WARNING.
        self._hint_streak: int = 0

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run())
        log.info(
            "heartbeat.started",
            interval_sec=self._interval,
            server_window_sec=10.0,
        )

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
        while self._running:
            await asyncio.sleep(self._interval)

            if not self._clob.is_configured():
                continue

            try:
                resp = await self._clob.post_heartbeat(self._heartbeat_id)
                self._consecutive_errors = 0
                if resp and resp.get("heartbeat_id"):
                    self._heartbeat_id = resp["heartbeat_id"]
                    self._hint_streak = 0
                    if not self._session_announced:
                        self._session_announced = True
                        log.info("heartbeat.session_ready")
                elif not resp.get("success", True):
                    err = resp.get("error", "")
                    hint = resp.get("suggested_heartbeat_id")
                    if hint:
                        # Server rejected our id but returned the session id
                        # to use (bootstrap / session re-issue). Adopt it now
                        # so the NEXT tick — 5 s later — is a valid
                        # heartbeat instead of another blind bootstrap. This
                        # is what turns the ~20 s startup gap into ~5 s.
                        self._heartbeat_id = str(hint)
                        self._session_announced = False
                        self._hint_streak += 1
                        if self._hint_streak == 4:
                            # Log ONCE per stuck streak — not every 5 s.
                            log.warning(
                                "heartbeat.bootstrap_stuck",
                                attempts=self._hint_streak,
                                hint=str(hint)[:12],
                            )
                        else:
                            log.debug(
                                "heartbeat.id_reassigned",
                                new_id=str(hint)[:12],
                            )
                    elif self._is_invalid_id_error(err):
                        # Server forgot our id (session expiry). Drop it and
                        # re-bootstrap on next tick by sending heartbeat_id=None.
                        log.debug("heartbeat.id_rejected",
                                  old_id=self._heartbeat_id[:12])
                        self._heartbeat_id = ""
                        self._session_announced = False
                        self._hint_streak = 0
                    else:
                        self._handle_error("error", err[:100])
            except Exception as exc:
                self._handle_error("exception", str(exc))

    @staticmethod
    def _is_invalid_id_error(msg: str) -> bool:
        """Server rejected our cached heartbeat_id — must re-bootstrap."""
        return "Invalid Heartbeat ID" in msg or "invalid heartbeat" in msg.lower()

    def _handle_error(self, kind: str, msg: str) -> None:
        """Route heartbeat errors to the right level so the log stays quiet.

        - bootstrap (no id yet)         → DEBUG (expected, server 400)
        - transient (<=3 in a row)      → DEBUG
        - persistent (>3 in a row)      → WARNING (once per failure streak)
        - established session lost      → WARNING immediately
        """
        if not self._heartbeat_id:
            log.debug("heartbeat.bootstrap", kind=kind, error=msg)
            return

        if not self._session_announced:
            log.warning("heartbeat.session_lost", kind=kind, error=msg)
            self._session_announced = False
            self._consecutive_errors += 1
            return

        self._consecutive_errors += 1
        if self._consecutive_errors <= 3:
            log.debug("heartbeat.transient", kind=kind, error=msg,
                      consecutive=self._consecutive_errors)
        elif self._consecutive_errors == 4:
            log.warning("heartbeat.failing", kind=kind, error=msg,
                        consecutive=self._consecutive_errors)
