"""Heartbeat bootstrap regression tests (2026-10-30).

Production symptom: on every fresh session the first heartbeat sent a null
id, the server replied HTTP 400 ``{"heartbeat_id": "...", "error_msg":
"Invalid Heartbeat ID"}``, and the SDK raised — dropping the id the server
handed back. The loop kept re-sending null and took 4 attempts (~20 s) to
reach ``session_ready``.

Polymarket cancels open GTDs after 10 s without a heartbeat, so that gap is
dangerous in a live round. Fix: surface ``suggested_heartbeat_id`` from the
400 body and adopt it on the next tick (~5 s).
"""
from __future__ import annotations

import httpx
import pytest
from py_clob_client_v2.exceptions import PolyApiException

from src.execution.clob_client import CLOBClient
from src.execution.heartbeat import HeartbeatLoop


def _sdk_raising(body: dict) -> object:
    class _Sdk:
        def post_heartbeat(self, heartbeat_id):  # noqa: ARG002
            raise PolyApiException(httpx.Response(400, json=body))

    return _Sdk()


@pytest.mark.asyncio
async def test_post_heartbeat_surfaces_server_hint() -> None:
    client = CLOBClient.__new__(CLOBClient)
    client._client = _sdk_raising(
        {"heartbeat_id": "srv-1234", "error_msg": "Invalid Heartbeat ID"}
    )

    resp = await CLOBClient.post_heartbeat(client)

    assert resp["success"] is False
    assert resp["suggested_heartbeat_id"] == "srv-1234"


@pytest.mark.asyncio
async def test_post_heartbeat_without_hint() -> None:
    client = CLOBClient.__new__(CLOBClient)
    client._client = _sdk_raising({"error_msg": "some other failure"})

    resp = await CLOBClient.post_heartbeat(client)

    assert resp["success"] is False
    assert "suggested_heartbeat_id" not in resp


class _SequenceClob:
    """Returns the scripted responses and stops the loop after N calls."""

    def __init__(self, responses: list[dict]) -> None:
        self._responses = responses
        self.sent: list[str] = []
        self.loop: HeartbeatLoop | None = None
        self._stop_after = len(responses)

    def is_configured(self) -> bool:
        return True

    async def post_heartbeat(self, heartbeat_id: str = "") -> dict:
        self.sent.append(heartbeat_id)
        resp = self._responses[len(self.sent) - 1]
        if len(self.sent) >= self._stop_after and self.loop is not None:
            self.loop._running = False
        return resp


@pytest.mark.asyncio
async def test_loop_adopts_hint_and_reaches_session_ready() -> None:
    clob = _SequenceClob([
        # tick 1: blind bootstrap (null id) → server hands back the id
        {"success": False, "error": "Invalid Heartbeat ID",
         "suggested_heartbeat_id": "srv-1234"},
        # tick 2: valid heartbeat
        {"success": True, "heartbeat_id": "srv-1234"},
        # tick 3: steady state
        {"success": True, "heartbeat_id": "srv-1234"},
    ])
    loop = HeartbeatLoop(clob)  # type: ignore[arg-type]
    clob.loop = loop
    loop._running = True
    loop._interval = 0.0  # no real waiting in the test

    await loop._run()

    assert clob.sent == ["", "srv-1234", "srv-1234"], (
        "the id returned by the 400 body must be used on the very next tick"
    )
    assert loop._heartbeat_id == "srv-1234"
    assert loop._session_announced is True
    assert loop._hint_streak == 0


@pytest.mark.asyncio
async def test_repeated_hints_are_tracked_for_warning() -> None:
    clob = _SequenceClob([
        {"success": False, "error": "Invalid Heartbeat ID",
         "suggested_heartbeat_id": f"srv-{i}"}
        for i in range(4)
    ])
    loop = HeartbeatLoop(clob)  # type: ignore[arg-type]
    clob.loop = loop
    loop._running = True
    loop._interval = 0.0

    await loop._run()

    assert loop._hint_streak == 4, (
        "a persistent hint streak must be visible so bootstrap_stuck fires"
    )
    assert loop._heartbeat_id == "srv-3"
