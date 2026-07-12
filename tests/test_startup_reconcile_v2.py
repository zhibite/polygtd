"""
Regression: order_manager.startup_reconcile must use the V2 field name.

H-NEW-2026-07-12 production logs (00:44:50Z):
    [warning] clob.get_orders error [src.execution.clob_client]
        error="'ClobClient' object has no attribute 'get_orders'"
    [info] order_manager.startup_reconcile cancelled=0
        note='no open orders on CLOB'

Two stacked bugs:

  Bug A (clob_client.py):
    `get_open_orders()` called `self._client.get_orders`. That method
    does NOT exist on py-clob-client-v2 — it was renamed to
    `get_open_orders()`. Each call raised AttributeError, the
    except clause logged it as warning, returned [].

  Bug B (order_manager.py):
    Even if Bug A is fixed and `get_open_orders` returns rows, the
    downstream consumer was reading `o.get("orderID", "")`. The V2
    SDK (and the actual /data/orders REST response) uses the field
    name `id`. So every row was filtered out and the bot believed
    there were zero open orders — leaving stale GTDs to consume
    maker allowance forever (audit v6-12 / H-NEW-2026-07-11 prod
    symptom: `0x3ede40a1f6c744` status=invalid blocking every SELL).

The test below wires up a fake CLOBClient that returns V2-shaped
rows and asserts that startup_reconcile:
  1. Reads `id` (not `orderID`)
  2. Picks LIVE / INVALID / UNKNOWN rows
  3. Skips already-terminal rows
  4. Calls cancel_orders with the right server_id list

The wrapper layer is exercised without touching the network.
"""
from __future__ import annotations

from typing import Any, List

import pytest

from src.execution.clob_client import CLOBClient
from src.execution.order_manager import OrderManager


class _FakeClob:
    """Minimal CLOBClient stand-in. Only the methods startup_reconcile
    calls are implemented."""

    def __init__(self, open_orders: List[dict]) -> None:
        self._open_orders = open_orders
        self.cancel_calls: List[List[str]] = []

    async def get_open_orders(self, condition_id: str | None = None) -> list[dict]:
        return self._open_orders

    async def cancel_orders(self, server_ids: list[str]) -> dict:
        self.cancel_calls.append(list(server_ids))
        # Pretend all cancellations succeeded.
        return {"canceled": list(server_ids), "not_canceled": []}

    async def cancel_order(self, server_id: str) -> bool:
        self.cancel_calls.append([server_id])
        return True


def _make_om_with(clob: _FakeClob) -> OrderManager:
    """OrderManager needs only `clob` for startup_reconcile."""
    om = OrderManager.__new__(OrderManager)
    om._clob = clob
    om._lock = __import__("asyncio").Lock()
    om._orders = {}
    om._by_server = {}
    om._pending_fills = {}
    om._startup_reconciled = False
    return om


@pytest.mark.asyncio
async def test_startup_reconcile_uses_v2_id_field() -> None:
    """
    Bug A+B repro. The fake CLOB returns rows with `id` (V2 schema).
    Old code looked for `orderID` and would have zero rows.
    """
    clob = _FakeClob([
        {"id": "0xaaa", "status": "LIVE",   "market": "0xmarket1"},
        {"id": "0xbbb", "status": "INVALID","market": "0xmarket1"},
        {"id": "0xccc", "status": "LIVE",   "market": "0xmarket2"},
    ])
    om = _make_om_with(clob)

    await om.startup_reconcile()

    # Cancel called with all 3 server ids (deduped from `id`, not `orderID`).
    assert clob.cancel_calls, "cancel_orders should have been called"
    cancelled = clob.cancel_calls[0]
    assert set(cancelled) == {"0xaaa", "0xbbb", "0xccc"}, (
        f"expected all 3 LIVE/INVALID rows cancelled; got {cancelled}"
    )


@pytest.mark.asyncio
async def test_startup_reconcile_skips_terminal_rows() -> None:
    """
    Even with V2 field name, the bot should not waste a cancel call
    on rows that the CLOB already considers terminal (matched, expired,
    cancelled). The audit v6-12 fix says: only cancel LIVE / INVALID /
    UNKNOWN rows.
    """
    clob = _FakeClob([
        {"id": "0xlive",      "status": "LIVE",    "market": "0x"},
        {"id": "0xmatched",   "status": "MATCHED", "market": "0x"},
        {"id": "0xcancelled", "status": "CANCELED","market": "0x"},
        {"id": "0xexpired",   "status": "EXPIRED", "market": "0x"},
    ])
    om = _make_om_with(clob)

    await om.startup_reconcile()

    cancelled = clob.cancel_calls[0]
    assert cancelled == ["0xlive"], (
        f"only LIVE should be cancelled; terminal rows (MATCHED/CANCELED/EXPIRED) "
        f"are inert and must NOT trigger a cancel call. got {cancelled}"
    )


@pytest.mark.asyncio
async def test_startup_reconcile_handles_empty_response() -> None:
    """
    When the CLOB genuinely has no open orders, the reconcile path is a
    no-op and sets _startup_reconciled=True so it never re-runs.
    """
    clob = _FakeClob([])
    om = _make_om_with(clob)

    await om.startup_reconcile()

    assert clob.cancel_calls == [], "no cancels expected on empty CLOB"
    assert om._startup_reconciled is True


@pytest.mark.asyncio
async def test_startup_reconcile_idempotent() -> None:
    """Calling it twice is a no-op the second time."""
    clob = _FakeClob([{"id": "0xaa", "status": "LIVE", "market": "0x"}])
    om = _make_om_with(clob)

    await om.startup_reconcile()
    await om.startup_reconcile()

    assert len(clob.cancel_calls) == 1, (
        "second call should be a no-op due to _startup_reconciled guard"
    )


@pytest.mark.asyncio
async def test_startup_reconcile_v1_orderID_field_still_works() -> None:
    """
    Defensive compat: if a future SDK version brings back the v1 field
    name `orderID` (some forks still emit it), the reconcile path must
    still pick it up. The code does `o.get("id") or o.get("orderID")`.
    """
    clob = _FakeClob([
        {"orderID": "0xv1row", "status": "LIVE"},  # only v1 field
    ])
    om = _make_om_with(clob)

    await om.startup_reconcile()

    cancelled = clob.cancel_calls[0]
    assert cancelled == ["0xv1row"], (
        f"v1 orderID field should still be honoured as a fallback; got {cancelled}"
    )