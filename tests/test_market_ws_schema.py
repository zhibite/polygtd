"""Regression tests for `market_ws._process` against the real Polymarket
Market Channel payload schema.

History:
  H-V6-9 (2026-07-11): the previous code looked for a nested
  `best_bid_ask: { bids: [...], asks: [...] }` dict, but the real
  Polymarket event is a TOP-LEVEL scalar pair on a dedicated
  `event_type: "best_bid_ask"` message. The original test pinned that
  parsing via `cache.best_bid[token]` / `cache.best_ask[token]`.

  H-NEW-2026-07-15: the `best_bid` / `best_ask` cache fields were
  REMOVED from `PriceCache` entirely. Strategy now trades off REST
  midpoint + WS `last_trade_price` only. Orderbook events
  (`best_bid_ask`, `book`, `price_change.price_changes[].best_bid`)
  are intentionally NOT parsed in `_process` — see the comment at
  the top of that branch in `market_ws.py`.

  The schema-shape regressions are still valuable: a future drift in
  the `last_trade_price` / `price_change` paths must surface as a
  failing test. These tests now assert what is actually consumed by
  the strategy, not the abandoned orderbook cache fields.
"""
from __future__ import annotations

import asyncio
import json

import pytest

from src.market.market_ws import MarketWS, PriceCache


def _make_ws() -> tuple[MarketWS, PriceCache]:
    cache = PriceCache()
    ws = MarketWS(cache=cache)
    return ws, cache


@pytest.mark.asyncio
async def test_best_bid_ask_event_is_ignored() -> None:
    """`event_type: "best_bid_ask"` carries only the top-of-book pair.

    H-NEW-2026-07-15: PriceCache no longer stores best_bid / best_ask —
    the strategy uses REST midpoint + WS last_trade_price. The event
    must not crash, must not write to any cache field, and must not
    leak the scalar pair into last_trade_price (this is an orderbook
    snapshot, not a trade execution).
    """
    ws, cache = _make_ws()
    msg = {
        "event_type": "best_bid_ask",
        "market": "0xabc",
        "asset_id": "TOKEN_UP",
        "best_bid": "0.42",
        "best_ask": "0.55",
        "spread": "0.13",
        "timestamp": "1757908892351",
    }
    await ws._process(json.dumps(msg))

    # No field should be populated from a best_bid_ask event.
    assert cache.last_trade_price.get("TOKEN_UP") is None
    assert cache.midpoint.get("TOKEN_UP") is None


@pytest.mark.asyncio
async def test_price_change_event_updates_last_trade_price_for_each_token() -> None:
    """`price_change.price_changes[]` carries per-asset trade executions.

    Each item has a `price` field that is the actual fill price — that
    goes into `last_trade_price`. Pre-fix the loop bailed after the
    first token; this pins the multi-token case.
    """
    ws, cache = _make_ws()
    msg = {
        "event_type": "price_change",
        "market": "0xabc",
        "price_changes": [
            {
                "asset_id": "TOKEN_UP",
                "price": "0.5",
                "size": "200",
                "side": "BUY",
                "hash": "0xdead",
                "best_bid": "0.5",
                "best_ask": "0.6",
            },
            {
                "asset_id": "TOKEN_DOWN",
                "price": "0.5",
                "size": "200",
                "side": "SELL",
                "hash": "0xbeef",
                "best_bid": "0.4",
                "best_ask": "0.5",
            },
        ],
        "timestamp": "1757908892351",
    }
    await ws._process(json.dumps(msg))

    # BOTH tokens must have last_trade_price set, not just the first.
    assert cache.last_trade_price["TOKEN_UP"] == pytest.approx(0.5)
    assert cache.last_trade_price["TOKEN_DOWN"] == pytest.approx(0.5)
    # midpoint is populated by the REST poller, NOT by WS events.
    assert cache.midpoint.get("TOKEN_UP") is None
    assert cache.midpoint.get("TOKEN_DOWN") is None


@pytest.mark.asyncio
async def test_book_event_is_ignored() -> None:
    """`event_type: "book"` is the full L2 snapshot — orderbook, not trade.

    H-NEW-2026-07-15: the book event is intentionally NOT parsed; it
    neither crashes nor populates any cache field. The strategy's
    orderbook view comes from REST midpoint polling, not WS.
    """
    ws, cache = _make_ws()
    msg = {
        "event_type": "book",
        "asset_id": "TOKEN_UP",
        "market": "0xabc",
        "bids": [
            {"price": "0.50", "size": "200"},
            {"price": "0.49", "size": "100"},
        ],
        "asks": [
            {"price": "0.55", "size": "50"},
        ],
        "timestamp": "1757908892351",
        "hash": "0x...",
    }
    await ws._process(json.dumps(msg))

    # No cache field should be populated from a book event.
    assert cache.last_trade_price.get("TOKEN_UP") is None
    assert cache.midpoint.get("TOKEN_UP") is None


@pytest.mark.asyncio
async def test_legacy_nested_best_bid_ask_is_ignored() -> None:
    """Defensive: if a server emits a legacy nested dict, we do nothing.

    H-NEW-2026-07-15: there is no `best_bid_ask` fallback path in
    `_process` anymore — the legacy nested form is silently ignored
    along with every other orderbook-shape variant. This test pins
    that defensive stance: an unrecognized orderbook shape must not
    clobber an existing `last_trade_price`.
    """
    ws, cache = _make_ws()
    # Pre-fill a known good last_trade_price from a previous trade event.
    cache.last_trade_price["TOKEN_UP"] = 0.99

    msg = {
        "event_type": "book",
        "asset_id": "TOKEN_UP",
        "bids": [{"price": "0.42", "size": "1"}],
        "asks": [{"price": "0.50", "size": "1"}],
        "best_bid_ask": {       # legacy / defensive path (now defunct)
            "asset_id": "TOKEN_UP",
            "bids": [{"price": "0.10", "size": "999"}],
            "asks": [{"price": "0.20", "size": "999"}],
        },
    }
    await ws._process(json.dumps(msg))

    # last_trade_price must NOT have been clobbered by the book event.
    assert cache.last_trade_price["TOKEN_UP"] == pytest.approx(0.99)
    # And nothing was written to midpoint either.
    assert cache.midpoint.get("TOKEN_UP") is None


@pytest.mark.asyncio
async def test_no_crash_on_malformed_message() -> None:
    ws, _ = _make_ws()
    # Garbage input must not kill the WS reader.
    await ws._process("not json at all")
    await ws._process(json.dumps([1, 2, 3]))  # array, not dict
    await ws._process(json.dumps({"event_type": "best_bid_ask"}))  # no asset_id
    # No assertion — just confirms the coroutine returned cleanly.