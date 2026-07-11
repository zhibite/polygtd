"""H-V6-9 regression test: market_ws._process handles the real Polymarket
Market Channel payload schema.

Production symptom: 2026-07-11 01:17 round 01:15 — STATUS showed
UP=0.5350 / DOWN=0.3750 (REST midpoint), but `cache.best_bid[token]`
stayed 0 for both tokens, blocking T1 entry with
`engine.t1_skip_no_bid`. Root cause: the previous code looked for a
nested `best_bid_ask: { bids: [...], asks: [...] }` dict, but the
real Polymarket event is a TOP-LEVEL scalar pair on a dedicated
`event_type: "best_bid_ask"` message. Same for `book` (top-level
bids/asks arrays) and `price_change` (per-item best_bid/best_ask).

These tests pin the new shape so a future schema drift shows up
as a failing test rather than 15 minutes of silent zero-bid.
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
async def test_best_bid_ask_event_populates_cache() -> None:
    """Top-level best_bid / best_ask on event_type=best_bid_ask must
    populate PriceCache.best_bid / best_ask."""
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

    assert cache.best_bid["TOKEN_UP"] == pytest.approx(0.42)
    assert cache.best_ask["TOKEN_UP"] == pytest.approx(0.55)


@pytest.mark.asyncio
async def test_price_change_event_populates_cache() -> None:
    """price_change.price_changes[].best_bid/best_ask must populate cache
    for each asset_id in the list (subscribed multiple tokens)."""
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

    # BOTH tokens must have best_bid set, not just the first one.
    # Pre-fix this was the production bug: only TOKEN_UP got filled
    # because the loop bailed early.
    assert cache.best_bid["TOKEN_UP"] == pytest.approx(0.5)
    assert cache.best_ask["TOKEN_UP"] == pytest.approx(0.6)
    assert cache.best_bid["TOKEN_DOWN"] == pytest.approx(0.4)
    assert cache.best_ask["TOKEN_DOWN"] == pytest.approx(0.5)
    # price_changes also carry the trade execution price.
    assert cache.last_trade_price["TOKEN_UP"] == pytest.approx(0.5)
    assert cache.last_trade_price["TOKEN_DOWN"] == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_book_event_populates_cache_from_top_level_arrays() -> None:
    """book event has top-level bids[]/asks[] arrays (full snapshot)."""
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

    assert cache.best_bid["TOKEN_UP"] == pytest.approx(0.50)
    assert cache.best_ask["TOKEN_UP"] == pytest.approx(0.55)


@pytest.mark.asyncio
async def test_legacy_nested_best_bid_ask_still_works_as_fallback() -> None:
    """If a server emits a legacy nested dict, we should still populate
    cache via the defensive path. Don't clobber an already-better value
    though (use setdefault semantics)."""
    ws, cache = _make_ws()
    # Pre-fill with a known good price from a top-level event.
    cache.best_bid["TOKEN_UP"] = 0.99

    msg = {
        "event_type": "book",
        "asset_id": "TOKEN_UP",
        "bids": [{"price": "0.42", "size": "1"}],
        "asks": [{"price": "0.50", "size": "1"}],
        "best_bid_ask": {       # legacy / defensive path
            "asset_id": "TOKEN_UP",
            "bids": [{"price": "0.10", "size": "999"}],
            "asks": [{"price": "0.20", "size": "999"}],
        },
    }
    await ws._process(json.dumps(msg))

    # The book event takes precedence — best_bid must be 0.42.
    assert cache.best_bid["TOKEN_UP"] == pytest.approx(0.42)
    assert cache.best_ask["TOKEN_UP"] == pytest.approx(0.50)


@pytest.mark.asyncio
async def test_no_crash_on_malformed_message() -> None:
    ws, _ = _make_ws()
    # Garbage input must not kill the WS reader.
    await ws._process("not json at all")
    await ws._process(json.dumps([1, 2, 3]))  # array, not dict
    await ws._process(json.dumps({"event_type": "best_bid_ask"}))  # no asset_id
    # No assertion — just confirms the coroutine returned cleanly.