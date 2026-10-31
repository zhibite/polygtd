"""Gamma resolution parsing used by settlement accounting.

The dashboard books a losing position at expiry from the market outcome,
so `get_market_resolution` must reliably distinguish a resolved market
from a closed/voided one. Gamma has exposed the result via `resolved`,
UMA status, or terminal outcome prices depending on the market; these
tests pin the accepted shapes.
"""
from __future__ import annotations

import asyncio

from src.market.pm_client import PolymarketClient


class _Response:
    def __init__(self, data) -> None:
        self._data = data

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._data


class _HTTP:
    def __init__(self, data) -> None:
        self.data = data

    async def get(self, url, params=None):  # noqa: ARG002
        return _Response(self.data)


def _client_with(data: dict) -> PolymarketClient:
    pm = PolymarketClient()

    async def _client():
        return _HTTP(data)

    pm._client = _client  # type: ignore[method-assign]
    return pm


def _resolve(data: dict) -> dict:
    return asyncio.run(_client_with(data).get_market_resolution("0xdead"))


def test_explicit_resolved_flag() -> None:
    out = _resolve({"resolved": True, "outcomePrices": '["0", "1"]'})
    assert out == {"resolved": True, "up_wins": False, "outcome": ""}


def test_closed_market_with_terminal_prices_is_resolved() -> None:
    """Some series omit `resolved`; closed + 0/1 prices is conclusive."""
    out = _resolve({"closed": True, "outcomePrices": '["1", "0"]'})
    assert out == {"resolved": True, "up_wins": True, "outcome": ""}


def test_up_index_follows_the_outcomes_label_order() -> None:
    """Do not assume Up is always outcomePrices[0]."""
    out = _resolve({
        "resolved": True,
        "outcomes": '["Down", "Up"]',
        "outcomePrices": '["1", "0"]',
    })
    assert out["up_wins"] is False


def test_uma_resolved_status_is_honoured() -> None:
    out = _resolve({
        "closed": False,
        "umaResolutionStatus": "resolved",
        "outcomePrices": '["0", "1"]',
    })
    assert out["resolved"] is True
    assert out["up_wins"] is False


def test_string_false_is_not_treated_as_resolved() -> None:
    """`bool("false")` is True — Gamma booleans are parsed explicitly."""
    out = _resolve({
        "resolved": "false",
        "closed": "false",
        "outcomePrices": '["0", "0"]',
    })
    assert out["resolved"] is False


def test_string_true_is_honoured() -> None:
    out = _resolve({
        "resolved": "true",
        "outcomePrices": '["0", "1"]',
    })
    assert out["resolved"] is True
    assert out["up_wins"] is False


def test_voided_prices_are_not_a_resolution() -> None:
    """A voided market can be closed with ["0","0"] — not a DOWN win."""
    out = _resolve({"closed": True, "outcomePrices": '["0", "0"]'})
    assert out == {"resolved": False, "up_wins": None, "outcome": ""}


def test_open_market_with_mid_prices_is_unresolved() -> None:
    out = _resolve({
        "closed": False,
        "resolved": False,
        "outcomePrices": '["0.52", "0.48"]',
    })
    assert out["resolved"] is False
