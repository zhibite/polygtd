"""Fill-price accuracy: the CLOB order payload carries no average price.

Production 2026-10-31 09:30: our fills were recorded at the LIMIT (0.40
buys and sells) while Polymarket's history showed the actual taker
executions (0.37-0.39 buys, 0.49 sells) — realized PnL was understated
roughly 3x. The order payload only has the limit `price` and
`associate_trades` (trade ids), so `get_order_fill_state` now derives the
weighted average from those trades.
"""
from __future__ import annotations

import pytest

from src.execution.clob_client import CLOBClient


class _Sdk:
    def __init__(self, order: dict, trades: dict) -> None:
        self._order = order
        self._trades = trades
        self.get_order_calls = 0

    def get_order(self, order_id: str):  # noqa: ANN201
        self.get_order_calls += 1
        return self._order

    def get_trades(self, params):  # noqa: ANN001,ANN201
        return self._trades.get(params.id, [])


def _client(order: dict, trades: dict) -> tuple[CLOBClient, _Sdk]:
    client = CLOBClient.__new__(CLOBClient)
    sdk = _Sdk(order, trades)
    client._client = sdk
    return client, sdk


@pytest.mark.asyncio
async def test_taker_fill_price_comes_from_the_trade_not_the_limit() -> None:
    order_id = "0xorder"
    trades = {
        "t1": [{
            "taker_order_id": order_id,
            "price": "0.39",
            "size": "5",
        }],
    }
    client, sdk = _client(
        {
            "size_matched": "5",
            "associate_trades": ["t1"],
            "status": "MATCHED",
            "price": "0.40",  # the LIMIT — must NOT be used as the fill price
        },
        trades,
    )

    state = await client.get_order_fill_state(order_id)

    assert state["size_matched"] == pytest.approx(5.0)
    assert state["avg_price"] == pytest.approx(0.39)

    # The trade lookup is cached: a second read re-fetches only the order.
    sdk.get_order_calls = 0
    state2 = await client.get_order_fill_state(order_id)
    assert state2["avg_price"] == pytest.approx(0.39)
    assert sdk.get_order_calls == 1


@pytest.mark.asyncio
async def test_maker_fill_uses_the_maker_row_price() -> None:
    order_id = "0xmaker"
    trades = {
        "t1": [{
            "taker_order_id": "0xother",
            "price": "0.68",       # opposite-side taker price
            "size": "9.36",
            "maker_orders": [{
                "order_id": order_id,
                "price": "0.32",   # our resting limit
                "matched_amount": "9.36",
            }],
        }],
    }
    client, _ = _client(
        {"size_matched": "9.36", "associate_trades": ["t1"], "status": "MATCHED"},
        trades,
    )

    state = await client.get_order_fill_state(order_id)

    assert state["avg_price"] == pytest.approx(0.32)


@pytest.mark.asyncio
async def test_weighted_average_across_multiple_trades() -> None:
    order_id = "0xmulti"
    trades = {
        "t1": [{"taker_order_id": order_id, "price": "0.40", "size": "2"}],
        "t2": [{"taker_order_id": order_id, "price": "0.30", "size": "3"}],
    }
    client, _ = _client(
        {
            "size_matched": "5",
            "associate_trades": ["t1", "t2"],
            "status": "MATCHED",
        },
        trades,
    )

    state = await client.get_order_fill_state(order_id)

    assert state["avg_price"] == pytest.approx((0.40 * 2 + 0.30 * 3) / 5)


@pytest.mark.asyncio
async def test_no_trade_data_falls_back_to_unset() -> None:
    order_id = "0xempty"
    client, _ = _client(
        {"size_matched": "5", "associate_trades": [], "status": "MATCHED"},
        {},
    )

    state = await client.get_order_fill_state(order_id)

    assert state["avg_price"] == 0.0
