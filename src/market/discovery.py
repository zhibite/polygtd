"""Market discovery via Gamma REST API.

Given a round_start_ts, derives the market slug and fetches:
  - condition_id
  - clob_token_ids (UP token, DOWN token)
  - tick_size
  - enableOrderBook
  - neg_risk
  - end_date (market expiry)

Slugs follow the pattern:  btc-updown-15m-{round_start_ts}
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Literal

import httpx

from src.infra.logging import get_logger

log = get_logger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"


@dataclass
class MarketInfo:
    """Resolved Polymarket market for a single round."""

    round_start_ts: int
    slug: str
    condition_id: str
    up_token: str
    down_token: str
    tick_size: float
    neg_risk: bool
    end_date: str
    enable_orderbook: bool
    # Optional: resolved price (filled after round ends)
    settlement_price: float | None = None

    @property
    def side_tokens(self) -> dict[Literal["UP", "DOWN"], str]:
        return {"UP": self.up_token, "DOWN": self.down_token}

    def token_for_side(self, side: Literal["UP", "DOWN"]) -> str:
        return self.side_tokens[side]

    @staticmethod
    def slug_for_round(round_start_ts: int) -> str:
        return f"btc-updown-15m-{round_start_ts}"


class MarketDiscovery:
    """Polls Gamma for active BTC 15m markets."""

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "MarketDiscovery":
        self._client = httpx.AsyncClient(
            base_url=GAMMA_BASE,
            timeout=httpx.Timeout(10.0),
        )
        return self

    async def __aexit__(self, *_) -> None:
        if self._client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_market(self, round_start_ts: int) -> MarketInfo:
        """Fetch market for a given round, retrying if not yet published."""
        slug = MarketInfo.slug_for_round(round_start_ts)

        for attempt in range(1, 21):  # up to 20 retries (~100s)
            market = await self._query_slug(slug, round_start_ts)
            if market is not None:
                return market

            next_retry = min(attempt * 5, 30)
            log.warning("gamma.market_not_found.retry",
                        slug=slug, attempt=attempt, retry_in_s=next_retry)
            await asyncio.sleep(next_retry)

        raise RuntimeError(
            f"Could not resolve market for round {round_start_ts} after 20 attempts"
        )

    async def try_fetch_market(
        self, round_start_ts: int, timeout_s: float = 30.0
    ) -> MarketInfo | None:
        """Non-throwing version — returns None if not found within timeout."""
        slug = MarketInfo.slug_for_round(round_start_ts)
        deadline = time.time() + timeout_s

        while time.time() < deadline:
            market = await self._query_slug(slug, round_start_ts)
            if market is not None:
                return market
            await asyncio.sleep(5.0)

        return None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _query_slug(self, slug: str, round_start_ts: int) -> MarketInfo | None:
        if self._client is None:
            raise RuntimeError("MarketDiscovery not entered (use async with)")

        try:
            resp = await self._client.get("/events", params={"slug": slug})
            resp.raise_for_status()
            events = resp.json()
        except httpx.HTTPStatusError as exc:
            log.warning("gamma.http_error", status=exc.response.status_code,
                        detail=exc.response.text[:200])
            return None
        except Exception as exc:
            log.warning("gamma.request_error", error=str(exc))
            return None

        if not events or not isinstance(events, list) or len(events) == 0:
            return None

        event = events[0]

        markets = event.get("markets", [])
        if not markets:
            return None

        mkt = markets[0]
        tokens = mkt.get("tokens", [])
        if len(tokens) < 2:
            log.warning("gamma.market.missing_tokens", slug=slug)
            return None

        def token_id(t: dict) -> str:
            return str(t.get("token_id", ""))

        tick_raw = mkt.get("minimum_tick_size", "0.01")
        try:
            tick = float(tick_raw)
        except (TypeError, ValueError):
            tick = 0.01

        market = MarketInfo(
            round_start_ts=round_start_ts,
            slug=slug,
            condition_id=str(mkt.get("condition_id", "")),
            up_token=token_id(tokens[0]),
            down_token=token_id(tokens[1]),
            tick_size=tick,
            neg_risk=bool(mkt.get("neg_risk", False)),
            end_date=str(mkt.get("end_date", "")),
            enable_orderbook=bool(mkt.get("enableOrderBook", True)),
        )

        log.info("gamma.market.resolved",
                 slug=slug,
                 condition_id=market.condition_id[:16],
                 up_token=market.up_token[:16],
                 down_token=market.down_token[:16],
                 tick_size=tick)

        return market
