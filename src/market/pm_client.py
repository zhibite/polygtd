"""
pm_client.py — Polymarket Gamma + CLOB v2 public API client.
Handles market discovery, midpoint prices, fee rates, and resolution queries.
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx

from src.infra.logging import get_logger

log = get_logger(__name__)


def _parse_end_dt(raw: str) -> Optional[datetime]:
    if not raw:
        return None
    raw = raw.strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


@dataclass(frozen=True)
class ClientSettings:
    gamma_api: str
    polymarket_host: str


class PolymarketClient:
    """
    v2 Gamma + CLOB public API client.
    No authentication required for all methods.
    """

    def __init__(
        self,
        *,
        gamma_api: Optional[str] = None,
        polymarket_host: Optional[str] = None,
    ):
        self.settings = ClientSettings(
            gamma_api=(gamma_api or os.getenv("GAMMA_API") or "https://gamma-api.polymarket.com"),
            polymarket_host=(polymarket_host or os.getenv("POLYMARKET_HOST") or "https://clob.polymarket.com"),
        )
        self._http: Optional[httpx.AsyncClient] = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=httpx.Timeout(15.0, connect=5.0),
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            )
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    # ── Market discovery ──────────────────────────────────────────────────────────

    DEFAULT_UPDOWN_INTERVAL_SLUG: str = "15m"
    DEFAULT_UPDOWN_WINDOW_SEC: int = 900

    async def get_asset_updown_markets(
        self,
        asset: str,
        *,
        slug_interval: str | None = None,
        window_sec: int | None = None,
        limit: int = 5,
    ) -> list[dict]:
        """
        Fetch active up/down ladder markets for the given asset (15m default, …).
        Slug shape: `{asset.lower()}-updown-{slug_interval}-{unix_slot}`.
        """
        if slug_interval is None:
            slug_interval = self.DEFAULT_UPDOWN_INTERVAL_SLUG
        if window_sec is None:
            window_sec = self.DEFAULT_UPDOWN_WINDOW_SEC
        asset = asset.upper()
        c = await self._client()
        prefix = f"{asset.lower()}-updown-{slug_interval}"
        now = int(time.time())
        if window_sec <= 0:
            window_sec = self.DEFAULT_UPDOWN_WINDOW_SEC
        base = (now // window_sec) * window_sec
        cand_ts = [base, base - window_sec, base + window_sec]
        seen: set[int] = set()
        candidates = [ts for ts in cand_ts if ts > 0 and not (ts in seen or seen.add(ts))]

        markets: list[dict] = []
        for ts in candidates:
            slug = f"{prefix}-{ts}"
            try:
                resp = await c.get(
                    f"{self.settings.gamma_api}/events",
                    params={"slug": slug},
                    timeout=8,
                )
                if resp.status_code != 200:
                    continue
                events = resp.json()
                if isinstance(events, dict):
                    events = [events] if events else []

                for ev in events:
                    if prefix not in (ev.get("slug") or ""):
                        continue
                    for m in ev.get("markets", []):
                        raw_ids = m.get("clobTokenIds") or "[]"
                        if isinstance(raw_ids, list):
                            token_ids = raw_ids
                        else:
                            try:
                                token_ids = json.loads(raw_ids)
                            except Exception:
                                token_ids = []
                        if not isinstance(token_ids, list):
                            token_ids = []

                        raw_prices = m.get("outcomePrices") or '["0.5", "0.5"]'
                        if isinstance(raw_prices, list):
                            prices = raw_prices
                        else:
                            try:
                                prices = json.loads(raw_prices)
                            except Exception:
                                prices = ["0.5", "0.5"]
                        if not isinstance(prices, list):
                            prices = []

                        if len(token_ids) < 2:
                            log.warning(
                                "get_asset_updown_markets missing token ids",
                                asset=asset, slug=ev.get("slug", ""),
                                token_count=len(token_ids),
                            )
                            continue

                        # The previous code hard-coded outcome labels to
                        # "Up"/"Down" and then "validated" them against
                        # itself — a vacuous check. Read the real Gamma
                        # `outcomes` array and map token/price indices from
                        # it, so a server-side reorder cannot silently make
                        # the bot buy the wrong side.
                        raw_outcomes = m.get("outcomes")
                        if isinstance(raw_outcomes, str):
                            try:
                                outcome_list = json.loads(raw_outcomes)
                            except Exception:
                                outcome_list = []
                        elif isinstance(raw_outcomes, list):
                            outcome_list = raw_outcomes
                        else:
                            outcome_list = []

                        def _outcome_index(label: str) -> Optional[int]:
                            aliases = ("up", "yes") if label == "up" else ("down", "no")
                            for idx, outcome in enumerate(outcome_list):
                                if str(outcome).strip().lower() in aliases:
                                    return idx
                            return None

                        idx_up = _outcome_index("up")
                        idx_down = _outcome_index("down")
                        if idx_up is None or idx_down is None:
                            if outcome_list:
                                log.warning(
                                    "get_asset_updown_markets unexpected outcomes",
                                    asset=asset,
                                    slug=ev.get("slug", ""),
                                    outcomes=outcome_list,
                                )
                                continue
                            # Gamma omitted `outcomes` entirely: fall back to
                            # the historical positional order, but surface it.
                            log.warning(
                                "get_asset_updown_markets outcomes missing",
                                asset=asset,
                                slug=ev.get("slug", ""),
                                note="assuming clobTokenIds[0]=Up, [1]=Down",
                            )
                            idx_up, idx_down = 0, 1

                        def _price_at(idx: int) -> float:
                            if idx >= len(prices):
                                return 0.5
                            try:
                                return float(prices[idx])
                            except (TypeError, ValueError):
                                return 0.5

                        m["tokens"] = [
                            {
                                "token_id": token_ids[idx_up],
                                "outcome": "Up",
                                "price": str(_price_at(idx_up)),
                            },
                            {
                                "token_id": token_ids[idx_down],
                                "outcome": "Down",
                                "price": str(_price_at(idx_down)),
                            },
                        ]
                        m["_event_slug"] = ev.get("slug", "")
                        m["_up_price"] = _price_at(idx_up)
                        m["_down_price"] = _price_at(idx_down)
                        m["_neg_risk"] = False
                        markets.append(m)
                if not markets and asset:
                    log.debug(
                        "get_asset_updown_markets returned 0 markets",
                        asset=asset, slug_interval=slug_interval, window_sec=window_sec, prefix=prefix,
                    )
            except Exception as e:
                log.debug("Slug query failed", slug=slug, error=str(e))

        return markets[:limit]

    async def get_markets_by_condition_id(self, condition_id: str) -> Optional[dict]:
        """Fetch a single market by its condition_id (CLOB market info)."""
        c = await self._client()
        try:
            resp = await c.get(
                f"{self.settings.polymarket_host}/markets/{condition_id}",
                timeout=8,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.debug("get_markets_by_condition_id failed", condition_id=condition_id[:12], error=str(e))
            return None

    # ── CLOB price data ─────────────────────────────────────────────────────────

    async def get_order_book(self, token_id: str) -> Optional[dict]:
        """
        Get order book (bids/asks) for a token from CLOB.
        Returns: {"bids": [{"price": "0.50", "size": "100"}, ...], "asks": [...]}
        """
        c = await self._client()
        try:
            resp = await c.get(
                f"{self.settings.polymarket_host}/orderbook",
                params={"token_id": token_id},
                timeout=3.0,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.debug("get_order_book failed", token_id=token_id[:12], error=str(e))
            return None

    async def get_midpoint(self, token_id: str) -> float:
        """Get current midpoint price (bid+ask)/2 for a token from CLOB."""
        c = await self._client()
        try:
            resp = await c.get(
                f"{self.settings.polymarket_host}/midpoint",
                params={"token_id": token_id},
                timeout=3.0,
            )
            resp.raise_for_status()
            data = resp.json()
            return float(data.get("mid", 0.0))
        except Exception:
            return 0.0

    async def get_last_trade_prices_batch(
        self, token_ids: list[str]
    ) -> dict[str, float]:
        """
        Batch fetch last trade prices for multiple tokens.
        CLOB: GET /last-trades-prices?token_ids=id1,id2,...
        Returns: {"token_id": price}
        """
        if not token_ids:
            return {}
        c = await self._client()
        try:
            resp = await c.get(
                f"{self.settings.polymarket_host}/last-trades-prices",
                params={"token_ids": ",".join(token_ids)},
                timeout=5.0,
            )
            resp.raise_for_status()
            data = resp.json()
            result: dict[str, float] = {}
            # The endpoint has returned both shapes across deployments:
            #   {"<token_id>": "0.53", ...}  (dict)
            #   [{"token_id": "...", "price": "0.53"}, ...]  (list)
            # Support both; the previous duplicate definitions only handled
            # one shape each, and the surviving one assumed a dict.
            if isinstance(data, dict):
                for tid, price_val in data.items():
                    if tid not in token_ids:
                        continue
                    try:
                        result[tid] = float(price_val)
                    except (TypeError, ValueError):
                        result[tid] = 0.0
            elif isinstance(data, list):
                for entry in data:
                    if not isinstance(entry, dict):
                        continue
                    tid = entry.get("token_id") or entry.get("asset_id")
                    if not tid or tid not in token_ids:
                        continue
                    try:
                        result[tid] = float(entry.get("price", 0.0))
                    except (TypeError, ValueError):
                        result[tid] = 0.0
            for tid in token_ids:
                result.setdefault(tid, 0.0)
            return result
        except Exception:
            return {tid: 0.0 for tid in token_ids}

    async def get_tick_size(self, token_id: str) -> str:
        """Get minimum tick size for a token (v2 CLOB API)."""
        c = await self._client()
        resp = await c.get(
            f"{self.settings.polymarket_host}/tick-size",
            params={"token_id": token_id},
            timeout=5.0,
        )
        resp.raise_for_status()
        data = resp.json()
        return str(data.get("minimum_tick_size", "0.0001"))

    async def get_neg_risk(self, token_id: str) -> bool:
        """Check if token uses Neg Risk exchange."""
        c = await self._client()
        resp = await c.get(
            f"{self.settings.polymarket_host}/neg-risk",
            params={"token_id": token_id},
            timeout=5.0,
        )
        resp.raise_for_status()
        data = resp.json()
        return bool(data.get("neg_risk", False))

    async def get_clob_market_info(self, condition_id: str) -> Optional[dict]:
        """Fetch all CLOB market parameters in one call (v2)."""
        c = await self._client()
        try:
            resp = await c.get(
                f"{self.settings.polymarket_host}/markets/{condition_id}",
                timeout=8,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.debug("get_clob_market_info failed", condition_id=condition_id[:12], error=str(e))
            return None

    # ── Fee rate ────────────────────────────────────────────────────────────────

    async def get_fee_rate_bps(self, token_id: str) -> int:
        """Get taker fee rate in basis points for a token."""
        c = await self._client()
        try:
            resp = await c.get(
                f"{self.settings.polymarket_host}/fee-rate",
                params={"token_id": token_id},
                timeout=8,
            )
            resp.raise_for_status()
            data = resp.json()
            rate = int(data.get("base_fee") or data.get("fee_rate_bps") or -1)
            if rate < 0:
                raise ValueError(f"fee-rate response has no base_fee/fee_rate_bps: {data}")
            return rate
        except Exception as e:
            raise RuntimeError(f"get_fee_rate_bps({token_id[:12]}) failed: {e}") from e

    # ── Market resolution ───────────────────────────────────────────────────────

    async def get_market_resolution(self, condition_id: str) -> Optional[dict]:
        """
        Query Gamma for market resolution result.
        condition_id is passed as query param `condition_ids` (array form).
        """
        c = await self._client()
        try:
            resp = await c.get(
                f"{self.settings.gamma_api}/markets",
                params={"condition_ids": [condition_id]},
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                if not data:
                    return {"resolved": False, "up_wins": None, "outcome": ""}
                data = data[0]
            elif not isinstance(data, dict):
                log.warning(
                    "get_market_resolution unexpected response type",
                    condition_id=condition_id[:12], got=type(data).__name__,
                )
                return None

            resolved = bool(data.get("resolved", False))
            if not resolved:
                return {"resolved": False, "up_wins": None, "outcome": ""}

            raw = data.get("outcomePrices") or '["0","0"]'
            try:
                prices = json.loads(raw)
            except Exception:
                prices = ["0", "0"]

            up_price = float(prices[0]) if prices else 0.0
            up_wins = up_price >= 0.5
            outcome_text = (data.get("outcome") or "").strip()
            if outcome_text.lower() in ("up", "yes"):
                up_wins = True
            elif outcome_text.lower() in ("down", "no"):
                up_wins = False
            return {"resolved": True, "up_wins": up_wins, "outcome": outcome_text}
        except Exception as e:
            is_422 = getattr(e, "status_code", None) == 422
            level = "debug" if is_422 else "warning"
            getattr(log, level)("get_market_resolution failed", condition_id=condition_id[:12], error=str(e))
            return None


def parse_window_end(market: dict | None) -> Optional[datetime]:
    if not market:
        return None
    raw = market.get("endDate") or market.get("end_date") or ""
    return _parse_end_dt(raw)


async def safe_gather(*coros):
    return await asyncio.gather(*coros, return_exceptions=True)
