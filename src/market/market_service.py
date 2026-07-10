"""
market_service.py — Polymarket market service.
Responsible: discover window markets, fetch real-time prices, select entry side.
No trading logic.
"""

from datetime import datetime, timezone
from typing import Optional

from src.infra.logging import get_logger
from src.market.models import MarketInfo
from src.market.pm_client import PolymarketClient, parse_window_end

log = get_logger(__name__)


class MarketService:
    """
    Encapsulates Polymarket market read operations:
    - Discover active up/down markets (interval configurable: 5m / 15m)
    - Get UP/DOWN real-time mid prices
    - Select entry side based on threshold
    """

    def __init__(
        self,
        pm: PolymarketClient,
        threshold: float,
        *,
        slug_interval: str = PolymarketClient.DEFAULT_UPDOWN_INTERVAL_SLUG,
        window_sec: int = PolymarketClient.DEFAULT_UPDOWN_WINDOW_SEC,
    ):
        self._pm = pm
        self.threshold = threshold
        self._slug_interval = slug_interval
        self._window_sec = window_sec if window_sec > 0 else PolymarketClient.DEFAULT_UPDOWN_WINDOW_SEC

    def update_threshold(self, threshold: float):
        self.threshold = threshold

    def update_interval(self, slug_interval: str, window_sec: int):
        dslug = PolymarketClient.DEFAULT_UPDOWN_INTERVAL_SLUG
        default_w = PolymarketClient.DEFAULT_UPDOWN_WINDOW_SEC
        self._slug_interval = slug_interval or dslug
        self._window_sec = window_sec if window_sec > 0 else default_w

    async def discover_market(self, asset: str) -> Optional[MarketInfo]:
        """Discover current active market that hasn't ended. Returns MarketInfo or None."""
        markets = await self._pm.get_asset_updown_markets(
            asset,
            slug_interval=self._slug_interval,
            window_sec=self._window_sec,
            limit=3,
        )
        now = datetime.now(timezone.utc)
        for m in markets:
            end_dt = parse_window_end(m)
            if end_dt is None:
                continue
            if end_dt <= now:
                continue
            tokens = m.get("tokens", [])
            if len(tokens) < 2:
                continue
            try:
                window_ts = int(m.get("_event_slug", "").split("-")[-1])
                window_start = datetime.fromtimestamp(window_ts, tz=timezone.utc)
                window_end = datetime.fromtimestamp(
                    window_ts + self._window_sec,
                    tz=timezone.utc,
                )
            except Exception:
                window_start = now
                window_end = end_dt

            if window_end <= now or window_start >= window_end:
                continue

            neg_risk = False
            token_id_for_check = tokens[0].get("token_id", "")
            try:
                neg_risk = await self._pm.get_neg_risk(token_id_for_check)
            except Exception:
                pass

            window_open_up = window_open_down = 0.5
            try:
                window_open_up = await self._pm.get_midpoint(tokens[0].get("token_id", ""))
            except Exception:
                pass
            try:
                window_open_down = await self._pm.get_midpoint(tokens[1].get("token_id", ""))
            except Exception:
                pass

            return MarketInfo(
                asset=asset,
                condition_id=m.get("conditionId", ""),
                token_up_id=tokens[0].get("token_id", ""),
                token_down_id=tokens[1].get("token_id", ""),
                window_start=window_start,
                window_end=window_end,
                neg_risk=neg_risk,
                window_open_up=window_open_up,
                window_open_down=window_open_down,
                slug=m.get("_event_slug", ""),
                round_start_ts=window_ts,
            )
        return None

    async def fetch_prices(self, market: MarketInfo) -> tuple[float, float]:
        """Fetch UP and DOWN real-time mid prices."""
        up_mid = down_mid = 0.5
        try:
            up_mid = await self._pm.get_midpoint(market.token_up_id)
        except Exception:
            pass
        try:
            down_mid = await self._pm.get_midpoint(market.token_down_id)
        except Exception:
            pass
        return up_mid, down_mid

    async def fetch_last_trade_prices(
        self, market: MarketInfo
    ) -> tuple[float, float]:
        """
        Fetch UP/DOWN last trade prices (last ~1s).
        Returns: (up_last, down_last)
        - No trade history → 0.0 for that token
        - CLOB call failed → (0.5, 0.5) fallback
        """
        try:
            prices = await self._pm.get_last_trade_prices_batch(
                [market.token_up_id, market.token_down_id]
            )
        except Exception as e:
            log.debug("fetch_last_trade_prices failed", error=str(e))
            return 0.5, 0.5
        up = prices.get(market.token_up_id, 0.0)
        dn = prices.get(market.token_down_id, 0.0)
        return up, dn
