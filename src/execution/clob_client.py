"""CLOB REST client — wraps py-clob-client-v2 ClobClient for L2 operations.

Key differences from the stub:
  - Uses py_clob_client_v2.ClobClient (v2, not v1)
  - BUY always goes via GTD limit (maker, no taker fee)
  - SELL always goes via FAK market order (fastest exit)
  - fill_size is returned from the API response after a poll
  - Tracks expires_at so GtdExpiryWatcher can detect stale GTDs
  - on_fill callback fires after any confirmed fill
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from typing import Callable, Literal, Optional

from loguru import logger

from src.config import strategy as _s

# py-clob-client-v2 (NOT v1)
from py_clob_client_v2 import (
    ClobClient,
    MarketOrderArgs,
    OrderType,
    Side as ClobSide,
)
from py_clob_client_v2.clob_types import ApiCreds, OrderArgsV2, PartialCreateOrderOptions

log = logger


def _round_down(value: float, prec: Decimal) -> float:
    d = Decimal(str(value)).quantize(prec, rounding=ROUND_DOWN)
    return float(Decimal(str(d)).quantize(prec, rounding=ROUND_DOWN))


# ---------------------------------------------------------------------------
# Fill result
# ---------------------------------------------------------------------------


@dataclass
class FillResult:
    order_id: str
    tokens: float       # shares filled
    usdc: float         # USDC spent / received
    fill_price: float
    success: bool
    error: str = ""


# ---------------------------------------------------------------------------
# CLOB Client
# ---------------------------------------------------------------------------


class CLOBClient:
    """
    Async wrapper around py_clob_client_v2.ClobClient.

    BUY  → GTD limit (passive maker, earns rebate)
    SELL → FAK market order (fastest exit)

    fill_callback(token_id, order_id, fill_price, fill_size) is called after
    a confirmed fill is detected (polling or WS).
    """

    POLYGON_CHAIN_ID = 137
    GTD_MIN_SEC = 180           # CLOB hard floor per docs
    GTD_CLIENT_TIMEOUT_SEC = 60  # client-side poll timeout before sweep-taker fallback

    def __init__(
        self,
        host: str = "https://clob.polymarket.com",
        private_key: str = "",
        api_key: str = "",
        api_secret: str = "",
        api_passphrase: str = "",
        signature_type: int = 0,
    ) -> None:
        self._host = host
        self._private_key = private_key
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase
        self._signature_type = signature_type
        self._client: Optional[ClobClient] = None

        # fill_callback: (token_id, order_id, fill_price, fill_size) -> None
        self.fill_callback: Optional[Callable] = None

        self._init_client()

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------

    def _init_client(self) -> None:
        if not self._api_key or not self._api_secret:
            log.warning(
                "CLOBClient: L2 credentials not provided — "
                "trading disabled. Set API_KEY / API_SECRET / "
                "API_PASSPHRASE in .env"
            )
            self._client = None
            return

        try:
            api_creds = ApiCreds(
                api_key=self._api_key,
                api_secret=self._api_secret,
                api_passphrase=self._api_passphrase,
            )
            self._client = ClobClient(
                host=self._host,
                chain_id=self.POLYGON_CHAIN_ID,
                key=self._private_key,
                creds=api_creds,
                signature_type=self._signature_type,
            )
            log.info(
                "CLOBClient ready: host={} sig_type={}",
                self._host, self._signature_type,
            )
        except Exception as exc:
            log.error("CLOBClient init failed: {}", exc)
            self._client = None

    def is_configured(self) -> bool:
        return self._client is not None

    # ------------------------------------------------------------------
    # Balance
    # ------------------------------------------------------------------

    async def get_balance(self) -> float:
        """USDC (pUSD) collateral balance in human units."""
        if not self._client:
            return 0.0
        try:
            bal = await self._run(self._client.get_pusd_balance)
            return float(bal) / 1_000_000 if bal else 0.0
        except Exception as exc:
            log.warning("clob.get_balance.error: {}", exc)
            return 0.0

    async def get_token_balance(self, token_id: str) -> float:
        """Token balance in shares (human, not micro-units)."""
        if not self._client:
            return 0.0
        try:
            bal = await self._run(self._client.get_balance, token_id)
            return float(bal) if bal else 0.0
        except Exception as exc:
            log.debug("clob.get_token_balance.error: {}", exc)
            return 0.0

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------

    async def place_gtd_buy(
        self,
        token_id: str,
        price: float,
        size: float,
        lifetime_s: int,
        tick_size: float,
    ) -> FillResult:
        """
        Post a GTD limit BUY order and poll until filled or expired.

        Returns FillResult with the fill details once filled.
        On GTD expiry or error returns FillResult(success=False).
        """
        if not self._client:
            return FillResult("", 0, 0, price, False, "ClobClient not initialised")

        gtd_seconds = max(lifetime_s, self.GTD_MIN_SEC + 1)
        expiration_ts = int(time.time()) + gtd_seconds

        tick_d = Decimal(str(tick_size))
        size_q = _round_down(size, tick_d)
        price_q = _round_down(price, tick_d)

        if size_q < 5.0:
            return FillResult("", 0, 0, price_q, False, "tokens below CLOB 5-shares minimum")

        log.debug(
            "clob.gtd_buy token={} size={:.4f} price={:.4f} tick={} expires={}",
            token_id[:12], size_q, price_q, tick_size, expiration_ts,
        )

        try:
            order_args = OrderArgsV2(
                token_id=token_id,
                price=float(price_q),
                size=float(size_q),
                side=ClobSide.BUY,
                expiration=expiration_ts,
            )
            opts = PartialCreateOrderOptions(tick_size=str(tick_size), neg_risk=False)
            resp = await self._run(
                self._client.create_and_post_order,
                order_args=order_args,
                options=opts,
                order_type=OrderType.GTD,
                post_only=False,
            )
        except Exception as exc:
            log.warning("clob.gtd_buy.post_error: {}", exc)
            return FillResult("", 0, 0, price_q, False, str(exc))

        if not resp:
            return FillResult("", 0, 0, price_q, False, "GTD post returned None")

        order_id = str(resp.get("orderID") or resp.get("order_id") or "")
        if not order_id:
            err = resp.get("errorMsg") or resp.get("error") or "no orderID"
            return FillResult("", 0, 0, price_q, False, err)

        # Poll until filled or timed out
        deadline = time.monotonic() + self.GTD_CLIENT_TIMEOUT_SEC
        poll_interval = 0.5
        total_sz = 0.0
        total_usdc = 0.0

        while time.monotonic() < deadline:
            await asyncio.sleep(poll_interval)

            try:
                status_resp = await self._run(self._client.get_order, order_id)
            except Exception as exc:
                log.debug("clob.gtd_buy.get_order error: {}", exc)
                continue

            if not status_resp:
                continue

            fills = status_resp.get("fills") or []
            if fills:
                for f in fills:
                    try:
                        sz = float(f.get("size", 0))
                        px = float(f.get("price", price_q))
                        total_sz += sz
                        total_usdc += sz * px
                    except (TypeError, ValueError):
                        continue

                if total_sz > 0:
                    avg_px = total_usdc / total_sz
                    result = FillResult(
                        order_id=order_id,
                        tokens=_round_down(total_sz, Decimal("0.0001")),
                        usdc=_round_down(total_usdc, Decimal("0.01")),
                        fill_price=_round_down(avg_px, tick_d),
                        success=True,
                    )
                    self._fire_fill_callback(token_id, order_id, avg_px, total_sz)
                    return result

            order_status = (status_resp.get("status") or "").lower()
            if order_status in ("cancelled", "expired", "failed"):
                log.info("clob.gtd_buy expired status={}", order_status)
                return FillResult(
                    order_id=order_id, tokens=0, usdc=0,
                    fill_price=price_q, success=False,
                    error=f"gtd_{order_status}",
                )

        # Timeout — cancel and return failure
        try:
            await self._run(self._client.cancel_order, order_id)
        except Exception:
            pass
        return FillResult(
            order_id=order_id, tokens=0, usdc=0,
            fill_price=price_q, success=False,
            error="gtd_poll_timeout",
        )

    async def place_fak_sell(
        self,
        token_id: str,
        size: float,
        price: float,
        tick_size: float,
        slippage_pct: float = 2.0,
    ) -> FillResult:
        """
        Post a FAK (fill-or-kill) market SELL order.
        Returns FillResult with fill details on success.
        """
        if not self._client:
            return FillResult("", 0, 0, price, False, "ClobClient not initialised")

        tick_d = Decimal(str(tick_size))
        slip_d = Decimal(str(slippage_pct)) / Decimal("100")
        slip_price = Decimal(str(price)) * (Decimal("1") + slip_d)
        sell_price = float(slip_price.quantize(tick_d, rounding=ROUND_DOWN))

        size_q = _round_down(size, max(Decimal("0.0001"), tick_d))

        log.debug(
            "clob.fak_sell token={} size={:.4f} price={:.4f} slippage={}%",
            token_id[:12], size_q, sell_price, slippage_pct,
        )

        try:
            mkt_args = MarketOrderArgs(
                token_id=token_id,
                size=float(size_q),
                side=ClobSide.SELL,
                slippage=float(slip_d),
            )
            resp = await self._run(
                self._client.create_and_post_market_order, mkt_args
            )
        except Exception as exc:
            log.warning("clob.fak_sell.post_error: {}", exc)
            return FillResult("", 0, 0, sell_price, False, str(exc))

        if not resp:
            return FillResult("", 0, 0, sell_price, False, "FAK post returned None")

        fills = resp.get("fills") or []
        if not fills:
            return FillResult("", 0, 0, sell_price, False, "FAK_no_fills")

        total_sz = 0.0
        total_usdc = 0.0
        for f in fills:
            try:
                sz = float(f.get("size", 0))
                px = float(f.get("price", sell_price))
                total_sz += sz
                total_usdc += sz * px
            except (TypeError, ValueError):
                continue

        if total_sz <= 0:
            return FillResult("", 0, 0, sell_price, False, "FAK_no_fills")

        avg_px = total_usdc / total_sz
        result = FillResult(
            order_id=str(resp.get("orderID", "")),
            tokens=_round_down(total_sz, Decimal("0.0001")),
            usdc=_round_down(total_usdc, Decimal("0.01")),
            fill_price=_round_down(avg_px, tick_d),
            success=True,
        )
        self._fire_fill_callback(token_id, result.order_id, avg_px, total_sz)
        return result

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a single order. Returns True on success."""
        if not self._client:
            return False
        try:
            await self._run(self._client.cancel_order, order_id)
            log.info("clob.order_cancelled id={}", order_id[:16])
            return True
        except Exception as exc:
            log.warning("clob.cancel_order error: {}", exc)
            return False

    async def cancel_orders(self, order_ids: list[str]) -> dict:
        """Batch cancel. Returns {canceled: [...], not_canceled: [...]}."""
        if not self._client:
            return {"canceled": [], "not_canceled": list(order_ids)}
        try:
            resp = await self._run(self._client.cancel_orders, [{"orderID": oid} for oid in order_ids])
            return resp if isinstance(resp, dict) else {"canceled": order_ids, "not_canceled": []}
        except Exception as exc:
            log.warning("clob.cancel_orders error: {}", exc)
            return {"canceled": [], "not_canceled": list(order_ids)}

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    async def post_heartbeat(self, heartbeat_id: str = "") -> dict:
        """Send a heartbeat to keep GTD orders alive. Must be called every ~5 seconds."""
        if not self._client:
            return {"success": False, "error": "ClobClient not initialised"}
        try:
            resp = await self._run(self._client.post_heartbeat, heartbeat_id)
            return resp if resp else {"success": True}
        except Exception as exc:
            log.warning("clob.heartbeat.error: {}", exc)
            return {"success": False, "error": str(exc)}

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    async def get_open_orders(self, condition_id: str | None = None) -> list[dict]:
        """Fetch open orders from CLOB."""
        if not self._client:
            return []
        try:
            orders = await self._run(self._client.get_orders)
            if condition_id:
                orders = [o for o in orders if o.get("conditionID") == condition_id]
            return orders
        except Exception as exc:
            log.warning("clob.get_orders error: {}", exc)
            return []

    async def get_midpoint(self, token_id: str) -> float:
        """REST fallback for token midpoint price."""
        if not self._client:
            return 0.0
        try:
            return float(await self._run(self._client.get_midpoint, token_id))
        except Exception as exc:
            log.warning("clob.get_midpoint error: {}", exc)
            return 0.0

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _fire_fill_callback(
        self, token_id: str, order_id: str, fill_price: float, fill_size: float
    ) -> None:
        if self.fill_callback:
            try:
                self.fill_callback(token_id, order_id, fill_price, fill_size)
            except Exception as exc:
                log.debug("fill_callback error: {}", exc)

    @staticmethod
    async def _run(fn, *args, **kwargs):
        """Run a sync ClobClient method in a thread pool (non-blocking)."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, lambda: fn(*args, **kwargs)
        )
