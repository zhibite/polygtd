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
from typing import Callable, Optional

from src.infra.logging import get_logger

from src.config import strategy as _s

# py-clob-client-v2 (NOT v1)
from py_clob_client_v2 import (
    ClobClient,
    MarketOrderArgs,
    OrderType,
    Side as ClobSide,
)
from py_clob_client_v2.clob_types import ApiCreds, OrderArgsV2, PartialCreateOrderOptions

log = get_logger(__name__)


def _round_down(value: float, prec: Decimal) -> float:
    d = Decimal(str(value)).quantize(prec, rounding=ROUND_DOWN)
    return float(d)


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
    # Server-side GTD expiration (Unix ts). Forwarded into TrackedOrder
    # so client/watch and CLOB share the exact same wall-clock deadline
    # (audit H-3 / L-3). 0 when not applicable.
    expiration_ts: int = 0


# ---------------------------------------------------------------------------
# Fill deduper
# ---------------------------------------------------------------------------


class FillDeduper:
    """Authoritative dedup layer for fill events.

    Multiple sources (per-order polling, UserWS maker-fill, FAK post-response)
    can each independently observe that a given `server_order_id` is "more
    filled than before". Previously every fire of the cumulative size was
    forwarded to the engine, which then did `size_filled += cumulative`,
    double-counting partial fills and double-firing tier promotion + TP.

    Design (B-3 fix):
      - `bookmark[order_id]` = the cumulative matched size we have already
        notified the engine about.  Strictly monotonic.
      - `_lock` protects `bookmark` and `seen_trade_ids` against concurrent
        fire paths from the same / different coroutines.
      - `seen_trade_ids` is a fallback dedup set keyed by `(order_id,
        trade_id)` for SDK responses that expose individual fill rows.
        Either path is sufficient by itself; we keep both for defence in
        depth (the monotonic bookmark is the primary guard, the trade-id
        set protects against curl-level replay / FAK re-send).
      - `submit(token_id, order_id, fill_price, fill_total_size, fill_id)`
        returns `(delta, total_after)`. If `delta <= 0` we have already
        accounted for this event and skip notification.
      - All exceptions raised by user callback are caught and logged so
        that a malformed fill never tears down the polling thread.
    """

    def __init__(self) -> None:
        # order_id -> last-acknowledged cumulative matched size
        self._bookmark: dict[str, float] = {}
        # (order_id, fill_id or "") -> True. Empty fill_id is treated as a
        # generic dedup marker for SDK responses without per-fill rows.
        self._seen_trades: set[tuple[str, str]] = set()
        self._lock = asyncio.Lock()

    async def submit(
        self,
        token_id: str,
        order_id: str,
        fill_price: float,
        fill_total_size: float,
        fill_id: str = "",
    ) -> tuple[float, float]:
        """Return (delta, total_after) for this fill observation.

        Always safe to call from any source. Returns (0.0, current_bookmark)
        for already-seen events so callers can skip downstream work.
        """
        if not order_id or fill_total_size <= 0:
            return 0.0, self._bookmark.get(order_id or "", 0.0)

        async with self._lock:
            trade_key = (order_id, fill_id) if fill_id else None
            if trade_key is not None and trade_key in self._seen_trades:
                # Exact fill-row already seen — return current bookmark, delta=0.
                return 0.0, self._bookmark.get(order_id, 0.0)

            prior = self._bookmark.get(order_id, 0.0)
            # Some SDK responses report cumulative size in the same value but
            # the network can rearrange event order; clamp to non-negative.
            delta = fill_total_size - prior
            if delta <= 1e-9:
                # Matched size has not advanced (or went sideways) since the
                # last fire — already covered by a previous observation.
                # Still record the trade_id so we don't repeat the comparison.
                if trade_key is not None:
                    self._seen_trades.add(trade_key)
                return 0.0, prior

            self._bookmark[order_id] = fill_total_size
            if trade_key is not None:
                self._seen_trades.add(trade_key)

            # Light garbage collection: cap seen_trades so it doesn't grow
            # unbounded for long-lived sessions. We prune any trade-id set
            # whose bookmark has not advanced in 24 hours of activity, and
            # the bookmark itself stays forever (it is tiny).
            if len(self._seen_trades) > 4096:
                self._seen_trades = {
                    k for k in self._seen_trades
                    if self._bookmark.get(k[0], 0.0) >= fill_total_size
                }

        return delta, fill_total_size

    def get_bookmark(self, order_id: str) -> float:
        """Current high-water for an order (debug / inspection)."""
        return self._bookmark.get(order_id, 0.0)


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

        # fill_callback signature (B-3 fix):
        #   (token_id, order_id, fill_price, fill_delta_size, fill_total_size)
        # fill_delta_size is monotonically increasing (cumulative - last
        # bookmark). The cumulative fill_total_size is provided so that the
        # OrderManager can keep its own size_matched bookkeeping in sync
        # without having to compute the cumulative itself.
        self.fill_callback: Optional[Callable] = None

        # H-NEW-1 fix: optional callback to pre-register a server_order_id
        # the moment it is known (right after the CLOB post-response).
        # OrderManager registers itself here so that the very first fill
        # callback — fired from inside place_gtd_buy / place_gtd_sell /
        # place_fak_sell before the awaited call returns — can be routed
        # to the correct TrackedOrder via by_server_id().
        self.register_pending: Optional[Callable[[str], None]] = None

        # Deduper protects against double-counting when WS, polling and FAK
        # all observe the same fill. See FillDeduper docstring.
        self._deduper = FillDeduper()

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
                "CLOBClient ready",
                host=self._host, sig_type=self._signature_type,
            )
        except Exception as exc:
            log.error("CLOBClient init failed", error=str(exc))
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
            log.warning("clob.get_balance.error", error=str(exc))
            return 0.0

    async def get_token_balance(self, token_id: str) -> float:
        """Token balance in shares (human, not micro-units)."""
        if not self._client:
            return 0.0
        try:
            bal = await self._run(self._client.get_balance, token_id)
            return float(bal) if bal else 0.0
        except Exception as exc:
            log.debug("clob.get_token_balance.error", error=str(exc))
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
            return FillResult("", 0, 0, price_q, False, "tokens below CLOB 5-shares minimum", expiration_ts)

        log.debug(
            "clob.gtd_buy",
            token=token_id[:12], size=size_q, price=price_q,
            tick=tick_size, expires=expiration_ts,
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
            log.warning("clob.gtd_buy.post_error", error=str(exc))
            return FillResult("", 0, 0, price_q, False, str(exc), expiration_ts)

        if not resp:
            return FillResult("", 0, 0, price_q, False, "GTD post returned None", expiration_ts)

        order_id = str(resp.get("orderID") or resp.get("order_id") or "")
        if not order_id:
            err = resp.get("errorMsg") or resp.get("error") or "no orderID"
            return FillResult("", 0, 0, price_q, False, err, expiration_ts)

        # H-NEW-1 fix: pre-register the server_id with the OrderManager
        # (best-effort) so the very first fill — potentially fired from the
        # poll loop below before the caller returns — can be routed via
        # by_server_id() even before the real TrackedOrder is inserted.
        if self.register_pending:
            try:
                self.register_pending(order_id)
            except Exception as exc:
                log.debug("clob.register_pending.error", error=str(exc))

        # Poll until filled or timed out
        deadline = time.monotonic() + self.GTD_CLIENT_TIMEOUT_SEC
        poll_interval = 0.5
        total_sz = 0.0
        total_usdc = 0.0
        # H-V6-2 fix: throttle the per-poll progress log so it doesn't flood
        # logs at 0.5s intervals, but emit enough rows to see the GTD's
        # lifetime progress (every ~10s, capped at one per poll cycle).
        last_progress_log_ts = 0.0
        progress_log_interval = 10.0
        order_size = size_q

        while time.monotonic() < deadline:
            await asyncio.sleep(poll_interval)

            try:
                status_resp = await self._run(self._client.get_order, order_id)
            except Exception as exc:
                log.debug("clob.gtd_buy.get_order error", error=str(exc))
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
                    # H-V6-2 fix: log the BUY fill result before returning
                    # so operators can see fills that came back through the
                    # polling loop (as opposed to the synchronous post-response
                    # callback). Pre-fix, only the post-response path logged.
                    log.info(
                        "clob.gtd_buy filled",
                        order_id=order_id[:16],
                        token=token_id[:16],
                        filled_tokens=total_sz,
                        avg_px=avg_px,
                        total_usdc=total_usdc,
                        order_size=order_size,
                        fills_count=len(fills),
                    )
                    result = FillResult(
                        order_id=order_id,
                        tokens=_round_down(total_sz, Decimal("0.0001")),
                        usdc=_round_down(total_usdc, Decimal("0.01")),
                        fill_price=_round_down(avg_px, tick_d),
                        success=True,
                        expiration_ts=expiration_ts,
                    )
                    await self._fire_fill_callback(token_id, order_id, avg_px, total_sz)
                    return result

            order_status = (status_resp.get("status") or "").lower()
            if order_status in ("cancelled", "expired", "failed"):
                # H-V6-2 fix: enrich the existing expiry log with structured
                # fields (matched so far, lifetime consumed, server status).
                # Pre-fix log was just `clob.gtd_buy expired status=X`.
                age_s = time.monotonic() - (deadline - self.GTD_CLIENT_TIMEOUT_SEC)
                log.warning(
                    "clob.gtd_buy expired",
                    order_id=order_id[:16],
                    token=token_id[:16],
                    status=order_status,
                    matched=total_sz,
                    order_size=order_size,
                    lifetime_s=gtd_seconds,
                    lifetime_consumed_s=round(age_s, 1),
                )
                return FillResult(
                    order_id=order_id, tokens=0, usdc=0,
                    fill_price=price_q, success=False,
                    error=f"gtd_{order_status}",
                    expiration_ts=expiration_ts,
                )

            # H-V6-2 fix: periodic progress log so operators can see that
            # the BUY is still live and waiting (the BUYs mostly cross
            # immediately, but for illiquid markets the BUY can be sitting
            # on the book waiting — this row lets us distinguish "alive
            # waiting" from "stuck").
            now_ts = time.monotonic()
            if now_ts - last_progress_log_ts >= progress_log_interval:
                last_progress_log_ts = now_ts
                age_s = now_ts - (deadline - self.GTD_CLIENT_TIMEOUT_SEC)
                log.info(
                    "clob.gtd_buy poll_progress",
                    order_id=order_id[:16],
                    token=token_id[:16],
                    matched=total_sz,
                    order_size=order_size,
                    order_status=order_status,
                    lifetime_consumed_s=round(age_s, 1),
                    lifetime_remaining_s=round(gtd_seconds - age_s, 1),
                )

        # Timeout — cancel and return failure
        # H-V6-2 fix: structured timeout log with full context.
        age_s = time.monotonic() - (deadline - self.GTD_CLIENT_TIMEOUT_SEC)
        log.warning(
            "clob.gtd_buy poll_timeout",
            order_id=order_id[:16],
            token=token_id[:16],
            matched=total_sz,
            order_size=order_size,
            client_timeout_s=self.GTD_CLIENT_TIMEOUT_SEC,
            lifetime_consumed_s=round(age_s, 1),
        )
        try:
            await self._run(self._client.cancel_order, order_id)
            log.info(
                "clob.gtd_buy poll_timeout_cancel_sent",
                order_id=order_id[:16],
            )
        except Exception as exc:
            log.warning(
                "clob.gtd_buy poll_timeout_cancel_failed",
                order_id=order_id[:16], error=str(exc),
            )
        return FillResult(
            order_id=order_id, tokens=0, usdc=0,
            fill_price=price_q, success=False,
            error="gtd_poll_timeout",
            expiration_ts=expiration_ts,
        )

    async def place_gtd_sell(
        self,
        token_id: str,
        price: float,
        size: float,
        lifetime_s: int,
        tick_size: float,
    ) -> FillResult:
        """
        Post a GTD limit SELL order (maker) and poll until filled or expired.

        Behaviour:
          - Returns a *live* GTD sell. Caller (OrderManager) decides how long to
            wait. We poll here for at most `GTD_CLIENT_TIMEOUT_SEC` (10 minutes)
            since the server enforces expiration.
          - Fires the fill callback incrementally on every fill so the position
            state can stay in sync while the order lives.
        """
        if not self._client:
            return FillResult("", 0, 0, price, False, "ClobClient not initialised")

        gtd_seconds = max(lifetime_s, self.GTD_MIN_SEC + 1)
        expiration_ts = int(time.time()) + gtd_seconds

        tick_d = Decimal(str(tick_size))
        size_q = _round_down(size, Decimal("0.0001"))
        price_q = _round_down(price, tick_d)

        if size_q < 5.0:
            return FillResult("", 0, 0, price_q, False, "tokens below CLOB 5-shares minimum", expiration_ts)

        log.debug(
            "clob.gtd_sell",
            token=token_id[:12], size=size_q, price=price_q,
            tick=tick_size, expires=expiration_ts,
        )

        try:
            order_args = OrderArgsV2(
                token_id=token_id,
                price=float(price_q),
                size=float(size_q),
                side=ClobSide.SELL,
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
            log.warning("clob.gtd_sell.post_error", error=str(exc))
            return FillResult("", 0, 0, price_q, False, str(exc), expiration_ts)

        if not resp:
            return FillResult("", 0, 0, price_q, False, "GTD post returned None", expiration_ts)

        order_id = str(resp.get("orderID") or resp.get("order_id") or "")
        if not order_id:
            err = resp.get("errorMsg") or resp.get("error") or "no orderID"
            return FillResult("", 0, 0, price_q, False, err, expiration_ts)

        # H-NEW-1 fix: pre-register server_id immediately — see place_gtd_buy.
        if self.register_pending:
            try:
                self.register_pending(order_id)
            except Exception as exc:
                log.debug("clob.register_pending.error", error=str(exc))

        # Poll until filled or GTD timeout.
        # B-3 fix: deduper is the single source of truth for delta
        # computation. We fire cumulative matched size on every poll and let
        # CLOBClient._fill_deduper translate to delta — that way WS / polling
        # / FAK responses can all observe the same order without double
        # counting regardless of who fires first.
        deadline = time.monotonic() + min(
            self.GTD_CLIENT_TIMEOUT_SEC, gtd_seconds + 5
        )
        poll_interval = 0.5
        total_sz = 0.0
        total_usdc = 0.0
        order_size = size_q
        # H-V6-2 fix: throttle the per-poll progress log so we don't flood
        # at 0.5s intervals, but emit enough rows to see the GTD SELL's
        # lifetime progress (~10s cadence).
        last_progress_log_ts = 0.0
        progress_log_interval = 10.0

        while time.monotonic() < deadline:
            await asyncio.sleep(poll_interval)

            try:
                status_resp = await self._run(self._client.get_order, order_id)
            except Exception as exc:
                log.debug("clob.gtd_sell.get_order error", error=str(exc))
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

            order_status = (status_resp.get("status") or "").lower()
            matched_sz = float(status_resp.get("size_matched") or total_sz)

            # Fire cumulative matched size. The CLOBClient-level FillDeduper
            # translates cumulative -> delta so partial fills remain monotonic
            # and idempotent regardless of which source (WS, polling, FAK)
            # observed the order. (B-3 fix — previously emitted_sz here was
            # a per-thread throttle that did not catch WS / FAK re-fires.)
            if matched_sz > 0:
                avg_px = (total_usdc / matched_sz) if matched_sz > 0 else price_q
                await self._fire_fill_callback(
                    token_id, order_id, avg_px, matched_sz
                )

            # Terminal states
            if order_status in ("matched", "filled"):
                if total_sz > 0:
                    avg_px = total_usdc / total_sz
                    # H-V6-2 fix: enrich the fully-filled terminal log
                    # with structured fields. Pre-fix the SELL loop never
                    # logged on success — only on partial_then_expired
                    # or poll_timeout. Operators couldn't tell whether
                    # the GTD SELL completed naturally vs hit a fallback.
                    log.info(
                        "clob.gtd_sell fully_filled",
                        order_id=order_id[:16],
                        token=token_id[:12],
                        filled_tokens=total_sz,
                        avg_px=avg_px,
                        total_usdc=total_usdc,
                        order_size=order_size,
                    )
                    return FillResult(
                        order_id=order_id,
                        tokens=_round_down(total_sz, Decimal("0.0001")),
                        usdc=_round_down(total_usdc, Decimal("0.01")),
                        fill_price=_round_down(avg_px, tick_d),
                        success=True,
                        expiration_ts=expiration_ts,
                    )
                log.warning(
                    "clob.gtd_sell matched_zero",
                    order_id=order_id[:16],
                    token=token_id[:12],
                    order_size=order_size,
                )
                return FillResult(
                    order_id=order_id, tokens=0, usdc=0,
                    fill_price=price_q, success=False, error="gtd_matched_zero",
                    expiration_ts=expiration_ts,
                )

            if order_status in ("cancelled", "expired", "failed"):
                # Partial fills are valid — return what we have.
                if total_sz > 0:
                    avg_px = total_usdc / total_sz
                    log.info(
                        "clob.gtd_sell partial_then_expired",
                        order_id=order_id[:16],
                        token=token_id[:12],
                        status=order_status, filled=total_sz,
                        order_size=order_size,
                        avg_px=avg_px,
                    )
                    return FillResult(
                        order_id=order_id,
                        tokens=_round_down(total_sz, Decimal("0.0001")),
                        usdc=_round_down(total_usdc, Decimal("0.01")),
                        fill_price=_round_down(avg_px, tick_d),
                        success=True,
                        expiration_ts=expiration_ts,
                    )
                return FillResult(
                    order_id=order_id, tokens=0, usdc=0,
                    fill_price=price_q, success=False,
                    error=f"gtd_{order_status}",
                    expiration_ts=expiration_ts,
                )

            # H-V6-2 fix: periodic progress log so operators can see how
            # full the GTD SELL is as it lives. This is the only place
            # where lifetime progress is observable between the post and
            # the terminal event.
            now_ts = time.monotonic()
            if now_ts - last_progress_log_ts >= progress_log_interval:
                last_progress_log_ts = now_ts
                age_s = now_ts - (deadline - min(self.GTD_CLIENT_TIMEOUT_SEC, gtd_seconds + 5))
                log.info(
                    "clob.gtd_sell poll_progress",
                    order_id=order_id[:16],
                    token=token_id[:12],
                    matched=matched_sz,
                    order_size=order_size,
                    order_status=order_status,
                    lifetime_consumed_s=round(age_s, 1),
                    lifetime_remaining_s=round(gtd_seconds - age_s, 1),
                )

        # Client poll timeout — but the order may still be live server-side.
        # Surface what we know to the caller, leaving the order book-still-posted.
        log.warning(
            "clob.gtd_sell poll_timeout",
            order_id=order_id[:16], token=token_id[:12],
            matched=total_sz, order_size=order_size,
            client_timeout_s=self.GTD_CLIENT_TIMEOUT_SEC,
            gtd_seconds=gtd_seconds,
        )
        return FillResult(
            order_id=order_id,
            tokens=_round_down(total_sz, Decimal("0.0001")),
            usdc=_round_down(total_usdc, Decimal("0.01")),
            fill_price=_round_down(
                (total_usdc / total_sz) if total_sz > 0 else 0, tick_d
            ),
            success=False,
            error="gtd_poll_timeout",
            expiration_ts=expiration_ts,
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
            "clob.fak_sell",
            token=token_id[:12], size=size_q, price=sell_price, slippage_pct=slippage_pct,
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
            # H-4 fix: classify post-error distinctly from "FAK got no
            # fills". The router treats these differently:
            #   * "FAK_post_error"  → network / SDK — keep in-flight mutex
            #                          so the next tick can re-fire;
            #   * "FAK_no_fills"    → book was empty, the round-trip
            #                          succeeded with zero size — release
            #                          the mutex, no work to do;
            #   * None / unexpected → keep mutex, log warning.
            log.warning("clob.fak_sell.post_error", error=str(exc))
            return FillResult("", 0, 0, sell_price, False, f"FAK_post_error:{exc}")

        if not resp:
            return FillResult("", 0, 0, sell_price, False, "FAK_no_response")

        fills = resp.get("fills") or []
        if not fills:
            # FAK was rejected by the book (no resting liquidity at our
            # price). This is the common "FAK gets nothing" case — the
            # router must distinguish it from a network error (above).
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

        # H-NEW-1 fix: FAK sells also race — _fire_fill_callback below runs
        # synchronously on this same coroutine, before the await returns to
        # OrderManager.place_sell. Pre-register so the sentinel mapping is
        # in place when the callback looks up by_server_id.
        fak_server_id = str(resp.get("orderID", ""))
        if fak_server_id and self.register_pending:
            try:
                self.register_pending(fak_server_id)
            except Exception as exc:
                log.debug("clob.register_pending.error", error=str(exc))

        avg_px = total_usdc / total_sz
        result = FillResult(
            order_id=fak_server_id,
            tokens=_round_down(total_sz, Decimal("0.0001")),
            usdc=_round_down(total_usdc, Decimal("0.01")),
            fill_price=_round_down(avg_px, tick_d),
            success=True,
        )
        await self._fire_fill_callback(token_id, result.order_id, avg_px, total_sz)
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
            log.info("clob.order_cancelled", order_id=order_id[:16])
            return True
        except Exception as exc:
            log.warning("clob.cancel_order error", order_id=order_id[:16], error=str(exc))
            return False

    async def cancel_orders(self, order_ids: list[str]) -> dict:
        """Batch cancel. Returns {canceled: [...], not_canceled: [...]}."""
        if not self._client:
            return {"canceled": [], "not_canceled": list(order_ids)}
        try:
            resp = await self._run(self._client.cancel_orders, [{"orderID": oid} for oid in order_ids])
            # H-V6-2 fix: previously `cancel_orders` was silent on success
            # (only logged on exception). Operators could not tell whether a
            # round-end cancel attempt actually reached the CLOB and what
            # the server split between canceled / not_canceled was. Now
            # log a structured summary so the operator can correlate
            # against `order_manager.cancel_market.target` / `not_cancelled`
            # in order_manager.py.
            if isinstance(resp, dict):
                log.info(
                    "clob.cancel_orders",
                    requested=len(order_ids),
                    canceled=len(resp.get("canceled", []) or []),
                    not_canceled=len(resp.get("not_canceled", []) or []),
                )
                return resp
            log.info(
                "clob.cancel_orders",
                requested=len(order_ids),
                canceled=len(order_ids),
                not_canceled=0,
            )
            return {"canceled": order_ids, "not_canceled": []}
        except Exception as exc:
            log.warning("clob.cancel_orders error", requested=len(order_ids), error=str(exc))
            return {"canceled": [], "not_canceled": list(order_ids)}

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    async def post_heartbeat(self, heartbeat_id: str = "") -> dict:
        """Send a heartbeat to keep GTD orders alive. Must be called every ~5 seconds.

        Errors are returned in the dict (not logged here) so the caller can
        decide the right log level — this runs every 5s, so unconditional
        warnings would flood the log.

        Note: Polymarket server treats "" (empty string) as an invalid id.
        Pass heartbeat_id=None (or omit) for the bootstrap / re-bootstrap call
        so the server issues a fresh session id instead of 400-ing.
        """
        if not self._client:
            return {"success": False, "error": "ClobClient not initialised"}
        sdk_arg: str | None = heartbeat_id if heartbeat_id else None
        try:
            resp = await self._run(self._client.post_heartbeat, sdk_arg)
            return resp if resp else {"success": True}
        except Exception as exc:
            return {"success": False, "error": str(exc), "exception": True}

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
            log.warning("clob.get_orders error", error=str(exc))
            return []

    async def get_midpoint(self, token_id: str) -> float:
        """REST fallback for token midpoint price."""
        if not self._client:
            return 0.0
        try:
            return float(await self._run(self._client.get_midpoint, token_id))
        except Exception as exc:
            log.warning("clob.get_midpoint error", error=str(exc))
            return 0.0

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _fire_fill_callback(
        self,
        token_id: str,
        order_id: str,
        fill_price: float,
        fill_size: float,
        fill_id: str = "",
    ) -> None:
        """Forward a confirmed fill to the user callback, exactly once per delta.

        `fill_size` is the *cumulative* matched size observed by this call
        site (whether polling or FAK post-response). The deduper translates
        cumulative -> delta so that the engine is never told about a fill it
        already accounted for, irrespective of how many parallel paths
        happened to observe it (WS / polling / FAK response).
        """
        if not self.fill_callback or not order_id:
            return
        try:
            delta, total_after = await self._deduper.submit(
                token_id=token_id,
                order_id=order_id,
                fill_price=fill_price,
                fill_total_size=fill_size,
                fill_id=fill_id,
            )
            if delta <= 0:
                return  # already notified by another source
            self.fill_callback(
                token_id, order_id, fill_price, delta, total_after
            )
        except Exception as exc:
            # Single malformed fill must not tear down the polling loop.
            log.exception(
                "fill_callback.error",
                order_id=order_id[:16],
                error=str(exc),
            )

    @staticmethod
    async def _run(fn, *args, **kwargs):
        """Run a sync ClobClient method in a thread pool (non-blocking)."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, lambda: fn(*args, **kwargs)
        )
