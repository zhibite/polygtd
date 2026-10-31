"""CLOB REST client — wraps py-clob-client-v2 ClobClient for L2 operations.

Key differences from the stub:
  - Uses py_clob_client_v2.ClobClient (v2, not v1)
  - BUY goes via GTD limit; a GTD is only *maker* while it rests. If the
    limit crosses the book it fills immediately as TAKER and pays the
    Polymarket fee (C × rate × p × (1-p))
  - SELL uses a GTD limit (production TP path) or a FAK market order
    (`place_fak_sell`); either can be the taker when it crosses, so the
    role is derived from the order's trades — never assumed from the
    order type
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

# py-clob-client-v2 (NOT v1)
from py_clob_client_v2 import (
    ClobClient,
    MarketOrderArgs,
    OrderType,
    Side as ClobSide,
)
from py_clob_client_v2.clob_types import (
    ApiCreds,
    OrderArgsV2,
    PartialCreateOrderOptions,
    TradeParams,
)

log = get_logger(__name__)

# How long a FAILED / empty trade-ledger lookup is remembered before the
# next `get_order_fill_state` retries it. Successful lookups are cached
# permanently per (order, trade-count). The TTL only exists so a burst of
# WS updates cannot re-walk the same trade ids on every wake; it is short
# enough that the terminal re-read still gets a fresh chance once the
# ledger catches up.
_AVG_PRICE_UNKNOWN_TTL_S = 0.1


def _round_down(value: float, prec: Decimal) -> float:
    d = Decimal(str(value)).quantize(prec, rounding=ROUND_DOWN)
    return float(d)


# SELL price rounding policy — FILL PRIORITY (operator decision 2026-10-30):
# the TP limit is floored to the tick (0.2382 → 0.23). Once the market
# reaches that floor the order crosses and fills at the prevailing best bid
# (>= limit), instead of sitting one full tick above the target and possibly
# never filling. BUY keeps rounding down too (never overspend the budget).
# Production 2026-10-30: ceil(target)=0.24 never filled while mid topped at
# 0.235; the position then rode the price down ~60%.


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

    H-NEW-2026-07-15 (audit follow-up — P2#11 dup parity): three dedup
    paths coexist in the bot, each with a different keying scheme. The
    table below summarises parity so future changes keep all three in
    lock-step:

      | Path                                    | Key                       | Bound                          | On hit                         |
      |-----------------------------------------|---------------------------|--------------------------------|--------------------------------|
      | FillDeduper (this class)                | (order_id, trade_id)      | log warn @ 4096 (no prune)     | debug log + delta=0 return     |
      | Position._applied_watcher_dedup         | (server_id, dedup_key)    | soft warn @ 8                  | debug log + early return       |
      | main._make_maker_fill_callback._seen    | trade_id                  | clear-and-keep @ 1024          | debug log + early return       |

    The three are NOT interchangeable: the bookmark monotonic guard
    (FillDeduper) is the only one that defends against WS-vs-polling
    double-fires. The watcher-dedup set defends against TierWatcher
    double-application. The maker-fill dedup defends against MATCHED→
    MINED→CONFIRMED status-transition re-fires on the WS TRADE event.
    Any new dedup site should declare which existing site it parallels
    in the table above.
    """

    def __init__(self) -> None:
        # order_id -> last-acknowledged cumulative matched size
        self._bookmark: dict[str, float] = {}
        # (order_id, fill_id or "") -> True. Empty fill_id is treated as a
        # generic dedup marker for SDK responses without per-fill rows.
        self._seen_trades: set[tuple[str, str]] = set()
        self._lock = asyncio.Lock()

    # H-NEW-2026-07-15 (audit follow-up — P2#11 dup parity): the dedup
    # `seen_trades` set is bounded via a soft LRU prune. We rebase the
    # comparison on the dedupe-owning `bookmark[order_id]` rather than
    # the incoming `fill_total_size` (the previous comparison
    # `bookmark >= fill_total_size` was logically inverted — it kept
    # exactly the entries we wanted to drop, so the prune never reduced
    # set size in practice). The new predicate drops trade-ids whose
    # owning order has been fully observed AND has not advanced for a
    # measurable interval (we approximate the interval via the bookmark
    # non-advancement: an order whose bookmark matches `fill_total_size`
    # is fully caught up; the trade-ids for that order are safe to drop
    # because any future observation of the same trade-id will still
    # short-circuit via the trade-key check).
    _DEDUP_PRUNE_HIGH_WATER: int = 4096

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
                # H-NEW-2026-07-15 (audit follow-up — P2#11 dup parity):
                # parity with Position._applied_watcher_dedup_skip and
                # router.maker_fill.trade_dedup. We log a debug so an
                # operator investigating double-counting reports can see
                # all three dedup paths firing symmetrically.
                log.debug(
                    "fill_deduper.trade_dedup_skip",
                    order_id=order_id[:16],
                    trade_id=(trade_key[1] or "")[:16],
                    bookmark=self._bookmark.get(order_id, 0.0),
                )
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
            # unbounded for long-lived sessions.
            #
            # H-NEW-2026-07-15 (audit follow-up — P2#11 dup parity): the
            # previous prune predicate `bookmark >= fill_total_size` was
            # logically inverted (it kept exactly the entries we wanted
            # to drop) so the prune never reduced the set in practice.
            # The honest fix is to NOT drop trade-ids in the deduper
            # itself — `bookmark` (the primary monotonic guard) already
            # makes any stale trade-id harmless (delta=0 short-circuit).
            # We log a warning instead so an operator can see if the
            # session is genuinely seeing too many unique trade-ids.
            if len(self._seen_trades) > self._DEDUP_PRUNE_HIGH_WATER:
                log.warning(
                    "fill_deduper.seen_trades_oversize",
                    size=len(self._seen_trades),
                    high_water=self._DEDUP_PRUNE_HIGH_WATER,
                    note="seen_trades set is large; bookmark monotonic "
                         "guard still prevents double-counting, but a "
                         "long-lived session may want a periodic restart",
                )

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
        funder_address: str = "",
    ) -> None:
        self._host = host
        self._private_key = private_key
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase
        self._signature_type = signature_type
        # H-V6-11 fix: the maker field on every /order post must point at the
        # wallet that actually HOLDS the funds. For SIG_TYPE=0 (EOA) this is
        # the signer itself; for SIG_TYPE=1/2/3 it's the Proxy / Safe /
        # deposit wallet configured in FUNDER_ADDRESS. Without it the CLOB
        # rejects with "maker address not allowed, please use the deposit
        # wallet flow" (Polymarket v2 enforcement since 2025-04-28). The
        # working sibling project polymart3 uses the same pattern:
        #   funder=self._funder or self._account_addr
        # so if the operator has configured FUNDER_ADDRESS we forward it
        # verbatim, otherwise we fall back to the EOA at first init and
        # patch it in once the address is derived (see _init_client).
        self._funder_address = funder_address
        # Resolved at init: FUNDER_ADDRESS when supplied, otherwise the EOA
        # derived from the private key. Surfaced to the dashboard.
        self._resolved_funder = ""
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
            # H-V6-11 fix: derive the EOA address from the private key so we
            # can fall back to it as `funder` when the operator has not
            # supplied FUNDER_ADDRESS in .env. Polymarket's order payload
            # requires `maker` to be the wallet that actually holds the
            # funds; leaving the SDK to default that to signer.address()
            # is fine for SIG_TYPE=0 EOA but breaks SIG_TYPE=1/2/3 when
            # the operator forgets to set FUNDER_ADDRESS.
            try:
                from eth_account import Account
                eoa_addr = Account.from_key(self._private_key).address if self._private_key else ""
            except Exception:
                eoa_addr = ""
            funder = self._funder_address or eoa_addr
            self._resolved_funder = funder
            self._client = ClobClient(
                host=self._host,
                chain_id=self.POLYGON_CHAIN_ID,
                key=self._private_key,
                creds=api_creds,
                signature_type=self._signature_type,
                funder=funder,
            )
            log.info(
                "CLOBClient ready",
                host=self._host, sig_type=self._signature_type,
                funder=funder or "(empty)",
            )
        except Exception as exc:
            log.error("CLOBClient init failed", error=str(exc))
            self._client = None

    def is_configured(self) -> bool:
        return self._client is not None

    @property
    def funder_address(self) -> str:
        """Wallet that holds the funds (FUNDER_ADDRESS or derived EOA)."""
        return self._resolved_funder or self._funder_address

    @property
    def signature_type(self) -> int:
        return self._signature_type

    # ------------------------------------------------------------------
    # Balance
    # ------------------------------------------------------------------

    async def get_collateral(self) -> dict | None:
        """Collateral (pUSD) balance AND allowance in human units.

        Same REST call as `get_balance`, but keeps both fields so the
        dashboard can show whether the wallet is funded and whether the
        CLOB is approved to spend it. Returns None on any failure — an
        API error must never be rendered as a zero balance.
        """
        if not self._client:
            return None
        try:
            from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

            resp = await self._run(
                self._client.get_balance_allowance,
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL),
            )
            if not isinstance(resp, dict) or resp.get("balance") is None:
                return None
            balance = float(resp["balance"]) / self._CLOB_BASE_UNITS
            raw_allowance = resp.get("allowance")
            allowance = (
                float(raw_allowance) / self._CLOB_BASE_UNITS
                if raw_allowance is not None
                else None
            )
            return {"balance": balance, "allowance": allowance}
        except Exception as exc:
            log.debug("clob.get_collateral.error", error=str(exc))
            return None

    async def get_balance(self) -> float | None:
        """USDC (pUSD) collateral balance in human units, or None on error.

        py-clob-client-v2 exposes `get_balance_allowance` (NOT `get_balance`
        / `get_pusd_balance` — those names do not exist in the published
        SDK). The response is ``{"balance": "<base units>", "allowance":
        "<base units>"}``; collateral has 6 decimals, so divide by 1e6.

        Returns None (not 0.0) on failure so callers can distinguish
        "query failed" from "balance really is zero". Treating an error
        as zero previously caused `_maybe_realize_closed` to wipe a live
        position after a transient REST failure.
        """
        if not self._client:
            return None
        try:
            from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

            resp = await self._run(
                self._client.get_balance_allowance,
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL),
            )
            if not isinstance(resp, dict) or resp.get("balance") is None:
                return None
            return float(resp["balance"]) / self._CLOB_BASE_UNITS
        except Exception as exc:
            log.warning("clob.get_balance.error", error=str(exc))
            return None

    async def get_token_balance(self, token_id: str) -> float | None:
        """Token balance in shares (human, not micro-units), or None on error.

        Uses `get_balance_allowance` with AssetType.CONDITIONAL. Outcome
        tokens are ERC-1155 with 6 decimals, so the raw balance is divided
        by 1e6. Returning None on failure is essential: the TierWatcher /
        realize checks must not treat an API error as a zero holding.
        """
        if not self._client:
            return None
        try:
            from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

            resp = await self._run(
                self._client.get_balance_allowance,
                BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL,
                    token_id=token_id,
                ),
            )
            if not isinstance(resp, dict) or resp.get("balance") is None:
                return None
            return float(resp["balance"]) / self._CLOB_BASE_UNITS
        except Exception as exc:
            log.debug("clob.get_token_balance.error", error=str(exc))
            return None

    # H-V6-15: the 6-decimal fixed-math convention applies to some CLOB
    # fields but NOT to `get_order` (the WS-fill REST fallback). Evidence
    # from the 2026-07-11 08:19 production trace:
    #   order_size = 5.12 (shares)
    #   REST size_matched raw = 5.12 (shares — NOT 5_120_000 base units)
    # So:
    #   - /data/order/<id>  .size_matched  → SHARES (no divide)
    #   - /trades           fills[].size    → BASE UNITS (divide, kept)
    #   - POST /order       .original_size  → SHARES (we already pass shares)
    # The remaining divides in place_gtd_buy / place_gtd_sell on fills[].size
    # are kept for now; if production ever shows them wrong, revisit with
    # the same unit-test pattern as the size_matched fix.
    _CLOB_BASE_UNITS = 1_000_000

    async def get_order_filled_size(self, order_id: str) -> float | None:
        """
        Read the authoritative `size_matched` for an order via REST.

        Used as a fallback when the User WS delivers a fill event but the
        WS payload's size field is 0 (a known Polymarket WS quirk — the
        size key is sometimes `size`, sometimes `size_matched`, and for
        some wallets the WS payload omits it entirely from the
        maker_orders[] entries). Polymart3's working sibling project
        uses the same fallback in its buy_token polling loop.

        H-V6-15: the previous H-V6-12 audit (finding C-1) interpreted
        Polymarket's docs as saying this endpoint returns `size_matched`
        in 6-decimal base units and added a divide-by-1_000_000. The
        2026-07-11 08:19 production trace shows that interpretation is
        WRONG for this endpoint:

          order_size        = 5.12     (shares we asked for)
          REST size_matched = 5.12     (raw — already in shares)
          post-divide       = 5.12e-06 (5.12 × 10⁻⁶ — the bug)

        The 5.12e-06 value flowed through the WS→REST fallback into
        the router, then into `position.total_qty`. STATUS rounded it
        to "0.00" so the operator couldn't see the problem, but the
        underlying position state was off by 1_000_000×.

        The 6-decimal convention DOES apply to:
          - `/trades` fills[].size (returned in base units — polling
            loops at line ~464 / ~697 still divide those)
          - The order request payload `original_size` field (when we
            POST `/order` we already pass shares; py-clob-client
            converts server-side)

        …but NOT to:
          - `GET /data/order/<id>` size_matched — returns shares.

        Returns size in SHARES, or None if the call fails.
        """
        if not self._client:
            return None
        try:
            resp = await self._run(self._client.get_order, order_id)
            if not isinstance(resp, dict):
                return None
            for k in (
                "size_matched",
                "sizeMatched",
                "amount_matched",
                "amountMatched",
                "filled_size",
                "filledSize",
            ):
                v = resp.get(k)
                if v is not None:
                    try:
                        f = float(v)
                        # H-V6-15: no divide. This endpoint returns
                        # shares directly (production-validated 2026-07-11).
                        if f > 0:
                            return f
                    except (TypeError, ValueError):
                        continue
            return 0.0
        except Exception as exc:
            log.debug(
                "clob.get_order_filled_size.error",
                order_id=order_id[:16], error=str(exc),
            )
            return None

    async def get_order_fill_state(self, order_id: str) -> dict | None:
        """Single REST fetch of `{size_matched, avg_price, status}` in shares.

        H-NEW-2026-07-11: the watcher needs both the matched size AND the
        real average fill price so it can hand a defensible price to
        `apply_tier_watcher_result` when the WS path missed a fill. Doing
        this in one round-trip keeps the fast-path latency low.
        Returns a dict like {"size_matched": 5.0, "avg_price": 0.40, "status": "..."}
        or None on transient error.
        """
        if not self._client:
            return None
        try:
            resp = await self._run(self._client.get_order, order_id)
            if not isinstance(resp, dict):
                return None
            size = 0.0
            for k in (
                "size_matched", "sizeMatched",
                "amount_matched", "amountMatched",
                "filled_size", "filledSize",
            ):
                v = resp.get(k)
                if v is not None:
                    try:
                        f = float(v)
                        if f > 0:
                            size = f
                            break
                    except (TypeError, ValueError):
                        continue
            avg_px = 0.0
            for k in (
                "avg_price", "avgPrice",
                "average_price", "averagePrice",
            ):
                v = resp.get(k)
                if v is not None:
                    try:
                        avg_px = float(v)
                        break
                    except (TypeError, ValueError):
                        continue
            # The order payload has NO average-fill-price field — only the
            # LIMIT (`price`) and `associate_trades` (trade ids). Without
            # this fallback every taker fill was recorded at its limit:
            # production 2026-10-31 09:30 recorded 0.40 BUs and 0.40 sells
            # while Polymarket's history showed 0.37-0.39 buys and 0.49
            # sells (PnL understated by ~3x).
            trader_side: str | None = None
            if size > 0:
                try:
                    trade_avg, trader_side = await self._avg_price_from_trades(
                        order_id, resp.get("associate_trades")
                    )
                    if avg_px <= 0 and trade_avg > 0:
                        avg_px = trade_avg
                except Exception as exc:  # noqa: BLE001
                    log.debug(
                        "clob.avg_price_from_trades.error",
                        order_id=order_id[:16], error=str(exc),
                    )
            return {
                "size_matched": size,
                "avg_price": avg_px,
                # Which role OUR order played in these trades. Takers pay
                # the Polymarket fee (C × rate × p × (1-p)); makers pay 0.
                # `is_taker` is kept for older callers; `trader_side` is
                # tri-state (TAKER / MAKER / None) so a maker fill observed
                # only through REST can still be labelled correctly.
                "is_taker": trader_side == "TAKER",
                "trader_side": trader_side,
                "status": (resp.get("status") or "").lower(),
            }
        except Exception as exc:
            log.debug(
                "clob.get_order_fill_state.error",
                order_id=order_id[:16], error=str(exc),
            )
            return None

    async def _avg_price_from_trades(
        self, order_id: str, trade_ids: object
    ) -> tuple[float, str | None]:
        """Weighted average execution price + OUR role from the order's trades.

        `GET /data/order/<id>` exposes `associate_trades` (trade ids) but no
        average price. For each associated trade:
          * if OUR order was the taker (`taker_order_id == order_id`), the
            trade's top-level `price` is our execution price;
          * if our order was a maker, use our row's `price` (a maker fill
            always executes at its resting limit, so weights do not matter).

        Returns `(avg_price, trader_side)` where `trader_side` is
        "TAKER", "MAKER", or None when the ledger is unavailable. The role
        drives the Polymarket fee (taker pays `C × rate × p × (1-p)`, maker
        pays 0) and the dashboard's maker-ratio stats.

        Results are cached per (order_id, trade-count) so the watcher's
        repeated fill-state reads do not re-query every trade.
        """
        ids = [str(t) for t in (trade_ids or []) if t]
        if not ids:
            return 0.0, None
        cache = getattr(self, "_avg_price_cache", None)
        if cache is None:
            self._avg_price_cache = cache = {}
        key = (order_id, len(ids))
        hit = cache.get(key)
        if hit is not None:
            cached_ts, cached_avg, cached_role = hit
            if (
                cached_avg > 0
                or cached_role is not None
                or (time.monotonic() - cached_ts) < _AVG_PRICE_UNKNOWN_TTL_S
            ):
                return float(cached_avg), cached_role
        total_qty = 0.0
        total_notional = 0.0
        taker_seen = False
        maker_seen = False
        for tid in ids[:20]:
            try:
                rows = await self._run(
                    self._client.get_trades, TradeParams(id=tid)
                )
            except Exception as exc:  # noqa: BLE001
                log.debug(
                    "clob.avg_price.trade_lookup_failed",
                    trade_id=tid[:12], error=str(exc),
                )
                continue
            for t in rows or []:
                try:
                    taker_id = str(t.get("taker_order_id") or "")
                    taker_px = float(t.get("price") or 0)
                    taker_sz = float(t.get("size") or 0)
                except (TypeError, ValueError):
                    continue
                qty = 0.0
                px = 0.0
                if taker_id == order_id:
                    taker_seen = True
                    qty, px = taker_sz, taker_px
                else:
                    for mo in t.get("maker_orders") or []:
                        if str(mo.get("order_id") or "") != order_id:
                            continue
                        maker_seen = True
                        try:
                            px = float(mo.get("price") or 0)
                            qty = float(mo.get("matched_amount") or 0)
                        except (TypeError, ValueError):
                            px, qty = 0.0, 0.0
                        if qty <= 0:
                            qty = taker_sz
                        break
                if qty > 0 and px > 0:
                    total_qty += qty
                    total_notional += px * qty
        avg = (total_notional / total_qty) if total_qty > 0 else 0.0
        # TAKER wins when the order took on any trade (matches the
        # order-wide semantics used by the fee); otherwise MAKER.
        role = "TAKER" if taker_seen else ("MAKER" if maker_seen else None)
        if len(cache) > 512:
            cache.clear()
        # Always store, tagged with the time so an UNRESOLVED lookup
        # (transient trade-endpoint failure / ledger lag) is retried after
        # a short TTL instead of being frozen as "no price, no role" — the
        # fee backfill depends on a later read seeing the trades.
        cache[key] = (time.monotonic(), avg, role)
        if avg > 0 or role is not None:
            log.info(
                "clob.avg_price_from_trades",
                order_id=order_id[:16],
                trade_count=len(ids),
                avg_price=round(avg, 6) if avg > 0 else None,
                trader_side=role,
            )
        return avg, role

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
        """Post a GTD limit BUY order and return immediately.

        H-NEW-2026-07-11 (fill-channel refactor): the previous
        implementation polled `get_order` here for up to 60 s and tried to
        cancel on timeout. That dual-path was the source of the 2026-07-11
        10:03 incident: the order filled on the CLOB but
        (a) the WS payload reported `size=0`,
        (b) this poll loop watched `fills[]` and missed the fill because
            SDK v2 returns size_matched only at the top level,
        (c) the timeout-fired cancel hit a 400 ("Invalid order payload"),
        (d) `place_buy` returned None → engine saw the BUY as failed.

        Now TierWatcher is the single source of truth for fill progress —
        it already polls `get_token_balance` every `TIER_FILL_POLL_S`. So
        this method is responsible only for:
          1. POST /order,
          2. pre-register the server_id with the OrderManager,
          3. return a `FillResult(success=True, order_id=...)` so the
             caller can spawn the watcher.
        The actual fill events flow through `_fire_fill_callback` (driven
        by TierWatcher and User WS), not through this function.
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

        # Pre-register the server_id so a fill that fires synchronously
        # from inside the POST (rare but observed) can be routed to the
        # right TrackedOrder before place_buy finishes inserting it.
        if self.register_pending:
            try:
                self.register_pending(order_id)
            except Exception as exc:
                log.debug("clob.register_pending.error", error=str(exc))

        log.info(
            "clob.gtd_buy.posted",
            order_id=order_id[:16],
            token=token_id[:16],
            size=size_q,
            price=price_q,
            tick=tick_size,
            expiration_ts=expiration_ts,
        )
        # Return success with order_id. Fill progress is owned by
        # TierWatcher + User WS — this method no longer polls.
        #
        # `tokens` here is the ACCEPTED PLACED size (post-CLOB round-down),
        # not a fill. OrderManager copies it into TrackedOrder.placed_size so
        # the engine's `size_expected` matches what the server actually
        # accepted (M-1). No fill has happened yet.
        return FillResult(
            order_id=order_id,
            tokens=float(size_q),
            usdc=0,
            fill_price=price_q,
            success=True,
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
        Post a GTD limit SELL order (maker) and return immediately.

        Behaviour:
          - POST /order, pre-register the server id, and return a *live*
            FillResult. Fill progress is owned by TierWatcher (per-order
            `size_matched` polling) and the UserWS UPDATE/TRADE channels.
          - This method deliberately does NOT poll. It used to poll for up
            to 60 s while OrderManager held its global lock, which blocked
            every other order operation and, on timeout, reported a still-live
            GTD as failed (the caller then dropped the tracking mapping).
        """
        if not self._client:
            return FillResult("", 0, 0, price, False, "ClobClient not initialised")

        gtd_seconds = max(lifetime_s, self.GTD_MIN_SEC + 1)
        expiration_ts = int(time.time()) + gtd_seconds

        tick_d = Decimal(str(tick_size))
        size_q = _round_down(size, Decimal("0.0001"))
        # SELL floors the price to the tick: fill priority over the last
        # tick of target (see the policy note above `_round_down`'s users).
        price_q = min(0.99, _round_down(price, tick_d))

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

        log.info(
            "clob.gtd_sell.posted",
            order_id=order_id[:16],
            token=token_id[:16],
            size=size_q,
            price=price_q,
            tick=tick_size,
            expiration_ts=expiration_ts,
        )
        # Non-blocking contract (see docstring): fills are owned by
        # TierWatcher + UserWS. Never poll here — this runs under
        # OrderManager's global lock.
        return FillResult(
            order_id=order_id,
            tokens=0,
            usdc=0,
            fill_price=price_q,
            success=True,
            expiration_ts=expiration_ts,
        )

    async def place_fak_buy(
        self,
        token_id: str,
        amount: float,
        price: float,
        tick_size: float,
    ) -> FillResult:
        """Post a FAK market BUY for `amount` USDC, walking the book to `price`.

        T1 uses this path. A 1-USDC tier at ~0.40 is only ~2.5 shares,
        below the 5-share minimum the CLOB enforces on resting GTDs; a
        market BUY is sized in USDC and crosses the book as taker.

        Returns the executed fill (shares + USDC spent). Deliberately does
        NOT fire `_fire_fill_callback`: the T1 tier is bound only after
        this returns, so the router feeds the fill to the engine there.
        Any later UserWS observation is deduped by OrderManager's monotonic
        `size_matched`.
        """
        if not self._client:
            return FillResult("", 0, 0, price, False, "ClobClient not initialised")

        tick_d = Decimal(str(tick_size))
        price_q = _round_down(price, tick_d)
        amount_q = _round_down(amount, Decimal("0.01"))
        if amount_q <= 0 or price_q <= 0:
            return FillResult(
                "", 0, 0, price_q, False, "invalid FAK buy amount/price"
            )

        log.debug(
            "clob.fak_buy",
            token=token_id[:12], amount=amount_q, price=price_q,
        )
        try:
            mkt_args = MarketOrderArgs(
                token_id=token_id,
                amount=float(amount_q),
                side=ClobSide.BUY,
                price=float(price_q),
                order_type=OrderType.FAK,
            )
            resp = await self._run(
                self._client.create_and_post_market_order,
                mkt_args,
                order_type=OrderType.FAK,
            )
        except Exception as exc:
            log.warning("clob.fak_buy.post_error", error=str(exc))
            return FillResult(
                "", 0, 0, price_q, False, f"FAK_post_error:{exc}"
            )

        if not resp:
            return FillResult("", 0, 0, price_q, False, "FAK_no_response")

        total_sz = 0.0
        total_usdc = 0.0
        for f in resp.get("fills") or []:
            try:
                sz = float(f.get("size", 0))
                px = float(f.get("price", price_q))
            except (TypeError, ValueError):
                continue
            if sz > 0 and px > 0:
                total_sz += sz
                total_usdc += sz * px

        # Market-order responses may only expose aggregate amounts. For a
        # BUY: takingAmount = shares received, makingAmount = USDC spent.
        if total_sz <= 0:
            try:
                taking = float(resp.get("takingAmount") or 0)
                making = float(resp.get("makingAmount") or 0)
            except (TypeError, ValueError):
                taking = making = 0.0
            if taking > 0 and making > 0:
                total_sz, total_usdc = taking, making

        # Some endpoints return 6-decimal base units. This bot's tiers are
        # at most 8 USDC, so a value in the millions is certainly base
        # units, not shares/USDC. Check each field independently — a
        # fills[].size in base units and price 0.40 makes USDC land at
        # exactly 1_000_000, which a combined "both > 1e6" test would miss.
        if total_sz > 1_000_000:
            total_sz /= self._CLOB_BASE_UNITS
        if total_usdc > 1_000_000:
            total_usdc /= self._CLOB_BASE_UNITS

        if total_sz <= 0 or total_usdc <= 0:
            return FillResult("", 0, 0, price_q, False, "FAK_no_fills")

        avg_px = total_usdc / total_sz
        if avg_px <= 0 or avg_px > 1:
            return FillResult("", 0, 0, price_q, False, "FAK_bad_avg_price")

        order_id = str(resp.get("orderID") or resp.get("order_id") or "")
        if order_id and self.register_pending:
            try:
                self.register_pending(order_id)
            except Exception as exc:
                log.debug("clob.register_pending.error", error=str(exc))

        result = FillResult(
            order_id=order_id,
            tokens=_round_down(total_sz, Decimal("0.0001")),
            usdc=_round_down(total_usdc, Decimal("0.01")),
            fill_price=_round_down(avg_px, tick_d),
            success=True,
        )
        log.info(
            "clob.fak_buy.filled",
            order_id=order_id[:16],
            token=token_id[:16],
            shares=result.tokens,
            usdc=result.usdc,
            avg_price=result.fill_price,
        )
        return result

    async def _fak_sell_core(
        self,
        token_id: str,
        size: float,
        sell_price: float,
        tick_size: float,
        *,
        fire_callback: bool,
    ) -> FillResult:
        """Shared FAK market-SELL post/parse. `sell_price` is the limit."""
        if not self._client:
            return FillResult(
                "", 0, 0, sell_price, False, "ClobClient not initialised"
            )

        tick_d = Decimal(str(tick_size))
        size_q = _round_down(size, max(Decimal("0.0001"), tick_d))

        log.debug(
            "clob.fak_sell",
            token=token_id[:12], size=size_q, price=sell_price,
        )

        try:
            # py-clob-client-v2 MarketOrderArgsV2 uses `amount` (shares for
            # a SELL) and `price`; there is no `size` / `slippage` keyword.
            # The limit price is computed by the caller and passed
            # explicitly so the market order walks the book only to it.
            mkt_args = MarketOrderArgs(
                token_id=token_id,
                amount=float(size_q),
                side=ClobSide.SELL,
                price=float(sell_price),
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
            return FillResult(
                "", 0, 0, sell_price, False, f"FAK_post_error:{exc}"
            )

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
        if fire_callback:
            # A FAK (market) sell only fills by crossing the book — by
            # definition WE are the taker, so the Polymarket fee applies.
            await self._fire_fill_callback(
                token_id, result.order_id, avg_px, total_sz, trader_side="TAKER"
            )
        return result

    async def place_fak_sell(
        self,
        token_id: str,
        size: float,
        price: float,
        tick_size: float,
        slippage_pct: float = 2.0,
    ) -> FillResult:
        """Legacy FAK sell (OrderManager.place_sell path).

        Keeps its historical price direction: the limit is `price` plus
        `slippage_pct`. The T1/compact-TP exits use
        `place_fak_sell_marketable` instead.
        """
        tick_d = Decimal(str(tick_size))
        slip_d = Decimal(str(slippage_pct)) / Decimal("100")
        slip_price = Decimal(str(price)) * (Decimal("1") + slip_d)
        sell_price = float(slip_price.quantize(tick_d, rounding=ROUND_DOWN))
        return await self._fak_sell_core(
            token_id, size, sell_price, tick_size, fire_callback=True
        )

    async def place_fak_sell_marketable(
        self,
        token_id: str,
        size: float,
        reference_price: float,
        tick_size: float,
        slippage_pct: float = 2.0,
    ) -> FillResult:
        """FAK market SELL that crosses below `reference_price`.

        For a SELL the order price is a FLOOR, so subtract slippage to make
        the order marketable. Compact positions (a sole 1-USD T1 is ~2.5
        shares) cannot use a GTD exit (5-share minimum), so their take
        profit goes through this path.

        Does not fire the fill callback: the router records the fill
        synchronously against the TP snapshot it registers right after
        this returns.
        """
        tick_d = Decimal(str(tick_size))
        slip_d = Decimal(str(slippage_pct)) / Decimal("100")
        ref = Decimal(str(reference_price)) * (Decimal("1") - slip_d)
        sell_price = float(ref.quantize(tick_d, rounding=ROUND_DOWN))
        if sell_price <= 0:
            sell_price = float(tick_d)
        return await self._fak_sell_core(
            token_id, size, sell_price, tick_size, fire_callback=False
        )

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a single order. Returns True on success.

        H-NEW-2026-07-11 (audit v6-12 production revisit): the
        py-clob-client-v2 SDK signature is `cancel_order(payload:
        OrderPayload)` where `OrderPayload.orderID` is a typed str
        attribute. Passing a bare string fails with
        "'str' object has no attribute 'orderID'", passing a dict
        fails with "'dict' object has no attribute 'orderID'"
        (production log 12:04:46.675). The correct shape is an
        `OrderPayload(orderID=...)` object. `cancel_orders()` is a
        different batch API that takes a list of dicts — DO NOT
        confuse the two.
        """
        if not self._client:
            return False
        try:
            from py_clob_client_v2.clob_types import OrderPayload
            payload = OrderPayload(orderID=order_id)
            await self._run(self._client.cancel_order, payload)
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
            result = {"success": False, "error": str(exc), "exception": True}
            # Bootstrap / re-bootstrap: the server rejects the null id with
            # HTTP 400 but hands back the session id to use, e.g.
            #   {"heartbeat_id": "6d04dd4b-...", "error_msg": "Invalid Heartbeat ID"}
            # The SDK raises instead of returning that body, so the caller
            # never saw the id and kept re-sending null — production
            # 2026-10-30 took 4 attempts (~20 s) to reach `session_ready`.
            # Polymarket cancels open GTDs after 10 s without a heartbeat,
            # so that gap is dangerous mid-round. Surface the id here and
            # `HeartbeatLoop` adopts it on the next tick.
            err = getattr(exc, "error_msg", None)
            if isinstance(err, dict):
                hint = err.get("heartbeat_id")
                if hint:
                    result["suggested_heartbeat_id"] = str(hint)
            return result

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    async def get_open_orders(
        self,
        condition_id: str | None = None,
        asset_id: str | None = None,
    ) -> list[dict] | None:
        """Fetch open orders from CLOB.

        Returns a list of dicts, or **None when the query failed**. The
        None sentinel is important: returning [] on a transient error let
        `OrderManager.reconcile()` conclude that every tracked order was
        gone and mark it cancelled while it was still live on the book.

        `asset_id` filters the response locally to one outcome token; it is
        used by the pre-post duplicate/orphan check.

        Each dict has at least:
            id, status, market (condition_id), asset_id, side,
            price, original_size, size_matched, expiration
        """
        if not self._client:
            return None
        try:
            # v2 SDK exposes get_open_orders() as the live-orders query.
            # Some 2.x point releases also added an alias `get_orders()`
            # that wraps the same endpoint — prefer the named alias and
            # only fall back if the SDK is too old.
            method = getattr(self._client, "get_open_orders", None) \
                or getattr(self._client, "get_orders", None)
            if method is None:
                log.warning(
                    "clob.get_open_orders.missing_method",
                    note="neither get_open_orders nor get_orders exists on ClobClient; "
                         "cannot reconcile order state for this SDK version",
                )
                return None
            orders = await self._run(method)
            if condition_id:
                # V2 schema uses `market` (= condition_id). Keep the V1
                # `conditionID` as a fallback so older SDK builds still
                # filter correctly.
                orders = [
                    o for o in orders
                    if o.get("market") == condition_id
                    or o.get("conditionID") == condition_id
                ]
            if asset_id:
                orders = [
                    o for o in orders
                    if str(
                        o.get("asset_id")
                        or o.get("assetId")
                        or o.get("token_id")
                        or ""
                    ) == asset_id
                ]
            return orders
        except Exception as exc:
            log.warning("clob.get_orders error", error=str(exc))
            return None

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
        trader_side: str | None = None,
    ) -> None:
        """Forward a confirmed fill to the user callback, exactly once per delta.

        `fill_size` is the *cumulative* matched size observed by this call
        site (whether polling or FAK post-response). The deduper translates
        cumulative -> delta so that the engine is never told about a fill it
        already accounted for, irrespective of how many parallel paths
        happened to observe it (WS / polling / FAK response).

        `trader_side` ("TAKER" for marketable fills) is forwarded so the
        router can book the Polymarket taker fee on the fill row.
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
                token_id, order_id, fill_price, delta, total_after, trader_side
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
