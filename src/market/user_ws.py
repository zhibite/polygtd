"""User WebSocket — receives own order/trade events.

Subscribes to:  wss://ws-subscriptions-clob.polymarket.com/ws/user
Auth: L2 API key (apiKey / secret / passphrase) or private-key derived.

Events of interest:
  - order PLACEMENT   -> record open order
  - order UPDATE      -> update size_matched (partial fills)
  - order CANCELLATION -> remove from open orders
  - trade MATCHED     -> multi-maker fill (non-final until CONFIRMED)
  - trade MINED       -> intermediate (rare)
  - trade CONFIRMED   -> final settlement

CRITICAL — TRADE event attribution:
  A polymarket TRADE event is a SINGLE broadcast that aggregates one
  taker order (`taker_order_id`) hitting 1..N maker orders
  (`maker_orders[]`). Each maker row carries its OWN asset_id and
  `matched_amount` — those are NOT our share of the trade. The
  authoritative OUR-share figures are the TOP-LEVEL fields: `size`
  (our trade volume) and `taker_order_id` (our hash when we're the
  taker).

  We fire the trade callback ONCE per TRADE event. The callback
  receives a list of candidate order_ids (taker_order_id ∪ every
  maker_orders[].order_id, full hashes, never truncated) plus a
  default MakerFill shaped from the top-level fields. The router
  uses OrderManager.by_server_id (full-hash dictionary) to pick the
  candidate that matches one of our tracked orders. NO prefix
  matching, NO trader_side assumption.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import time
from dataclasses import dataclass, field
from typing import Callable

import websockets

from src.infra.logging import get_logger

log = get_logger(__name__)

USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"


# ---------------------------------------------------------------------------
# Event types
# ---------------------------------------------------------------------------


@dataclass
class OrderEvent:
    """Fired when an order PLACEMENT / UPDATE / CANCELLATION arrives."""
    order_id: str           # complete server order_id hash
    event_type: str         # "PLACEMENT" | "UPDATE" | "CANCELLATION"
    side: str               # "BUY" | "SELL"
    token_id: str           # asset_id of this order
    price: float
    size: float
    size_matched: float
    expires_at: int
    ts_ms: int


@dataclass
class MakerFill:
    """
    One maker-order fill inside a trade event.

    This is the authoritative unit — each maker_orders[] entry is one MakerFill.
    The parent TradeEvent is only a container.
    """
    maker_order_id: str     # complete server order_id of the maker order
    asset_id: str           # token this maker order was on (UP or DOWN token address)
    price: float
    size: float             # shares filled on this maker order
    trade_id: str
    status: str             # "MATCHED" | "MINED" | "CONFIRMED"
    ts_ms: int


@dataclass
class TradeEvent:
    """Legacy wrapper — kept for OrderManager compatibility."""
    order_id: str
    trade_id: str
    status: str
    price: float
    size: float
    ts_ms: int


# ---------------------------------------------------------------------------
# Typed callbacks
# ---------------------------------------------------------------------------

# (MakerFill) -> None
UserMakerFillCallback = Callable[[MakerFill], None]
# (OrderEvent) -> None
UserOrderCallback = Callable[[OrderEvent], None]


# ---------------------------------------------------------------------------
# Order cache (optional, for reference)
# ---------------------------------------------------------------------------

@dataclass
class UserOrderCache:
    """Tracks open orders locally keyed by full server order_id."""

    orders: dict[str, dict] = field(default_factory=dict)

    def upsert(self, ev: OrderEvent) -> None:
        self.orders[ev.order_id] = {
            "side": ev.side,
            "token_id": ev.token_id,
            "price": ev.price,
            "size": ev.size,
            "size_matched": ev.size_matched,
            "expires_at": ev.expires_at,
            "ts_ms": ev.ts_ms,
        }

    def remove(self, order_id: str) -> bool:
        return self.orders.pop(order_id, None) is not None

    def size_matched(self, order_id: str) -> float:
        return self.orders.get(order_id, {}).get("size_matched", 0.0)


# ---------------------------------------------------------------------------
# UserWS
# ---------------------------------------------------------------------------

class UserWS:
    """Async WebSocket consumer for authenticated user order/trade events."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        on_maker_fill: UserMakerFillCallback | None = None,
        on_order: UserOrderCallback | None = None,
        order_cache: UserOrderCache | None = None,
        clob: object | None = None,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase
        self._on_maker_fill = on_maker_fill
        self._on_order = on_order
        # Detect ONCE whether the callback accepts the `candidates=` keyword.
        # The previous try/except TypeError fallback could not distinguish a
        # signature mismatch from a TypeError raised INSIDE the callback, and
        # would then invoke the callback a second time (double-booking the
        # same fill).
        self._maker_fill_accepts_candidates = False
        if on_maker_fill is not None:
            try:
                params = inspect.signature(on_maker_fill).parameters
                self._maker_fill_accepts_candidates = (
                    "candidates" in params
                    or any(
                        p.kind is inspect.Parameter.VAR_KEYWORD
                        for p in params.values()
                    )
                )
            except (TypeError, ValueError):
                # Builtins / C callables: default to the 2-arg form.
                self._maker_fill_accepts_candidates = True
        self._cache = order_cache or UserOrderCache()
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._url = USER_WS_URL
        self._pending_markets: set[str] = set()
        # H-V6-12 fix (audit M-1): init here instead of relying on the
        # AttributeError-as-init pattern in _process. Allows us to also
        # reset the counter explicitly on each successful WS reconnect.
        self._trade_dump_count = 0
        # H-V6-12 fix (audit M-2): track the cumulative fill size we last saw
        # via REST for each order_id. Used to convert REST's cumulative
        # `size_matched` into a true delta before forwarding to the
        # router. Without this, a second WS MATCHED event for the same
        # order would have the router compute
        # new_cumulative = prev + fill_size_inc, where fill_size_inc is
        # actually the CUMULATIVE value (REST returns cumulative, not
        # delta). The deduper math `new_cumulative - prev_cumulative
        # <= 1e-9` would then compare cumulative vs 2*cumulative and
        # wrongly pass — engine double-counts. Cleared on reconnect.
        self._last_rest_cumulative: dict[str, float] = {}
        # H-V6-12 fix: optional reference to CLOBClient so that when the WS
        # delivers a trade event with size=0 (Polymarket WS quirk — see
        # market_ws raw sample in production 2026-07-11), we can fall
        # back to `clob.get_order_filled_size(order_id)` to read the
        # authoritative `size_matched` from REST. This is what
        # D:\05.project2\1.bquant\polymart3 does in its polling loop
        # to recover from MATCHED-no-fills events.
        self._clob = clob

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def subscribe_markets(self, condition_ids: list[str]) -> None:
        for cid in condition_ids:
            self._pending_markets.add(cid)

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run())
        log.info("user_ws.started", url=self._url)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("user_ws.stopped")

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        delay = 1.0
        max_delay = 30.0

        while self._running:
            try:
                async with websockets.connect(self._url, ping_interval=10) as ws:
                    delay = 1.0
                    log.debug("user_ws.connected")

                    # H-V6-12 fix (audit M-1 + M-2): on each fresh WS
                    # connection, reset the raw-dump counter and the
                    # cumulative tracker. Without this, after one
                    # reconnect neither dump nor dedup state re-arms.
                    self._trade_dump_count = 0
                    self._last_rest_cumulative = {}

                    await self._authenticate(ws)
                    if self._pending_markets:
                        await self._subscribe(ws, list(self._pending_markets))

                    async for raw in ws:
                        if not self._running:
                            break
                        await self._process(raw)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning("user_ws.error", error=str(exc), retry_in_s=delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)

    async def _authenticate(self, ws: websockets.WebSocketClientProtocol) -> None:
        await ws.send(json.dumps({
            "type": "user",
            "auth": {
                "apiKey": self._api_key,
                "secret": self._api_secret,
                "passphrase": self._api_passphrase,
            },
        }))
        log.info("user_ws.auth_sent")

    async def _subscribe(
        self, ws: websockets.WebSocketClientProtocol, condition_ids: list[str]
    ) -> None:
        await ws.send(json.dumps({
            "type": "user",
            "markets": condition_ids,
        }))
        log.info("user_ws.subscribed", markets=[c[:16] for c in condition_ids])

    async def _process(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        # The user channel can emit JSON arrays (batched events) as well as
        # objects. The old code called `msg.get(...)` unconditionally, which
        # raised AttributeError and forced a WS reconnect on every array.
        if not isinstance(msg, dict):
            return

        event_type = msg.get("event_type", "")
        type_ = msg.get("type", "")
        content = msg.get("content", msg)
        if not isinstance(content, dict):
            return

        # H-V6-12 debug: dump every trade message at info level so we can see
        # which fields Polymarket actually sends (size, maker_orders, etc.).
        # Throttled to first 5 per connection to avoid log spam in steady
        # state. Counter is reset on each successful WS reconnect (see
        # _run). Pre-fix the counter was only AttributeError-init on first
        # use, so reconnect never re-armed the dump (audit M-1).
        if event_type == "trade" or type_ == "trade":
            if self._trade_dump_count < 5:
                self._trade_dump_count += 1
                log.info(
                    "user_ws.trade_raw",
                    keys=list((content or {}).keys()),
                    content=str(content)[:600],
                )

        if event_type == "order" or type_ == "order":
            await self._handle_order(content)
        elif event_type == "trade" or type_ == "trade":
            await self._handle_trade(content)

    # ------------------------------------------------------------------
    # Order events
    # ------------------------------------------------------------------

    async def _handle_order(self, content: dict) -> None:
        try:
            order_id = str(content.get("orderID", "") or content.get("order_id", ""))
            side = str(content.get("side", "BUY")).upper()
            token_id = str(content.get("token", "") or content.get("asset_id", ""))
            price = float(content.get("price", 0))
            size = float(content.get("size", 0))
            size_matched = float(content.get("size_matched", 0))
            expires_at = int(content.get("expiration", 0))
            ts_ms = int(content.get("ts", 0)) or int(time.time() * 1000)
            evt_type = str(content.get("type", "PLACEMENT")).upper()
        except Exception as exc:
            log.debug("user_ws.order_parse_error", content=str(content)[:200], error=str(exc))
            return

        if not order_id:
            log.debug("user_ws.order_missing_id", content=str(content)[:200])
            return

        ev = OrderEvent(
            order_id=order_id,
            event_type=evt_type,
            side=side,
            token_id=token_id,
            price=price,
            size=size,
            size_matched=size_matched,
            expires_at=expires_at,
            ts_ms=ts_ms,
        )

        if evt_type == "PLACEMENT":
            self._cache.upsert(ev)
        elif evt_type == "UPDATE":
            self._cache.upsert(ev)
        elif evt_type == "CANCELLATION":
            self._cache.remove(order_id)

        log.info("user_ws.order_event",
                 type=evt_type, order_id=order_id,
                 token=token_id[:16], side=side,
                 price=price, size=size, size_matched=size_matched)

        if self._on_order:
            try:
                self._on_order(ev)
            except Exception:
                log.exception("user_ws.on_order callback raised")

    # ------------------------------------------------------------------
    # Trade events — CRITICAL: process maker_orders[] one-by-one
    # ------------------------------------------------------------------

    async def _handle_trade(self, content: dict) -> None:
        """
        Process a TRADE event from the UserWS.

        Polymarket TRADE semantics (per docs.polymarket.com User Channel):
          - Top-level `size`           = total trade shares (the AUTH volume).
          - Top-level `taker_order_id` = taker order hash.
          - Top-level `trader_side`    = "TAKER" / "MAKER" — which role WE
                                        played in this trade. May be missing
                                        on legacy payloads; do NOT trust it
                                        as the sole truth source.
          - `maker_orders[].order_id`  = complete hash of one maker order
                                        that was filled by this trade. There
                                        can be MULTIPLE rows even for one
                                        side of the trade (liquidity from
                                        multiple makers).
          - `maker_orders[].asset_id`  = token that particular maker was on.
                                        AUTHORITATIVE for that maker row.
          - `maker_orders[].matched_amount`
                                      = opposite-side volume that maker
                                        contributed (NOT our share).

        IMPORTANT (H-NEW-2026-07-14):
          We must NOT use `[:16]` order-id prefixes for any attribution
          decision. order_id hashes are 0x-prefixed 32-byte strings;
          uniqueness guarantees do NOT survive truncation. We pass full
          hashes everywhere.

        Attribution strategy (deterministic, no prefix matching):
          1. CANDIDATE SET = { taker_order_id } ∪ { each maker_orders[].order_id }
                              filtered to non-empty, full hashes.
          2. We pass the WHOLE trade context (candidates + top-level fields)
             to the router via an extended callback that includes the full
             candidates list. The router queries OrderManager.by_server_id
             (which itself uses the full hash) to decide which candidate is
             ours. This pushes attribution to the only place that can do it
             safely — the side that holds our order table.

          When the router is unavailable, we fall back to logging a
          breadcrumb and SKIPPING the callback so we don't emit phantom
          fills based on incomplete attribution.
        """
        if not self._on_maker_fill:
            return

        trade_id = str(content.get("id", "") or content.get("tradeID", "") or content.get("trade_id", ""))
        status = str(content.get("status", "MATCHED")).upper()
        ts_ms = int(content.get("ts", 0)) or int(content.get("timestamp", 0)) or int(time.time() * 1000)

        # ----- Authoritative top-level fields -----
        top_size = self._extract_size_from_payload(content)
        top_price = float(content.get("price", 0) or 0)
        top_asset_id = str(
            content.get("asset_id", "")
            or content.get("assetID", "")
            or ""
        )
        top_side = str(content.get("side", "")).upper()  # BUY or SELL
        taker_order_id = str(
            content.get("taker_order_id", "")
            or content.get("taker_orderID", "")
            or ""
        )
        trader_side_hint = str(content.get("trader_side", "")).upper()  # TAKER / MAKER (hint only)

        # ----- Build the candidate set: every full hash the payload mentions.
        # Preserve first-occurrence order so a taker-trade with one maker
        # row doesn't accidentally appear as "MAKER" by repeating the taker
        # order in both fields.
        # Each candidate is one row from maker_orders[] (or the taker row).
        # Tuple layout: (order_id, asset_id, matched_amount, maker_price,
        #                rest_filled_size). The asset_id and matched_amount
        # are the AUTHORITATIVE values for THIS maker row — never to be
        # silently swapped for the top-level trade values. maker_price is
        # the row's own matched price (often == top-level taker price for
        # the simplest case, but always trust the row when present so we
        # attribute per-maker correctly). rest_filled_size is the latest
        # REST `get_order` size_matched for this candidate — used as the
        # AUTHORITATIVE fallback when the WS payload's maker row doesn't
        # carry an amount (which happens on `trader_side=MAKER` events
        # where polymarket collapses the maker row's amount field).
        # H-NEW-2026-07-11 (audit, prod 14:03 round): prior code used
        # `fill.price` / `fill.size` from the top-level TRADE event, which
        # describe the TAKER side. When our maker is one of many makers
        # in the same trade, this overcounts our shares by the ratio
        # (taker_total / our_maker_amount) and uses the wrong-side price
        # (e.g. we BUY DOWN @0.32, taker BUY UP, top_price=UP price).
        # H-NEW-2026-07-11-2 (audit, prod 14:30 round): even the
        # maker-row amount can be 0 when the WS event's `trader_side` is
        # MAKER and the maker row's amount field is collapsed. The router
        # then fell back to `fill.size` (= taker side) and over-counted
        # again. The REST fallback per candidate provides an
        # order-specific authoritative size.
        seen: set[str] = set()
        candidates: list[tuple[str, str, float, float, float]] = []
        # (oid, aid, amt, price, rest_filled_size)

        def _add_candidate(oid: str, aid: str, amt: float, mprice: float = 0.0) -> None:
            if not oid:
                return
            if oid in seen:
                return
            seen.add(oid)
            candidates.append(
                (oid, aid or "", float(amt or 0), float(mprice or 0), 0.0)
            )

        if taker_order_id:
            _add_candidate(taker_order_id, top_asset_id, top_size, top_price)

        # maker_orders[].order_id  with their PER-ROW asset_id (the row's
        # asset is the AUTHORITATIVE token that maker was on — never
        # silently swap it for the top-level asset).
        for mo in (content.get("maker_orders") or []):
            mo_id = str(mo.get("order_id", "") or mo.get("orderID", "") or "")
            mo_asset = str(
                mo.get("asset_id", "")
                or mo.get("assetID", "")
                or ""
            )
            mo_amount = self._extract_size_from_payload(mo)
            mo_price_raw = mo.get("price", "") or mo.get("px", "") or ""
            try:
                mo_price = float(mo_price_raw) if mo_price_raw else 0.0
            except (TypeError, ValueError):
                mo_price = 0.0
            _add_candidate(mo_id, mo_asset, mo_amount, mo_price)

        if not candidates:
            log.debug(
                "user_ws.trade.no_candidates",
                content=str(content)[:200],
            )
            return

        # ----- REST size fallback (H-V6-12 + H-NEW-2026-07-11-2):
        # For each candidate that has no amount from the WS payload
        # (mo_amount == 0), recover the AUTHORITATIVE size_matched from
        # CLOB REST get_order. This is the same trick polymart3 uses for
        # its MATCHED-no-fills recovery path. We populate candidates[i][4]
        # so the router can pick the right per-candidate value instead
        # of falling back to the top-level fill.size (which is the TAKER
        # side and overcounts our maker).
        for i, (oid, _aid, amt, _mp, _rest) in enumerate(candidates):
            if amt > 0:
                continue
            if status not in ("MATCHED", "MINED", "CONFIRMED"):
                continue
            filled = await self._rest_filled_size(oid)
            if filled is None or filled <= 0:
                continue
            prev_cum = self._last_rest_cumulative.get(oid, 0.0)
            delta = max(0.0, filled - prev_cum)
            if delta <= 0:
                # Already accounted for this trade's REST delta — skip.
                continue
            self._last_rest_cumulative[oid] = filled
            # Replace candidate's amount with the REST cumulative so the
            # router can use it directly. (router applies monotonic max
            # against tracked.size_matched; using cumulative is correct.)
            candidates[i] = (
                oid,
                candidates[i][1],
                filled,
                candidates[i][3],
                delta,
            )
            log.info(
                "user_ws.rest_filled_size",
                order_id=oid[:16],
                cumulative=filled,
                delta=delta,
                prev_cumulative=prev_cum,
                mo_amount_was_zero=True,
            )

        # ----- Top-level effective_size (legacy gate only):
        # effective_size is still used for the zero-fill hard gate. We
        # compute it from the largest authoritative candidate value when
        # top-level is 0, but the real routing uses chosen[2] / chosen[4].
        effective_size = top_size
        if effective_size <= 0:
            for _oid, _aid, amt, _mp, _rest in candidates:
                if amt > 0:
                    effective_size = amt
                    break

        # Hard gate: zero size across the board means no fill. Emit a
        # breadcrumb and skip the callback.
        if effective_size <= 0:
            log.debug(
                "user_ws.trade.zero_size",
                candidates=[oid[:16] for oid, _, _, _, _ in candidates],
                trader_side_hint=trader_side_hint or "UNKNOWN",
                top_size=top_size, status=status,
            )
            return

        # ----- Emit a SINGLE callback carrying ALL candidates -----
        # The router (which has OrderManager) picks the one that matches our
        # tracked server_order_ids via full-hash lookup. This avoids any
        # guesswork based on trader_side semantics or which position in
        # the candidates list we picked.
        # `maker_order_id` legacy slot is filled with candidates[0] for
        # 1-arg-callers; the candidates list is the AUTHORITATIVE source
        # and takes precedence (router uses `by_server_id` on each).
        fill = MakerFill(
            maker_order_id=candidates[0][0],  # legacy slot — see note above
            asset_id=candidates[0][1] or top_asset_id,
            price=top_price,
            size=effective_size,
            trade_id=trade_id,
            status=status,
            ts_ms=ts_ms,
        )

        log.info(
            "user_ws.trade.fill",
            trade_id=trade_id[:16],
            order_id=candidates[0][0][:16],  # log-only truncation
            asset_id=(candidates[0][1] or top_asset_id)[:16],
            side=top_side,
            trader_side_hint=trader_side_hint or "UNKNOWN",
            candidates=[oid[:16] for oid, _, _, _, _ in candidates],
            top_size=top_size,
            status=status,
        )

        try:
            if self._maker_fill_accepts_candidates:
                self._on_maker_fill(fill, candidates=candidates)  # type: ignore[call-arg]
            else:
                # Legacy 1-arg callback: it can only see the representative
                # fill. Log a warning once so the wiring gets fixed.
                if not getattr(self, "_legacy_fill_cb_warned", False):
                    self._legacy_fill_cb_warned = True
                    log.warning(
                        "user_ws.legacy_fill_callback",
                        note="on_maker_fill does not accept `candidates`; "
                             "per-maker attribution is unavailable",
                    )
                self._on_maker_fill(fill)  # type: ignore[call-arg]
        except Exception:
            log.exception("user_ws.on_maker_fill callback raised")

    # ------------------------------------------------------------------
    # Helpers (H-V6-12: WS size=0 fallback to REST)
    # ------------------------------------------------------------------

    _SIZE_KEYS = (
        "size_matched",
        "matched_size",
        "matchedSize",
        "fill_size",
        "fillSize",
        "amount",
        "amount_matched",
        "size",
    )

    def _extract_size_from_payload(self, payload: dict) -> float:
        """Try each known WS size key; return the first non-zero value."""
        for size_key in self._SIZE_KEYS:
            v = payload.get(size_key)
            if v is None:
                continue
            try:
                candidate = float(v)
            except (TypeError, ValueError):
                continue
            if candidate > 0:
                return candidate
        return 0.0

    async def _rest_filled_size(self, order_id: str) -> float | None:
        """
        Fall back to CLOB REST `get_order` to read the authoritative
        size_matched. Polymart3 (working sibling project) uses the same
        pattern in its buy_token MATCHED-no-fills recovery path.
        Returns None on error, 0.0 if order exists but isn't filled.
        """
        if not self._clob or not order_id:
            return None
        try:
            return await self._clob.get_order_filled_size(order_id)
        except Exception:
            log.exception(
                "user_ws._rest_filled_size.error", order_id=order_id[:16],
            )
            return None
