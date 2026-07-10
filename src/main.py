"""Polymarket BTC 15m Up/Down — GTD Martingale Bot.

Entry point. Coordinates:
  1. WindowManager — tracks 15-min window boundaries
  2. BinanceFeed   — BTC spot WS + ring buffer
  3. MarketWS      — CLOB market data WS (best_bid / last_trade_price)
  4. MarketDiscovery — Gamma REST to find market metadata
  5. StrategyEngine — decision loop (emits typed Actions)
  6. OrderManager   — serialised order executor
  7. UserWS        — CLOB user WS for fill / cancel events
  8. GtdExpiryWatcher — detects expired GTD orders
  9. HeartbeatLoop  — independent keepalive

Usage:
  pip install -e .              # install dependencies
  cp .env.example .env          # fill in credentials
  python -m src.main            # start bot

All trading is live. DRY_RUN has been removed.
"""
from __future__ import annotations

import asyncio
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from src.config import general, binance, baseline_cfg, log_config, strategy
from src.clob_l1_auth import create_or_derive_clob_api_creds, ClobL1Error
from src.execution.clob_client import CLOBClient
from src.execution.gtd_expiry_watcher import GtdExpiryWatcher
from src.execution.heartbeat import HeartbeatLoop
from src.execution.order_manager import OrderManager
from src.feeds.binance_spot import BinanceCache, BinanceFeed, BinanceSpotService, RingBuffer
from src.infra.logging import configure_logging, get_logger
from src.market.market_service import MarketService
from src.market.models import MarketInfo
from src.market.pm_client import PolymarketClient
from src.market.window_manager import WindowManager
from src.market.market_ws import MarketWS, PriceCache
from src.market.user_ws import UserWS
from src.persistence.db import DB, init_db
from src.strategy.engine import (
    Action,
    ActionOpenT1,
    ActionOpenTN,
    ActionTakeProfit,
    StrategyEngine,
)

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Shared mutable state
# ---------------------------------------------------------------------------

class AppState:
    """Bundles all shared state for the application."""

    def __init__(self) -> None:
        self.round_start_ts: int = 0
        self.binance_cache = BinanceCache()
        self.price_cache = PriceCache()
        self.ring = RingBuffer(retention_s=binance().BUFFER_RETENTION_S)
        self.db_conn = init_db()
        self.db = DB(self.db_conn)
        self.current_market = None   # MarketInfo | None
        self.engine: StrategyEngine | None = None
        self.binance_feed: BinanceFeed | None = None
        self.market_ws: MarketWS | None = None
        self.user_ws: UserWS | None = None
        self.order_manager: OrderManager | None = None
        self.heartbeat: HeartbeatLoop | None = None
        self.gtd_expiry: GtdExpiryWatcher | None = None
        self.clob: CLOBClient | None = None  # reference for fill_callback wiring
        self.pm_client: PolymarketClient | None = None
        self.market_svc: MarketService | None = None
        self.wm: WindowManager | None = None  # window lifecycle manager
        self.binance_spot: BinanceSpotService | None = None  # BTC spot via REST
        self._shutdown = asyncio.Event()


STATE = AppState()


# ---------------------------------------------------------------------------
# Action router — bridges StrategyEngine -> OrderManager + Position state
# ---------------------------------------------------------------------------

class ActionRouter:
    """Converts StrategyEngine Actions into OrderManager calls and wires fill events."""

    def __init__(self, state: AppState) -> None:
        self._state = state
        self._pending_round: int = 0  # round_start_ts for the tier being placed

    async def __call__(self, action: Action) -> None:
        eng = self._state.engine
        om = self._state.order_manager
        if not eng or not om:
            return

        tick_size = eng._round.tick_size if eng._round else 0.01
        round_ts = self._state.round_start_ts

        if isinstance(action, ActionOpenT1):
            size = action.notional / action.price
            order = await om.place_buy(
                token_id=action.token,
                price=action.price,
                size=size,
                round_start_ts=round_ts,
                tier_name="T1",
                tick_size=tick_size,
            )
            if order:
                # Update engine state
                eng._round.position.entry_side = action.side
                eng._round.position.entry_token = action.token
                expires = order.expires_at
                eng.bind_server_order_id(
                    tier=1,
                    server_order_id=order.server_order_id or "",
                    token_id=action.token,
                    expires_at=expires,
                    expected_price=action.price,
                )
                # Wire fill callback so engine knows when order fills
                self._state.clob.fill_callback = self._make_fill_callback()
                log.info(
                    "router.t1_placed",
                    side=action.side,
                    token=action.token[:16],
                    price=action.price,
                    size=size,
                    server_id=order.server_order_id or "none",
                )

        elif isinstance(action, ActionOpenTN):
            size = action.notional / action.price
            order = await om.place_buy(
                token_id=action.token,
                price=action.price,
                size=size,
                round_start_ts=round_ts,
                tier_name=f"T{action.tier}",
                tick_size=tick_size,
            )
            if order:
                expires = order.expires_at
                eng.bind_server_order_id(
                    tier=action.tier,
                    server_order_id=order.server_order_id or "",
                    token_id=action.token,
                    expires_at=expires,
                    expected_price=action.price,
                )
                self._state.clob.fill_callback = self._make_fill_callback()
                log.info(
                    "router.tN_placed",
                    tier=action.tier,
                    token=action.token[:16],
                    price=action.price,
                    size=size,
                    server_id=order.server_order_id or "none",
                )

        elif isinstance(action, ActionTakeProfit):
            order = await om.place_sell(
                token_id=action.token,
                size=action.qty,
                price=action.price,
                round_start_ts=round_ts,
                tier_name="TP",
                tick_size=tick_size,
            )
            if order:
                log.info(
                    "router.tp_placed",
                    token=action.token[:16],
                    price=action.price,
                    qty=action.qty,
                    server_id=order.server_order_id or "none",
                )

    def _make_fill_callback(self):
        """Create a fill callback that routes to engine (used by CLOBClient polling)."""
        eng = self._state.engine
        om = self._state.order_manager

        def on_fill(
            token_id: str,
            order_id: str,
            fill_price: float,
            fill_size: float,
        ):
            if eng and om:
                om.mark_filled(order_id)
                eng.on_order_filled(token_id, order_id, fill_price, fill_size)
            log.info(
                "router.fill",
                token=token_id[:16],
                order_id=order_id,
                fill_price=fill_price,
                fill_size=fill_size,
            )
        return on_fill


# ---------------------------------------------------------------------------
# Round lifecycle
# ---------------------------------------------------------------------------

async def _on_binance_connect() -> None:
    log.debug("binance_feed.connected")


def _on_market_discovered(asset: str, mkt) -> None:
    """Called by WindowManager as soon as market info (token IDs) is resolved."""
    state = STATE

    # Detect if this is a window transition (condition_id changed)
    prev_up = getattr(state.current_market, "token_up_id", None) if state.current_market else None
    prev_cid = getattr(state.current_market, "condition_id", "") if state.current_market else ""
    is_transition = prev_cid and prev_cid != mkt.condition_id

    # Clear old token prices from cache to avoid stale data
    if state.current_market and state.price_cache:
        old_up = getattr(state.current_market, "token_up_id", None)
        old_down = getattr(state.current_market, "token_down_id", None)
        for old_t in [old_up, old_down]:
            if old_t and old_t != mkt.token_up_id and old_t != mkt.token_down_id:
                state.price_cache.clear_token(old_t)

    if state.market_ws:
        state.market_ws.subscribe_tokens([mkt.token_up_id, mkt.token_down_id])
    state.current_market = mkt

    if is_transition:
        log.info(
            "market.transition",
            asset=asset,
            slug=mkt.slug or "",
            cid=mkt.condition_id[:16],
            up=mkt.token_up_id[:16],
            down=mkt.token_down_id[:16],
            round_start_ts=mkt.round_start_ts,
            window_start=mkt.window_start.isoformat(),
            window_end=mkt.window_end.isoformat(),
            prev_cid=prev_cid[:16],
            prev_up=(prev_up[:16] if prev_up else "none"),
        )
    else:
        log.info(
            "market.discovered",
            asset=asset,
            slug=mkt.slug or "",
            cid=mkt.condition_id[:16],
            up=mkt.token_up_id[:16],
            down=mkt.token_down_id[:16],
        )


async def _on_new_round(round_start_ts: int) -> None:
    """
    Called by WindowManager on every 15-min boundary.
    Handles end-of-round cleanup; WindowManager.tick() is called separately by WM._run().
    """
    state = STATE

    # End previous round
    if state.round_start_ts > 0:
        prev = state.round_start_ts
        if state.engine:
            await state.engine.on_round_end()
        if state.order_manager:
            await state.order_manager.cancel_market(prev)
        log.info("round.transition", prev_round_start=prev, new_round_start=round_start_ts)

    state.round_start_ts = round_start_ts

    # ── Use WindowManager for market discovery ─────────────────────────────────
    # Note: market subscription (token IDs) is handled in _on_market_discovered.
    # Here we just check that market is available.
    if state.wm:
        ctx = state.wm.get("BTC")
        if not (ctx and ctx.market):
            log.warning("round.market_not_found", round_start_ts=round_start_ts)
    else:
        state.current_market = None
        log.warning("round.market_not_found.wm_unavailable", round_start_ts=round_start_ts)

    # Notify engine of new round
    if state.engine:
        await state.engine.on_new_round(round_start_ts, state.current_market)

    # Persist round start
    mkt = state.current_market
    state.db.upsert_round(
        round_start_ts=round_start_ts,
        slug=mkt.slug if mkt else None,
        condition_id=mkt.condition_id if mkt else None,
        up_token=mkt.up_token if mkt else None,
        down_token=mkt.down_token if mkt else None,
        tick_size=mkt.tick_size if mkt else 0.01,
    )

    # Wait for baseline window to close
    bcfg = baseline_cfg()
    deadline = round_start_ts + bcfg.WINDOW_S + 10
    log.info(
        "round.baseline_waiting",
        round_start_ts=round_start_ts,
        window_s=bcfg.WINDOW_S,
        deadline=deadline,
    )

    while time.time() < deadline and state.binance_feed:
        await asyncio.sleep(1)
        try:
            if state.binance_feed is None:
                break
            baseline_val, n_samples = state.binance_feed.compute_baseline(round_start_ts)
            if state.engine:
                state.engine.set_binance_baseline(baseline_val)
            log.info(
                "round.baseline_ready",
                round_start_ts=round_start_ts,
                baseline=baseline_val,
                samples=n_samples,
            )
            break
        except ValueError:
            continue



# ---------------------------------------------------------------------------
# REST midpoint poller (stable prices for strategy decisions)
# ---------------------------------------------------------------------------

async def _midpoint_poll_loop() -> None:
    """Poll /midpoint REST API every 2s to keep stable price cache updated."""
    last_condition_id = ""
    last_up_token = ""
    last_down_token = ""
    while True:
        await asyncio.sleep(2.0)
        s = STATE
        if not s.pm_client:
            continue

        wm = s.wm
        if not wm:
            continue

        ctx = wm.get("BTC")
        mkt = ctx.market if ctx else None
        if not mkt or not mkt.token_up_id or not mkt.token_down_id:
            continue

        condition_id = mkt.condition_id or ""
        up_token = mkt.token_up_id
        down_token = mkt.token_down_id

        # Log window transition when condition_id changes
        if condition_id != last_condition_id and last_condition_id:
            log.info(
                "midpoint.window_changed",
                prev_condition_id=last_condition_id[:16],
                new_condition_id=condition_id[:16],
                up_token=up_token[:16],
                down_token=down_token[:16],
                round_start_ts=mkt.round_start_ts,
            )
        last_condition_id = condition_id
        last_up_token = up_token
        last_down_token = down_token

        try:
            up_mid = await s.pm_client.get_midpoint(up_token)
            down_mid = await s.pm_client.get_midpoint(down_token)
            s.price_cache.midpoint[up_token] = up_mid
            s.price_cache.midpoint[down_token] = down_mid
            log.info(
                "midpoint.poll",
                condition_id=condition_id[:16],
                up_token=up_token[:16],
                down_token=down_token[:16],
                up_mid=up_mid,
                down_mid=down_mid,
            )
        except Exception as exc:
            log.warning("midpoint.poll error", condition_id=condition_id[:16], up_token=up_token[:16], down_token=down_token[:16], error=str(exc))


# ---------------------------------------------------------------------------
# Status printer
# ---------------------------------------------------------------------------

async def _status_loop() -> None:
    cfg = log_config()
    last_status_ts = 0.0
    while True:
        await asyncio.sleep(cfg.HEARTBEAT_S)
        s = STATE

        # Throttle status log to every 5s
        now = time.time()
        if now - last_status_ts < 5.0:
            continue
        last_status_ts = now

        bc = s.binance_cache.last_price
        gate_str = ""
        if s.engine and s.engine._gate and bc > 0:
            gs = s.engine._gate.state(bc)
            flag = "PASS" if gs.within_limit else "FAIL"
            gate_str = f"drift={gs.drift:.4f} [{flag}] |"

        wm = s.wm
        mkt = wm.get("BTC").market if wm and wm.get("BTC") else None
        up_token = mkt.up_token if mkt else ""
        down_token = mkt.down_token if mkt else ""
        up_last = s.price_cache.get_midpoint(up_token) if up_token else 0
        down_last = s.price_cache.get_midpoint(down_token) if down_token else 0

        next_in = wm.seconds_until_next() if wm else 0
        window_end = wm.next_round_start() if wm else 0
        window_end_str = datetime.fromtimestamp(window_end, tz=timezone.utc).strftime("%H:%M:%S")

        # Position state
        pos_str = ""
        if s.engine and s.engine._round and s.engine._round.position:
            pos = s.engine._round.position
            tiers_str = ",".join(
                f"T{t.tier}={'✓' if t.is_complete else ('⏳' if t.placed else '○')}"
                for t in pos.tiers
            )
            pos_str = f" | tiers=[{tiers_str}] qty={pos.total_qty:.2f} cost=${pos.total_cost:.2f}"

        log.info(
            f"STATUS  binance={bc:,.1f}  {gate_str}"
            f"  window_end={window_end_str}({next_in:.0f}s)  "
            f"UP={up_last:.4f}  DOWN={down_last:.4f}{pos_str}"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    configure_logging()
    s = STATE
    log.info("bot.starting", version="0.1.0")

    cfg_strategy = strategy()

    # ── CLOB credentials ─────────────────────────────────────────────────
    # Priority: env L2 creds > L1 private key derivation
    api_key = cfg_strategy.API_KEY
    api_secret = cfg_strategy.API_SECRET
    api_passphrase = cfg_strategy.API_PASSPHRASE

    if not (api_key and api_secret and api_passphrase):
        if cfg_strategy.WALLET_PRIVATE_KEY:
            try:
                creds = create_or_derive_clob_api_creds(
                    host=cfg_strategy.POLYMARKET_HOST,
                    private_key=cfg_strategy.WALLET_PRIVATE_KEY,
                    chain_id=137,
                )
                api_key = creds.api_key
                api_secret = creds.api_secret
                api_passphrase = creds.api_passphrase
                log.info(
                    "clob_l1_auth.derived",
                    api_key=api_key[:8],
                )
            except ClobL1Error as exc:
                log.error(
                    "clob_l1_auth.failed — trading disabled",
                    error=str(exc),
                )
        else:
            log.warning(
                "CLOB credentials missing: set API_KEY/API_SECRET/API_PASSPHRASE "
                "or WALLET_PRIVATE_KEY in .env"
            )

    # ── CLOB client ──────────────────────────────────────────────────────
    s.clob = CLOBClient(
        host=cfg_strategy.POLYMARKET_HOST,
        private_key=cfg_strategy.WALLET_PRIVATE_KEY,
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
        signature_type=cfg_strategy.SIGNATURE_TYPE,
    )

    # ── Market / Window management ────────────────────────────────────────────
    # PolymarketClient: Gamma + CLOB public API (market discovery, prices, resolution)
    s.pm_client = PolymarketClient(
        gamma_api="https://gamma-api.polymarket.com",
        polymarket_host=cfg_strategy.POLYMARKET_HOST,
    )
    s.market_svc = MarketService(
        pm=s.pm_client,
        threshold=0.5,
        slug_interval="15m",
        window_sec=900,
    )
    # BinanceSpotService: REST-based BTC spot (K-line backtrack for window-open price)
    s.binance_spot = BinanceSpotService()
    # WindowManager: unified window lifecycle (discovery + price sync + BTC drift guard)
    # WindowManager internally polls 15-min boundaries and fires _on_new_round callback.
    s.wm = WindowManager(
        market_svc=s.market_svc,
        symbols=["BTC"],
        on_new_round=_on_new_round,
        on_market_discovered=_on_market_discovered,
        binance_spot=s.binance_spot,
        entry_cfg=None,
    )

    # ── Execution layer ─────────────────────────────────────────────────
    s.order_manager = OrderManager(s.clob, price_cache=s.price_cache, pm_client=s.pm_client)
    s.heartbeat = HeartbeatLoop(s.clob)

    # ── GTD expiry watcher ──────────────────────────────────────────────
    s.gtd_expiry = GtdExpiryWatcher(
        order_manager=s.order_manager,
        round_state_provider=lambda: s.engine._round if s.engine else None,
        poll_interval_s=2.0,
    )

    # ── Strategy engine ──────────────────────────────────────────────────
    router = ActionRouter(s)
    s.engine = StrategyEngine(s.binance_cache, s.price_cache, router)

    # ── Feeds ───────────────────────────────────────────────────────────
    s.binance_feed = BinanceFeed(
        cache=s.binance_cache,
        ring=s.ring,
        on_connect=_on_binance_connect,
    )
    s.market_ws = MarketWS(
        cache=s.price_cache,
        on_connect=None,
        on_disconnect=None,
    )

    # ── User WebSocket (order events) ────────────────────────────────────
    if cfg_strategy.API_KEY and cfg_strategy.API_SECRET:
        s.user_ws = UserWS(
            api_key=cfg_strategy.API_KEY,
            api_secret=cfg_strategy.API_SECRET,
            api_passphrase=cfg_strategy.API_PASSPHRASE,
            on_maker_fill=_make_maker_fill_callback(s),
            on_order=_make_user_ws_order_callback(s),
        )
    else:
        log.warning(
            "bot.no_credentials",
            msg="API_KEY not set — UserWS disabled, fill events will not be received"
        )
        s.user_ws = None

    # ── Start services ──────────────────────────────────────────────────
    await s.binance_feed.start()
    await s.market_ws.start()
    await s.wm.start()
    s.gtd_expiry.start()

    if s.user_ws:
        await s.user_ws.start()

    if s.clob.is_configured():
        await s.heartbeat.start()
    else:
        log.error(
            "bot.no_clob_credentials",
            msg="CLOB not configured — cannot start heartbeat. "
                "Set API_KEY / API_SECRET / API_PASSPHRASE in .env",
        )

    # Status printer + midpoint poller
    status_task = asyncio.create_task(_status_loop())
    midpoint_task = asyncio.create_task(_midpoint_poll_loop())

    # ── Graceful shutdown ───────────────────────────────────────────────
    def _signal_handler() -> None:
        log.info("bot.shutdown_signal_received")
        s._shutdown.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except (NotImplementedError, OSError):
            pass

    await s._shutdown.wait()

    # ── Cleanup ─────────────────────────────────────────────────────────
    log.info("bot.shutdown_initiated")
    status_task.cancel()
    midpoint_task.cancel()

    if s.gtd_expiry:
        await s.gtd_expiry.stop()
    if s.wm:
        await s.wm.stop()
    if s.market_ws:
        await s.market_ws.stop()
    if s.user_ws:
        await s.user_ws.stop()
    if s.binance_feed:
        await s.binance_feed.stop()
    if s.heartbeat:
        await s.heartbeat.stop()
    if s.binance_spot:
        await s.binance_spot.close()
    if s.pm_client:
        await s.pm_client.close()
    s.db_conn.close()
    log.info("bot.shutdown_complete")


# ---------------------------------------------------------------------------
# User WS callbacks — route order/trade events to engine
# ---------------------------------------------------------------------------

def _make_maker_fill_callback(state: AppState):
    """
    Route each individual maker fill to engine.

    This is the primary fill path from the UserWS.  Each call corresponds to one
    maker_orders[] entry with a full order_id and its own asset_id (UP or DOWN).
    We pass the full order_id — NO prefix truncation.
    """
    eng = state.engine
    om = state.order_manager

    def on_maker_fill(fill) -> None:
        if not eng or not om:
            return

        order_id = fill.maker_order_id
        asset_id = fill.asset_id
        fill_price = fill.price
        fill_size = fill.size

        if not order_id or fill_size <= 0:
            log.debug(
                "router.maker_fill.skip",
                order_id=order_id, fill_size=fill_size,
            )
            return

        # Mark in order manager
        om.mark_filled(order_id)

        # Route to engine with authoritative asset_id
        eng.on_order_filled(asset_id, order_id, fill_price, fill_size)

        log.info(
            "router.maker_fill",
            maker_order_id=order_id,    # full id — never truncate
            asset_id=asset_id[:16],
            fill_price=fill_price,
            fill_size=fill_size,
            status=fill.status,
        )

    return on_maker_fill


def _make_user_ws_order_callback(state: AppState):
    """
    Route CLOB order events to engine.

    Handles PLACEMENT (bind server order id), UPDATE (size_matched),
    and CANCELLATION events from the UserWS order channel.
    """
    eng = state.engine

    def on_order(ev) -> None:  # ev: OrderEvent from user_ws.py
        if not eng:
            return

        order_id = ev.order_id
        evt_type = ev.event_type.upper()

        if evt_type == "CANCELLATION":
            eng.on_order_cancelled(order_id, reason="user_ws")
        elif evt_type == "UPDATE":
            # Partial fill update — size_matched changed
            # Engine tracks fills via on_order_filled separately,
            # but UPDATE gives us the authoritative size_matched for the order.
            log.debug(
                "router.order_update",
                order_id=order_id, size_matched=ev.size_matched,
            )

    return on_order


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
