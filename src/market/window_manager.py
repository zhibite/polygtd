"""
window_manager.py — Window lifecycle manager.

Core responsibilities:
1. Window discovery (discover_market)
2. Window switch detection (condition_id change → new WindowContext)
3. Price sync (MarketInfo._up_mid / _down_mid)
4. Window-level state reset (new window, position close, take-profit)

Relationship with strategy:
- Strategy holds WindowManager instance, calls tick(asset) each tick to get latest WindowContext
- Strategy reads/writes ctx.position, ctx.market, no longer directly manipulates dicts
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import Callable, Awaitable, Dict, Optional

from src.infra.logging import get_logger
from src.market.market_service import MarketService
from src.market.models import MarketInfo, Side
from src.market.window_context import WindowContext

log = get_logger(__name__)

WindowCallback = Callable[[int], Awaitable[None]]
# int = round_start_ts (Unix seconds, UTC)

WindowBaselineCallback = Callable[[str, float, str], None]
# (asset, baseline_price, source_tag) — source_tag is e.g. "window_open_kline",
# "boot_now", or "retry_now". Synchronous callback (called from tick()); the
# strategy layer should make any state mutation atomic.

WindowContextCallback = Callable[[str, "WindowContext", "MarketInfo"], Awaitable[None]]


class WindowManager:
    """
    Unified manager for all assets' window lifecycles.

    Each asset has exactly one active WindowContext at any moment.
    """

    MarketDiscoveredCallback = Callable[[str, "MarketInfo"], None]  # asset, market

    def __init__(
        self,
        market_svc: MarketService,
        symbols: list[str],
        on_new_round: WindowCallback | None = None,
        on_market_discovered: MarketDiscoveredCallback | None = None,
        binance_spot=None,
        entry_cfg=None,
        round_period_s: int = 900,
        on_window_baseline: WindowBaselineCallback | None = None,
        on_window_context_changed: WindowContextCallback | None = None,
    ):
        self._svc = market_svc
        self._symbols = symbols
        self._on_new = on_new_round  # fires on every 15-min boundary
        self._on_market = on_market_discovered  # fires when market info is ready
        self._binance_spot = binance_spot
        self._entry_cfg = entry_cfg
        self._round_period = round_period_s
        # H-V6-1 fix: synchronous callback fired with the BTC spot at each
        # window's open, exactly once per (asset, round). The strategy layer
        # uses this to seed its drift gate, replacing the previous median /
        # latest-WS trade approximation that produced a baseline several
        # seconds offset from the true window-open spot — sometimes enough
        # to make the gate FAIL right after a round transition.
        self._on_baseline = on_window_baseline
        # B-V6-2 fix: async callback fired when the active WindowContext for
        # an asset changes (window switch). The strategy layer can use this
        # to set up per-window state without coupling to the discover_market
        # internals.
        self._on_ctx_changed = on_window_context_changed
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._last_round_start: int | None = None  # detect transitions
        self._ctxs: Dict[str, WindowContext] = {
            s: WindowContext(
                asset=s,
                condition_id="",
                window_start=datetime.min.replace(tzinfo=timezone.utc),
                window_end=datetime.min.replace(tzinfo=timezone.utc),
            )
            for s in symbols
        }
        self._prev_condition_id: Dict[str, str] = {}
        self._first_seen: Dict[str, bool] = {s: False for s in symbols}

    # ── Lifecycle (15-min boundary polling) ─────────────────────────────────────

    async def start(self) -> None:
        """Start the 15-min boundary polling loop."""
        self._running = True
        self._last_round_start = self._round_start_of(time.time())
        log.info("window_mgr.started",
                 symbols=self._symbols,
                 period_s=self._round_period,
                 current_round=self._last_round_start)
        self._task = asyncio.create_task(self._run())
        # H-V6-6: surface any silent exception that kills _run() (the boundary
        # poll loop has no top-level try/except and asyncio would otherwise
        # swallow task exceptions quietly). When this fires, the engine never
        # receives `_on_new()` again and stays stuck on the boot round.
        self._task.add_done_callback(self._on_run_done)

    def _on_run_done(self, task: asyncio.Task) -> None:
        if task.cancelled():
            log.warning("window_mgr._run cancelled")
            return
        exc = task.exception()
        if exc is not None:
            log.error(
                "window_mgr._run crashed",
                error=str(exc),
                exc_type=type(exc).__name__,
            )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("window_mgr.stopped")

    def _round_start_of(self, ts: float | int) -> int:
        """Floor ts to nearest `round_period_s` boundary."""
        return int(ts // self._round_period) * self._round_period

    def next_round_start(self) -> int:
        return self._round_start_of(time.time()) + self._round_period

    def seconds_until_next(self) -> float:
        return self.next_round_start() - time.time()

    async def _run(self) -> None:
        """
        Drive window round boundaries.

        15-min round starts are deterministic arithmetic:
            round_start_ts = floor(unix_time / 900) * 900

        So we don't need to *detect* the boundary — we just sleep until the
        next one and switch.

        Boot step: resolve the current round, fetch its market (for token
        IDs), and fire `_on_new` so engine / strategy state is seeded.

        Steady state: sleep until 0.5 s after the next boundary, advance
        round_start_ts by one period, repeat.

        Every step is wrapped in try/except so a transient Gamma / network
        blip cannot kill the boundary loop (otherwise one bad fetch would
        leave us stuck on the boot round forever, exactly the bug from
        2026-07-11 00:30 where `window_mgr.round_transition` never logged).
        """
        # ── Boot: resolve current round, fetch its market, fire _on_new ────────
        self._last_round_start = self._round_start_of(time.time())
        log.info(
            "window_mgr.boot",
            current_round=self._last_round_start,
            period_s=self._round_period,
        )
        await self._on_round_start(self._last_round_start)

        # ── Steady state: deterministic boundary switching ─────────────────────
        while self._running:
            next_boundary = self._last_round_start + self._round_period
            secs_to_boundary = next_boundary - time.time()
            # Sleep until 0.5 s after the boundary so we're past any race
            # with the 15-min clock on the server.
            sleep_for = max(0.0, secs_to_boundary + 0.5)
            try:
                await asyncio.sleep(sleep_for)
            except asyncio.CancelledError:
                raise
            if not self._running:
                return

            prev = self._last_round_start
            self._last_round_start = next_boundary
            log.info(
                "window_mgr.round_transition",
                prev_start=prev,
                new_start=self._last_round_start,
            )
            await self._on_round_start(self._last_round_start)

    async def _on_round_start(self, round_start_ts: int) -> None:
        """
        Single round-start handler: refresh market for the new round and
        fire `_on_new` exactly once. Exceptions are logged and swallowed so
        a transient failure (Gamma 5xx, network timeout) cannot kill the
        boundary loop. The next boundary will retry.
        """
        try:
            await self.tick(self._symbols, force_discovery=True)
        except Exception:
            log.exception(
                "window_mgr.tick failed",
                round_start_ts=round_start_ts,
            )

        if self._on_new:
            try:
                await self._on_new(round_start_ts)
            except Exception:
                log.exception(
                    "window_mgr.on_new_round failed",
                    round_start_ts=round_start_ts,
                )

    # ── Public API ──────────────────────────────────────────────────────────────

    def get(self, asset: str) -> Optional[WindowContext]:
        """Get current window context for an asset. Returns None only if asset is unknown."""
        return self._ctxs.get(asset)

    async def tick(self, assets: list[str], force_discovery: bool = False) -> Dict[str, WindowContext]:
        """
        Called every tick by main loop:
        1. discover_market only when needed: first time, round transition, or force flag.
           Once a market is discovered for the current window, the up/down token IDs
           are cached on WindowContext and reused — no further discovery calls until
           the next 15-min window boundary.
        2. Detect window switches, reset state
        3. Fetch latest prices
        Returns {asset: ctx} dict (only assets with active windows).
        """
        now_ts = time.time()
        now_start = self._round_start_of(now_ts)
        # _run() keeps _last_round_start fresh before calling tick(), so we just read it.
        round_changed = (now_start != self._last_round_start)

        # Discover only when: first run, round just ticked over, or explicit force.
        needs_discovery = [
            a for a in assets
            if force_discovery
            or round_changed
            or not self._first_seen.get(a, False)
        ]
        if not needs_discovery:
            log.debug(
                "window_mgr.tick skipped discover",
                assets=list(assets),
                round_changed=round_changed,
                now_start=now_start,
                last_round_start=self._last_round_start,
                first_seen=dict(self._first_seen),
            )
        if needs_discovery:
            tasks = [self._svc.discover_market(a) for a in needs_discovery]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                asset = needs_discovery[i]
                self._first_seen[asset] = True

                if isinstance(result, BaseException):
                    log.warning("discover_market failed", asset=asset, error=str(result))
                    continue

                if result is None:
                    log.debug("discover_market returned None (no active window)", asset=asset)
                    continue

                mkt = result  # MarketInfo
                ctx = self._ctxs[asset]

                # Always update ctx.market on discovery (refreshes token IDs, prices, etc.)
                ctx.market = mkt

                # Detect window switch by condition_id change (more reliable than window_end)
                cid_changed = ctx.condition_id and ctx.condition_id != mkt.condition_id

                # Snapshot prev window BTC close for cross-window drift guard
                prev_close: Optional[float] = None
                if cid_changed and ctx.market is not None:
                    try:
                        prev_close = float(getattr(ctx.market, "btc_spot_now", 0.0) or 0.0)
                        if prev_close > 0:
                            ctx.prev_window_btc_close = prev_close
                    except Exception:
                        pass

                if cid_changed:
                    log.info(
                        "window_mgr.window_switch_detected",
                        asset=asset,
                        prev_cid=ctx.condition_id[:12],
                        new_cid=mkt.condition_id[:12],
                        up_token=mkt.token_up_id[:16],
                        down_token=mkt.token_down_id[:16],
                    )
                    await self._on_new_window(ctx, mkt)

                # Notify consumer whenever we have a valid market
                # (first discovery, new window, or even re-confirm of current window)
                if self._on_market:
                    self._on_market(asset, mkt)

                # Record BTC spot at window open (use K-line for backtrack)
                if self._binance_spot is not None:
                    btc_price = await self._binance_spot.fetch_at_window_open(
                        window_start=mkt.window_start,
                    )
                    if btc_price is not None and btc_price > 0:
                        mkt.btc_spot_at_window_open = btc_price
                        age_sec = (
                            datetime.now(timezone.utc) - mkt.window_start
                        ).total_seconds()
                        log.info(
                            "window_mgr BTC spot at window_open",
                            asset=asset, btc_price=btc_price,
                            window_start=mkt.window_start.isoformat(), age_sec=age_sec,
                        )
                        # H-V6-1 fix: hand the window-open BTC spot to the
                        # strategy layer exactly once per (asset, round).
                        # Previously the gate baseline was seeded lazily via
                        # binance_ws.last_price inside _on_new_round, which
                        # is sampled at the moment WindowManager's poll loop
                        # detected the boundary (1-15s late) — not at the
                        # window boundary itself. That offset, combined with
                        # a 0.15% drift threshold on a 64k BTC price, made
                        # the gate FAIL every time BTC moved more than ~96
                        # USD between the boundary and the lazy seed. Now we
                        # fire as soon as the K-line value is backfilled.
                        if self._on_baseline is not None:
                            try:
                                self._on_baseline(asset, btc_price, "window_open_kline")
                            except Exception as exc:
                                log.warning(
                                    "window_mgr.baseline_callback_failed",
                                    asset=asset, error=str(exc),
                                )
                    else:
                        log.warning(
                            "window_mgr failed to get BTC spot at window_open",
                            asset=asset, window_start=mkt.window_start.isoformat(),
                        )

                # Cross-window BTC drift guard
                drift_pct_cfg = float(
                    getattr(self._entry_cfg, "cross_window_drift_skip_pct", 0.5)
                )
                if (
                    drift_pct_cfg > 0
                    and prev_close is not None
                    and prev_close > 0
                    and mkt.btc_spot_at_window_open is not None
                    and mkt.btc_spot_at_window_open > 0
                ):
                    drift_pct = abs(
                        mkt.btc_spot_at_window_open - prev_close
                    ) / prev_close * 100.0
                    if drift_pct > drift_pct_cfg + 1e-9:
                        ctx.skip_until_next_window = True
                        log.warning(
                            "window_mgr cross-window BTC drift > threshold → SKIP new window",
                            asset=asset, drift_pct=drift_pct,
                            drift_pct_cfg=drift_pct_cfg,
                            prev_close=prev_close,
                            new_open=mkt.btc_spot_at_window_open,
                        )
                    else:
                        log.info(
                            "window_mgr cross-window BTC drift ≤ threshold → normal trading",
                            asset=asset, drift_pct=drift_pct,
                            drift_pct_cfg=drift_pct_cfg,
                            prev_close=prev_close,
                            new_open=mkt.btc_spot_at_window_open,
                        )
                else:
                    reasons = []
                    if prev_close is None or prev_close <= 0:
                        reasons.append("prev_close unavailable")
                    if (
                        mkt.btc_spot_at_window_open is None
                        or mkt.btc_spot_at_window_open <= 0
                    ):
                        reasons.append("btc_spot_at_window_open unavailable")
                    if drift_pct_cfg <= 0:
                        reasons.append("cfg disabled")
                    log.warning(
                        "window_mgr cross-window BTC drift check skipped",
                        asset=asset, reasons="; ".join(reasons),
                    )
                log.info(
                    "window_mgr discovered",
                    asset=asset, cid=mkt.condition_id[:12], end=mkt.window_end,
                )

        # Fetch prices for all assets with a valid market
        active_assets = [a for a in assets if self._ctxs[a].market is not None]
        if active_assets:
            price_tasks = {a: self._svc.fetch_prices(self._ctxs[a].market) for a in active_assets}
            price_results = await asyncio.gather(*price_tasks.values(), return_exceptions=True)
            for asset, price_result in zip(price_tasks.keys(), price_results):
                if isinstance(price_result, BaseException):
                    log.warning("fetch_prices failed", asset=asset, error=str(price_result))
                    ctx = self._ctxs[asset]
                    if ctx.market:
                        ctx.market._up_mid = 0.5
                        ctx.market._down_mid = 0.5
                    continue
                up_mid, down_mid = price_result
                ctx = self._ctxs[asset]
                if ctx.market:
                    ctx.market._up_mid = up_mid
                    ctx.market._down_mid = down_mid

        # Sync BTC spot price for offset checks
        if self._binance_spot is not None and active_assets:
            btc_now = await self._binance_spot.fetch_price()
            if btc_now is not None and btc_now > 0:
                for asset in active_assets:
                    ctx = self._ctxs[asset]
                    if ctx.market:
                        ctx.market.btc_spot_now = btc_now

        return {a: self._ctxs[a] for a in assets if self._ctxs[a].market is not None}

    def on_position_opened(self, asset: str, side: Side) -> None:
        """Called after successful entry: mark side as traded."""
        ctx = self.get(asset)
        if ctx:
            ctx.mark_traded(side)
            ctx.reset_hot_restart()

    def on_take_profit(
        self,
        asset: str,
        side: Side,
        fills: int,
        lock_side: bool = True,
        lock_whole_window: bool = False,
    ) -> None:
        """Called after take-profit: record fills, activate hot-restart."""
        ctx = self.get(asset)
        if ctx is None:
            return
        ctx.on_take_profit(side)
        ctx.hot_exit_fills = fills
        if lock_side:
            ctx.lock_side_after_take_profit(side)
        if lock_whole_window:
            ctx.no_new_order_locked = True

    def on_position_closed(self, asset: str) -> None:
        """Called after position close (any reason): clear position reference."""
        ctx = self.get(asset)
        if ctx:
            pos = ctx.position
            ctx.position = None
            if pos is not None:
                try:
                    pos._pending_close = False
                except Exception:
                    pass

    # ── Internal methods ───────────────────────────────────────────────────────

    async def _on_new_window(self, ctx: WindowContext, mkt: MarketInfo) -> None:
        """
        Reset all window-level state on window switch.
        Note: position state is managed by strategy layer separately, not cleared here.
        """
        prev_id = ctx.condition_id

        ctx.condition_id = mkt.condition_id
        ctx.window_start = mkt.window_start
        ctx.window_end = mkt.window_end
        ctx.market = mkt
        ctx.reset_for_new_window()

        log.info(
            "window_mgr.new_window",
            asset=ctx.asset,
            slug=mkt.slug or "",
            prev_cid=(prev_id[:12] if prev_id else "none"),
            new_cid=mkt.condition_id[:12],
            round_start_ts=mkt.round_start_ts,
            window_start=mkt.window_start.isoformat(),
            window_end=mkt.window_end.isoformat(),
            up_token=mkt.token_up_id[:16],
            down_token=mkt.token_down_id[:16],
            window_open_up=mkt.window_open_up,
            window_open_down=mkt.window_open_down,
        )

        # Boot-time window age guard: skip if window already open > grace_sec
        first_seen = self._first_seen.get(ctx.asset, False)
        if not first_seen:
            self._first_seen[ctx.asset] = True
            if self._entry_cfg is not None and getattr(
                self._entry_cfg, "skip_if_late_window_at_boot", True
            ):
                grace_sec = float(getattr(self._entry_cfg, "entry_open_offset_grace_sec", 180.0))
                age_sec = ctx.elapsed()
                if age_sec > grace_sec:
                    ctx.skip_until_next_window = True
                    log.warning(
                        "window_mgr boot-time window is late → SKIP this window",
                        asset=ctx.asset, age_sec=age_sec, grace_sec=grace_sec,
                    )
                else:
                    log.info(
                        "window_mgr boot-time window within grace",
                        asset=ctx.asset, age_sec=age_sec, grace_sec=grace_sec,
                    )

        # B-V6-2 fix: notify subscribers that the active window context just
        # changed. Subscribers (strategy) can use this to seed per-window
        # state without having to walk discover_market internals.
        if self._on_ctx_changed is not None:
            try:
                await self._on_ctx_changed(ctx.asset, ctx, mkt)
            except Exception as exc:
                log.warning(
                    "window_mgr.ctx_changed_callback_failed",
                    asset=ctx.asset, error=str(exc),
                )
