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


class WindowManager:
    """
    Unified manager for all assets' window lifecycles.

    Each asset has exactly one active WindowContext at any moment.
    """

    def __init__(
        self,
        market_svc: MarketService,
        symbols: list[str],
        on_new_round: WindowCallback | None = None,
        binance_spot=None,
        entry_cfg=None,
        round_period_s: int = 900,
    ):
        self._svc = market_svc
        self._symbols = symbols
        self._on_new = on_new_round  # fires on every 15-min boundary
        self._binance_spot = binance_spot
        self._entry_cfg = entry_cfg
        self._round_period = round_period_s
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

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("window_mgr.stopped")

    @staticmethod
    def _round_start_of(ts: float | int) -> int:
        """Floor ts to nearest 15-min boundary."""
        return int(ts // 900) * 900

    def next_round_start(self) -> int:
        return self._round_start_of(time.time()) + self._round_period

    def seconds_until_next(self) -> float:
        return self.next_round_start() - time.time()

    async def _run(self) -> None:
        """Poll 15-min boundaries every 0.5s; on each transition, run tick()."""
        while self._running:
            await asyncio.sleep(0.5)
            now_start = self._round_start_of(time.time())
            if now_start != self._last_round_start:
                prev = self._last_round_start
                self._last_round_start = now_start
                log.info("window_mgr.round_transition",
                         prev_start=prev,
                         new_start=now_start,
                         next_start=self.next_round_start())
                try:
                    await self.tick(self._symbols)
                except Exception:
                    log.exception("window_mgr.tick raised")
                if self._on_new:
                    await self._on_new(now_start)

    # ── Public API ──────────────────────────────────────────────────────────────

    def get(self, asset: str) -> Optional[WindowContext]:
        """Get current window context for an asset (None if no active window)."""
        ctx = self._ctxs.get(asset)
        if ctx is None or ctx.condition_id == "":
            return None
        return ctx

    async def tick(self, assets: list[str]) -> Dict[str, WindowContext]:
        """
        Called every tick by main loop:
        1. discover_market all assets
        2. Detect window switches, reset state
        3. Fetch latest prices
        Returns {asset: ctx} dict (only assets with active windows)
        """
        tasks = [self._svc.discover_market(a) for a in assets]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            asset = assets[i]
            ctx = self._ctxs[asset]

            if isinstance(result, BaseException):
                log.warning("discover_market {} failed: {}", asset, result)
                continue

            if result is None:
                log.debug("discover_market {} returned None (no active window)", asset)
                continue

            mkt = result  # MarketInfo

            # Detect new window: window_end increases = new window online
            prev_end = ctx.window_end if ctx.condition_id else None
            is_new = (prev_end is None) or (mkt.window_end > prev_end)

            if is_new:
                # Save prev window BTC close price before reset
                prev_close: Optional[float] = None
                if prev_end is not None and ctx.market is not None:
                    try:
                        prev_close = float(
                            getattr(ctx.market, "btc_spot_now", 0.0) or 0.0
                        )
                        if prev_close > 0:
                            ctx.prev_window_btc_close = prev_close
                    except Exception:
                        pass

                # New window: allocate new WindowContext (or reset existing)
                await self._on_new_window(ctx, mkt)

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
                            "window_mgr {} BTC spot at window_open = {} "
                            "(window_start={}, age={:.1f}s)",
                            asset, btc_price, mkt.window_start.isoformat(), age_sec,
                        )
                    else:
                        log.warning(
                            "window_mgr {} failed to get BTC spot at window_open "
                            "(window_start={}) — entry_open_offset check will be stale",
                            asset, mkt.window_start.isoformat(),
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
                            "window_mgr {} cross-window BTC drift {:.3f}% > "
                            "{:.3f}% → SKIP new window (prev_close={} new_open={})",
                            asset, drift_pct, drift_pct_cfg,
                            prev_close, mkt.btc_spot_at_window_open,
                        )
                    else:
                        log.info(
                            "window_mgr {} cross-window BTC drift {:.3f}% ≤ "
                            "{:.3f}% → normal trading enabled (prev_close={} new_open={})",
                            asset, drift_pct, drift_pct_cfg,
                            prev_close, mkt.btc_spot_at_window_open,
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
                        "window_mgr {} cross-window BTC drift check skipped: "
                        "{} (continuing without skip)",
                        asset, "; ".join(reasons),
                    )
                log.info(
                    "window_mgr {} discovered: cid={} end={}",
                    asset, mkt.condition_id[:12], mkt.window_end,
                )

        # Fetch prices for all active windows in parallel
        active_assets = [a for a in assets if self.get(a) is not None]
        if active_assets:
            price_tasks = {a: self._svc.fetch_prices(self._ctxs[a].market) for a in active_assets}
            price_results = await asyncio.gather(*price_tasks.values(), return_exceptions=True)
            for asset, price_result in zip(price_tasks.keys(), price_results):
                if isinstance(price_result, BaseException):
                    log.warning("fetch_prices {} failed: {}", asset, price_result)
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

        return {a: self._ctxs[a] for a in active_assets if self.get(a) is not None}

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

        if prev_id and prev_id != mkt.condition_id:
            log.info(
                "window_mgr {} window changed: {} -> {}",
                ctx.asset, prev_id[:12], mkt.condition_id[:12],
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
                        "window_mgr {} boot-time window is late (elapsed={:.1f}s > grace={:.1f}s) "
                        "→ SKIP this window, wait for next one",
                        ctx.asset, age_sec, grace_sec,
                    )
                else:
                    log.info(
                        "window_mgr {} boot-time window age={:.1f}s ≤ grace={:.1f}s → "
                        "normal trading enabled",
                        ctx.asset, age_sec, grace_sec,
                    )
