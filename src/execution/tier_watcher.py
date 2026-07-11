"""TierWatcher — single source of truth for an in-flight GTD order.

Refactor (H-NEW-2026-07-11): replaces the previous "CLOBClient poll loop
+ UserWS fill events + GtdExpiryWatcher sweep" triad with ONE background
task per placed order. The watcher:

  1. Polls the CLOB token balance (ground truth from the exchange) every
     `poll_s` seconds.
  2. Decides the order's terminal state from the balance alone:
       - balance >= expected_size * fill_ratio → cancel GTD, mark complete
       - balance >= expected_size * (1 + tiny) → over-fill (shouldn't
         happen, but we cancel the residual to lock in cost basis)
       - GTD expired server-side → mark expired (cancel is a no-op)
       - mid price has crossed into the next tier's trigger zone →
         cancel GTD so the next tier can fire cleanly
  3. Pushes the terminal state back into Position / TierSnapshot via
     callbacks so the engine can advance.

Why balance instead of fill callbacks:
  - The CLOB is the only authoritative source for "how many shares do I
    actually hold for this token". Fill events from WS can be dropped
    (size=0 quirks), aggregated late (UPDATE-style events), or arrive
    after a restart. Reading balance once per poll is deterministic.
  - We do not need to react to FAK sells here — TPs use a separate
    watcher (TierWatcher with side='SELL').

Lifecycle:
  - spawn from ActionRouter after `place_buy_by_strategy_price` returns
  - run until a terminal state is reached or the bot shuts down
  - one watcher per tier per round — only the most recent watcher for a
    given (round, tier) is retained; stale watchers self-cancel
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable, Optional

from src.execution.clob_client import CLOBClient
from src.infra.logging import get_logger

log = get_logger(__name__)


class TierOutcome(str, Enum):
    """Terminal states a TierWatcher can land in."""

    FILLED = "filled"             # balance hit expected_size * fill_ratio
    EXPIRED = "expired"           # GTD lifetime crossed, no fill
    CROSSED = "crossed"           # mid dropped into next tier zone, cancel
    CANCELLED = "cancelled"       # manual or external cancel
    OVER_FILLED = "over_filled"   # balance > expected_size (defensive)
    ERROR = "error"               # repeated CLOB errors


@dataclass
class WatcherResult:
    """Result emitted when a TierWatcher terminates."""

    outcome: TierOutcome
    server_order_id: str
    token_id: str
    side: str                       # "BUY" or "SELL"
    strategy_price: float           # the limit price we posted
    expected_size: float            # the notional / strategy_price
    filled_qty: float               # final balance at termination (shares)
    cancel_sent: bool               # True if we issued cancel_order
    error: str = ""
    # H-NEW-2026-07-11: real average fill price as observed by the CLOB.
    # Used by `apply_tier_watcher_result` as the AUTHORITATIVE price when
    # the watcher is the first to report a fill — instead of falling back
    # to `notional / size_expected` (which is the strategy price, not the
    # actual fill price and can diverge by a few bp under thin books /
    # crossing flows).
    avg_fill_price: float = 0.0
    # H-NEW-2026-07-11: explicit dedup key. Downstream subscribers use it
    # to ensure the same terminal state is not applied twice to position
    # totals.
    dedup_key: str = ""


# Callback signatures the ActionRouter / engine wire up.
OnComplete = Callable[[WatcherResult], Awaitable[None]]
OnCrossTier = Callable[[WatcherResult], Awaitable[None]]


class TierWatcher:
    """Watch a single GTD order's progress via token-balance polling.

    Args:
        clob:                  CLOBClient (for get_token_balance, cancel_order)
        server_order_id:       server-side order id from the post-response
        token_id:              UP or DOWN token
        side:                  "BUY" or "SELL"
        strategy_price:        the exact limit price we posted (no adjustment)
        expected_size:         shares the GTD was sized for (= notional / price)
        expires_at:            unix ts when the GTD self-cancels server-side
        poll_s:                poll interval (default from config)
        fill_ratio:            fraction of expected_size at which we declare
                               the tier "complete enough" and cancel the GTD
                               (default from config)
        cross_threshold:       optional (mid_price) — if set and current
                               midpoint drops at or below this level,
                               cancel the GTD early (e.g. when price has
                               dropped into the next tier's trigger zone)
        mid_price_provider:    async callable returning current mid for
                               `token_id`; required when cross_threshold is set
        on_complete:           async callback invoked once on terminal state
        is_still_active:       callable returning False if this watcher
                               should self-terminate (e.g. round rolled over)
    """

    def __init__(
        self,
        clob: CLOBClient,
        server_order_id: str,
        token_id: str,
        side: str,
        strategy_price: float,
        expected_size: float,
        expires_at: float,
        poll_s: float = 1.5,
        fill_ratio: float = 0.995,
        cross_threshold: Optional[float] = None,
        mid_price_provider: Optional[Callable[[], Awaitable[float]]] = None,
        on_complete: Optional[OnComplete] = None,
        is_still_active: Optional[Callable[[], bool]] = None,
    ) -> None:
        self._clob = clob
        self._server_order_id = server_order_id
        self._token_id = token_id
        self._side = side
        self._strategy_price = strategy_price
        self._expected_size = expected_size
        self._expires_at = float(expires_at)
        self._poll_s = max(0.5, float(poll_s))
        self._fill_ratio = float(fill_ratio)
        self._cross_threshold = cross_threshold
        self._mid_provider = mid_price_provider
        self._on_complete = on_complete
        self._is_active = is_still_active or (lambda: True)
        self._task: Optional[asyncio.Task[None]] = None
        self._result: Optional[WatcherResult] = None
        self._stopped = False
        # Idempotency guard: `_fire_complete` may be invoked more than once
        # if the outer `_run` re-enters after an exception, or if a future
        # change wires additional callbacks. Without this guard the
        # downstream `apply_tier_watcher_result` would double-count (B-NEW-3).
        self._fire_complete_called = False

    def start(self) -> asyncio.Task[None]:
        """Spawn the background task. Idempotent."""
        if self._task is not None and not self._task.done():
            return self._task
        self._stopped = False
        self._task = asyncio.create_task(self._run(), name=f"tier_watcher_{self._side}")
        return self._task

    async def stop(self) -> Optional[WatcherResult]:
        """Cancel the watcher; safe to call multiple times."""
        self._stopped = True
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        return self._result

    @property
    def result(self) -> Optional[WatcherResult]:
        return self._result

    async def _run(self) -> None:
        """Main loop. One watcher = one GTD = one terminal state."""
        try:
            await self._watch()
        except asyncio.CancelledError:
            # External cancellation — leave _result unset if not yet decided.
            raise
        except Exception as exc:
            log.exception(
                "tier_watcher.crashed",
                server_id=self._server_order_id[:16],
                error=str(exc),
            )
            self._result = WatcherResult(
                outcome=TierOutcome.ERROR,
                server_order_id=self._server_order_id,
                token_id=self._token_id,
                side=self._side,
                strategy_price=self._strategy_price,
                expected_size=self._expected_size,
                filled_qty=0.0,
                cancel_sent=False,
                error=str(exc),
            )
            await self._fire_complete(self._result)

    async def _watch(self) -> None:
        """Poll until a terminal state is reached.

        H-NEW-2026-07-11 (fill-channel refactor): this watcher is now the
        single source of truth for a GTD BUY's fill progress. We first try
        a one-shot REST read of `size_matched` + `avg_price` so any fill
        that already landed during place_gtd_buy is detected within ~100 ms
        of spawn. Only if that returns size_matched=0 do we fall through to
        the balance-poll loop.
        """
        next_log_ts = 0.0
        prev_balance = 0.0

        # Fast path: detect fills that completed BEFORE the watcher
        # spawned (very common — fills that crossed during place_gtd_buy).
        fast_state = await self._read_fill_state()
        fast_size = fast_state.get("size_matched", 0.0) if fast_state else 0.0
        fast_avg = fast_state.get("avg_price", 0.0) if fast_state else 0.0
        if fast_size and fast_size > 0:
            cancel_sent = await self._safe_cancel()
            self._result = WatcherResult(
                outcome=TierOutcome.FILLED,
                server_order_id=self._server_order_id,
                token_id=self._token_id,
                side=self._side,
                strategy_price=self._strategy_price,
                expected_size=self._expected_size,
                filled_qty=fast_size,
                cancel_sent=cancel_sent,
                avg_fill_price=fast_avg if fast_avg > 0 else self._strategy_price,
                dedup_key=self._dedup_key(TierOutcome.FILLED),
            )
            log.info(
                "tier_watcher.fast_fill_on_spawn",
                server_id=self._server_order_id[:16],
                side=self._side,
                size_matched=fast_size,
                avg_price=fast_avg,
                expected=self._expected_size,
                cancel_sent=cancel_sent,
            )
            await self._fire_complete(self._result)
            return

        while True:
            if self._stopped or not self._is_active():
                # Caller wants us gone — leave _result unset so the router
                # knows the watcher did not reach a terminal state.
                log.info(
                    "tier_watcher.aborted_external",
                    server_id=self._server_order_id[:16],
                    side=self._side,
                )
                return

            # 1) Read the ground-truth balance.
            balance = await self._read_balance()
            if balance is None:
                # Transient error — back off, retry on next tick.
                await asyncio.sleep(self._poll_s)
                continue

            # 2) Over-fill safety net (defensive — should not happen).
            if balance > self._expected_size * 1.01 and self._side == "BUY":
                log.warning(
                    "tier_watcher.over_filled",
                    server_id=self._server_order_id[:16],
                    side=self._side,
                    balance=balance,
                    expected=self._expected_size,
                )
                cancel_sent = await self._safe_cancel()
                self._result = WatcherResult(
                    outcome=TierOutcome.OVER_FILLED,
                    server_order_id=self._server_order_id,
                    token_id=self._token_id,
                    side=self._side,
                    strategy_price=self._strategy_price,
                    expected_size=self._expected_size,
                    filled_qty=balance,
                    cancel_sent=cancel_sent,
                    avg_fill_price=self._strategy_price,
                    dedup_key=self._dedup_key(TierOutcome.OVER_FILLED),
                )
                await self._fire_complete(self._result)
                return

            # 3) Filled enough — cancel the residual GTD and exit.
            if balance >= self._expected_size * self._fill_ratio:
                cancel_sent = await self._safe_cancel()
                self._result = WatcherResult(
                    outcome=TierOutcome.FILLED,
                    server_order_id=self._server_order_id,
                    token_id=self._token_id,
                    side=self._side,
                    strategy_price=self._strategy_price,
                    expected_size=self._expected_size,
                    filled_qty=balance,
                    cancel_sent=cancel_sent,
                    avg_fill_price=self._strategy_price,
                    dedup_key=self._dedup_key(TierOutcome.FILLED),
                )
                log.info(
                    "tier_watcher.filled",
                    server_id=self._server_order_id[:16],
                    side=self._side,
                    balance=balance,
                    expected=self._expected_size,
                    cancel_sent=cancel_sent,
                )
                await self._fire_complete(self._result)
                return

            # 4) Cross into next tier zone — cancel so the next tier can
            #    fire without waiting on this GTD's expiry.
            if (
                self._cross_threshold is not None
                and self._mid_provider is not None
            ):
                cur_mid = await self._safe_mid()
                if cur_mid is not None and cur_mid <= self._cross_threshold:
                    cancel_sent = await self._safe_cancel()
                    self._result = WatcherResult(
                        outcome=TierOutcome.CROSSED,
                        server_order_id=self._server_order_id,
                        token_id=self._token_id,
                        side=self._side,
                        strategy_price=self._strategy_price,
                        expected_size=self._expected_size,
                        filled_qty=balance,
                        cancel_sent=cancel_sent,
                        avg_fill_price=self._strategy_price,
                        dedup_key=self._dedup_key(TierOutcome.CROSSED),
                    )
                    log.info(
                        "tier_watcher.crossed_into_next_tier",
                        server_id=self._server_order_id[:16],
                        side=self._side,
                        balance=balance,
                        cur_mid=cur_mid,
                        cross_threshold=self._cross_threshold,
                        cancel_sent=cancel_sent,
                    )
                    await self._fire_complete(self._result)
                    return

            # 5) GTD expired server-side (or about to).
            now = time.time()
            if now >= self._expires_at:
                # Cancel is a no-op (already self-cancelled) but we call it
                # anyway to be defensive against client/server clock skew.
                cancel_sent = await self._safe_cancel()
                self._result = WatcherResult(
                    outcome=TierOutcome.EXPIRED,
                    server_order_id=self._server_order_id,
                    token_id=self._token_id,
                    side=self._side,
                    strategy_price=self._strategy_price,
                    expected_size=self._expected_size,
                    filled_qty=balance,
                    cancel_sent=cancel_sent,
                    avg_fill_price=self._strategy_price,
                    dedup_key=self._dedup_key(TierOutcome.EXPIRED),
                )
                log.info(
                    "tier_watcher.expired",
                    server_id=self._server_order_id[:16],
                    side=self._side,
                    balance=balance,
                    expected=self._expected_size,
                    lifetime_s=self._expires_at - (now - (self._expires_at - 0)),
                )
                await self._fire_complete(self._result)
                return

            # 6) Periodic progress log (~10s) so operators see the GTD is
            #    alive without flooding the log on every poll.
            if now - next_log_ts >= 10.0:
                next_log_ts = now
                log.info(
                    "tier_watcher.poll_progress",
                    server_id=self._server_order_id[:16],
                    side=self._side,
                    balance=balance,
                    expected=self._expected_size,
                    fill_pct=(balance / self._expected_size * 100.0) if self._expected_size > 0 else 0.0,
                    remaining_s=max(0.0, self._expires_at - now),
                )

            prev_balance = balance
            await asyncio.sleep(self._poll_s)

    def _dedup_key(self, outcome: TierOutcome) -> str:
        """Stable key for downstream subscribers to dedup terminal states.

        ``<server_order_id>|<outcome>`` — same key regardless of which
        fast-path / balance-path branch fires first.
        """
        return f"{self._server_order_id}|{outcome.value}"

    async def _read_balance(self) -> Optional[float]:
        try:
            return await self._clob.get_token_balance(self._token_id)
        except Exception as exc:
            log.debug(
                "tier_watcher.balance_error",
                server_id=self._server_order_id[:16],
                error=str(exc),
            )
            return None

    async def _read_size_matched(self) -> Optional[float]:
        """Fast path: ask CLOB for this order's authoritative size_matched.

        This is the post that landed during place_gtd_buy — checking it
        first means a fill that crossed during the post gets reported
        within ~100 ms of the watcher spawning, instead of waiting for the
        first balance poll to tick.

        Kept for callers that only care about the size; the watcher itself
        uses `_read_fill_state` so it can also capture avg_price.
        """
        if not self._server_order_id:
            return None
        try:
            return await self._clob.get_order_filled_size(self._server_order_id)
        except Exception as exc:
            log.debug(
                "tier_watcher.size_matched_error",
                server_id=self._server_order_id[:16],
                error=str(exc),
            )
            return None

    async def _read_fill_state(self) -> Optional[dict]:
        if not self._server_order_id:
            return None
        try:
            return await self._clob.get_order_fill_state(self._server_order_id)
        except Exception as exc:
            log.debug(
                "tier_watcher.fill_state_error",
                server_id=self._server_order_id[:16],
                error=str(exc),
            )
            return None

    async def _safe_mid(self) -> Optional[float]:
        if self._mid_provider is None:
            return None
        try:
            return float(await self._mid_provider())
        except Exception as exc:
            log.debug(
                "tier_watcher.mid_error",
                server_id=self._server_order_id[:16],
                error=str(exc),
            )
            return None

    async def _safe_cancel(self) -> bool:
        """Best-effort cancel. Logs but does not raise."""
        try:
            ok = await self._clob.cancel_order(self._server_order_id)
            return bool(ok)
        except Exception as exc:
            log.debug(
                "tier_watcher.cancel_error",
                server_id=self._server_order_id[:16],
                error=str(exc),
            )
            return False

    async def _fire_complete(self, result: WatcherResult) -> None:
        # H-NEW-2026-07-11: idempotent. A watcher can reach its terminal
        # state via multiple code paths (the fast fill path on spawn, the
        # normal balance path, an exception in _run → _fire_complete(error)).
        # Without this guard, calling on_complete twice would feed
        # `apply_tier_watcher_result` the same filled_qty twice.
        if self._fire_complete_called:
            return
        self._fire_complete_called = True
        if self._on_complete is None:
            return
        try:
            await self._on_complete(result)
        except Exception as exc:
            log.exception(
                "tier_watcher.on_complete_error",
                server_id=self._server_order_id[:16],
                error=str(exc),
            )
