"""Strategy engine — decision loop that watches prices and emits actions.

The engine is purely reactive:
  - Driven by DecisionLoop on a fixed tick (250ms by default)
  - Maintains RoundState internally
  - Calls the supplied callbacks for each action

Order state is driven by the fill/cancel/expire callbacks. A tier's
placed=True flag is cleared only when the GTD either:
  a) fills completely (is_complete=True), or
  b) expires without a fill (on_order_expired -> mark_expired)
This ensures a tier cannot be triggered twice.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Literal, Protocol

from src.config import strategy as _s
from src.feeds.binance_spot import BinanceCache
from src.market.models import MarketInfo
from src.market.market_ws import PriceCache
from src.strategy.gatekeeper import GateKeeper
from src.strategy.round_state import (
    PositionSnapshot,
    RoundState,
    TierSnapshot,
)
from src.infra.logging import get_logger

log = get_logger(__name__)


def _ts(value) -> float:
    """Coerce an optional timestamp to float (test stubs may be MagicMock)."""
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------------------
# Action types
# ---------------------------------------------------------------------------


@dataclass
class ActionOpenT1:
    side: Literal["UP", "DOWN"]
    token: str
    price: float    # best_bid at moment of decision
    notional: float


@dataclass
class ActionOpenTN:
    tier: int        # 2, 3, 4
    token: str
    price: float
    notional: float
    trigger_price: float


@dataclass
class ActionTakeProfit:
    token: str
    price: float    # best_bid at moment of decision
    qty: float     # total position qty


@dataclass
class ActionCancelTier:
    tier: int           # which tier to cancel
    server_order_id: str
    reason: str


Action = ActionOpenT1 | ActionOpenTN | ActionTakeProfit | ActionCancelTier


class ActionHandler(Protocol):
    """Protocol for callbacks that consume strategy actions."""

    async def __call__(self, action: Action) -> None: ...


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class StrategyEngine:
    """Reactive strategy that monitors prices and emits action callbacks."""

    def __init__(
        self,
        binance_cache: BinanceCache,
        price_cache: PriceCache,
        handler: ActionHandler,
        killswitch: object | None = None,
    ) -> None:
        self._bc = binance_cache
        self._pc = price_cache
        self._handler = handler
        # Shared emergency-stop flag. When engaged `_evaluate` returns
        # immediately, so no T1 / averaging / TP action is emitted.
        self._kill = killswitch
        self._round: RoundState | None = None
        self._gate: GateKeeper | None = None
        self._s = _s()
        # B-1 fix: prevents concurrent tick sources (midpoint poll loop +
        # binance + WS book ticker) from re-entering _evaluate while the
        # previous run is awaiting an action handler. Drop-on-re-entry.
        self._eval_inflight: bool = False
        # Per-reason throttle for T1 skip logs. DecisionLoop ticks at 250ms;
        # without throttling a 5-minute round where UP stays above ENTRY_PRICE_CAP
        # would produce ~1200 `t1_skip_too_expensive` lines. We collapse to one
        # line per (reason, side) every 5 seconds — enough to diagnose from
        # production logs while keeping the firehose down.
        self._t1_skip_last_log_ts: dict[str, float] = {}
        # Per-round breadcrumb for the price-cap rejection. The throttled
        # `t1_skip_too_expensive` lines themselves are DEBUG (H-V6-15), but we
        # still want a single INFO per round that says "T1 was rejected on
        # price for X seconds this round" so operators can confirm the bot
        # is alive and patiently waiting without scrolling through DEBUG.
        # Key: (round_start_ts, side). Reset on round rollover when
        # `on_round_end` swaps in a new round_start_ts.
        self._t1_cap_reject_first_seen: dict[tuple[int, str], float] = {}
        self._t1_cap_reject_summary_logged: dict[tuple[int, str], bool] = {}
        # H-NEW-2026-07-11 (audit v6-12): per-(reason, tier) throttle for the
        # averaging-down `_check_average_down` skip events. Mirrors
        # `_t1_skip_last_log_ts` on the entry side.
        self._tN_skip_last_log_ts: dict[str, float] = {}
        # H-NEW-2026-07-11 (audit v6-12 production revisit): backoff state
        # for the TP path. `_tp_last_post_error_ts` is set whenever
        # `router.tp_failed` is emitted (POST was rejected); subsequent
        # `_evaluate` ticks must NOT re-fire trigger_tp until
        # `_tp_backoff_s` has elapsed. This breaks the
        # `engine.trigger_tp → POST 400 → router.tp_failed → mutex
        # cleared → next tick fires again` infinite loop.
        self._tp_last_post_error_ts: float = 0.0
        self._tp_backoff_s: float = 5.0
        # H-NEW-2026-07-12 (P0-1 fix per prod window 1783818900): track how
        # many consecutive TP POSTs have failed so we can apply a progressive
        # backoff instead of a flat 60s gate. Production saw T1 fill at +1s
        # but mid cross into the TP fire-line only ~2-3s later; a flat 60s
        # backoff ate the entire TP window. With progressive backoff:
        #   - 1st failure: 5s   (allow CLOB settlement of buy tokens)
        #   - 2nd:        10s
        #   - 3rd:        20s
        #   - 4th+:       60s cap (still prevents tight retry loops)
        # The counter resets on notify_tp_recovered (router confirms POST ok)
        # or on a mid-retreated early-exit (don't keep retrying when the
        # opportunity has slipped away anyway).
        self._tp_post_failure_count: int = 0
        # H-NEW-2026-07-12: throttle state for the per-tick `engine.t2_diag`
        # breadcrumb. The previous version logged every 250ms tick (~3600
        # lines per 15m round), drowning the operator signal in identical
        # INFO rows. We collapse to:
        #   - one line every T2_DIAG_THROTTLE_S seconds per (round, side)
        #   - one extra line immediately whenever the cache key set or
        #     entry_token fingerprint changes (so we still see a state
        #     transition even if it happens to fall between throttle
        #     windows).
        # The diagnostic payload itself is unchanged — it is a still-valid
        # "is T2 evaluating at all?" breadcrumb for the H-NEW-2026-07-11
        # audit v6-12 production symptom where T1 had filled at 0.39 and
        # mid had crashed to 0.155 but `_check_average_down` never fired.
        self._t2_diag_last_log_ts: dict[tuple[int, str], float] = {}
        self._t2_diag_last_fingerprint: dict[tuple[int, str], str] = {}
        # ── BTC feed health / short-term volatility blackout ──────────────
        # `(monotonic_wall_ts, price)` samples of the Binance spot cache,
        # pruned to BTC_VOL_WINDOW_S. The range over the window is the
        # sharp-move detector; a breach blocks new entries/averaging for
        # BTC_VOL_COOLDOWN_S and cancels resting BUY GTDs.
        self._btc_samples: list[tuple[float, float]] = []
        self._btc_vol_block_until: float = 0.0
        self._btc_vol_last_log_ts: float = 0.0
        self._btc_stale_last_log_ts: float = 0.0
        self._btc_guard_last_log_ts: dict[tuple[str, str], float] = {}
        # One `engine.t4_time_bypass` breadcrumb per round.
        self._t4_bypass_logged_round: int | None = None

    # H-NEW-2026-07-11: helper that reads PRICE_PRE_MARGIN safely even for
    # tests that construct engine via __new__ (bypassing __init__).
    def _get_price_pre_margin(self) -> float:
        if hasattr(self, "_s") and self._s is not None:
            return float(getattr(self._s, "PRICE_PRE_MARGIN", 0.0) or 0.0)
        # Fallback to config directly for test stubs.
        return float(getattr(_s(), "PRICE_PRE_MARGIN", 0.0) or 0.0)

    # ── BTC feed health / volatility guards (2026-10-30) ─────────────────

    def _cfg_float(self, name: str, default: float) -> float:
        """Read a float setting safely for __new__-constructed test engines."""
        value = None
        if getattr(self, "_s", None) is not None:
            value = getattr(self._s, name, None)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            value = getattr(_s(), name, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return float(default)
        return float(value)

    def _btc_age_s(self) -> float | None:
        """Seconds since the Binance cache last saw a trade (None = unknown)."""
        ts_ms = getattr(getattr(self, "_bc", None), "ts_ms", None)
        if (
            isinstance(ts_ms, bool)
            or not isinstance(ts_ms, (int, float))
            or ts_ms <= 0
        ):
            return None
        return max(0.0, time.time() - float(ts_ms) / 1000.0)

    def _btc_feed_stale(self) -> bool:
        """True when the BTC spot feed has not updated within the limit.

        A stale feed makes both the drift gate and the volatility detector
        meaningless, so new entries/averaging are paused (fail-closed).
        TP exits are deliberately NOT blocked. Missing `ts_ms` (test stubs)
        counts as unknown → fail-open.
        """
        max_age = self._cfg_float("BTC_STALE_MAX_AGE_S", 5.0)
        if max_age <= 0:
            return False
        age = self._btc_age_s()
        if age is None or age <= max_age:
            return False
        now = time.time()
        if now - getattr(self, "_btc_stale_last_log_ts", 0.0) >= 30.0:
            self._btc_stale_last_log_ts = now
            log.warning(
                "engine.btc_feed_stale",
                age_s=round(age, 1),
                max_age_s=max_age,
                note="pausing new entries/averaging (TP still active)",
            )
        return True

    def _update_btc_volatility(self, price: float) -> None:
        """Track the rolling BTC range and arm the blackout after a spike.

        (max-min)/min over `BTC_VOL_WINDOW_S` is a cheap sharp-move
        detector: it fires on both a flash crash and a vertical rally, and
        it is immune to the slow drift the existing gate already covers.
        """
        window_s = self._cfg_float("BTC_VOL_WINDOW_S", 60.0)
        threshold_pct = self._cfg_float("BTC_VOL_MAX_MOVE_PCT", 0.3)
        if window_s <= 0 or threshold_pct <= 0 or price <= 0:
            return
        now = time.time()
        samples = getattr(self, "_btc_samples", None)
        if samples is None:
            self._btc_samples = samples = []
        samples.append((now, float(price)))
        cutoff = now - window_s
        while samples and samples[0][0] < cutoff:
            samples.pop(0)
        if len(samples) < 2:
            return
        low = min(p for _, p in samples)
        high = max(p for _, p in samples)
        if low <= 0:
            return
        move_pct = (high - low) / low * 100.0
        if move_pct <= threshold_pct:
            return
        cooldown_s = max(0.0, self._cfg_float("BTC_VOL_COOLDOWN_S", 60.0))
        self._btc_vol_block_until = max(
            getattr(self, "_btc_vol_block_until", 0.0), now + cooldown_s
        )
        if now - getattr(self, "_btc_vol_last_log_ts", 0.0) >= 30.0:
            self._btc_vol_last_log_ts = now
            log.warning(
                "engine.btc_vol_spike",
                move_pct=round(move_pct, 3),
                threshold_pct=threshold_pct,
                window_s=window_s,
                low=round(low, 2),
                high=round(high, 2),
                block_s=round(self._btc_vol_block_until - now, 1),
                note="pausing new entries/averaging and cancelling BUY GTDs",
            )

    def _btc_vol_blocked(self) -> bool:
        return time.time() < getattr(self, "_btc_vol_block_until", 0.0)

    def _btc_guard_skip(self, scope: str, reason: str) -> None:
        """Throttled breadcrumb when a feed/volatility guard blocks an action."""
        now = time.time()
        key = (scope, reason)
        table = getattr(self, "_btc_guard_last_log_ts", None)
        if table is None:
            self._btc_guard_last_log_ts = table = {}
        if now - table.get(key, 0.0) < 30.0:
            return
        table[key] = now
        log.info("engine.btc_guard_skip", scope=scope, reason=reason)

    # ── T4 time-based drift bypass (2026-10-31) ──────────────────────────

    def _window_remaining_s(self) -> float | None:
        """Seconds left in the current window (None when unknown)."""
        rnd = getattr(self, "_round", None)
        market = getattr(rnd, "market", None) if rnd is not None else None
        end = getattr(market, "window_end", None) if market is not None else None
        if end is None:
            return None
        try:
            end_ts = end.timestamp() if hasattr(end, "timestamp") else float(end)
        except Exception:  # noqa: BLE001
            return None
        return max(0.0, end_ts - time.time())

    def _t4_time_bypass_active(self) -> bool:
        """True when T4 may ignore a failing BTC drift gate.

        T4 is the last/largest tier and has no momentum bypass, so a
        trending window blocks it exactly when the token crashes into its
        zone. When the window still has enough time for a recovery, the
        operator wants T4 to average in anyway.
        """
        min_remaining = self._cfg_float(
            "T4_DRIFT_BYPASS_MIN_REMAINING_S", 540.0
        )
        if min_remaining <= 0:
            return False
        remaining = self._window_remaining_s()
        return remaining is not None and remaining >= min_remaining

    def _log_t4_time_bypass(self, mid: float, t1_entry: float) -> None:
        rnd = getattr(self, "_round", None)
        round_ts = getattr(rnd, "round_start_ts", 0) if rnd is not None else 0
        if getattr(self, "_t4_bypass_logged_round", None) == round_ts:
            return
        self._t4_bypass_logged_round = round_ts
        remaining = self._window_remaining_s()
        log.info(
            "engine.t4_time_bypass",
            mid=mid,
            t1_entry=t1_entry,
            remaining_s=round(remaining, 1) if remaining is not None else None,
            note="T4 drift gate bypassed: enough window time left to recover",
        )

    async def _cancel_placed_buys(self, reason: str) -> None:
        """Cancel every resting BUY GTD (T1..T4) during a volatility blackout.

        New-order suppression alone is not enough: the pre-armed GTDs are
        already on the book and a flash move can fill them mid-spike.
        """
        if not self._round:
            return
        pos = self._round.position
        for tier in pos.tiers:
            if not tier.placed or tier.is_complete:
                continue
            if tier.cancel_pending:
                continue
            # Respect the router's cancel-failure backoff so a CLOB that
            # keeps rejecting the cancel is not hammered every 250 ms.
            if time.time() < _ts(getattr(tier, "cancel_retry_after_ts", 0.0)):
                continue
            sid = tier.server_order_id
            if not sid:
                continue
            tier.cancel_pending = True
            log.warning(
                "engine.vol_blackout.cancel_buy",
                tier=tier.tier,
                server_id=sid[:16],
                reason=reason,
            )
            await self._handler(ActionCancelTier(
                tier=tier.tier, server_order_id=sid, reason=reason,
            ))

    def _get_cancel_hysteresis(self) -> float:
        """Trigger→cancel buffer, as a fraction of the trigger line.

        Without it every cancel condition reuses the exact line that armed
        the order, so a mid oscillating around the boundary produces a
        post/cancel loop (each cycle costs a CLOB round-trip and burns
        maker allowance). Clamped to a sane range. A MagicMock `_s` stub
        is treated as unset and the process config is consulted instead.
        """
        def _as_float(value: object) -> float | None:
            # MagicMock stubs (older tests) answer any attribute with a
            # Mock, which is not a number — treat that as "unset".
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                return None
            return float(value)

        value = None
        if hasattr(self, "_s") and self._s is not None:
            value = _as_float(getattr(self._s, "CANCEL_HYSTERESIS_PCT", None))
        if value is None:
            value = _as_float(getattr(_s(), "CANCEL_HYSTERESIS_PCT", 0.0))
        if value is None:
            return 0.0
        return max(0.0, min(0.5, value))

    def _get_tp_require_drift_gate(self) -> bool:
        """Whether TP exits must also pass the BTC drift gate.

        Default False: exits should not be blocked by spot drift (the gate
        exists to filter entries). A MagicMock `_s` stub is treated as
        unset and the process config is consulted instead.
        """
        value = None
        if hasattr(self, "_s") and self._s is not None:
            value = getattr(self._s, "TP_REQUIRE_DRIFT_GATE", None)
        if not isinstance(value, bool):
            value = getattr(_s(), "TP_REQUIRE_DRIFT_GATE", False)
        return bool(value)

    def _t1_skip_log(self, reason: str, *, side: str, **fields) -> None:
        """Log a T1 skip event with per-(reason,side) throttle.

        Production symptom this exists for: bot is alive, evaluate loop is
        running, STATUS shows `tiers=[T1=○]`, but no `engine.trigger_t1` ever
        fires. Without these logs the operator cannot tell which condition
        is the blocker (price cap, drift, no best_bid, etc.) — they are all
        silent `continue` branches upstream.

        H-V6-12: throttle bumped from 5s to 30s for `t1_skip_no_bid`.
        Reason: the REST /book fallback takes ~2-4s to fill the cache on a
        fresh round; a 5s throttle would still fire 12 times in that
        window with no diagnostic value. Other skip reasons stay at 5s
        because their price/drift values change meaningfully every tick.

        H-V6-15: `engine.t1_skip_too_expensive` is downgraded to DEBUG.
        Operationally this reason fires every 5s for as long as the token
        stays above ENTRY_PRICE_CAP — easily 20-40 INFO lines per round
        on a typical BTC 15m. The decision itself is fully reflected by
        STATUS `tiers=[T1=○]`, and a single per-round INFO summary is
        emitted via `_t1_cap_reject_summary()` so operators can still
        confirm T1 was rejected on price without the firehose.
        """
        key = f"{reason}|{side}"
        now = time.time()
        throttle = 30.0 if reason == "engine.t1_skip_no_bid" else 5.0
        last = self._t1_skip_last_log_ts.get(key, 0.0)
        if now - last < throttle:
            return
        self._t1_skip_last_log_ts[key] = now
        # H-V6-15: price-cap rejection is a high-frequency, low-information
        # event once a token is well above cap. Surface at DEBUG so it is
        # still inspectable via --log-level=DEBUG without spamming INFO.
        if reason == "engine.t1_skip_too_expensive":
            log.debug(reason, side=side, **fields)
            # ...and record first-occurrence so _t1_cap_reject_summary can
            # emit a single per-round INFO breadcrumb about how long the
            # cap rejection lasted.
            if self._round is not None:
                rk = (self._round.round_start_ts, side)
                self._t1_cap_reject_first_seen.setdefault(rk, now)
            return
        log.info(reason, side=side, **fields)

    # H-NEW-2026-07-11 (audit v6-12): throttle logger for the averaging-down
    # (`_check_average_down`) skip paths. The previous implementation had
    # seven distinct `continue` branches with NO logging — operators could
    # only see the consequence (`tiers=[T2=○]` never advancing) without any
    # breadcrumb indicating which condition was blocking. Production symptom
    # 2026-07-11 11:15: T1 filled at 0.39, mid dropped to 0.155 (a 60% drop
    # from 0.40), but NO `engine.trigger_tN` ever fired. Without this method
    # we had no way to determine whether the blocker was:
    #   - mid_price cache empty / mid > fire_line / gate.can_double_down=False
    #   - tier.watching stuck True / saturation / stake_cap
    # Mirrors the T1-side `_t1_skip_log` design. Keyed by (reason, tier) so
    # each tier gets its own throttle slot, and we collapse to ~1 line per
    # 5s per blocker to keep the firehose manageable.
    def _tN_skip_log(self, reason: str, *, tier: int, **fields) -> None:
        key = f"{reason}|T{tier}"
        now = time.time()
        # Tolerate tests that construct engine via __new__ without
        # running __init__ (which would have created _tN_skip_last_log_ts).
        bucket = getattr(self, "_tN_skip_last_log_ts", None)
        if bucket is None:
            self._tN_skip_last_log_ts = {}
            bucket = self._tN_skip_last_log_ts
        last = bucket.get(key, 0.0)
        if now - last < 5.0:
            return
        bucket[key] = now
        log.info(reason, tier=tier, **fields)

    def _t1_cap_reject_summary(self) -> None:
        """Emit one INFO summary line per (round, side) after T1 enters or
        the round ends — reports how long the price-cap rejection lasted.

        H-V6-15: companion to the DEBUG-downgrade of
        `engine.t1_skip_too_expensive`. The actual per-tick line is now
        silent in INFO logs; this preserves a single human-readable
        breadcrumb per round at INFO so operators can see "T1 was
        rejected on price for X seconds this round" without scrolling
        DEBUG.
        """
        if self._round is None:
            return
        rk_round = self._round.round_start_ts
        # Iterate over all sides we've tracked for this round
        seen = list(self._t1_cap_reject_first_seen.keys())
        for rk in seen:
            if rk[0] != rk_round:
                continue  # stale entry from previous round — clear below
            side = rk[1]
            if self._t1_cap_reject_summary_logged.get(rk):
                continue
            first_seen = self._t1_cap_reject_first_seen[rk]
            duration_s = time.time() - first_seen
            log.info(
                "engine.t1_cap_rejected_summary",
                side=side,
                duration_s=round(duration_s, 1),
                cap=self._s.ENTRY_PRICE_CAP,
                note="T1 was above ENTRY_PRICE_CAP for this many seconds; "
                     "no T1 entry was made this side",
            )
            self._t1_cap_reject_summary_logged[rk] = True

    def _t1_cap_reject_reset_round(self) -> None:
        """Drop per-round price-cap tracking when a new round starts.

        Called from on_new_round() and on_round_end(). Without this, dict
        entries from the previous round would leak across round rollovers
        and become stale (their round_start_ts no longer matches the
        current `_round.round_start_ts`). The summary() path filters out
        stale entries already, but this is a cleaner GC for prod memory.
        """
        if not self._t1_cap_reject_first_seen:
            return
        current = self._round.round_start_ts if self._round else None
        for rk in list(self._t1_cap_reject_first_seen.keys()):
            if current is None or rk[0] != current:
                self._t1_cap_reject_first_seen.pop(rk, None)
                self._t1_cap_reject_summary_logged.pop(rk, None)

    # ------------------------------------------------------------------
    # Round lifecycle
    # ------------------------------------------------------------------

    async def on_new_round(self, round_start_ts: int, market: MarketInfo | None = None) -> None:
        """Called when the 15-min window rolls over.

        H-V6-4: pin the gate baseline HERE from `market.btc_spot_at_window_open`
        (the value the WindowManager already fetched via fetch_at_window_open()).
        Previously we waited for the separate `_on_window_baseline` callback to
        arrive, but the WM tick ordering caused a timing race: the baseline was
        applied to the just-finished round and immediately wiped by
        `on_round_end`, leaving the gate at None for the full 15-min window
        (STATUS logged `baseline=pending`, no drift gating ran).
        """
        log.info("engine.new_round", round_start_ts=round_start_ts)
        # H-V6-15: clear stale price-cap tracking from the previous round
        # (if on_round_end ran) so the new round starts with a clean slate.
        # summary() will be re-armed when t1_skip_log observes the first
        # too-expensive event under the new round_start_ts.
        self._t1_cap_reject_reset_round()
        self._round = RoundState(round_start_ts=round_start_ts, market=market)
        self._gate = None

        # Pin baseline from the market the WM just discovered. The WM has
        # already called `fetch_at_window_open()` for this asset; the price
        # lives on `market.btc_spot_at_window_open`. If it's missing (rare
        # boot case where WM fetched via spot fallback but did not write back),
        # fall back to the most recent Binance WS price, and as a last resort
        # leave _gate as None — `_on_window_baseline` will cover it within a
        # couple of ticks.
        baseline: float | None = None
        baseline_src = ""
        if market is not None and getattr(market, "btc_spot_at_window_open", 0.0) > 0:
            baseline = float(market.btc_spot_at_window_open)
            baseline_src = "market.btc_spot_at_window_open"
        elif self._bc.last_price > 0:
            baseline = self._bc.last_price
            baseline_src = "binance_cache.last_price"

        if baseline is not None and baseline > 0:
            self._gate = GateKeeper(baseline)
            self._round.binance_baseline = baseline
            log.info(
                "engine.baseline_set",
                baseline=baseline,
                round_start_ts=round_start_ts,
                source=baseline_src,
            )
        else:
            log.warning(
                "engine.baseline_pending",
                round_start_ts=round_start_ts,
                hint="neither market.btc_spot_at_window_open nor binance_cache.last_price available",
            )

    def set_binance_baseline(self, baseline: float) -> None:
        """Replace the gate baseline (called by `_on_window_baseline`).

        H-NEW-2026-07-15 (audit follow-up): guard against pathological
        baseline jumps (e.g. WM K-line backtrack feeding a stale price,
        or a mid-flight API error returning an order-of-magnitude value).
        We log a WARNING if the new baseline differs from the previous
        one by more than 1% — that's well beyond BTC's normal intra-round
        drift and almost always indicates data corruption. We still apply
        the new baseline (operator-visible alarm, auto-correct) instead
        of dropping on the floor (which would freeze the gate).
        """
        prev_baseline = (
            self._round.binance_baseline
            if self._round and self._round.binance_baseline
            else (self._gate.baseline if self._gate else None)
        )
        if (
            prev_baseline is not None
            and baseline > 0
            and prev_baseline > 0
        ):
            jump_pct = abs(baseline - prev_baseline) / prev_baseline
            if jump_pct > 0.01:
                log.warning(
                    "engine.baseline_jump",
                    prev=prev_baseline,
                    new=baseline,
                    jump_pct=round(jump_pct, 4),
                    note="baseline moved >1% from previous value; likely "
                         "WM K-line backtrack or upstream data issue. "
                         "Applying anyway — operator visibility only.",
                )
        self._gate = GateKeeper(baseline)
        if self._round is not None:
            self._round.binance_baseline = baseline
        log.info("engine.baseline_set", baseline=baseline, prev_baseline=prev_baseline)

    def attach_market(self, market: MarketInfo | None) -> None:
        """Late-bind market metadata discovered after the round started.

        WindowManager can fail to discover the new round's market exactly
        at the boundary (Gamma lag / transient error) and retries a few
        seconds later. `on_new_round` then creates the RoundState with
        `market=None`; this method fills in the token ids once discovery
        succeeds, so the engine can still trade the current window instead
        of idling until the next 15-minute boundary.
        """
        if self._round is None or market is None:
            return
        rs = self._round
        if rs.up_token:
            # Already bound (normal path) — keep the first market.
            return
        if market.round_start_ts and market.round_start_ts != rs.round_start_ts:
            log.warning(
                "engine.attach_market.round_mismatch",
                engine_round_start_ts=rs.round_start_ts,
                market_round_start_ts=market.round_start_ts,
                slug=market.slug or "",
            )
            return
        rs.market = market
        rs.slug = market.slug
        rs.condition_id = market.condition_id
        rs.up_token = market.up_token
        rs.down_token = market.down_token
        rs.tick_size = market.tick_size
        if getattr(market, "btc_spot_at_window_open", 0.0) > 0:
            self.set_binance_baseline(float(market.btc_spot_at_window_open))
        log.info(
            "engine.market_attached",
            round_start_ts=rs.round_start_ts,
            slug=market.slug or "",
            condition_id=(market.condition_id or "")[:16],
            up_token=(market.up_token or "")[:16],
            down_token=(market.down_token or "")[:16],
        )

    async def on_round_end(self) -> None:
        """Called at round close.

        H-V6-4: keep `self._gate` here — don't clear it. The new round will
        re-pin the baseline inside `on_new_round` from
        `market.btc_spot_at_window_open` (which the WM already fetched via
        `fetch_at_window_open()`). Until that runs, the previous round's
        baseline stays in place so drift checks keep responding (better
        than 0–15 min of `baseline=pending`).
        """
        if self._round:
            self._round.finished = True
            log.info(
                "engine.round_ended",
                round_start_ts=self._round.round_start_ts,
                position=self._round.position,
            )
            # H-V6-15: emit any pending per-round price-cap rejection
            # summary *before* we drop the round reference, then clear
            # the tracking dicts.
            self._t1_cap_reject_summary()
        self._round = None
        self._t1_cap_reject_reset_round()

    # ------------------------------------------------------------------
    # Order event handlers — these drive tier state machine
    # ------------------------------------------------------------------

    def on_order_filled(
        self,
        token_id: str,
        server_order_id: str,
        fill_price: float,
        fill_size: float,
    ) -> None:
        """
        Called when a confirmed fill delta is received.

        Args:
            token_id:        authoritative asset_id of the filled maker order (UP or DOWN)
            server_order_id: complete server order_id hash (NOT a prefix)
            fill_price:      price of this fill
            fill_size:       tokens filled IN THIS EVENT (a delta, NOT a
                              cumulative). Multiple sources (WS, polling,
                              FAK response) may observe the same order — a
                              FillDeduper inside CLOBClient ensures this is
                              only called with the un-processed delta so that
                              `record_fill`'s `size_filled += size` does not
                              double-count (B-3 fix).
        """
        if not self._round:
            log.debug(
                "engine.fill_ignored no_round",
                token=token_id[:16], order_id=server_order_id,
            )
            return

        if self._round.position.is_empty:
            log.debug(
                "engine.fill_ignored no_position",
                token=token_id[:16], order_id=server_order_id,
            )
            return

        pos = self._round.position
        for tier in pos.tiers:
            if tier.server_order_id == server_order_id:
                # Verify asset_id matches — reject mismatches (cross-token contamination)
                if token_id and token_id != tier.token_id and tier.token_id:
                    log.warning(
                        "engine.fill_asset_mismatch",
                        token=token_id[:16], expected=tier.token_id[:16], order_id=server_order_id,
                    )
                    return

                pos.record_fill(tier, fill_price, fill_size)
                log.info(
                    "engine.tier_filled",
                    tier=tier.tier,
                    token_id=token_id[:16],
                    fill_price=fill_price,
                    fill_size=fill_size,
                    cumulative_size=tier.size_filled,
                    expected_size=tier.size_expected,
                    avg_cost=pos.avg_cost,
                    total_qty=pos.total_qty,
                    tp_target=pos.tp_target(),
                )
                return

        # Not a BUY tier fill — check if it is a registered TP (SELL) fill.
        if pos.record_tp_fill(server_order_id, fill_price, fill_size):
            log.info(
                "engine.tp_filled",
                server_order_id=server_order_id[:16],
                token_id=token_id[:16],
                fill_price=fill_price,
                fill_size=fill_size,
                total_qty=pos.total_qty,
                avg_cost=pos.avg_cost,
                tp_target=pos.tp_target(),
            )
            return

        log.debug("engine.fill_unknown", order_id=server_order_id)

    def notify_tp_post_error(self, reason: str) -> None:
        """Called by router when a TP SELL POST was rejected by the CLOB.

        Sets the backoff gate so `_check_take_profit` will not re-fire
        trigger_tp for `_tp_backoff_s` seconds. Also clears `_tp_inflight`
        since the previous attempt definitively failed.

        H-NEW-2026-07-11 (audit v6-12 production revisit): without this,
        the engine deadlocks into a hot retry loop on every tick when the
        CLOB rejects the SELL with `not enough balance / allowance`. The
        position remains `total_qty > 0` in-memory because no SELL fill
        ever landed, but the bot cannot sell — and without a backoff it
        would re-attempt the impossible POST ~4 times per second.

        The retry-blocking is best-effort: when the user (or a manual
        cleanup) re-supplies balance, a successful place call (which
        clears `_tp_inflight` via `apply_sell_fill` path or via the
        `tp_runner` returned-order branch) is enough to start firing
        again — or if 60s elapses without a successful POST we re-arm,
        because the user's manual side-sell may have freed up the
        allowance by then.
        """
        # H-NEW-2026-07-12 (P0-1 fix per prod window 1783818900): progressive
        # backoff. The first attempt typically fails because the CLOB has
        # not yet settled the just-bought tokens into the maker's
        # conditional-token balance (settlement lags the WS TRADE event by
        # 1-3s on a normal round, longer when the chain is congested).
        # Capping the backoff at 60s for the first attempt means we lose
        # the entire TP window when mid crosses 1 tick later. Instead:
        #   failure 1 -> 5s
        #   failure 2 -> 10s
        #   failure 3 -> 20s
        #   failure 4+ -> 60s cap (still prevents ~4/s hot retries)
        prev_count = getattr(self, "_tp_post_failure_count", 0)
        self._tp_post_failure_count = prev_count + 1
        # Step table: 5, 10, 20, 60 (capped). Index bounded by len-1.
        backoff_steps = (5.0, 10.0, 20.0, 60.0)
        self._tp_backoff_s = backoff_steps[min(self._tp_post_failure_count - 1, len(backoff_steps) - 1)]
        self._tp_last_post_error_ts = time.time()
        if self._round is not None:
            self._round.position._tp_inflight = False
        log.warning(
            "engine.tp_backoff_engaged",
            reason=reason,
            backoff_s=self._tp_backoff_s,
            failure_count=self._tp_post_failure_count,
            note="trigger_tp suppressed until backoff elapses; "
                 "backoff is now PROGRESSIVE (5/10/20/60s) instead of flat 60s",
        )

    def notify_tp_recovered(self) -> None:
        """Called by router when a TP SELL POST succeeded (or was
        invalidated by external sync). Clears the backoff gate so the
        next `_check_take_profit` tick can re-arm TP if still eligible.

        H-NEW-2026-07-12 (P0-1 fix): also resets the progressive backoff
        counter. Otherwise a successful POST after several failures would
        have left `_tp_post_failure_count=3` in the dict, so the NEXT
        failure (if any) would skip straight to the 60s cap instead of
        starting back at 5s.

        H-NEW-2026-07-12 (structural-fix): historically this method's
        body was accidentally copy-pasted from `on_order_filled` (it
        referenced `token_id`, `server_order_id`, `fill_price`, and
        `fill_size` which are NOT parameters of `notify_tp_recovered`).
        That latent bug meant a NameError fired the moment the router
        called this method — caught silently by the caller's
        try/except, leaving the backoff engaged for the full 60s flat
        gate. P0-1 fix is meaningless without clearing that.
        """
        self._tp_last_post_error_ts = 0.0
        self._tp_post_failure_count = 0
        self._tp_backoff_s = 5.0  # reset to the first-step value

    def on_order_cancelled(self, server_order_id: str, reason: str = "manual") -> None:
        """
        Called when an order is cancelled (manual, deviation, or cancel-all).
        Resets tier.placed so the strategy can re-arm if conditions still hold.
        """
        if not self._round:
            return
        pos = self._round.position
        # TP SELL orders are tracked in `_tp_snapshots`, not in the BUY
        # tier slots. Close the snapshot so `has_open_tp_orders` releases
        # and a future TP can be posted (otherwise the round wedges).
        if pos.tp_size_sold(server_order_id) is not None:
            pos.close_tp_order(server_order_id)
            log.info(
                "engine.tp_order_cancelled",
                server_order_id=server_order_id[:16],
                reason=reason,
                total_qty=pos.total_qty,
            )
            return
        for tier in pos.tiers:
            if tier.server_order_id == server_order_id:
                tier.placed = False
                tier.server_order_id = None
                log.info(
                    "engine.order_cancelled",
                    tier=tier.tier,
                    server_order_id=server_order_id,
                    reason=reason,
                )
                return

    def on_order_expired(self, server_order_id: str) -> None:
        """
        Called when a GTD order has expired server-side without a fill.
        Mark the tier as expired so the strategy can re-arm.
        """
        if not self._round:
            return
        for tier in self._round.position.tiers:
            if tier.server_order_id == server_order_id:
                self._round.position.mark_expired(tier, reason="gtd_expired")
                tier.placed = False
                tier.server_order_id = None
                log.warning(
                    "engine.tier_expired",
                    tier=tier.tier,
                    server_order_id=server_order_id,
                    size_filled=tier.size_filled,
                    size_expected=tier.size_expected,
                )
                return

    def bind_server_order_id(
        self,
        tier: int,
        server_order_id: str,
        token_id: str,
        expires_at: float,
        expected_price: float,
        placed_size: float | None = None,
    ) -> None:
        """
        Called by the action router after an order is placed.

        Records the authoritative token_id so fill events can be validated.
        size_expected is the CLOB-accepted placed_size when supplied (M-1
        fix — the CLOB may round down size to the nearest tick, so
        notional / expected_price can be off by one tick and cause
        is_complete to never flip on a clean fill). Falls back to the
        notional/expected_price quotient if placed_size is missing.
        """
        if not self._round:
            return
        t = self._round.position.tiers[tier - 1]
        t.placed = True
        t.server_order_id = server_order_id
        t.token_id = token_id
        t.expires_at = expires_at
        # A successful placement clears any post-failure backoff.
        t.retry_after_ts = 0.0
        if tier == 1:
            self._round.position.t1_retry_after_ts = 0.0
        if placed_size is not None and placed_size > 0:
            # Re-posts after a partial fill only reserve the remaining shares.
            # Keep size_expected cumulative so is_complete still represents the
            # whole configured tier rather than just the latest child order.
            t.size_expected = max(t.size_expected, t.size_filled + placed_size)
        elif expected_price > 0:
            t.size_expected = t.notional / expected_price
        else:
            t.size_expected = 0.0

        # H-NEW-3 fix: anchor the martingale trigger baseline to the price
        # the CLOB actually accepted (`expected_price` here is the adjusted
        # price from TrackedOrder, after bid+BUY_STEP snap). Previously
        # _check_t1_entry pre-set baseline_target_price from the WS-cached
        # `bid_price`, which can be stale by the time the order lands on
        # the CLOB and produces a different adjusted_price. Re-anchoring
        # here (idempotent overwrite with the same value during T1, and
        # meaningful for T2..T4) keeps the martingale math aligned with
        # what we are actually paying per share.
        if tier == 1 and expected_price > 0:
            self._round.position.baseline_target_price = expected_price

    # ------------------------------------------------------------------
    # Core evaluation
    # ------------------------------------------------------------------

    async def _evaluate(self) -> None:
        """Re-evaluate the round state and emit at most one action.

        Wrapped in an in-flight guard so concurrent tick sources (B-1 fix:
        midpoint poll, WS book ticker, binance trades) cannot re-enter the
        decision loop while a previous evaluate is awaiting the action
        handler. Without this, two ticks arriving in the same window could
        each emit ActionOpenT1 / ActionOpenTN and the router would queue
        two placements.
        """
        if not self._round or not self._gate:
            return
        if getattr(self._round, "skip_reason", None):
            # Window pre-check (late boot / cross-window BTC gap) — do not
            # open or average any position this round.
            return
        ks = getattr(self, "_kill", None)
        if ks is not None and getattr(ks, "active", False):
            # Emergency stop: emit nothing. The order manager has its own
            # guard for actions already in flight.
            return

        # Single-flight: drop re-entry rather than queue it. The next tick
        # will pick up the new state and call _evaluate again.
        #
        # R1 (audit 2026-07-12): invariant — `_eval_inflight` is set True
        # on line 645 below and reset to False in the matching `finally`.
        # This holds even when _check_t1_entry / _check_average_down /
        # _check_take_profit raise exceptions (any exception in a try-block
        # runs the finally before propagation). The flag is the only
        # serialization point between ticks — if it ever became True
        # without being cleared, the engine would freeze on the next tick
        # (no log row, just silent no-ops). Tests cover this invariant
        # in test_audit_fixes_2026_07_11.py::test_r1_eval_inflight_reset
        # so a regression that lets the flag leak surfaces in CI rather
        # than in prod at 3am.
        if self._eval_inflight:
            log.debug(
                "engine.evaluate.skip re_entry",
                round_start_ts=self._round.round_start_ts,
            )
            return
        self._eval_inflight = True
        try:
            bc_now = self._bc.last_price
            if bc_now <= 0:
                return

            # Feed health + short-term volatility state. A stale feed is not
            # sampled (the price is old); the guards in the check_* helpers
            # decide whether to pause new actions.
            if not self._btc_feed_stale():
                self._update_btc_volatility(bc_now)

            gate_state = self._gate.state(bc_now)

            new_state = "PASS" if gate_state.within_limit else "FAIL"
            if self._round.gate_last_state != new_state:
                self._round.gate_last_state = new_state
                log.info("engine.gate", drift=gate_state.drift, state=new_state)

            # NOTE: do NOT early-return on drift here. Each decision path
            # enforces the gate itself:
            #   * T1 entry   -> GateKeeper.can_enter() (drift + price cap)
            #   * averaging  -> can_double_down() (documented drift bypass
            #                   once the next-tier trigger is reached)
            #   * TP posting -> can_act()
            # The old early-return made the averaging bypass unreachable and
            # froze the cancel sweep / TP-retreat cancel during any drift
            # spike, leaving stale GTDs on the book.

            pos = self._round.position
            # H-NEW-2026-07-15 tick-driven refactor: compute a single
            # canonical snapshot of the position's decision-relevant state.
            # All four `_check_*` / `_reconcile_*` helpers consume this
            # snapshot instead of re-deriving their own view of position.
            snap = pos.snapshot()

            # ── Volatility blackout: cancel resting BUY GTDs ──────────────
            # A flash move must not fill pre-armed entries/averaging orders;
            # TP SELLs are left alone (exits stay enabled).
            if self._btc_vol_blocked():
                await self._cancel_placed_buys("btc_vol_spike")

            # ── Tick-driven cancel sweep — MUST run before any new place ───
            # Reconcile any BUY GTDs that are still on the book but no
            # longer wanted (mid already dropped into the next-tier zone,
            # or mid has risen into the TP fire zone). This is the SOLE
            # place that cancels BUY GTDs in the new model; the TierWatcher
            # is passive and only emits lifecycle events.
            if snap.entry_token is not None:
                await self._reconcile_inflight_buys(bc_now, snap)

            # ── Not yet entered → check T1 entry ────────────────────────────
            if pos.entry_token is None:
                await self._check_t1_entry(bc_now, snap)

            # ── In position → check averaging-down and take-profit ───────────
            if pos.entry_token is not None:
                # Re-snapshot after reconcile; the cancel sweep may have
                # flipped some `tier.placed` flags and we want the latest
                # state for the place-side helpers.
                snap = pos.snapshot()
                await self._check_average_down(bc_now, snap)
                await self._check_take_profit(bc_now, snap)
        finally:
            self._eval_inflight = False

    async def _reconcile_inflight_buys(
        self, bc_now: float, snap: PositionSnapshot | None = None
    ) -> None:
        """Cancel BUY GTDs that the tick no longer wants on the book.

        H-NEW-2026-07-15 tick-driven refactor: this is the SOLE BUY-side
        cancel path under the new model. The TierWatcher is passive and
        no longer cancels on fill-ratio or mid-cross; instead, every tick
        inspects the live mid and decides:

          1. If a tier's GTD is still on the book AND mid has crossed
             into the next-tier fire zone (strategy_price * (1+margin)),
             cancel it so the next tier can fire cleanly.
          2. If a tier's GTD is still on the book AND mid has risen into
             the TP fire zone (tp_target * (1-margin)), cancel all BUY
             GTDs so the TP SELL can take over.

        Each cancel emits an `ActionCancelTier` to the router, which calls
        `OrderManager.cancel_by_server_id`. The router flips
        `tier.placed = False` so `has_inflight_tier` releases and the
        place-side helpers can advance.

        Tier.is_complete tiers are skipped (no GTD to cancel). Already-
        cancelled tiers (placed=False) are skipped.
        """
        if not self._round or not self._gate:
            return
        pos = self._round.position
        if snap is None:
            snap = pos.snapshot()
        if snap.entry_token is None:
            return
        mid_price = self._pc.get_midpoint(snap.entry_token)
        if mid_price <= 0:
            return
        margin = self._get_price_pre_margin()
        hysteresis = self._get_cancel_hysteresis()
        cfg = self._s

        # Compute TP fire line once. None if no TP target yet.
        tp_target = snap.tp_target
        tp_fire_line = (
            tp_target * (1.0 - margin)
            if (tp_target is not None and tp_target > 0)
            else None
        )
        # Hysteresis (CANCEL_HYSTERESIS_PCT): buys are armed while mid is
        # BELOW this line, so cancelling them only once mid is a bit ABOVE
        # it stops a mid oscillating at the boundary from cancelling and
        # re-arming the same tier orders every tick.
        tp_cancel_line = (
            tp_fire_line * (1.0 + hysteresis)
            if tp_fire_line is not None
            else None
        )
        # TP path: mid crossed the TP fire line. Cancel every BUY GTD.
        if tp_cancel_line is not None and mid_price >= tp_cancel_line:
            for tier_idx in range(1, 4):  # T2, T3, T4 — averaging tiers
                tier = pos.tiers[tier_idx]
                if not tier.placed or tier.is_complete:
                    continue
                # Cancel-failure backoff (set by the router).
                if time.time() < _ts(getattr(tier, "cancel_retry_after_ts", 0.0)):
                    continue
                # H-NEW-2026-07-15 (audit follow-up): single-flight guard.
                # Don't re-emit Cancel if a previous dispatch hasn't been
                # resolved yet. Clearing happens in the router after
                # `om.cancel_by_server_id` returns.
                if tier.cancel_pending:
                    continue
                sid = tier.server_order_id
                if not sid:
                    continue
                tier.cancel_pending = True
                log.info(
                    "engine.reconcile.cancel_for_tp",
                    tier=tier.tier,
                    server_id=sid[:16],
                    mid=mid_price,
                    tp_fire_line=tp_fire_line,
                    size_filled=tier.size_filled,
                )
                await self._handler(ActionCancelTier(
                    tier=tier.tier,
                    server_order_id=sid,
                    reason="mid_above_tp_fire",
                ))
            # Also cancel T1 if still on the book and unfilled.
            tier1 = pos.tiers[0]
            if (
                tier1.placed
                and not tier1.is_complete
                and tier1.server_order_id
                and not tier1.cancel_pending
                and time.time()
                >= _ts(getattr(tier1, "cancel_retry_after_ts", 0.0))
            ):
                tier1.cancel_pending = True
                log.info(
                    "engine.reconcile.cancel_t1_for_tp",
                    server_id=tier1.server_order_id[:16],
                    mid=mid_price,
                    tp_fire_line=tp_fire_line,
                    size_filled=tier1.size_filled,
                )
                await self._handler(ActionCancelTier(
                    tier=1,
                    server_order_id=tier1.server_order_id,
                    reason="mid_above_tp_fire",
                ))
            return  # do not also do the next-tier cancellation in same tick

        # T1 entry anchor (token price, NOT BTC). Use realised (preferred)
        # or target fallback. Same resolution chain as `_check_average_down`.
        t1_entry = snap.baseline_realized_price or snap.baseline_target_price
        if t1_entry is None or t1_entry <= 0:
            return
        drops = cfg.tier_drop_pcts

        # Iterate T1..T3 (T4 has no next-tier).
        for tier_idx in range(0, 3):
            tier = pos.tiers[tier_idx]
            if not tier.placed or tier.is_complete:
                continue
            # Cancel-failure backoff (set by the router).
            if time.time() < _ts(getattr(tier, "cancel_retry_after_ts", 0.0)):
                continue
            # H-NEW-2026-07-15 (audit follow-up): single-flight guard.
            if tier.cancel_pending:
                continue
            sid = tier.server_order_id
            if not sid:
                continue
            next_drop = drops[tier_idx + 1] if tier_idx + 1 < len(drops) else None
            if next_drop is None:
                continue  # last tier, no next-tier to fire
            next_strategy_price = t1_entry * (1.0 - next_drop)
            next_fire_line = next_strategy_price * (1.0 + margin)
            # Hysteresis: the current tier is cancelled a little deeper
            # into the next-tier zone than where that tier would arm, so a
            # mid hovering at the boundary does not cancel/re-arm every
            # tick (the zone is entered once, decisively).
            next_cancel_line = next_fire_line * (1.0 - hysteresis)
            if mid_price <= next_cancel_line:
                tier.cancel_pending = True
                log.info(
                    "engine.reconcile.cancel_for_next_tier",
                    tier=tier.tier,
                    server_id=sid[:16],
                    mid=mid_price,
                    next_fire_line=next_fire_line,
                    next_cancel_line=next_cancel_line,
                    next_strategy_price=next_strategy_price,
                    size_filled=tier.size_filled,
                )
                await self._handler(ActionCancelTier(
                    tier=tier.tier,
                    server_order_id=sid,
                    reason="mid_below_next_fire",
                ))

    async def _check_t1_entry(
        self, bc_now: float, snap: PositionSnapshot | None = None
    ) -> None:
        """Check both sides for T1 entry trigger.

        H-NEW-2026-07-11 refactor — pre-arm trigger model:

          Trigger line : `strategy_price = ENTRY_PRICE_CAP` (e.g. 0.40)
          Fire line    : mid <= strategy_price * (1 + PRICE_PRE_MARGIN)
                         (e.g. mid <= 0.44 when margin=0.10)
          GTD posted at: strategy_price exactly (e.g. 0.40)

        The previous version fired only when mid had already crossed
        strategy_price (i.e. mid <= 0.40). The new model posts the GTD
        at 0.40 as soon as mid drops to 0.44, so the maker sits on the
        book and gets filled when a taker crosses 0.40. This both:

          1. Eliminates the orderbook fetch (the previous
             `_adjust_buy_price` REST /book call) — the bot no longer
             needs to know the live bid, only the mid.
          2. Removes timing latency: by the time the previous code saw
             mid <= 0.40 and adjusted to bid+half_tick, the taker had
             usually already cleared 0.40 and moved on.

        Output is still `ActionOpenT1(price=strategy_price)`; the router
        calls `place_buy_by_strategy_price` which uses `price_lock=True`.

        H-NEW-2026-07-15: takes a `PositionSnapshot` (optional for back-
        compat with direct test calls). When called from `_evaluate` we
        pass the freshly-built snapshot; tests / stale callers may pass
        None and we build one locally. This keeps the snapshot as the
        canonical source while not breaking existing call sites.
        """
        if not self._round or not self._gate or not self._round.up_token:
            return

        # Feed-health / volatility guards: never open during a data outage
        # or a flash move.
        if self._btc_feed_stale():
            self._btc_guard_skip("t1", "btc_stale")
            return
        if self._btc_vol_blocked():
            self._btc_guard_skip("t1", "btc_vol_spike")
            return

        pos = self._round.position
        if snap is None:
            snap = pos.snapshot()
        if snap.entry_token is not None:
            return  # already entered
        # Post-failure backoff (set by the router when a T1 POST fails).
        if time.time() < _ts(getattr(pos, "t1_retry_after_ts", 0.0)):
            return

        s = self._s
        up_tok = self._round.up_token
        down_tok = self._round.down_token

        margin = float(getattr(s, "PRICE_PRE_MARGIN", 0.0) or 0.0)
        # Strategy price = ENTRY_PRICE_CAP; the new model pre-arms at this
        # level. We use ENTRY_PRICE_CAP as the "design price" so the trigger
        # margin is consistent with the price-cap semantics.
        # R2 (audit 2026-07-12): the same value is also read in
        # gatekeeper.can_enter() and in `_t1_cap_reject_summary()`. The
        # three readers all funnel through `_s().ENTRY_PRICE_CAP` so a
        # single-source-of-truth is already in place — this comment
        # exists to make the implicit invariant explicit so a future
        # refactor that introduces a fourth reader can still reason
        # about drift between the three call sites.
        strategy_price = float(s.ENTRY_PRICE_CAP)
        # Fire when mid has dropped to (or below) strategy_price * (1 + margin).
        trigger_line = strategy_price * (1.0 + margin)

        # CLOB minimum order size is 5 shares. Catch a misconfigured
        # TIER1_NOTIONAL here instead of POSTing a guaranteed rejection on
        # every 250 ms tick.
        min_shares = 5.0
        if (
            strategy_price > 0
            and s.TIER1_NOTIONAL / strategy_price < min_shares - 1e-9
        ):
            self._t1_skip_log(
                "engine.t1_skip_below_min_size",
                side="BOTH",
                notional=s.TIER1_NOTIONAL,
                strategy_price=strategy_price,
                min_shares=min_shares,
            )
            return

        for side, token in [("UP", up_tok), ("DOWN", down_tok)]:
            mid_price = self._pc.get_midpoint(token)
            if mid_price <= 0:
                self._t1_skip_log(
                    "engine.t1_skip_no_mid", side=side, token=token[:16],
                    detail="midpoint cache empty (REST /midpoint poll hasn't seeded it yet)",
                )
                continue

            # ── Pre-arm trigger: fire when mid <= strategy_price * (1 + margin) ──
            # We do NOT require mid <= strategy_price exactly — the new model
            # pre-arms the GTD ahead of time so it can sit on the book.
            if mid_price > trigger_line:
                self._t1_skip_log(
                    "engine.t1_skip_too_expensive",
                    side=side, token=token[:16],
                    mid_price=mid_price, cap=trigger_line,
                    strategy_price=strategy_price,
                    detail="pre-arm: mid > strategy_price * (1 + PRICE_PRE_MARGIN)",
                )
                continue

            # ── T1 entry time gate ───────────────────────────────────────
            # T1 must enter within T1_ENTRY_WINDOW_S of window open.
            # This prevents chasing at the very end of the 15-min window.
            elapsed_in_window = time.time() - self._round.round_start_ts
            if elapsed_in_window > s.T1_ENTRY_WINDOW_S:
                # Log exactly ONCE per round. This check runs on every
                # decision tick (~4 Hz); the old 60 s throttle still emitted
                # one INFO line per minute for the rest of the window
                # (production 2026-10-30: 8 identical lines in one round),
                # contradicting the "log once" intent.
                round_ts = self._round.round_start_ts
                if getattr(self, "_t1_gate_closed_round_ts", None) != round_ts:
                    self._t1_gate_closed_round_ts = round_ts
                    log.info(
                        "engine.t1_gate_closed",
                        elapsed_s=int(elapsed_in_window),
                        limit_s=int(s.T1_ENTRY_WINDOW_S),
                    )
                continue

            # BTC drift gate
            if not self._gate.can_enter(bc_now, mid_price):
                gs = self._gate.state(bc_now)
                self._t1_skip_log(
                    "engine.t1_skip_drift",
                    side=side, mid_price=mid_price,
                    drift=gs.drift, within_limit=gs.within_limit,
                )
                continue

            # Stake cap check — don't overcommit
            if pos.stake_cap_remaining < s.TIER1_NOTIONAL:
                self._t1_skip_log(
                    "engine.t1_skip_cap_reached",
                    side=side,
                    stake_remaining=pos.stake_cap_remaining,
                    needed=s.TIER1_NOTIONAL,
                )
                continue

            log.info(
                "engine.trigger_t1",
                side=side,
                token=token[:16],
                mid=mid_price,
                trigger_line=trigger_line,
                strategy_price=strategy_price,
                drift=self._gate.state(bc_now).drift,
            )

            # H-1 v2 fix: claim the entry slot BEFORE awaiting the handler.
            # Previously `_round.position.entry_token` was written by the
            # router only AFTER `await place_buy` resolved — during that
            # yield window a concurrent `_evaluate` tick could observe
            # `entry_token is None` and emit a duplicate ActionOpenT1.
            # Writing it here, under the same `_eval_inflight` guard that
            # is still held, closes the race. The router will skip its own
            # `entry_token = action.token` write because the field is no
            # longer None (idempotent overwrite with the same value).
            #
            # H-NEW-3 fix: `baseline_target_price` is now anchored inside
            # `bind_server_order_id` to the actual adjusted_price (the price
            # the CLOB accepted). Previously it was set here to the
            # WS-cached bid which can be stale by 200-500ms vs the REST
            # orderbook the CLOB saw. The bind-server-id path runs after
            # the order has been posted, so we get a more accurate anchor.
            pos.entry_side = side
            pos.entry_token = token
            # H-V6-15: if we hit a clean T1 entry while a price-cap
            # rejection was happening on the same side (e.g. mid dropped
            # below cap during the round), flush that summary now so the
            # operator sees the cap-spell duration in INFO alongside the
            # actual entry decision.
            self._t1_cap_reject_summary()
            # H-NEW-2026-07-11: pass `strategy_price` (not bid_price).
            # The GTD is posted at this exact limit; the watcher waits
            # for the maker to be lifted by a taker crossing strategy_price.
            await self._handler(ActionOpenT1(
                side=side,
                token=token,
                price=strategy_price,
                notional=s.TIER1_NOTIONAL,
            ))
            return  # enter only once per round

    async def _check_average_down(
        self, bc_now: float, snap: PositionSnapshot | None = None
    ) -> None:
        """
        Check if the next averaging tier should be triggered.

        H-NEW-2026-07-11 refactor — pre-arm trigger model:

          Anchor       : `t1_entry` = T1 actual fill-weighted avg
                         (NOT BTC baseline, NOT T1 pre-fill cap)
          Trigger line : `strategy_price = t1_entry * (1 - tier_drop_pct)`
          Fire line    : mid <= strategy_price * (1 + PRICE_PRE_MARGIN)
                         (e.g. T2 with t1_entry=0.40, drop=20%, margin=10%
                              → strategy_price=0.32, fire when mid <= 0.352)
          GTD posted at: strategy_price exactly

        H-NEW-2026-07-15 (audit follow-up — naming): the parameter
        previously called `baseline` here was the T1 entry price (token
        market), which has no relation to the `GateKeeper.btc_baseline`
        used for drift checks. They shared a name historically; renamed
        the averaging anchor to `t1_entry` for clarity. See `gatekeeper.py`
        for the BTC-vs-token contrast.

        This matches the T1 pre-arm design. The bot no longer waits until
        mid crosses strategy_price before posting the GTD; it posts early
        and the maker waits for takers to come up.

        Mutex rules:
          - Skip a tier if it's already complete (`is_complete=True`)
          - Skip a tier if it's currently being watched (`watching=True`)
          - Trigger only if `placed=False, watching=False`

        Refactor: replaced the `tier.placed` mutex with `tier.watching`
        (set by the watcher spawn, cleared when the watcher terminates).
        `placed=True` alone no longer holds the slot — the GTD may have
        been cancelled / expired while the watcher is still terminating.

        H-NEW-2026-07-15: takes a `PositionSnapshot`. Most of the
        baseline-resolution chain is unchanged; we just consult the
        snapshot for `baseline_realized_price` / `baseline_target_price`
        instead of reading `pos` repeatedly.
        """
        if not self._round or not self._gate:
            return

        pos = self._round.position
        if snap is None:
            snap = pos.snapshot()
        if snap.entry_token is None:
            return

        # Feed-health / volatility guards: no averaging into a data outage
        # or a flash move (T2/T3's momentum bypass must not fight this).
        if self._btc_feed_stale():
            self._btc_guard_skip("tier", "btc_stale")
            return
        if self._btc_vol_blocked():
            self._btc_guard_skip("tier", "btc_vol_spike")
            return

        # NOTE: legacy `Position.baseline_set` / `Position.baseline_price`
        # fields removed entirely (H-NEW-2026-07-15). Previously the
        # `baseline_set` flag was only flipped in `record_fill` (the WS
        # fill path), but the TierWatcher fast-fill path bypassed
        # `record_fill` and went straight into `apply_tier_watcher_result`,
        # which set `baseline_set_realized` but NOT `baseline_set`.
        # Averaging-down was therefore stuck in the first-branch
        # early-return for every round whose T1 filled via the fast-fill
        # path (production saw uPnL -46% with no T2 trigger). The
        # replacement `baseline_set_realized` anchor is written by ALL
        # fill paths. The legacy fields had no remaining reader anywhere
        # in the engine, so they were removed entirely. Tick-level drift
        # gating is the caller's responsibility (see _evaluate line ~595).

        if snap.tp_inflight:
            return

        # A fully closed position (take-profit sold everything) must not
        # re-enter through the averaging path. T1 re-entry is already
        # blocked by `entry_token` (kept until the next round), but T2..T4
        # only inspect their own tier state — without this guard a mid
        # drop back into the next-tier zone AFTER a TP exit would open a
        # fresh position with no T1 base in the same window.
        if snap.total_qty <= 1e-9:
            return

        mid_price = self._pc.get_midpoint(pos.entry_token)
        if mid_price <= 0:
            return

        # ── T1 entry-anchor resolution ────────────────────────────────────
        # Refill thresholds are measured from the realised T1 entry price
        # (token price, NOT BTC/USD). T2/T3/T4 averaging must wait for at
        # least one confirmed T1 fill so the anchor reflects what was
        # actually paid (vs the pre-fill limit price, which can be stale
        # by 200-500ms when T1 fills quickly).
        #
        # Resolution priority:
        #   1. baseline_realized_price — T1 fill-weighted avg (preferred)
        #   2. baseline_target_price   — pre-fill intended bid (only used
        #      when no fill has arrived yet; covers the common case where
        #      T1 and T2 both fire on the same tick)
        #   3. (REMOVED) legacy baseline_price — was a stale price from a
        #      previous strategy design that has since been deprecated.
        #      Falling back to it gave prod window 1783819800 its T2 fire
        #      at baseline=0.36 even when T1 had not filled.
        if snap.baseline_set_realized and snap.baseline_realized_price is not None:
            t1_entry = snap.baseline_realized_price
            if not getattr(pos, "_t1_rebased_logged", False):
                log.info(
                    "engine.t1_rebased",
                    side=pos.entry_side,
                    t1_target=snap.baseline_target_price,
                    t1_realized=snap.baseline_realized_price,
                    tick_size=pos.tiers[0].size_expected if pos.tiers else None,
                    note="T2/T3/T4 trigger lines now anchored to actual T1 "
                         "fill avg (weighted across partials) instead of "
                         "intended bid at T1 decision time",
                )
                pos._t1_rebased_logged = True
        elif snap.baseline_target_price is not None:
            # No realised fill yet — use the pre-fill T1 intended bid.
            t1_entry = snap.baseline_target_price
        else:
            log.info(
                "engine.tN_skip_t1_not_yet_filled",
                side=pos.entry_side,
                t1_target=snap.baseline_target_price,
                note="averaging-down has no resolvable T1 anchor; "
                     "loop returns until T1 first fill lands",
            )
            return
        pos._last_used_t1_entry = t1_entry
        s = self._s
        margin = self._get_price_pre_margin()

        for tier in pos.tiers[1:]:  # T2, T3, T4
            # H-NEW-2026-07-12 (operator clarification, prod window 1783819800):
            # T2/T3/T4 averaging MUST wait for at least one confirmed T1 fill
            # before evaluating. The 30%/60% drop is measured from the
            # T1 entry price (which we only know after T1 has filled),
            # not from the T1 strategy/intended bid.
            #
            # In prod 1783819800, T1 and T2 both fired on the same tick
            # because the engine `_evaluate` loop saw `mid=0.215 <=
            # strategy_price*(1+margin)` for both tiers. T1 filled at
            # 0.22 but T2's strategy_price had been computed against the
            # T1 pre-fill limit (0.36), so T2 ended up bidding at 0.252 —
            # ~50% above the operator's intended 30%-drop-from-entry
            # trigger (~0.154). The per-tier guard below prevents T2/T3
            # from evaluating at all until T1 has produced a fill.
            #
            # R3 (audit 2026-07-12): the early-out here is a SILENT
            # continue. The function-top check at line ~1010 already logs
            # `engine.tN_skip_t1_not_yet_filled` once per (round, side)
            # when `baseline` falls back to baseline_target_price; emitting
            # a per-tier throttled skip HERE as well produced redundant
            # ~4 Hz log rows (T2 + T3 each throttled at 5s) that just
            # repeated the top-level breadcrumb. Operators reading the
            # diagnostic trace were confused into thinking the per-tier
            # skip was a different condition. We keep the guard (the
            # behavior is unchanged) but drop the per-tier skip log.
            if not snap.baseline_set_realized or snap.baseline_realized_price is None:
                continue
            if tier.is_complete:
                # H-NEW-2026-07-11 (audit v6-12): was silent. Without this
                # breadcrumb an operator cannot distinguish "tier already
                # filled, skip" from "tier watching in-flight, skip" in a
                # single log line.
                self._tN_skip_log(
                    "engine.tN_skip_complete",
                    tier=tier.tier, filled=tier.filled,
                    placed_qty=tier.placed_qty, size_filled=tier.size_filled,
                    size_expected=tier.size_expected,
                )
                continue
            # H-NEW-2026-07-11: `watching` is the new in-flight mutex.
            # Skip if a watcher is still actively polling this tier.
            if tier.watching:
                # H-NEW-2026-07-11 (audit v6-12): was silent. This is a
                # particularly important breadcrumb because a stuck
                # `watching=True` (e.g. after an unhandled exception in the
                # watcher task) blocks ALL subsequent attempts at this tier
                # for the rest of the round.
                self._tN_skip_log(
                    "engine.tN_skip_watching",
                    tier=tier.tier, server_id=(tier.server_order_id or "")[:16],
                    expires_at=tier.expires_at, size_filled=tier.size_filled,
                )
                continue
            # M-V4-2 fallback: stale placed=True without watcher (e.g.
            # bind failed and watcher never spawned). Reset and re-fire.
            if tier.placed and not tier.server_order_id:
                log.warning(
                    "engine.tier_stale_placed_reset",
                    tier=tier.tier,
                    size_filled=tier.size_filled,
                    cost_filled=tier.cost_filled,
                )
                tier.placed = False
                tier.expires_at = 0.0

            # Post-failure backoff (set by the router when this tier's POST
            # is rejected). Prevents a 4 Hz POST/retry loop.
            if time.time() < _ts(getattr(tier, "retry_after_ts", 0.0)):
                continue

            strategy_price = tier.trigger_price(t1_entry)
            if strategy_price is None:
                # H-NEW-2026-07-11 (audit v6-12): was silent. Should never
                # happen for T2..T4 because tier_drop_pcts has entries for
                # those tiers, but surface it if it does.
                self._tN_skip_log(
                    "engine.tN_skip_no_trigger_price",
                    tier=tier.tier, t1_entry=t1_entry,
                    drop_pcts=s.tier_drop_pcts,
                )
                continue

            # Pre-arm fire line: mid <= strategy_price * (1 + margin).
            fire_line = strategy_price * (1.0 + margin)
            if mid_price > fire_line:
                # H-NEW-2026-07-11 (audit v6-12): was silent. This is the
                # single most common reason averaging does not fire — the
                # mid has not yet dropped enough to cross the pre-arm
                # fire line. With the breadcrumb operators can see "we are
                # waiting for mid ≤ X, currently at Y".
                self._tN_skip_log(
                    "engine.tN_skip_price_above_fire",
                    tier=tier.tier, mid=mid_price, fire_line=fire_line,
                    strategy_price=strategy_price, t1_entry=t1_entry,
                    margin=margin,
                )
                continue

            # BTC drift gate for averaging.
            next_drop = TierSnapshot._next_tier_drop(tier.tier)
            if not self._gate.can_double_down(bc_now, mid_price, tier.tier, next_drop, t1_entry):
                # T4-only time-based bypass (operator decision 2026-10-31):
                # T4 has no momentum bypass, so a trending window blocks the
                # last/largest tier exactly when the token crashes into its
                # zone. With enough window time left for a recovery, place
                # T4 at its normal 60%-drop trigger despite the drift FAIL.
                if tier.tier == 4 and self._t4_time_bypass_active():
                    self._log_t4_time_bypass(mid_price, t1_entry)
                    # fall through: budget / min-size / stake-cap checks below
                else:
                    # H-NEW-2026-07-11 (audit v6-12): was silent. Drift
                    # blocking averaging is rare (the gate has a bypass once
                    # price has dropped past the next-tier zone) but when it
                    # happens the operator needs to see why.
                    gs = self._gate.state(bc_now)
                    self._tN_skip_log(
                        "engine.tN_skip_drift_gate",
                        tier=tier.tier, mid=mid_price, t1_entry=t1_entry,
                        drift=gs.drift, within_limit=gs.within_limit,
                        bc_now=bc_now, next_drop=next_drop,
                    )
                    continue

            # Budget for THIS placement = the tier's planned USD minus what
            # has already been filled on it. This is the invariant that makes
            # re-arms after a partial fill/cancel safe: a tier can never
            # spend more than its configured notional in total.
            #
            # (The old code always used the full `tier.notional`, so a tier
            # that partially filled, got cancelled, and later re-armed could
            # spend ~1.5-2x its budget.)
            order_notional = max(0.0, tier.notional - tier.cost_filled)
            if order_notional <= 1e-9:
                self._tN_skip_log(
                    "engine.tN_skip_budget_exhausted",
                    tier=tier.tier,
                    planned=tier.notional,
                    spent=tier.cost_filled,
                )
                continue

            # CLOB rejects orders below 5 shares. Catch it here so we do not
            # POST a guaranteed rejection on every tick.
            min_shares = 5.0
            if (
                strategy_price > 0
                and order_notional / strategy_price < min_shares - 1e-9
            ):
                self._tN_skip_log(
                    "engine.tN_skip_below_min_size",
                    tier=tier.tier,
                    notional=order_notional,
                    strategy_price=strategy_price,
                    min_shares=min_shares,
                )
                continue

            if pos.stake_cap_remaining < order_notional:
                log.debug(
                    "engine.tN_skip_cap",
                    tier=tier.tier, remaining=pos.stake_cap_remaining, needed=order_notional,
                )
                continue

            log.info(
                "engine.trigger_tN",
                tier=tier.tier,
                mid=mid_price,
                fire_line=fire_line,
                strategy_price=strategy_price,
                t1_entry=t1_entry,
                notional=order_notional,
            )

            # Same pre-arm logic as T1: post at the configured strategy price.
            await self._handler(ActionOpenTN(
                tier=tier.tier,
                token=pos.entry_token,
                price=strategy_price,
                notional=order_notional,
                trigger_price=fire_line,
            ))
            return  # place one tier at a time

    async def _check_take_profit(
        self, bc_now: float, snap: PositionSnapshot | None = None
    ) -> None:
        """Check if take-profit should trigger.

        H-NEW-2026-07-11 refactor — pre-arm trigger model (SELL side):

          strategy_price = tp_target (e.g. 0.40)
          fire line      = strategy_price * (1 - PRICE_PRE_MARGIN)
                           (e.g. 0.36 when margin=0.10)
          GTD SELL posted at: strategy_price exactly

        Mutex: while a TP sell is in-flight (awaiting CLOB ack / partial fill
        callback), do not enqueue a new TP action. The flag is cleared by the
        next fill event so residual position can re-arm if mid is still
        above the target.

        H-NEW-2026-07-15: takes a `PositionSnapshot`. Behaviour is unchanged;
        we just read state from the snapshot instead of `pos.*`.
        """
        if not self._round or not self._gate:
            return

        pos = self._round.position
        if snap is None:
            snap = pos.snapshot()
        if not snap.entry_token:
            return

        # Position fully closed — a TP that sells the EXACT quantity leaves
        # `_total_qty == 0`, and pro-rata arithmetic can leave a small FP
        # residual. Run the cleanup exactly once: the old
        # `snap.total_qty <= 0` guard returned without cleaning the tier
        # state, so the dashboard kept showing filled tiers after the
        # position was gone (production 2026-10-31 09:02).
        #
        # A TP exit is FINAL for the window: `realize_position_closed`
        # deliberately keeps `entry_token`, so `_check_t1_entry` cannot
        # start another ladder until the next 15-minute round. Re-entering
        # after a profit was judged too risky (operator decision 2026-10-31).
        #
        # IMPORTANT: `_total_qty == 0` is ALSO the state while a freshly
        # decided T1 order is still resting on the book (entry_token is set
        # at decision time, before any fill). Only treat it as "closed" when
        # there is evidence of prior inventory — a tier fill or shares sold
        # by TP. Otherwise a pending T1 would be wiped before it fills.
        qty_now = pos._total_qty
        if isinstance(qty_now, float) and qty_now < 1e-2:
            had_inventory = (
                float(getattr(snap, "tp_sold_qty", 0.0) or 0.0) > 1e-9
                or sum(snap.tier_size_filled) > 1e-9
            )
            if had_inventory and not getattr(pos, "_position_closed", False):
                pos.realize_position_closed(
                    source="engine._check_take_profit",
                    note="total_qty near zero after TP fill; clearing residual state",
                )
            return

        # R6 (audit 2026-07-12): avg_cost sanity gate.
        #
        # tp_target() = _avg_cost * (1 + tp_pct) drives the entire TP fire
        # line. If `_avg_cost` is stale or partially reconciled, the SELL
        # GTD would post at the wrong price — too low and we leave money
        # on the table, too high and the SELL sits unfilled while uPnL
        # bleeds.
        #
        # Cross-check: every tier contributes `tier.size_filled` shares
        # and `tier.cost_filled` USDC. If the TierWatcher has caught fills
        # the WS path missed and reconciled them via
        # `apply_tier_watcher_result`, the per-tier totals reflect them.
        # If we have `_total_qty < sum(tier.size_filled) - slack` the
        # position aggregate is *behind* at least one tier — the TierWatcher
        # is mid-reconcile, and the TP target we would compute is
        # potentially off by the missing cost basis.
        #
        # `slack = 1e-6 * max(_total_qty, 1.0)` tolerates FP rounding
        # without dropping a real 6+ share gap (which would mean a TP
        # fired against a partial position view).
        sum_tier_qty = sum(snap.tier_size_filled)
        qty_slack = max(1e-6 * max(snap.total_qty, 1.0), 1e-9)
        # `record_tp_fill` reduces total_qty but deliberately keeps
        # tier.size_filled intact, so shares already sold by an earlier
        # (partial) TP must be added back before declaring the tiers
        # unreconciled. Production 2026-10-30: T1's 5-share TP sale left
        # sum(tier fills)=37.65 vs total_qty=32.65; the old check read the
        # 5-share difference as "watcher lag" and blocked every later TP
        # for the rest of the round.
        # getattr: older test snapshots (SimpleNamespace stubs) predate the
        # field; a missing value means "nothing sold yet".
        tp_sold_qty = float(getattr(snap, "tp_sold_qty", 0.0) or 0.0)
        expected_qty = snap.total_qty + tp_sold_qty
        if sum_tier_qty - expected_qty > qty_slack:
            log.warning(
                "engine.tp_blocked_avg_cost_unreconciled",
                total_qty=snap.total_qty,
                tp_sold_qty=tp_sold_qty,
                sum_tier_qty=sum_tier_qty,
                qty_gap=sum_tier_qty - expected_qty,
                tiers_qty={
                    f"T{t.tier}": {
                        "placed_qty": t.placed_qty,
                        "size_filled": t.size_filled,
                        "watching": t.watching,
                        "is_complete": t.is_complete,
                    } for t in pos.tiers
                },
                note="tp_target is _avg_cost-driven; deferring TP until "
                     "TierWatcher reconciles the missing shares (R6 audit).",
            )
            return

        # H-NEW-2026-07-11 (audit v6-12 production revisit): backoff after
        # a TP attempt that POSTed but was rejected by the CLOB (typically
        # `not enough balance / allowance`). Without this, every
        # `_evaluate` tick re-fires trigger_tp, POSTs another SELL,
        # receives the same 400, and the cycle never terminates —
        # production saw 11 retries in <3 seconds.
        # Tolerate tests that construct engine via __new__ without
        # running __init__.
        last_err = getattr(self, "_tp_last_post_error_ts", 0.0) or 0.0
        backoff = getattr(self, "_tp_backoff_s", 5.0) or 5.0
        now = time.time()

        # H-NEW-2026-07-12 (P0-1 fix per prod window 1783818900): when the
        # bot was stuck in TP-POST-error backoff for 60s, the price often
        # drifted back BELOW the fire line by the time the backoff elapsed
        # (the TP window closed while we waited on a flat 60s gate). Now
        # we observe the live mid FIRST and skip even the backoff window
        # if the opportunity is gone — saving both the wasted retry and
        # any unnecessary 400-spam from a CLOB that would now reject for a
        # different reason (price too far above best bid, for instance).
        # Doing this BEFORE the backoff gate also lets us naturally reset
        # the failure counter — there's no point keeping "3 failures" on
        # the books for an opportunity that has expired.
        if pos.entry_token:
            cur_mid_pre = self._pc.get_midpoint(pos.entry_token)
            if cur_mid_pre > 0:
                tp_target_pre = snap.tp_target
                if tp_target_pre is not None:
                    margin_pre = self._get_price_pre_margin()
                    hysteresis_pre = self._get_cancel_hysteresis()
                    fire_line_pre = tp_target_pre * (1.0 - margin_pre)
                    # Cancel only once mid is hysteresis-deeper below the
                    # fire line: posting and cancelling on the same line
                    # made a mid hovering around it flap the TP GTD.
                    cancel_line_pre = fire_line_pre * (1.0 - hysteresis_pre)
                    if cur_mid_pre < cancel_line_pre:
                        # Opportunity retreated — clear backoff and bail.
                        #
                        # H-NEW-2026-07-15 (audit follow-up): release the
                        # in-flight mutex in lockstep with the backoff
                        # reset. Without this, an earlier
                        # `_check_take_profit` could have set
                        # `_tp_inflight=True` while it waited on the
                        # backoff timer (see `notify_tp_post_error`), so
                        # clearing only the backoff fields leaves the
                        # TP path wedged in the next tick
                        # (`_tp_inflight` would short-circuit
                        # `trigger_tp`). Reset both as an atomic
                        # state-machine transition.
                        prev_inflight = bool(pos._tp_inflight)
                        # `tp_server_order_id` resolves from the open TP
                        # snapshot now (partial fills clear `_tp_inflight`
                        # while the GTD is still live), so this is the
                        # authoritative live TP order id.
                        prev_tp_sid = snap.tp_server_order_id
                        if prev_inflight:
                            pos._tp_inflight = False
                        if getattr(self, "_tp_post_failure_count", 0) > 0 or prev_inflight:
                            log.info(
                                "engine.tp_backoff_reset_no_opportunity",
                                failure_count=getattr(self, "_tp_post_failure_count", 0),
                                prev_tp_inflight=prev_inflight,
                                tp_server_order_id=(
                                    prev_tp_sid[:16] if prev_tp_sid else None
                                ),
                                mid=cur_mid_pre,
                                fire_line=fire_line_pre,
                                cancel_line=cancel_line_pre,
                                note="mid retreated below fire-line during TP-backoff window; "
                                     "no point retrying with progressive backoff",
                            )
                        self._tp_last_post_error_ts = 0.0
                        self._tp_post_failure_count = 0
                        self._tp_backoff_s = 5.0
                        # H-NEW-2026-07-15 (audit follow-up): when an
                        # in-flight TP GTD is alive on the CLOB but the
                        # opportunity has retreated, cancel the GTD
                        # instead of letting it sit for the full
                        # GTD_LIFETIME_S. Without this the GTD blocks
                        # the maker allowance for up to 5 minutes while
                        # uPnL bleeds. The router uses `tier=5` as a
                        # sentinel meaning "this is a TP GTD, not a BUY
                        # tier"; on its way out it releases `_tp_inflight`
                        # so the next tick can re-evaluate cleanly.
                        if (
                            prev_tp_sid
                            and time.time()
                            >= _ts(getattr(pos, "tp_cancel_retry_after_ts", 0.0))
                        ):
                            try:
                                await self._handler(ActionCancelTier(
                                    tier=-1,               # sentinel for TP GTD
                                    server_order_id=prev_tp_sid,
                                    reason="tp_mid_retreated",
                                ))
                            except Exception as exc:
                                # Defensive: if the router dispatch fails,
                                # we still released the mutex — next tick
                                # can re-fire. Log so operators see it.
                                log.warning(
                                    "engine.tp_retreated_cancel_dispatch_failed",
                                    tp_server_order_id=prev_tp_sid[:16],
                                    error=str(exc),
                                    note="mutex already released; the "
                                         "GTD may sit on the CLOB until "
                                         "natural expiry",
                                )
                        return

        # Backoff gate (AFTER the mid-retreat check above, so a live TP
        # GTD can still be cancelled while a retry is backing off).
        if last_err > 0.0 and now - last_err < backoff:
            return

        if snap.tp_inflight:
            return

        # A registered TP GTD may still be live on the CLOB even after a
        # partial fill cleared `_tp_inflight` (record_tp_fill releases the
        # mutex for re-arming). Posting a second TP for the residual while
        # the first order still has shares reserved would oversell the
        # position. Wait for the watcher/cancel path to close the snapshot.
        if getattr(pos, "has_open_tp_orders", False):
            return

        # H-V5-1 fix: TP must wait for any in-flight averaging tier to settle.
        if snap.has_inflight_tier:
            return

        mid_price = self._pc.get_midpoint(pos.entry_token)
        if mid_price <= 0:
            return

        tp_target = snap.tp_target
        if tp_target is None:
            return

        # Pre-arm fire line: mid >= tp_target * (1 - margin).
        # The TP GTD posts at tp_target exactly; we just wait for mid to
        # reach the pre-arm zone.
        # get_price_pre_margin() is a module-level helper so tests that
        # construct engine via __new__ (bypassing __init__) still work.
        margin = self._get_price_pre_margin()
        fire_line = tp_target * (1.0 - margin)

        if mid_price < fire_line:
            return

        # The BTC drift gate is an ENTRY risk filter. Applying it to an
        # exit blocks profit-taking exactly when BTC has moved — production
        # 2026-10-30: mid crossed the TP fire line while drift was FAIL, the
        # SELL was never posted, and the token then collapsed ~60%. Default
        # off; `TP_REQUIRE_DRIFT_GATE=true` restores the old behaviour.
        if self._get_tp_require_drift_gate() and not self._gate.can_act(bc_now):
            round_ts = getattr(self._round, "round_start_ts", 0)
            if getattr(self, "_tp_drift_skip_round_ts", None) != round_ts:
                self._tp_drift_skip_round_ts = round_ts
                gs = self._gate.state(bc_now)
                log.info(
                    "engine.tp_skip_drift_gate",
                    drift=gs.drift,
                    mid=mid_price,
                    fire_line=fire_line,
                    note="TP suppressed by BTC drift gate "
                         "(TP_REQUIRE_DRIFT_GATE=true)",
                )
            return

        # Set mutex BEFORE emitting the action so re-entrant _evaluate()
        # calls (called again from any subsequent token/bnb tick before the
        # TP sell's fill callback lands) do not stack duplicate actions.
        pos._tp_inflight = True

        log.info(
            "engine.trigger_tp",
            mid=mid_price,
            fire_line=fire_line,
            tp_target=tp_target,
            tp_pct=pos.tp_pct_for_current_state(),
            avg_cost=snap.avg_cost,
            qty=snap.total_qty,
        )

        # H-NEW-2026-07-11: pass tp_target as the GTD limit (not bid_price).
        # The SELL GTD posts at tp_target exactly; the watcher waits for
        # the maker to be lifted by a taker crossing tp_target.
        #
        # H-NEW-2026-07-12 (mutex-leak guard): the post-mutex is held
        # across the action emission so that a re-entrant `_evaluate`
        # tick cannot stack a duplicate TP within the same `_eval_inflight`
        # window. On the SUCCESS path the watcher (or router
        # `notify_tp_post_error`) is responsible for releasing it. On
        # ANY exception path (including `asyncio.CancelledError`,
        # subclass of `BaseException`) we release it here so a bot-side
        # crash, router-dispatch failure, or task cancellation cannot
        # wedge `_check_average_down` into the silent-return path for
        # the rest of the round.
        try:
            await self._handler(ActionTakeProfit(
                token=pos.entry_token,
                price=tp_target,
                qty=snap.total_qty,
            ))
        except BaseException:
            # Any failure (Exception or CancelledError) — drop the mutex
            # before propagating so the next tick can re-evaluate. The
            # CLOB order, if one was already POSTed, lives on until the
            # watcher (or GTD expiry) tears it down — this guard is only
            # about the in-memory state machine.
            pos._tp_inflight = False
            raise
