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

import asyncio
import time
from dataclasses import dataclass
from typing import Callable, Literal, Protocol

from src.config import strategy as _s
from src.feeds.binance_spot import BinanceCache
from src.market.models import MarketInfo
from src.market.market_ws import PriceCache
from src.strategy.gatekeeper import GateKeeper
from src.strategy.round_state import Position, RoundState, TierSnapshot
from src.infra.logging import get_logger

log = get_logger(__name__)


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
    ) -> None:
        self._bc = binance_cache
        self._pc = price_cache
        self._handler = handler
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
        self._tp_backoff_s: float = 60.0

    # H-NEW-2026-07-11: helper that reads PRICE_PRE_MARGIN safely even for
    # tests that construct engine via __new__ (bypassing __init__).
    def _get_price_pre_margin(self) -> float:
        if hasattr(self, "_s") and self._s is not None:
            return float(getattr(self._s, "PRICE_PRE_MARGIN", 0.0) or 0.0)
        # Fallback to config directly for test stubs.
        return float(getattr(_s(), "PRICE_PRE_MARGIN", 0.0) or 0.0)

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

        Always overwrites whatever the gate was — safer than guarding on
        `_round.round_start_ts` because WM sometimes re-fires the same
        window's K-line value a few seconds later (K-line backtrack lag).
        """
        self._gate = GateKeeper(baseline)
        log.info("engine.baseline_set", baseline=baseline)

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
        self._tp_last_post_error_ts = time.time()
        if self._round is not None:
            self._round.position._tp_inflight = False
        log.warning(
            "engine.tp_backoff_engaged",
            reason=reason,
            backoff_s=self._tp_backoff_s,
            note="trigger_tp suppressed until backoff elapses",
        )

    def notify_tp_recovered(self) -> None:
        """Called by router when a TP SELL POST succeeded (or was
        invalidated by external sync). Clears the backoff gate so the
        next `_check_take_profit` tick can re-arm TP if still eligible.
        """
        self._tp_last_post_error_ts = 0.0

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

    def on_order_cancelled(self, server_order_id: str, reason: str = "manual") -> None:
        """
        Called when an order is cancelled (manual, deviation, or cancel-all).
        Resets tier.placed so the strategy can re-arm if conditions still hold.
        """
        if not self._round:
            return
        for tier in self._round.position.tiers:
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
        if placed_size is not None and placed_size > 0:
            t.size_expected = placed_size
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

        # Single-flight: drop re-entry rather than queue it. The next tick
        # will pick up the new state and call _evaluate again.
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

            gate_state = self._gate.state(bc_now)

            new_state = "PASS" if gate_state.within_limit else "FAIL"
            if self._round.gate_last_state != new_state:
                self._round.gate_last_state = new_state
                log.info("engine.gate", drift=gate_state.drift, state=new_state)

            if not gate_state.within_limit:
                return  # drift out of bounds — no new orders

            pos = self._round.position

            # ── Not yet entered → check T1 entry ────────────────────────────
            if pos.entry_token is None:
                await self._check_t1_entry(bc_now)

            # ── In position → check averaging-down and take-profit ───────────
            if pos.entry_token is not None:
                await self._check_average_down(bc_now)
                await self._check_take_profit(bc_now)
        finally:
            self._eval_inflight = False

    async def _check_t1_entry(self, bc_now: float) -> None:
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
        """
        if not self._round or not self._gate or not self._round.up_token:
            return

        pos = self._round.position
        if pos.entry_token is not None:
            return  # already entered

        s = self._s
        up_tok = self._round.up_token
        down_tok = self._round.down_token

        margin = float(getattr(s, "PRICE_PRE_MARGIN", 0.0) or 0.0)
        # Strategy price = ENTRY_PRICE_CAP; the new model pre-arms at this
        # level. We use ENTRY_PRICE_CAP as the "design price" so the trigger
        # margin is consistent with the price-cap semantics.
        strategy_price = float(s.ENTRY_PRICE_CAP)
        # Fire when mid has dropped to (or below) strategy_price * (1 + margin).
        trigger_line = strategy_price * (1.0 + margin)

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
                # Throttle: log once when gate closes, then stay quiet
                now = time.time()
                last = getattr(self, "_t1_gate_closed_log_ts", 0.0)
                if now - last > 60:
                    self._t1_gate_closed_log_ts = now
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

    async def _check_average_down(self, bc_now: float) -> None:
        """
        Check if the next averaging tier should be triggered.

        H-NEW-2026-07-11 refactor — pre-arm trigger model:

          Trigger line : `strategy_price = baseline * (1 - tier_drop_pct)`
          Fire line    : mid <= strategy_price * (1 + PRICE_PRE_MARGIN)
                         (e.g. T2 with baseline=0.40, drop=20%, margin=10%
                              → strategy_price=0.32, fire when mid <= 0.352)
          GTD posted at: strategy_price exactly

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
        """
        if not self._round or not self._gate:
            return

        pos = self._round.position
        if not pos.baseline_set or pos.entry_token is None:
            return

        # ── DIAG-2026-07-11: trace why T2 averaging never fires ────────────
        # Placed BEFORE the silent early-returns so we always see the live
        # state. Remove once the bug is located.
        try:
            _trace_mid = self._pc.get_midpoint(pos.entry_token) if pos.entry_token else 0.0
            _cache_keys = list(self._pc.midpoint.keys())
            log.info(
                "engine.t2_diag",
                entry_token=pos.entry_token[:32] if pos.entry_token else None,
                cache_key_match=pos.entry_token in self._pc.midpoint if pos.entry_token else False,
                cache_keys_count=len(_cache_keys),
                cache_first_key=_cache_keys[0][:32] if _cache_keys else None,
                cache_first_mid=self._pc.midpoint[_cache_keys[0]] if _cache_keys else None,
                mid=_trace_mid,
                tp_inflight=pos._tp_inflight,
                baseline_realized=pos.baseline_realized_price,
                baseline_set_realized=pos.baseline_set_realized,
                baseline_target=pos.baseline_target_price,
                tier2_placed=pos.tiers[1].placed,
                tier2_watching=pos.tiers[1].watching,
                tier2_is_complete=pos.tiers[1].is_complete,
                tier2_server_id=pos.tiers[1].server_order_id,
            )
        except Exception as _diag_exc:
            log.warning("engine.t2_diag.error", error=str(_diag_exc))

        if pos._tp_inflight:
            return

        mid_price = self._pc.get_midpoint(pos.entry_token)
        if mid_price <= 0:
            return

        # Baseline anchoring (unchanged from H-V6-14 — realized price wins
        # once T1 has any fill, else target/legacy fallback).
        if pos.baseline_set_realized and pos.baseline_realized_price is not None:
            new_baseline = pos.baseline_realized_price
            if not getattr(pos, "_baseline_rebased_logged", False):
                log.info(
                    "baseline.rebased",
                    side=pos.entry_side,
                    from_target=pos.baseline_target_price,
                    from_realized=pos.baseline_realized_price,
                    tick_size=pos.tiers[0].size_expected if pos.tiers else None,
                    note="T2/T3/T4 trigger lines now anchored to actual T1 "
                         "fill avg (weighted across partials) instead of "
                         "intended bid at T1 decision time",
                )
                pos._baseline_rebased_logged = True
            baseline = new_baseline
        else:
            baseline = pos.baseline_target_price or pos.baseline_price
        pos._last_used_baseline_value = baseline
        s = self._s
        margin = self._get_price_pre_margin()

        for tier in pos.tiers[1:]:  # T2, T3, T4
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

            if pos.tier_is_saturated(tier):
                log.warning(
                    "engine.tier_saturated_skip",
                    tier=tier.tier,
                    cost_filled=tier.cost_filled,
                    cap=tier.notional * s.TIER_NOTIONAL_MAX_MULT,
                    planned=tier.notional,
                )
                continue

            strategy_price = tier.trigger_price(baseline)
            if strategy_price is None:
                # H-NEW-2026-07-11 (audit v6-12): was silent. Should never
                # happen for T2..T4 because tier_drop_pcts has entries for
                # those tiers, but surface it if it does.
                self._tN_skip_log(
                    "engine.tN_skip_no_trigger_price",
                    tier=tier.tier, baseline=baseline,
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
                    strategy_price=strategy_price, baseline=baseline,
                    margin=margin,
                )
                continue

            # BTC drift gate for averaging.
            next_drop = TierSnapshot._next_tier_drop(tier.tier)
            if not self._gate.can_double_down(bc_now, mid_price, tier.tier, next_drop, baseline):
                # H-NEW-2026-07-11 (audit v6-12): was silent. Drift
                # blocking averaging is rare (the gate has a bypass once
                # price has dropped past the next-tier zone) but when it
                # happens the operator needs to see why.
                gs = self._gate.state(bc_now)
                self._tN_skip_log(
                    "engine.tN_skip_drift_gate",
                    tier=tier.tier, mid=mid_price, baseline=baseline,
                    drift=gs.drift, within_limit=gs.within_limit,
                    bc_now=bc_now, next_drop=next_drop,
                )
                continue

            # Per-tier remaining cap (defense-in-depth)
            tier_remaining = pos.tier_notional_remaining(tier)
            if tier_remaining < tier.notional * 0.5:
                log.warning(
                    "engine.tier_remaining_low_skip",
                    tier=tier.tier, tier_remaining=tier_remaining,
                )
                continue

            if pos.stake_cap_remaining < tier.notional:
                log.debug(
                    "engine.tN_skip_cap",
                    tier=tier.tier, remaining=pos.stake_cap_remaining, needed=tier.notional,
                )
                continue

            log.info(
                "engine.trigger_tN",
                tier=tier.tier,
                mid=mid_price,
                fire_line=fire_line,
                strategy_price=strategy_price,
                baseline=baseline,
                notional=tier.notional,
            )

            # H-NEW-2026-07-11: pass strategy_price (not bid_price).
            # Same pre-arm logic as T1.
            await self._handler(ActionOpenTN(
                tier=tier.tier,
                token=pos.entry_token,
                price=strategy_price,
                notional=tier.notional,
                trigger_price=fire_line,
            ))
            return  # place one tier at a time

    async def _check_take_profit(self, bc_now: float) -> None:
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
        """
        if not self._round or not self._gate:
            return

        pos = self._round.position
        if not pos.entry_token or pos.total_qty <= 0:
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
        backoff = getattr(self, "_tp_backoff_s", 60.0) or 60.0
        now = time.time()
        if last_err > 0.0 and now - last_err < backoff:
            return

        if pos._tp_inflight:
            return

        # H-V5-1 fix: TP must wait for any in-flight averaging tier to settle.
        if pos.has_inflight_tier:
            return

        mid_price = self._pc.get_midpoint(pos.entry_token)
        if mid_price <= 0:
            return

        tp_target = pos.tp_target()
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

        if not self._gate.can_act(bc_now):
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
            avg_cost=pos.avg_cost,
            qty=pos.total_qty,
        )

        # H-NEW-2026-07-11: pass tp_target as the GTD limit (not bid_price).
        # The SELL GTD posts at tp_target exactly; the watcher waits for
        # the maker to be lifted by a taker crossing tp_target.
        await self._handler(ActionTakeProfit(
            token=pos.entry_token,
            price=tp_target,
            qty=pos.total_qty,
        ))
