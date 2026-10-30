"""Round and position state for the strategy engine.

Key changes vs the original:
  - TierSnapshot now distinguishes between *placed*, *partial fill*, and *fully filled*.
  - size_filled accumulates across partial fills; filled flag only flips once
    the expected stake (notional / price) has been delivered.
  - A baseline_tracker records the running weighted-avg fill price so
    averaging tiers can be triggered against the real cost basis.
  - expected_size/expected_cost are recorded at placement time so we know
    when a partial fill is "complete enough".
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from src.config import strategy as _s
from src.infra.logging import get_logger

if TYPE_CHECKING:
    from src.market.models import MarketInfo

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# TierSnapshot
# ---------------------------------------------------------------------------


@dataclass
class TierSnapshot:
    """State for a single martingale tier (T1..T4).

    Lifecycle:
        placed=False, watching=False   -> not yet sent
        placed=True,  watching=True    -> GTD live on CLOB, watcher polling balance
        watching=False, filled=False   -> GTD cancelled/expired, no fill
        filled=True                    -> done (size_filled >= size_expected * 0.995)

    size_filled accumulates across partial fills; filled flag only flips once
    the expected stake has been delivered. The TierWatcher writes
    `placed_qty` from the authoritative CLOB balance so that `is_complete`
    reflects ground truth even when WS fill events are missed.

    Two parallel paths drive `is_complete`:
      - UserWS / CLOBClient fill callbacks → `record_fill` increments
        `size_filled`, which feeds `is_complete`.
      - TierWatcher → sets `filled=True` directly when the watched
        balance reaches the full expected size (`>= expected_size`,
        not 99.5% — the watcher only reports NATURAL terminal
        conditions; mid-fill policy lives in the strategy tick).
        This is the primary path under the new model.

    A tier can be "complete" via either path; both feed the same flag.
    """

    tier: int                         # 1, 2, 3, 4
    notional: float                   # USD amount for this tier
    placed: bool = False
    # H-NEW-2026-07-11: True while a TierWatcher is actively polling this
    # tier's GTD. Set when the watcher spawns, cleared when the watcher
    # terminates. The engine uses `watching` (not `placed`) to decide
    # whether a tier is "in flight" — a cancelled/expired GTD keeps
    # `placed=True` for the cancel-grace window but `watching=False`.
    watching: bool = False
    filled: bool = False
    server_order_id: str | None = None
    avg_fill_price: float | None = None
    size_filled: float = 0.0          # total shares filled (sum of partial fills)
    size_expected: float = 0.0        # expected shares = notional / price at placement
    # H-NEW-2026-07-11: authoritative ground-truth fill count from
    # TierWatcher polling get_token_balance. Always >= size_filled when
    # fills were missed by the WS path. Used by `is_complete` as a
    # second source of truth.
    placed_qty: float = 0.0
    cost_filled: float = 0.0          # running notional of fills = Σ(px * sz)
    placed_at: float = 0.0
    expires_at: float = 0.0
    last_fill_ts: float = 0.0
    cancel_reason: str = ""
    token_id: str | None = None       # bound to the server order so fills can be matched
    # H-NEW-2026-07-15 (audit follow-up): single-flight guard for cancel
    # dispatches. The engine's `_reconcile_inflight_buys` runs every tick
    # (250 ms) and re-evaluates whether an outstanding BUY GTD should be
    # cancelled. Without this guard, the router could issue `cancel`
    # back-to-back across ticks if mid oscillates around the next-tier
    # fire line, and each cancel request would race the previous one at
    # the CLOB. Set to True when an ActionCancelTier is dispatched; the
    # router clears it after `om.cancel_by_server_id` resolves. The
    # engine skips re-emit when this flag is True.
    cancel_pending: bool = False
    # Post-failure backoff: the strategy tick will not attempt to place this
    # tier again before this Unix timestamp. Without it, a rejected POST
    # (CLOB error / min-size / verification failure) is retried every
    # 250 ms tick, producing an order-spam loop.
    retry_after_ts: float = 0.0
    # Cancel-failure backoff: don't re-issue a cancel every 250 ms tick when
    # the CLOB keeps rejecting it. The slot stays reserved (placed=True)
    # until a cancel is confirmed or the order expires.
    cancel_retry_after_ts: float = 0.0
    # Optional: id of the spawned TierWatcher task (for diagnostics only).
    watcher_task: object | None = None

    def trigger_price(self, t1_entry: float) -> float | None:
        """Strategy price for this averaging tier, expressed as a fraction
        of the T1 entry price.

        H-NEW-2026-07-15 (audit follow-up — naming): the parameter is
        `t1_entry` (token T1 fill price), NOT the BTC/binance baseline
        used by `GateKeeper`. The two concepts share a name ("baseline")
        historically but they are unrelated anchors — the gate baseline
        is BTC/USD drift cap, this anchor is the token price. Renamed
        to remove the BTC-vs-token confusion.

        Formula: `t1_entry * (1 - tier_drop_pct)`
          T2: 0.80 * t1_entry   (20% drop)
          T3: 0.60 * t1_entry   (40% drop)
          T4: 0.40 * t1_entry   (60% drop)
        Returns None for tier 1 (no averaging drop defined).
        """
        cfg = _s()
        drops = cfg.tier_drop_pcts  # [None, 0.20, 0.40, 0.60]
        drop = drops[self.tier - 1]
        if drop is None:
            return None
        return t1_entry * (1 - drop)

    @staticmethod
    def _next_tier_drop(tier: int) -> float | None:
        """Drop-pct of the next tier, or None if this is the last tier."""
        cfg = _s()
        drops = cfg.tier_drop_pcts
        if tier >= len(drops):
            return None
        return drops[tier]

    @property
    def fill_rate(self) -> float:
        """Fraction of expected size that has been filled (0..1)."""
        if self.size_expected <= 0:
            return 0.0
        return min(1.0, self.size_filled / self.size_expected)

    @property
    def is_complete(self) -> bool:
        """True if the tier has fully filled (>= 99.5% of expected size).

        H-NEW-2026-07-11: consults BOTH fill-callback state (`size_filled`)
        and TierWatcher ground truth (`placed_qty`). Whichever reports
        "complete" first wins — this guarantees `is_complete` flips even
        when the UserWS missed fill events and only the watcher
        observation caught it.

        For SELL tiers (TP), `placed_qty` decreases over time (we are
        selling), so we use `size_filled` only — the watcher's job is just
        to know when to cancel the GTD (i.e. when balance hits 0).
        """
        if self.size_expected <= 0:
            return self.filled
        # Watcher-driven path: balance >= 99.5% of expected.
        if self.placed_qty + 1e-9 >= self.size_expected * 0.995:
            return True
        # Fill-callback-driven path: still kept as a backstop.
        return self.size_filled >= self.size_expected * 0.995


# ---------------------------------------------------------------------------
# Take-profit snapshot
# ---------------------------------------------------------------------------


@dataclass
class TpSnapshot:
    """State for a single take-profit (SELL) order.

    Sell orders are not martingale tiers and do not drive the round's
    averaging-down logic, but they still need authoritative book-keeping so
    that fills can be matched back to a specific server_order_id and routed
    into the Position's _total_qty / _total_cost reducer (fixes B-2 — without
    this, TP sells never update position state, _tp_inflight never clears,
    and the engine deadlocks after one TP).

    Lifecycle:
        placed=False  -> registered but no fill yet (GTD still open)
        placed=True   -> at least one fill has been accounted for

    size_sold accumulates across partial fills. The Position reduces
    _total_qty / _total_cost as fills land; when size_sold reaches target,
    the entry is dropped from the registry.
    """

    server_order_id: str           # complete server order_id hash
    token_id: str                  # token we are selling
    target_qty: float              # shares we intended to sell
    placed: bool = False           # True once we have seen any fill
    size_sold: float = 0.0         # cumulative shares sold (sum of partial fills)
    usdc_received: float = 0.0     # cumulative proceeds (Σ price * size)


# ---------------------------------------------------------------------------
# Position snapshot — tick-driven refactor (H-NEW-2026-07-15)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PositionSnapshot:
    """Frozen view of a Position's decision-relevant state at one tick.

    Built by `Position.snapshot()` and consumed by every `Engine._evaluate`
    sub-step (`_check_t1_entry`, `_check_average_down`, `_check_take_profit`,
    `_reconcile_inflight_buys`). Tuple fields are positional across the
    four tiers (T1, T2, T3, T4) — callers index by `tier-1`.

    All numeric fields are floats; bool fields are pure. The snapshot is
    intentionally immutable so a helper cannot accidentally mutate the
    live Position through it.
    """

    total_qty: float
    avg_cost: float
    total_cost: float
    tier_size_filled: tuple[float, float, float, float]
    tier_cost_filled: tuple[float, float, float, float]
    tier_size_expected: tuple[float, float, float, float]
    tier_placed: tuple[bool, bool, bool, bool]
    tier_filled: tuple[bool, bool, bool, bool]
    tier_is_complete: tuple[bool, bool, bool, bool]
    tier_server_order_id: tuple[str | None, str | None, str | None, str | None]
    tier_expires_at: tuple[float, float, float, float]
    baseline_realized_price: float | None
    baseline_set_realized: bool
    baseline_target_price: float | None
    tp_target: float | None
    tp_inflight: bool
    # H-NEW-2026-07-15 (audit follow-up): server_order_id of the in-flight
    # TP GTD, if any. Surfaced so `_check_take_profit` can issue an
    # `ActionCancelTier` when mid retreats below the fire line after the
    # TP has been posted but not yet filled — without this, the GTD sits
    # on the book for the full GTD_LIFETIME_S while uPnL bleeds. None
    # when no TP is in flight or multiple TPs are stacked (defensive —
    # the current model only ever has one at a time).
    tp_server_order_id: str | None
    has_inflight_tier: bool
    entry_token: str | None
    entry_side: str | None
    # Shares already sold by TP this round. Tier fills keep their original
    # size, so `sum(tier_size_filled)` == total_qty + tp_sold_qty.
    tp_sold_qty: float = 0.0


# ---------------------------------------------------------------------------
# Position
# ---------------------------------------------------------------------------

# Helpers must be defined at module level so default_factory lambdas
# can reference them without capturing _s() at class-definition time.
def _tier1_factory() -> TierSnapshot:
    return TierSnapshot(tier=1, notional=_s().TIER1_NOTIONAL)


def _tier2_factory() -> TierSnapshot:
    return TierSnapshot(tier=2, notional=_s().TIER2_NOTIONAL)


def _tier3_factory() -> TierSnapshot:
    return TierSnapshot(tier=3, notional=_s().TIER3_NOTIONAL)


def _tier4_factory() -> TierSnapshot:
    return TierSnapshot(tier=4, notional=_s().TIER4_NOTIONAL)


@dataclass
class Position:
    """Live position within a round."""

    entry_side: Literal["UP", "DOWN"] | None = None
    entry_token: str | None = None

    tier1: TierSnapshot = field(default_factory=_tier1_factory)
    tier2: TierSnapshot = field(default_factory=_tier2_factory)
    tier3: TierSnapshot = field(default_factory=_tier3_factory)
    tier4: TierSnapshot = field(default_factory=_tier4_factory)

    # H-3 v2 fix: baseline_target_price records the INTENDED entry price
    # (the bid at the moment _check_t1_entry decided to enter) so that
    # the martingale averaging-tier triggers stay anchored to the design
    # price, not to the actual fill average. The actual fill price still
    # drives `_avg_cost` and TP via `baseline_realized_price` — only the
    # T2/T3/T4 trigger lines use this target.
    baseline_target_price: float | None = None
    # H-V6-14 fix: baseline_realized_price is the T1 avg fill price we
    # actually paid (Σ price*size / Σ size across all T1 partial fills),
    # used as the authoritative baseline for T2/T3/T4 trigger lines once
    # at least one T1 fill has landed. Before any T1 fill, the engine falls
    # back to baseline_target_price (the intended bid) — this preserves the
    # "don't fire T2 before T1 is partially filled" guard from H-NEW-2 but
    # rebases the trigger math to the *real* cost basis, not the design
    # price. Symptom this fixes: prod 2026-07-11 around the 17m-ago round,
    # T1 GTD submitted @ 0.41, fill landed @ 0.34, T2 then triggered at
    # 0.41*0.80 = 0.328 (in user's words: "why did T2 fire at 0.33? it
    # should have waited for a real 20% drop from 0.34 → 0.27"). Now that
    # baseline is 0.34 once T1 has at least one fill, T2's real trigger
    # is 0.272 — i.e. it would not have fired at 0.33.
    baseline_realized_price: float | None = None
    baseline_set_realized: bool = False

    # Cached position-level stats (recalculated after each fill)
    _total_qty: float = 0.0
    _total_cost: float = 0.0
    _avg_cost: float = 0.0
    # Shares already sold by take-profit orders this round. `record_tp_fill`
    # reduces `_total_qty` but deliberately leaves each tier's `size_filled`
    # untouched, so `sum(tier.size_filled)` exceeds `_total_qty` by exactly
    # this amount. The TP avg-cost reconciliation guard must add it back or
    # a partial TP would block every later TP ("unreconciled shares").
    _tp_sold_qty: float = 0.0

    # TP in-flight mutex — prevents the engine from issuing a new TP
    # action while a previous TP sell is still being processed by the
    # OrderManager / CLOB. Resets whenever _total_qty changes (i.e. a fill
    # landed, whether that fill is the TP itself or residual positioning).
    _tp_inflight: bool = False
    _last_qty_signature: float = 0.0
    # Same post-failure backoff as TierSnapshot, for the T1 entry.
    t1_retry_after_ts: float = 0.0
    # Cancel-failure backoff for the TP GTD (avoids a cancel storm when the
    # CLOB keeps rejecting the cancel).
    tp_cancel_retry_after_ts: float = 0.0

    # In-flight TP sells keyed by server_order_id. Lets engine.on_order_filled
    # match a SELL fill back to a specific sell order so it can:
    #   1) reduce _total_qty / _total_cost / _avg_cost by the fill delta;
    #   2) clear _tp_inflight so the engine can re-fire a residual TP on
    #      the next tick if mid is still above target.
    _tp_snapshots: dict[str, "TpSnapshot"] = field(default_factory=dict)

    @property
    def tiers(self) -> list[TierSnapshot]:
        return [self.tier1, self.tier2, self.tier3, self.tier4]

    @property
    def has_inflight_tier(self) -> bool:
        """True if any averaging tier is currently in-flight (placed but not complete).

        H-V5-1: used by engine._check_take_profit to suppress TP while an
        averaging tier is mid-fill, since tp_target() relies on _avg_cost
        which is stale until the in-flight tier actually fills.

        H-NEW-2026-07-15 tick-driven refactor: the previous condition was
        `t.placed and not t.is_complete`, but `placed` was used as the
        engine's "is this GTD still alive on the book" mutex, while the
        TierWatcher's `watching` flag independently gated the slot. With
        the watcher now passive (it no longer cancels on fill-ratio),
        the engine's cancel decisions live in `_reconcile_inflight_buys`,
        which sets `tier.placed = False` once it cancels the GTD. So we
        keep the same `placed and not is_complete` semantics: `placed=True`
        means "an unfilled GTD is on the book that the engine has not yet
        decided to cancel".
        """
        return any(t.placed and not t.is_complete for t in self.tiers[1:])

    @property
    def is_empty(self) -> bool:
        return all(not t.placed and not t.filled for t in self.tiers) and self._total_qty == 0

    @property
    def total_qty(self) -> float:
        return self._total_qty

    @property
    def total_cost(self) -> float:
        return self._total_cost

    @property
    def avg_cost(self) -> float:
        return self._avg_cost

    @property
    def total_notional_used(self) -> float:
        """Sum of notional actually spent (= cost filled so far)."""
        return self._total_cost

    @property
    def total_notional_committed(self) -> float:
        """USD already filled or reserved by currently placed tier orders."""
        return sum(
            max(t.cost_filled, t.notional if t.placed else 0.0)
            for t in self.tiers
        )

    # H-NEW-2026-07-15 tick-driven refactor: a single snapshot object that
    # captures everything the strategy tick needs to make a decision in one
    # place. Pre-snapshot, each `_check_*` helper re-derived its own view of
    # position state from `Position`, and there was no canonical place that
    # said "this is the round's state at this tick". Now `Engine._evaluate`
    # computes one snapshot per tick and passes it to all four decision
    # helpers (`_check_t1_entry`, `_check_average_down`,
    # `_check_take_profit`, `_reconcile_inflight_buys`).
    def snapshot(self) -> "PositionSnapshot":
        """Return a frozen view of the position's decision-relevant state.

        Recomputed on every `_evaluate` tick. Tuple fields are positional
        over the four tiers (T1, T2, T3, T4); callers index by `tier-1`.
        """
        ts = self.tiers
        return PositionSnapshot(
            total_qty=self._total_qty,
            avg_cost=self._avg_cost,
            total_cost=self._total_cost,
            tier_size_filled=tuple(t.size_filled for t in ts),
            tier_cost_filled=tuple(t.cost_filled for t in ts),
            tier_size_expected=tuple(t.size_expected for t in ts),
            tier_placed=tuple(t.placed for t in ts),
            tier_filled=tuple(t.filled for t in ts),
            tier_is_complete=tuple(t.is_complete for t in ts),
            tier_server_order_id=tuple(t.server_order_id for t in ts),
            tier_expires_at=tuple(t.expires_at for t in ts),
            baseline_realized_price=self.baseline_realized_price,
            baseline_set_realized=self.baseline_set_realized,
            baseline_target_price=self.baseline_target_price,
            tp_target=self.tp_target(),
            tp_inflight=self._tp_inflight,
            # H-NEW-2026-07-15: surface the in-flight TP GTD's
            # server_order_id so the engine can cancel it on mid retreat.
            # We pick the most-recently registered snapshot whose fill is
            # still incomplete; if no snapshot matches, None.
            tp_server_order_id=self._resolve_inflight_tp_server_id(),
            has_inflight_tier=self.has_inflight_tier,
            entry_token=self.entry_token,
            entry_side=self.entry_side,
            tp_sold_qty=self._tp_sold_qty,
        )

    def _resolve_inflight_tp_server_id(self) -> str | None:
        """Return the server_order_id of the currently in-flight TP GTD.

        H-NEW-2026-07-15 (audit follow-up): the engine needs this id to
        emit an `ActionCancelTier` when mid retreats below the TP fire
        line after the GTD has been posted. Returns the most-recent
        snapshot that is still partially or completely unfilled; None
        if no TP is in flight or all registered TPs are already fully
        filled.

        The dict-iteration order is insertion order in CPython 3.7+, so
        this picks the latest registered TP when multiple are stacked
        (defensive — current model only ever has one in flight).
        """
        if not self._tp_snapshots:
            return None
        for sid, snap in reversed(list(self._tp_snapshots.items())):
            if snap.size_sold + 1e-9 < snap.target_qty:
                return sid
        return None

    @property
    def has_open_tp_orders(self) -> bool:
        """True while a registered TP SELL may still be live on the CLOB.

        Snapshots are removed by `close_tp_order()` when the TP watcher
        reports a terminal state (or the order is confirmed cancelled).
        Until then, the engine must not post a second TP GTD: the first
        one still has unfilled shares reserved at the same price.
        """
        return self._resolve_inflight_tp_server_id() is not None

    def tp_size_sold(self, server_order_id: str) -> float | None:
        """Cumulative shares already booked for a TP order.

        Returns None when the snapshot no longer exists (the order was
        fully accounted for and closed). Used by the terminal watcher
        callback to apply only the delta it has not already booked.
        """
        snap = self._tp_snapshots.get(server_order_id)
        return snap.size_sold if snap is not None else None

    def close_tp_order(self, server_order_id: str) -> None:
        """Drop a TP snapshot after its order reached a terminal state."""
        if not server_order_id:
            return
        self._tp_snapshots.pop(server_order_id, None)
        self._tp_inflight = False

    @property
    def stake_cap_remaining(self) -> float:
        """How much more USD could be deployed before hitting total_max_notional.

        H-V4-2 fix (audit v4): uses `total_notional_committed` (placed +
        filled) instead of `_total_cost` (filled only). Without this, an
        in-flight tier's `notional` does not consume the cap, so the
        check is ineffective while a tier sits between post and fill.
        The "one tier in-flight at a time" invariant currently hides
        this — but the cap is structural defence-in-depth and should
        hold even if that invariant ever changes.
        """
        return max(0.0, _s().total_max_notional - self.total_notional_committed)

    # H-NEW-2026-07-15 (tick-driven refactor): `tier_is_saturated` /
    # `tier_notional_remaining` are removed. The new model never actively
    # estimates USD already exposed at a tier before placement; rather, the
    # strategy tick treats `tier.placed=True` as the single in-flight mutex,
    # and `ActionOpenTN` only emits when the slot is free. Overspend at a
    # tier is impossible by construction: only ONE GTD is ever on the book
    # per (round, tier), and `_spawn_tier_watcher` rejects double-spawns.

    def register_tp(
        self,
        server_order_id: str,
        token_id: str,
        target_qty: float,
    ) -> None:
        """Register an in-flight take-profit order so its fills can be matched.

        Idempotent on server_order_id (a re-registration of the same order is
        a no-op). Called from ActionRouter when a GTD or FAK TP sell is placed.
        """
        if not server_order_id or target_qty <= 0:
            return
        if server_order_id in self._tp_snapshots:
            return
        self._tp_snapshots[server_order_id] = TpSnapshot(
            server_order_id=server_order_id,
            token_id=token_id,
            target_qty=target_qty,
        )

    def realize_position_closed(self, *, source: str, note: str = "") -> None:
        """Authoritatively clear ALL position state — used when external
        evidence proves the inventory is gone (CLOB balance reconciliation,
        user sold manually via UI, etc.).

        H-NEW-2026-07-11 (audit v6-12 production revisit): the engine's
        in-memory Position was the *only* source of truth for inventory.
        When the user's manual UI-side SELL drained the wallet but the
        UserWS still fed `size=0` fill events to the engine, every tier
        was kept at `qty=5.00` even though no token existed on-chain. The
        next TP trigger then deadlocked into a 400-not-enough-balance
        retry loop — production saw 11 retries in <3 seconds.

        This method is the explicit "I have authoritative evidence that
        the position is gone; forget what we thought we knew" hook:

          - clears _tp_inflight, _tp_snapshots
          - resets each tier's placed/watching/filled/server_order_id
            but PRESERVES the *fill* state on T1 (so the round does not
            ghost-re-fire trigger_t1 the next tick).
          - sets _total_qty / _total_cost / _avg_cost to zero so the
            STATUS line stops reporting a phantom position.
          - leaves the round itself alive (so the engine can still
            process averaging tiers if they were in flight) but the
            position is_empty property will return True.

        Called by router.tp_runner when balance/allowance POST 400s and a
        subsequent REST `get_token_balance` returns 0.
        """
        prev_qty = self._total_qty
        prev_cost = self._total_cost

        # Reset aggregator state.
        self._total_qty = 0.0
        self._total_cost = 0.0
        self._avg_cost = 0.0
        self._tp_sold_qty = 0.0
        self._tp_inflight = False
        self._last_qty_signature = 0.0
        self._tp_snapshots.clear()
        # H-NEW-2026-07-15 (audit follow-up): when `realize_position_closed`
        # wipes the position we also drop the watcher-result dedup set so a
        # future legitimate watcher fire starts with a clean slate. The
        # previous behaviour cleared silently; if a TierWatcher re-fired
        # with the SAME `dedup_key` after a realize (e.g. stale buffered
        # callback that the bot hadn't drained yet), it would be silently
        # dropped by the dedupe registry. We log it so an operator can
        # see that the dedupe set was rebuilt — by design, the round is
        # effectively over once `realize_position_closed` runs, so any
        # subsequent watcher fires are noise.
        prev_dedup_size = len(self._applied_watcher_dedup)
        if prev_dedup_size > 0:
            log.info(
                "position.realize_closed.dedup_reset",
                prev_dedup_size=prev_dedup_size,
                note="watcher dedup registry cleared alongside position; "
                     "post-realize watcher fires with the same dedup_key "
                     "will be re-applied (round is effectively over)",
            )
        self._applied_watcher_dedup.clear()

        # Reset tier mutex state but PRESERVE buy fills so the engine
        # does not re-fire trigger_t1 next tick.
        for tier in self.tiers:
            tier.placed = False
            tier.watching = False
            tier.placed_qty = 0.0
            tier.server_order_id = None
            tier.expires_at = 0.0
            tier.watcher_task = None
            tier.token_id = None
            # NOTE: tier.filled, tier.size_filled, tier.cost_filled,
            # tier.avg_fill_price preserved intentionally.

        log.warning(
            "position.realize_closed",
            source=source,
            note=note,
            prev_qty=prev_qty,
            prev_cost=prev_cost,
            tiers_preserved=[t.tier for t in self.tiers if t.filled],
            note_2=(
                "all in-memory position state cleared; tier BUY fills "
                "preserved so engine does not ghost-fire trigger_t1"
            ),
        )

    def record_tp_fill(
        self,
        server_order_id: str,
        price: float,
        size: float,
    ) -> bool:
        """
        Reduce position state by a take-profit (SELL) fill.

        Returns:
            True  if this fill was matched to a registered TP snapshot
                  and position state was updated (caller should refresh
                  TP state via the response; _tp_inflight is cleared as
                  a side-effect).
            False if no matching snapshot exists.

        Side-effects:
            - _total_qty     -= size  (clamped at 0)
            - _total_cost    -= avg-cost * size  (proportional, weighted)
            - _avg_cost      recomputed from the residual
            - _tp_inflight   cleared (so engine can re-fire TP on residual)
            - snapshot       updated; dropped once size_sold >= target_qty
        """
        if size <= 0 or price <= 0:
            return False
        snap = self._tp_snapshots.get(server_order_id)
        if snap is None:
            return False

        snap.placed = True
        snap.size_sold += size
        snap.usdc_received += price * size

        # Reduce position. Cap each component to zero so a sell of "more than
        # we hold" (defensive: should never happen, but FAK can over-shoot
        # in shallow books) cannot push totals negative.
        prev_qty = self._total_qty
        prev_cost = self._total_cost
        sold = min(size, prev_qty)
        # Track shares disposed of by TP: tier.size_filled is intentionally
        # preserved, so the reconciliation guard needs this to distinguish
        # "already sold" from "watcher has not caught up yet".
        self._tp_sold_qty += max(0.0, sold)
        if sold <= 0:
            # No inventory to reduce. Don't touch avg_cost.
            self._tp_inflight = False
            if snap.size_sold + 1e-9 >= snap.target_qty:
                self._tp_snapshots.pop(server_order_id, None)
            return True

        # Pro-rata cost removal keeps the avg_cost basis intact.
        avg_before = prev_cost / prev_qty if prev_qty > 1e-9 else 0.0
        self._total_qty = max(0.0, prev_qty - sold)
        self._total_cost = max(0.0, prev_cost - avg_before * sold)
        if self._total_qty > 1e-9:
            self._avg_cost = self._total_cost / self._total_qty
        else:
            self._total_cost = 0.0
            self._avg_cost = 0.0

        # Any TP fill means a previous TP either fully drained the
        # inventory or partially filled — either way, release the mutex so
        # the engine can re-arm if mid is still above target.
        self._tp_inflight = False

        log.info(
            "position.tp_fill_recorded",
            server_order_id=server_order_id[:16],
            fill_price=price,
            fill_size=size,
            size_sold=snap.size_sold,
            target_qty=snap.target_qty,
            total_qty=self._total_qty,
            total_cost=self._total_cost,
            avg_cost=self._avg_cost,
            avg_before=avg_before,
        )

        # Snapshot is "done" once we have hit target; drop it so the next
        # fill for the same server_order_id is ignored. Keeps registry clean.
        if snap.size_sold + 1e-9 >= snap.target_qty:
            self._tp_snapshots.pop(server_order_id, None)
            # H-NEW-2026-07-12: confirmed full TP exit — position total_qty
            # is now 0 (set by the caller via `eng.on_tp_completed` / position
            # dedup). entry_token stays intact so _check_t1_entry guard keeps
            # blocking re-entry until the next round.
            if self._total_qty <= 1e-9:
                log.info(
                    "position.fully_closed_after_tp",
                    token=self.entry_token or "",
                    avg_before=avg_before,
                )

        return True

    def record_fill(self, tier: TierSnapshot, price: float, size: float) -> None:
        """Called when a tier receives (more) fills — supports partial fills.

        Strategy: size_filled and cost_filled always accumulate; filled flag
        only flips once size_filled has reached >= 99.5% of expected_size
        (via `tier.is_complete` which also consults `placed_qty` from the
        TierWatcher).

        For tier 1, baseline_price is set ONCE (on first partial fill) so
        averaging tiers can be triggered against the real cost basis.
        """
        if size <= 0 or price <= 0:
            return

        # If this is the first time we see a fill for this tier, mark placed=True.
        if not tier.placed:
            tier.placed = True

        tier.size_filled += size
        tier.cost_filled += price * size
        tier.avg_fill_price = tier.cost_filled / tier.size_filled if tier.size_filled > 1e-9 else price
        tier.last_fill_ts = __import__("time").time()

        if tier.is_complete:
            tier.filled = True

        # Position-level stats
        self._total_qty += size
        self._total_cost += price * size
        if self._total_qty > 0:
            self._avg_cost = self._total_cost / self._total_qty

        # Anchor baseline on first T1 fill (any partial fill counts).
        # NOTE: BUY-side fills must NOT clear _tp_inflight here — that flag
        # tracks a SELL-side TP runner in flight, and an averaging BUY fill
        # landing while TP is waiting is independent of TP's progress. Clearing
        # it here would let the next _evaluate() re-arm a second TP sell while
        # the first GTD sell is still live on the CLOB (B-NEW-1, audit v3).
        # _tp_inflight is cleared only inside record_tp_fill() when a real
        # TP fill actually lands.
        #
        # H-NEW-2026-07-15: the legacy `baseline_price` / `baseline_set`
        # fields used to be written here too (as the v1 anchor for T2/T3/T4
        # trigger lines). They became dead when `baseline_realized_price`
        # was introduced in H-V6-14 — every consumer now reads the realized
        # series. The fields are gone from `Position` (see top-of-class
        # field block). The realized series below is the single source of
        # truth for T1 fill-weighted anchoring.

        # H-V6-14: baseline_realized_price is the AUTHORITATIVE baseline for
        # T2/T3/T4 trigger lines once T1 has any fills. We compute it as the
        # LIVE volume-weighted average fill price on every T1 partial fill
        # (= Σ price*size / Σ size at the moment of this fill), so that
        # subsequent trigger evaluations are anchored to the real cost basis
        # even when T1 fills in many small partials (or slips between
        # partials against a thin orderbook).
        #
        # The first time baseline_set_realized flips True is the moment T2+
        # trigger math rebases from "design price" to "real cost basis"; the
        # transition is observable in logs as a one-shot `engine.t1_rebased`
        # event. After that, baseline_realized_price continues to track the
        # live T1 VWAP (so a continued drop on T1 — e.g. another 6-share
        # partial at 0.30 — would shift the baseline lower, and T2 trigger
        # re-evaluates against the new value on the next tick).
        #
        # IMPORTANT: this only updates on T1 fills. T2/T3/T4 fills do NOT
        # change baseline_realized_price — the anchor reflects what we paid
        # for our *initial position size*, not the running average across
        # all averaging tiers (which lives in _avg_cost for PnL math
        # already; we don't want averaging tiers to rebaseline themselves
        # — that would defeat the whole martingale math).
        if tier.tier == 1 and tier.size_filled > 1e-9:
            new_realized = tier.cost_filled / tier.size_filled
            if self.baseline_realized_price is None or abs(new_realized - self.baseline_realized_price) > 1e-9:
                self.baseline_realized_price = new_realized
            if not self.baseline_set_realized:
                self.baseline_set_realized = True

        log.info(
            "position.fill_recorded",
            tier=tier.tier,
            fill_price=price,
            fill_size=size,
            cumulative_size=tier.size_filled,
            expected_size=tier.size_expected,
            avg_fill_price=tier.avg_fill_price,
            tier_filled=tier.filled,
            total_qty=self._total_qty,
            avg_cost=self._avg_cost,
            # NOTE: legacy baseline_price field removed (H-NEW-2026-07-15) —
            # use baseline_realized_price / baseline_set_realized for T1
            # anchoring; baseline_target_price for the pre-fill intent.
        )

    def mark_expired(self, tier: TierSnapshot, reason: str = "gtd_expired") -> None:
        """Mark a tier as expired (e.g. GTD expired without filling).

        We do NOT clear size_filled/cost_filled — partial fills must still
        be counted. We just reset placed so the strategy can decide whether
        to try again at the next averaging opportunity.

        H-NEW-2026-07-11: also clear `watching` so the engine does not
        treat this tier as in-flight any longer.

        H-NEW-2026-07-15 (audit follow-up): clear `cancel_pending` so a
        stale dispatch latch can never block a future legitimate cancel.
        Without this, a cancel dispatched just before the watcher's
        EXPIRED terminal state could leave `cancel_pending=True` forever
        (the router's `cancel_pending=False` line only runs if the
        cancel call actually resolves; a watcher-first terminal race
        could pre-empt the router's latch clear).
        """
        tier.placed = False
        tier.watching = False
        tier.server_order_id = None
        tier.cancel_reason = reason
        tier.cancel_pending = False
        log.warning(
            "position.tier_expired",
            tier=tier.tier,
            size_filled=tier.size_filled,
            size_expected=tier.size_expected,
            reason=reason,
        )

    # H-NEW-2026-07-11: dedup registry for apply_tier_watcher_result.
    # TierWatcher's _fire_complete is already idempotent at the watcher
    # layer, but as defence in depth we keep a per-(server_order_id, dedup_key)
    # set here so two different watcher objects observing the same order
    # (e.g. BUY + TP overlap during a single round) cannot both advance
    # position totals twice.
    #
    # H-NEW-2026-07-15 (audit follow-up — P2#11 dup parity): bound the
    # dedup set with a soft warning rather than a hard cap. Per round
    # we expect ≤ 4 entries (one terminal outcome per averaging tier +
    # one for TP). A soft warning at 8 lets us catch runaway accumulation
    # (e.g. a buggy watcher re-firing) without dropping legitimate dedup
    # records on the floor. Reset on `realize_position_closed` — see
    # P2#12 fix below for the companion logging change.
    _applied_watcher_dedup: set[tuple[str, str]] = field(default_factory=set)
    _DEDUP_SOFT_LIMIT: int = 8

    def apply_tier_watcher_result(
        self,
        tier: TierSnapshot,
        outcome: str,
        filled_qty: float,
        dedup_key: str = "",
        avg_fill_price: float = 0.0,
    ) -> None:
        """Commit TierWatcher terminal state into the tier / position.

        Called by the ActionRouter's watcher callback when a tier's GTD
        has reached a final state (filled / expired / crossed / cancelled
        / over_filled / error).

        Args:
            tier:           TierSnapshot to update.
            outcome:        one of TierOutcome string values.
            filled_qty:     shares the watcher observed at termination.
            dedup_key:      ``<server_id>|<outcome>`` from WatcherResult;
                            if it has already been applied for this
                            position, this call is a no-op.
            avg_fill_price: real average fill price reported by the CLOB.
                            If > 0 and the WS path missed fills, this is
                            the price used for cost accumulation instead
                            of falling back to ``notional/size_expected``.

        The watcher is the ground-truth source for "how many shares do I
        actually hold"; this method reconciles that into:

          * tier.placed_qty = max(prior, filled_qty)   # monotonic
          * tier.filled     = True if outcome == "filled" (or over_filled)
          * tier.watching   = False (watcher is done)
          * position totals _total_qty / _total_cost if filled_qty >
            tier.size_filled (catches fills missed by the WS path).

        R5 (audit v6-14 hardening): tier.watching is cleared inside a
        try/finally so an exception during the in-branch reconciliation
        (e.g. a transient log sink failure mid-call) cannot leave the
        engine believing a tier is still being watched. The flag is the
        in-flight mutex used by `_check_average_down` / `_check_take_profit`
        via `has_inflight_tier`, so leaking it True deadlocks both paths.
        """
        from src.execution.tier_watcher import TierOutcome

        # Defence-in-depth dedup. Even if a watcher somehow fires its
        # on_complete twice (or two watchers observe the same order_id),
        # only the first application of a given dedup_key touches
        # position totals.
        if dedup_key:
            if dedup_key in self._applied_watcher_dedup:
                log.debug(
                    "position.watcher_result_dedup_skip",
                    dedup_key=dedup_key[:32],
                    tier=tier.tier,
                    filled=tier.filled,
                )
                tier.watching = False
                return
            self._applied_watcher_dedup.add(dedup_key)
            # H-NEW-2026-07-15 (audit follow-up — P2#11 dup parity):
            # soft bound on the dedup set. A clean round adds ≤ 4 entries
            # (T1..T4 each contribute one terminal outcome) plus
            # optionally one for TP. Beyond the soft limit, something
            # is leaking (likely a buggy watcher re-firing the same
            # outcome). We log a warning so an operator can investigate
            # rather than silently truncating legitimate dedup records.
            if len(self._applied_watcher_dedup) > self._DEDUP_SOFT_LIMIT:
                log.warning(
                    "position.watcher_dedup_oversize",
                    size=len(self._applied_watcher_dedup),
                    soft_limit=self._DEDUP_SOFT_LIMIT,
                    note="dedup set exceeds soft limit; check for a watcher "
                         "re-firing the same (server_id, dedup_key) — the "
                         "round may be leaking watcher outcomes",
                )

        try:
            # Monotonic — fill events may have already incremented size_filled.
            if filled_qty > tier.placed_qty:
                tier.placed_qty = filled_qty

            if outcome in (TierOutcome.FILLED.value, TierOutcome.OVER_FILLED.value):
                tier.filled = True
                # If the watcher caught fills that the WS path missed, advance
                # position-level totals so STATUS / TP math see them.
                if filled_qty > tier.size_filled + 1e-9:
                    # Price precedence for the missed-fill reconciliation:
                    #   1. Real avg_fill_price from the watcher (preferred).
                    #   2. tier.avg_fill_price previously set by record_fill.
                    #   3. Strategy price = notional / size_expected.
                    fill_px = 0.0
                    if avg_fill_price > 0:
                        fill_px = float(avg_fill_price)
                    elif tier.avg_fill_price and tier.avg_fill_price > 0:
                        fill_px = float(tier.avg_fill_price)
                    elif tier.size_expected > 0 and tier.notional > 0:
                        fill_px = tier.notional / tier.size_expected

                    missed = filled_qty - tier.size_filled
                    if missed > 1e-9 and fill_px > 0:
                        self._total_qty += missed
                        self._total_cost += missed * fill_px
                        if self._total_qty > 0:
                            self._avg_cost = self._total_cost / self._total_qty
                        tier.size_filled += missed
                        tier.cost_filled += missed * fill_px
                        log.info(
                            "position.watcher_reconciled_missed_fills",
                            tier=tier.tier,
                            missed=missed,
                            fill_px=fill_px,
                            fill_px_source="avg_fill_price" if avg_fill_price > 0 else (
                                "tier.avg_fill_price" if (tier.avg_fill_price and tier.avg_fill_price > 0)
                                else "notional/size_expected"
                            ),
                            total_qty=self._total_qty,
                        )

                # Anchor baseline_realized on first T1 watcher fill (same as
                # record_fill does for the size_filled path).
                if tier.tier == 1 and tier.size_filled > 1e-9:
                    new_realized = tier.cost_filled / tier.size_filled
                    if (
                        self.baseline_realized_price is None
                        or abs(new_realized - self.baseline_realized_price) > 1e-9
                    ):
                        self.baseline_realized_price = new_realized
                    self.baseline_set_realized = True

            elif outcome == TierOutcome.EXPIRED.value:
                # Cancel-after-expiry — do not mark filled (it didn't fill).
                # keep partial fills in size_filled/cost_filled; just close out.
                # H-NEW-2026-07-11 (audit v6-12): if the GTD partially filled
                # before expiring, the realised T1 fill avg MUST still anchor
                # the T2+ trigger lines. Without this, a T1 that partially
                # filled and then expired would leave `baseline_set_realized`
                # stuck at False, forcing T2+ to use the design
                # `baseline_target_price` (= ENTRY_PRICE_CAP) instead of the
                # real cost basis. The martingale trigger lines would then be
                # permanently miscalibrated against the actual fill average.
                if (
                    tier.tier == 1
                    and tier.size_filled > 1e-9
                    and not self.baseline_set_realized
                ):
                    new_realized = tier.cost_filled / tier.size_filled
                    self.baseline_realized_price = new_realized
                    self.baseline_set_realized = True
                    log.info(
                        "position.baseline_anchored_on_expire",
                        tier=tier.tier,
                        missed=tier.size_filled,
                        fill_px=avg_fill_price,
                        new_realized=new_realized,
                        note="T1 GTD partially filled before expiry; "
                             "rebasing T2+ trigger lines to realised avg",
                    )
            elif outcome in (TierOutcome.CANCELLED.value, TierOutcome.ERROR.value):
                # External cancel / watcher crash — keep partial fills, close
                # out. Same partial-fill baseline anchoring as EXPIRED.
                if (
                    tier.tier == 1
                    and tier.size_filled > 1e-9
                    and not self.baseline_set_realized
                ):
                    new_realized = tier.cost_filled / tier.size_filled
                    self.baseline_realized_price = new_realized
                    self.baseline_set_realized = True
                    log.info(
                        "position.baseline_anchored_on_cancel",
                        tier=tier.tier,
                        missed=tier.size_filled,
                        fill_px=avg_fill_price,
                        new_realized=new_realized,
                    )
                # Release the in-flight slot. We keep `filled=False` so a
                # late on-chain fill is still counted via record_fill, and
                # keep server_order_id so it can still be attributed.
                tier.placed = False
                log.info(
                    "position.tier_cleared_after_cancel",
                    tier=tier.tier,
                    server_order_id=(
                        tier.server_order_id[:16] if tier.server_order_id else None
                    ),
                    outcome=outcome,
                    note="tier.placed reset so downstream checks "
                         "(has_inflight_tier / TP gating) release this slot",
                )

            log.info(
                "position.tier_watcher_done",
                tier=tier.tier,
                outcome=outcome,
                filled_qty=filled_qty,
                avg_fill_price=avg_fill_price,
                placed_qty=tier.placed_qty,
                size_filled=tier.size_filled,
                size_expected=tier.size_expected,
                filled=tier.filled,
            )
        except Exception as exc:
            # R5: if any branch above blew up, the outer finally still
            # releases the in-flight mutex. Without this, an exception in
            # log.info (e.g. serialiser trouble on a custom field) would
            # leak `tier.watching=True` forever, dead-locking has_inflight_tier.
            log.exception(
                "position.apply_tier_watcher_result.exception",
                tier=tier.tier,
                outcome=outcome,
                filled_qty=filled_qty,
                error=str(exc),
            )
            # R5 re-raise: callers (ActionRouter) catch this and decide
            # whether to retry or surface to the operator. Swallowing
            # would silently lose the exception and make production
            # debugging impossible. The watching=False cleanup has
            # already run in the `finally` block below.
            raise
        finally:
            # R5 hardening: ALWAYS release the in-flight mutex, even on
            # exception. This is the only flag that gates TP / next-tier
            # decisions, so a leaked True would soft-lock the round.
            tier.watching = False

    def tp_target(self) -> float | None:
        """Take-profit trigger price. None if no position.

        Decrements as more averaging tiers fire so the strategy captures
        profit sooner when already deep in a martingale sequence.
        Formula:
            TP_pct = max(TP_MIN_PCT,
                         TP_PROFIT_PCT - N * TP_DECREMENT_PCT)
        where N = number of averaging tiers with an actual fill
                 (T1 only → N=0; T1+T2 → N=1; T1..T4 → N=3).
        With the configured 3% decrement, the four states are
        20%, 17%, 14%, and 11% above average cost.
        """
        if self._avg_cost <= 0:
            return None
        cfg = _s()
        n = sum(
            1 for t in self.tiers[1:]
            if t.size_filled > 1e-9 or t.cost_filled > 1e-9
        )
        tp_pct = cfg.TP_PROFIT_PCT - n * cfg.TP_DECREMENT_PCT
        tp_pct = max(cfg.TP_MIN_PCT, tp_pct)
        return self._avg_cost * (1 + tp_pct)

    def tp_pct_for_current_state(self) -> float:
        """The TP percentage currently in effect (for logging / tests).

        Same formula as `tp_target` but always returns a float even when
        `_avg_cost <= 0`. Useful for diagnostic logs that want to show the
        current TP target regardless of position state.
        """
        cfg = _s()
        n = sum(
            1 for t in self.tiers[1:]
            if t.size_filled > 1e-9 or t.cost_filled > 1e-9
        )
        tp_pct = cfg.TP_PROFIT_PCT - n * cfg.TP_DECREMENT_PCT
        return max(cfg.TP_MIN_PCT, tp_pct)


# ---------------------------------------------------------------------------
# RoundState
# ---------------------------------------------------------------------------


@dataclass
class RoundState:
    """Complete per-round state machine."""

    round_start_ts: int
    market: "MarketInfo | None" = field(default=None, repr=False)

    # Derived from market — init=False so they don't appear in __init__ signature.
    # Defaults are set in __post_init__.
    slug: str | None = field(init=False, default=None)
    condition_id: str | None = field(init=False, default=None)
    up_token: str | None = field(init=False, default=None)
    down_token: str | None = field(init=False, default=None)
    tick_size: float = field(init=False, default=0.01)

    position: Position = field(default_factory=Position)
    finished: bool = False
    # Set by main._on_new_round when the window pre-check (late boot /
    # cross-window BTC gap) says "do not trade this round". The engine
    # still runs housekeeping/cancels but never opens a position.
    skip_reason: str | None = None
    binance_baseline: float | None = None
    gate_last_state: str = ""

    def __post_init__(self) -> None:
        # Derived fields are set here because init=False excludes them from
        # the __init__ signature. When market is None they retain their defaults.
        if self.market is not None:
            self.slug = self.market.slug
            self.condition_id = self.market.condition_id
            self.up_token = self.market.up_token
            self.down_token = self.market.down_token
            self.tick_size = self.market.tick_size