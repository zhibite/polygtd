"""Regression tests for the 2026-10-30 production log review.

Covered defects:

  1. `WindowManager` never detected the first window (`cid_changed` required
     a pre-existing `ctx.condition_id`, but only `_on_new_window()` writes
     it) — so the boot-time late-window guard, the cross-window BTC drift
     guard and `reset_for_new_window()` were unreachable dead code.
     Production: bot booted 356 s into a 900 s window and idled silently
     instead of logging `window_mgr boot-time window is late → SKIP`.

  2. `main._on_window_baseline` used `STATE.round_start_ts` as the dedupe
     key, but `WindowManager.tick()` runs BEFORE `_on_new_round`, so at
     boot the key was 0 (log line `gate.baseline_applied round_start_ts=0`).
     The round id now travels with the callback.

  3. `UserWS.subscribe_markets()` had zero callers, so the user channel
     authenticated and then never subscribed to any market — no order /
     trade events ever arrived (fills fell back to 1.5 s REST polling).

  4. `engine.t1_gate_closed` re-logged every 60 s for the rest of the
     window (8 lines per round) despite the "log once" comment.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from src.market.models import MarketInfo
from src.market.user_ws import UserWS
from src.market.window_manager import WindowManager


# ---------------------------------------------------------------------------
# 1) first discovery must count as a window switch
# ---------------------------------------------------------------------------


class _FakeSvc:
    def __init__(self, mkt: MarketInfo) -> None:
        self.mkt = mkt

    async def discover_market(self, asset: str):  # noqa: ARG002
        return self.mkt

    async def fetch_prices(self, market):  # noqa: ARG002
        return (0.5, 0.5)


class _FakeSpot:
    async def fetch_at_window_open(self, window_start):  # noqa: ARG002
        return 50_000.0

    async def fetch_price(self):
        return 50_000.0


def _make_market(age_sec: float) -> MarketInfo:
    now = datetime.now(timezone.utc)
    start = now - timedelta(seconds=age_sec)
    return MarketInfo(
        asset="BTC",
        condition_id="0xdeadbeef",
        token_up_id="111",
        token_down_id="222",
        window_start=start,
        window_end=start + timedelta(seconds=900),
        slug="btc-updown-15m-test",
        round_start_ts=int(start.timestamp()),
    )


@pytest.mark.asyncio
async def test_first_discovery_runs_on_new_window_and_boot_guard() -> None:
    """Boot 400 s into the window must set skip_until_next_window.

    Pre-fix `_on_new_window()` was unreachable (chicken-and-egg on
    `ctx.condition_id`), so the guard never ran and the round was traded
    as if it were safe.
    """
    mkt = _make_market(age_sec=400)
    baseline_calls: list[tuple] = []

    wm = WindowManager(
        market_svc=_FakeSvc(mkt),  # type: ignore[arg-type]
        symbols=["BTC"],
        binance_spot=_FakeSpot(),
        entry_cfg=SimpleNamespace(
            SKIP_IF_LATE_WINDOW_AT_BOOT=True,
            ENTRY_OPEN_OFFSET_GRACE_SEC=180.0,
            CROSS_WINDOW_DRIFT_SKIP_PCT=0.5,
        ),
        on_window_baseline=lambda *a: baseline_calls.append(a),
    )

    await wm.tick(["BTC"], force_discovery=True)

    ctx = wm.get("BTC")
    assert ctx is not None
    # Fix 1: the first discovery is treated as a switch and binds the cid.
    assert ctx.condition_id == mkt.condition_id
    assert ctx.window_start == mkt.window_start
    # Boot guard fired: 400 s > 180 s grace.
    assert ctx.skip_until_next_window is True
    # Fix 2: the baseline callback carries the round the price belongs to.
    assert baseline_calls, "on_window_baseline must fire for the window open"
    assert baseline_calls[0][1] == 50_000.0
    assert baseline_calls[0][3] == mkt.round_start_ts


@pytest.mark.asyncio
async def test_early_boot_window_is_not_skipped() -> None:
    """Negative control: a fresh window (age < grace) must stay tradeable."""
    mkt = _make_market(age_sec=10)

    wm = WindowManager(
        market_svc=_FakeSvc(mkt),  # type: ignore[arg-type]
        symbols=["BTC"],
        binance_spot=_FakeSpot(),
        entry_cfg=SimpleNamespace(
            SKIP_IF_LATE_WINDOW_AT_BOOT=True,
            ENTRY_OPEN_OFFSET_GRACE_SEC=180.0,
            CROSS_WINDOW_DRIFT_SKIP_PCT=0.5,
        ),
    )

    await wm.tick(["BTC"], force_discovery=True)

    ctx = wm.get("BTC")
    assert ctx is not None
    assert ctx.condition_id == mkt.condition_id
    assert ctx.skip_until_next_window is False


# ---------------------------------------------------------------------------
# 2) baseline dedupe key uses the round passed by WindowManager
# ---------------------------------------------------------------------------


class _FakeEngine:
    def __init__(self) -> None:
        self._last_applied_baseline_round_ts = None
        self.applied: list[float] = []

    def set_binance_baseline(self, price: float) -> None:
        self.applied.append(price)


def test_on_window_baseline_dedupes_on_market_round() -> None:
    import src.main as main_mod

    fake = _FakeEngine()
    saved_engine = main_mod.STATE.engine
    saved_round = main_mod.STATE.round_start_ts
    try:
        main_mod.STATE.engine = fake  # type: ignore[assignment]
        # Simulate boot: the WM tick fires before _on_new_round sets this.
        main_mod.STATE.round_start_ts = 0

        main_mod._on_window_baseline("BTC", 77_644.0, "window_open_kline", 1_793_358_900)
        # Repeated discovery inside the same window must not re-apply.
        main_mod._on_window_baseline("BTC", 77_644.0, "window_open_kline", 1_793_358_900)

        assert fake.applied == [77_644.0]
        assert fake._last_applied_baseline_round_ts == 1_793_358_900

        # Next round: applies again.
        main_mod._on_window_baseline("BTC", 77_650.0, "window_open_kline", 1_793_359_800)
        assert fake.applied == [77_644.0, 77_650.0]
    finally:
        main_mod.STATE.engine = saved_engine
        main_mod.STATE.round_start_ts = saved_round


# ---------------------------------------------------------------------------
# 3) user channel must actually subscribe to markets
# ---------------------------------------------------------------------------


def test_user_ws_subscribe_payload_carries_auth_and_markets() -> None:
    uw = UserWS(api_key="k", api_secret="s", api_passphrase="p")

    uw.subscribe_markets(["0xabc"])
    payload = uw._subscribe_payload()

    assert payload["type"] == "user"
    assert payload["auth"] == {"apiKey": "k", "secret": "s", "passphrase": "p"}
    assert payload["markets"] == ["0xabc"]

    # Same set again: no change.
    uw.subscribe_markets(["0xabc"])
    assert uw._pending_markets == {"0xabc"}

    # New window replaces the previous market (bounded payload).
    uw.subscribe_markets(["0xdef"])
    assert uw._pending_markets == {"0xdef"}
    assert uw._subscribe_payload()["markets"] == ["0xdef"]

    # Empty input never wipes an existing subscription.
    uw.subscribe_markets([])
    assert uw._pending_markets == {"0xdef"}


def test_user_ws_pending_subscription_survives_offline() -> None:
    """subscribe_markets while disconnected must not raise (no loop/ws)."""
    uw = UserWS(api_key="k", api_secret="s", api_passphrase="p")
    uw.subscribe_markets(["0xabc"])  # _ws is None -> deferred
    assert uw._ws is None
    assert uw._subscribe_payload()["markets"] == ["0xabc"]


# ---------------------------------------------------------------------------
# 4) engine.t1_gate_closed logs once per round
# ---------------------------------------------------------------------------


class _LogRecorder:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def info(self, event: str, **kw) -> None:
        self.events.append((event, kw))

    def warning(self, event: str, **kw) -> None:  # pragma: no cover
        self.events.append((event, kw))

    def debug(self, event: str, **kw) -> None:  # pragma: no cover
        pass

    def error(self, event: str, **kw) -> None:  # pragma: no cover
        self.events.append((event, kw))

    def exception(self, event: str, **kw) -> None:  # pragma: no cover
        self.events.append((event, kw))


def _make_engine_and_market():
    from src.feeds.binance_spot import BinanceCache
    from src.market.market_ws import PriceCache
    from src.strategy.engine import StrategyEngine
    from src.strategy.gatekeeper import GateKeeper
    from src.strategy.round_state import RoundState

    async def _handler(action) -> None:  # pragma: no cover
        raise AssertionError(f"no action expected, got {action}")

    pc = PriceCache()
    eng = StrategyEngine(BinanceCache(), pc, _handler)

    mkt = MarketInfo(
        asset="BTC",
        condition_id="0xdeadbeef",
        token_up_id="UPTOKEN",
        token_down_id="DOWNTOKEN",
        window_start=datetime.now(timezone.utc) - timedelta(seconds=400),
        window_end=datetime.now(timezone.utc) + timedelta(seconds=500),
        round_start_ts=int(time.time()) - 400,
    )
    eng._round = RoundState(round_start_ts=mkt.round_start_ts, market=mkt)
    eng._gate = GateKeeper(100.0)
    # Both sides below ENTRY_PRICE_CAP*(1+PRICE_PRE_MARGIN)=0.44 so the
    # price check passes and execution reaches the T1 time gate.
    pc.midpoint["UPTOKEN"] = 0.30
    pc.midpoint["DOWNTOKEN"] = 0.30
    return eng, mkt


@pytest.mark.asyncio
async def test_t1_gate_closed_logs_once_per_round(monkeypatch) -> None:
    import src.strategy.engine as engine_mod

    eng, mkt = _make_engine_and_market()
    recorder = _LogRecorder()
    monkeypatch.setattr(engine_mod, "log", recorder)

    # Many decision ticks in the same round.
    for _ in range(5):
        await eng._check_t1_entry(100.0)

    gate_closed = [e for e in recorder.events if e[0] == "engine.t1_gate_closed"]
    assert len(gate_closed) == 1, (
        f"expected exactly one t1_gate_closed line per round, got {len(gate_closed)}"
    )
    assert gate_closed[0][1]["elapsed_s"] >= 300

    # Next round logs again (once).
    from src.strategy.round_state import RoundState

    # +60 s keeps the new round ~340 s old, i.e. still past the 300 s gate.
    eng._round = RoundState(
        round_start_ts=mkt.round_start_ts + 60,
        market=mkt,
    )
    await eng._check_t1_entry(100.0)
    await eng._check_t1_entry(100.0)

    gate_closed = [e for e in recorder.events if e[0] == "engine.t1_gate_closed"]
    assert len(gate_closed) == 2
