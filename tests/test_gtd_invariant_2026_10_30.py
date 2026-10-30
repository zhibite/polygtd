"""Regression tests for the tier_placed_without_watcher invariant.

Production 2026-10-30: fully filled T1/T3 tiers kept `placed=True` with
`watching=False` for the rest of the round (the TierWatcher ends, `placed`
stays as "a fill has been accounted for", and it is only cleared on
cancel/expire). The old `placed and not watching` condition therefore
warned `invariant.tier_placed_without_watcher` every 5 s on a perfectly
healthy round — pure log noise that looked like a missed-fill incident.

The audit now asks OrderManager whether a live GTD still exists for the
tier, so only genuinely unattended live orders warn.
"""
from __future__ import annotations

from types import SimpleNamespace

from src.main import _audit_gtd_invariants


class _FakeLog:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def error(self, event: str, **kw) -> None:
        self.calls.append((event, kw))

    def warning(self, event: str, **kw) -> None:
        self.calls.append((event, kw))

    def info(self, event: str, **kw) -> None:
        self.calls.append((event, kw))


def _state(tiers, live_orders) -> SimpleNamespace:
    pos = SimpleNamespace(tiers=tiers, total_notional_committed=0.0)
    eng = SimpleNamespace(_round=SimpleNamespace(position=pos))
    om = SimpleNamespace(open_orders_for_round=lambda ts: live_orders)
    return SimpleNamespace(engine=eng, order_manager=om, round_start_ts=1_000)


def _tier(no: int, *, placed: bool, watching: bool, filled: bool = False):
    return SimpleNamespace(
        tier=no,
        notional=2.0,
        cost_filled=2.0 if filled else 0.0,
        placed=placed,
        watching=watching,
    )


def _order(tier_name: str, cid: str = "c1") -> SimpleNamespace:
    return SimpleNamespace(tier_name=tier_name, client_order_id=cid)


def _invariant_events(fake: _FakeLog) -> list[tuple[str, dict]]:
    return [
        (e, kw) for e, kw in fake.calls if e.startswith("invariant.")
    ]


def test_completed_tier_with_no_live_order_does_not_warn(monkeypatch) -> None:
    """A filled tier (placed=True, watching=False) is not an incident."""
    fake = _FakeLog()
    monkeypatch.setattr("src.main.log", fake)

    state = _state(
        tiers=[_tier(1, placed=True, watching=False, filled=True)],
        live_orders=[],
    )
    _audit_gtd_invariants(state)

    assert _invariant_events(fake) == []


def test_live_order_without_watcher_still_warns(monkeypatch) -> None:
    """The real incident — a live GTD nobody is watching — is still caught."""
    fake = _FakeLog()
    monkeypatch.setattr("src.main.log", fake)

    state = _state(
        tiers=[_tier(2, placed=True, watching=False)],
        live_orders=[_order("T2", cid="abc123")],
    )
    _audit_gtd_invariants(state)

    events = [
        kw for e, kw in fake.calls
        if e == "invariant.tier_placed_without_watcher"
    ]
    assert len(events) == 1
    assert events[0]["tier"] == 2
    assert events[0]["live_client_ids"] == ["abc123"]


def test_live_order_with_watcher_does_not_warn(monkeypatch) -> None:
    fake = _FakeLog()
    monkeypatch.setattr("src.main.log", fake)

    state = _state(
        tiers=[_tier(3, placed=True, watching=True)],
        live_orders=[_order("T3", cid="def456")],
    )
    _audit_gtd_invariants(state)

    assert _invariant_events(fake) == []
