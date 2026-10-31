"""Tests for the T4 decision/exit JSONL journal (2026-10-31).

The journal is a structlog processor that mirrors the T4-relevant subset
of events to an append-only file so weeks of decisions survive the
container's rotating stdout logs long enough to tune the gates.
"""
from __future__ import annotations

import json

import pytest

from src.config import strategy
from src.infra import t4_journal as journal


@pytest.fixture()
def journal_file(tmp_path, monkeypatch):
    """Point the journal at a temp file and enable it for the test."""
    cfg = strategy()
    monkeypatch.setattr(cfg, "T4_JOURNAL_ENABLED", True)
    path = tmp_path / "t4_journal.jsonl"
    monkeypatch.setattr(cfg, "T4_JOURNAL_PATH", str(path))
    return path


def _run(event: dict) -> dict:
    return journal.journal_processor(None, "info", event)


def test_is_t4_event_selects_decisions_fills_and_exits() -> None:
    assert journal.is_t4_event({"event": "engine.trigger_tN", "tier": 4})
    assert journal.is_t4_event({"event": "engine.tN_skip_t4_strike", "tier": 4})
    assert journal.is_t4_event({"event": "engine.tN_skip_drift_gate", "tier": 4})
    assert journal.is_t4_event({"event": "engine.tier_filled", "tier": 4})
    assert journal.is_t4_event({"event": "engine.t4_time_bypass"})
    assert journal.is_t4_event({"event": "engine.custom", "t4_z": 1.2})
    assert journal.is_t4_event({"event": "engine.trigger_tp"})
    assert journal.is_t4_event({"event": "engine.tp_filled"})
    assert journal.is_t4_event({"event": "engine.trigger_stop_loss"})
    assert journal.is_t4_event({"event": "engine.stop_loss_filled"})
    assert journal.is_t4_event({"event": "position.tp_fill_recorded"})
    assert journal.is_t4_event({"event": "round.resolved"})
    assert journal.is_t4_event({"event": "order_manager.fill_record", "tier": "T4"})
    assert journal.is_t4_event({"event": "order_manager.fill_record", "tier": "TP"})
    assert journal.is_t4_event({"event": "order_manager.fill_record", "tier": "SL1"})


def test_is_t4_event_ignores_unrelated_events() -> None:
    assert not journal.is_t4_event({"event": "engine.tier_filled", "tier": 2})
    assert not journal.is_t4_event({"event": "order_manager.fill_record", "tier": "T2"})
    assert not journal.is_t4_event({"event": "engine.gate"})
    assert not journal.is_t4_event({"event": "STATUS"})
    # State-only rows would turn the journal into a second STATUS stream.
    assert not journal.is_t4_event(
        {"event": "engine.tN_skip_price_above_fire", "tier": 4}
    )


def test_processor_appends_one_json_line(journal_file) -> None:
    event = {
        "event": "engine.trigger_tN",
        "tier": 4,
        "t4_z": 1.25,
        "t4_remaining_s": 300.0,
        "note": "中文也要能读",
    }
    returned = _run(event)
    assert returned is event

    lines = journal_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["event"] == "engine.trigger_tN"
    assert record["t4_z"] == pytest.approx(1.25)
    assert record["note"] == "中文也要能读"
    assert record["log_level"] == "info"
    assert isinstance(record["ts_ms"], int)

    # Non-T4 events do not append.
    _run({"event": "engine.gate", "drift": 0.0001})
    assert len(journal_file.read_text(encoding="utf-8").splitlines()) == 1


def test_journal_can_be_disabled(journal_file, monkeypatch) -> None:
    monkeypatch.setattr(strategy(), "T4_JOURNAL_ENABLED", False)

    _run({"event": "engine.trigger_tN", "tier": 4})

    assert not journal_file.exists()


def test_write_failure_never_raises(tmp_path, monkeypatch) -> None:
    cfg = strategy()
    monkeypatch.setattr(cfg, "T4_JOURNAL_ENABLED", True)
    # A directory is not a valid append target; the processor must swallow it.
    monkeypatch.setattr(cfg, "T4_JOURNAL_PATH", str(tmp_path))

    _run({"event": "engine.trigger_tN", "tier": 4})


def test_default_path_follows_the_data_dir(tmp_path, monkeypatch) -> None:
    cfg = strategy()
    monkeypatch.setattr(cfg, "T4_JOURNAL_PATH", "")
    monkeypatch.setenv("POLYMARKET_DATA_DIR", str(tmp_path))

    assert journal.journal_path() == tmp_path / "t4_journal.jsonl"
