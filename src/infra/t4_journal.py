"""Append-only JSONL journal for T4 entry/exit decisions (2026-10-31).

Why a dedicated file and not just the console logs:

  * the container's json-file logs are capped (3 x 10 MB) and rotate, so a
    T4 decision from two weeks ago is usually gone;
  * Coolify replaces the container on every deploy, which drops both the
    Docker logs and anything under ``/app/data`` unless a volume is
    mounted;
  * tuning the T4 gates needs a low-volume, machine-readable history of
    exactly the decision fields (z, remaining time, sigma, mid) plus the
    eventual exit (TP / stop-loss / settlement).

The journal is a structlog processor: every event selected by
``is_t4_event`` is appended to ``<data dir>/t4_journal.jsonl`` as one JSON
object per line, *in addition to* the normal console output. Writes are
best-effort — a full disk or a read-only mount must never break trading.

NOTE (2026-10-31): the Coolify app currently has no persistent volume on
``/app/data``. The journal therefore survives restarts but is lost on a
redeploy. Add a persistent volume at ``/app/data`` to accumulate across
deploys; ``journal_persistent()`` reports whether it looks mounted.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

from src.config import strategy

_LOCK = threading.Lock()
_last_error_log_ts = 0.0
_ERROR_LOG_INTERVAL_S = 60.0

# Events that describe a T4 decision or a position exit. The prefix match
# keeps this list short while still catching the variants
# (engine.tp_backoff_engaged, router.stop_loss_filled, ...).
_JOURNAL_PREFIXES = (
    "engine.t4",              # t4_time_bypass, t4_journal markers
    "engine.tN_skip_t4",      # late-window / strike-gate / gap-gate skips
    "engine.trigger_tp",      # TP decisions (incl. trigger_tp_taker)
    "engine.tp_",             # tp_filled / tp_backoff / tp_retreated...
    "engine.trigger_stop_loss",
    "engine.stop_loss_",      # armed / filled / error
    "router.tp_",             # tp_placed / tp_failed
    "router.stop_loss",       # stop_loss_filled / stop_loss_failed
    "position.tp_",           # tp_fill_recorded / tp_fill_terms...
    "position.fully_closed_after_tp",
    "position.realize_closed",
    "round.resolved",         # final up/down outcome (join key: round_start_ts)
    "round.transition",
    "t4_journal.",            # boot markers (path / persistent flag)
)

# High-frequency state rows that carry no decision: keeping them would turn
# the journal into a second STATUS stream (~one line per 5 s for most of
# every round). The actual gate skips (`engine.tN_skip_t4_*`) and the other
# T4 blockers are still recorded.
_T4_STATE_ONLY_EVENTS = {
    "engine.tN_skip_price_above_fire",
    "engine.tN_skip_complete",
    "engine.tN_skip_watching",
}


def journal_enabled() -> bool:
    try:
        return bool(strategy().T4_JOURNAL_ENABLED)
    except Exception:  # noqa: BLE001 — never let config trouble break logging
        return False


def journal_path() -> Path:
    """Resolve the journal file.

    Read through the settings singleton (not ``os.getenv``) so values set
    in ``.env.docker`` are honoured — pydantic-settings parses that file
    without exporting it to the process environment.

    ``POLYMARKET_DATA_DIR`` is not a settings field (the DB uses the same
    env lookup), so it stays a direct getenv for consistency with db.py.
    """
    try:
        explicit = str(strategy().T4_JOURNAL_PATH or "").strip()
    except Exception:  # noqa: BLE001
        explicit = ""
    if explicit:
        return Path(explicit)
    data_dir = os.getenv("POLYMARKET_DATA_DIR", "").strip()
    base = (
        Path(data_dir)
        if data_dir
        else Path(__file__).resolve().parent.parent.parent / "data"
    )
    return base / "t4_journal.jsonl"


def journal_persistent() -> bool:
    """Best-effort check: is the journal on a mounted volume?

    ``os.path.ismount`` only knows about real mounts/bind mounts; a plain
    directory in the container layer returns False. That is exactly the
    signal we want for the startup warning.
    """
    path = journal_path().parent
    while not path.exists() and path != path.parent:
        path = path.parent
    try:
        return os.path.ismount(str(path))
    except OSError:
        return False


def _is_tier_4(value: Any) -> bool:
    try:
        return int(value) == 4
    except (TypeError, ValueError):
        return False


def is_t4_event(event: dict[str, Any]) -> bool:
    """True for events worth keeping for T4 calibration."""
    name = str(event.get("event") or "")
    if name.startswith(_JOURNAL_PREFIXES):
        return True
    # Any event carrying a T4 gate metric (trigger_tN) or tier=4
    # (tier_filled, tN_skip_*, ...).
    if any(str(key).startswith("t4_") for key in event):
        return True
    if _is_tier_4(event.get("tier")) and name not in _T4_STATE_ONLY_EVENTS:
        return True
    if name == "order_manager.fill_record":
        tier = str(event.get("tier") or "").upper()
        if tier == "T4" or tier == "TP" or tier.startswith("SL"):
            return True
    return False


def journal_processor(logger: Any, method_name: str, event_dict: dict) -> dict:
    """structlog processor: append matching events to the JSONL journal.

    Returns ``event_dict`` unchanged so the console renderer still gets it.
    Never raises: journal failures are logged at most once a minute and
    trading continues.
    """
    try:
        if not journal_enabled() or not is_t4_event(event_dict):
            return event_dict
        record = dict(event_dict)
        record.setdefault("ts_ms", int(time.time() * 1000))
        record.setdefault("log_level", method_name)
        line = json.dumps(record, ensure_ascii=False, default=str)
        with _LOCK:
            path = journal_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            # Open per write: no stale handle across log rotation / a
            # volume that is (un)mounted while the process runs.
            with path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
    except Exception:  # noqa: BLE001 — journaling must never break trading
        _log_write_error()
    return event_dict


def _log_write_error() -> None:
    global _last_error_log_ts
    now = time.time()
    if now - _last_error_log_ts < _ERROR_LOG_INTERVAL_S:
        return
    _last_error_log_ts = now
    logging.getLogger(__name__).warning(
        "t4_journal.write_failed path=%s", journal_path(), exc_info=True
    )
