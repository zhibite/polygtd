"""Structured logging setup via structlog."""
from __future__ import annotations

import logging
import os
import sys
import structlog

from src.config import log_config
from src.infra import t4_journal


def configure_logging() -> None:
    cfg = log_config()
    level = getattr(logging, cfg.LEVEL.upper(), logging.INFO)

    # Silence HTTP library logs (httpx, httpcore)
    for lib in ("httpx", "httpcore", "http"):
        logging.getLogger(lib).setLevel(logging.WARNING)

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(level),
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            # Mirror the T4 decision/exit subset into an append-only JSONL
            # file before the console renderer collapses the event dict.
            t4_journal.journal_processor,
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Boot markers: make the journal's location and its durability (or
    # lack of it) visible in both the console and the journal itself.
    if t4_journal.journal_enabled():
        log = structlog.get_logger(__name__)
        path = t4_journal.journal_path()
        persistent = t4_journal.journal_persistent()
        log.info("t4_journal.started", path=str(path), persistent=persistent)
        if not persistent and os.name == "posix":
            log.warning(
                "t4_journal.ephemeral",
                path=str(path),
                note="/app/data is not a mount — the journal is lost on "
                     "redeploy; add a Coolify persistent volume at /app/data",
            )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
