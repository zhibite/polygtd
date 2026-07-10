"""Structured logging setup via structlog."""
from __future__ import annotations

import logging
import sys
import structlog

from src.config import log_config


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
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
