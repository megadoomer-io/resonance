"""Structured logging configuration with structlog.

JSON output in production (non-TTY), pretty console output in development (TTY).
Log level controlled by LOG_LEVEL env var (default: INFO).

Usage:
    import structlog
    log = structlog.get_logger()
    log.info("event_name", key="value")

    # Bind context for a scope (e.g., sync job):
    log = log.bind(sync_job_id="...", service="spotify")
    log.info("page_fetched", page=3, items=100)
"""

import logging
import sys

import structlog


def configure_logging(log_level: str = "INFO") -> None:
    """Configure structlog + stdlib logging integration.

    Args:
        log_level: Log level string (DEBUG, INFO, WARNING, ERROR).
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Detect if we're outputting to a terminal (dev) or pipe/file (prod)
    is_tty = sys.stderr.isatty()

    # Shared processors for both structlog and stdlib
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    if is_tty:
        # Development: pretty console output
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer()
    else:
        # Production: JSON output
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Configure stdlib logging to use structlog's formatter
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)

    # Root logger
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Suppress noisy third-party loggers
    for noisy_logger in [
        "uvicorn",
        "uvicorn.access",
        "uvicorn.error",
        "sqlalchemy.engine",
        "httpx",
        "httpcore",
        "asyncpg",
    ]:
        logging.getLogger(noisy_logger).setLevel(max(level, logging.WARNING))

    # Keep uvicorn.error at the app level (startup/shutdown messages)
    logging.getLogger("uvicorn.error").setLevel(level)
