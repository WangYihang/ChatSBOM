import logging
import os
import sys
from typing import Any

import structlog


def setup_logging(level: str = 'INFO') -> None:
    """
    Configure structured logging for the application.
    SSOT for logging configuration.
    """
    # Disable standard logging handlers to avoid duplication
    logging.basicConfig(format='%(message)s', stream=sys.stdout, level=level)

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt='iso'),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    # Different formatters for Dev (Console) vs Prod (JSON)
    if os.getenv('ENV') == 'production':
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]
    else:
        # Development mode: Nice colored console output
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(),
        ]

    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
