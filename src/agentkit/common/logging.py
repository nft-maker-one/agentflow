"""Structured logging via structlog.

Every framework log line is a single JSON object with consistent
field names. See ``Doc10_Observability.md §3.3`` for the agreed schema.

Usage::

    from agentkit.common import get_logger, setup_logging

    setup_logging()  # call once at process start
    log = get_logger(__name__)
    log.info("agent.started", agent_id=agt_id, run_id=run_id)
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

from agentkit.common.config import CommonSettings, get_settings


def _build_processors(*, json_output: bool) -> list[Any]:
    """Build the structlog processor chain.

    Exposed (with the underscore prefix to mark "framework-internal")
    so tests can wire it into a custom :class:`structlog.WriteLoggerFactory`
    when they need to assert on rendered output.

    Order matters here:

    1. Merge contextvars (so ``bind_contextvars`` works).
    2. Inject log level + ISO timestamp (the Doc10 strict fields).
    3. Render exception/stack info if present.
    4. Final renderer — JSON in production, colored console in dev.
    """
    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="ts"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    if json_output:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=True))
    return processors


def setup_logging(*, force: bool = False) -> None:
    """Configure both stdlib ``logging`` and ``structlog``.

    Idempotent — calling twice is safe but a no-op the second time
    unless ``force=True`` (used by tests that want to reset state).
    """
    settings = get_settings(CommonSettings)
    level_name = settings.log_level.upper()
    level = getattr(logging, level_name, logging.INFO)

    # Configure stdlib logging so any third-party library that uses
    # ``logging`` ends up funneled through the same handler.
    root = logging.getLogger()
    if force or not root.handlers:
        for h in list(root.handlers):
            root.removeHandler(h)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        root.addHandler(handler)
        root.setLevel(level)

    structlog.configure(
        processors=_build_processors(json_output=settings.log_format == "json"),
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a bound structured logger for ``name``."""
    return structlog.get_logger(name)
