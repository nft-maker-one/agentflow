"""Common utilities used across all AgentKit modules.

This is the lowest layer of the dependency graph: it MUST NOT
import from any other ``agentkit.*`` package. Higher layers
import from here freely.
"""

from agentkit.common.errors import (
    AgentKitError,
    ConfigError,
    PermanentError,
    TransientError,
    ValidationError,
)
from agentkit.common.ids import (
    new_agent_id,
    new_event_id,
    new_reservation_id,
    new_run_id,
    new_trace_id,
    new_ulid,
    prefixed_id,
)
from agentkit.common.logging import get_logger, setup_logging
from agentkit.common.time import from_iso, to_iso, utcnow

__all__ = [
    # errors
    "AgentKitError",
    "ConfigError",
    "PermanentError",
    "TransientError",
    "ValidationError",
    # ids
    "new_agent_id",
    "new_event_id",
    "new_reservation_id",
    "new_run_id",
    "new_trace_id",
    "new_ulid",
    "prefixed_id",
    # logging
    "get_logger",
    "setup_logging",
    # time
    "from_iso",
    "to_iso",
    "utcnow",
]
