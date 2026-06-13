"""AgentKit — distributed event-driven Agent orchestration framework.

This is the top-level package. It re-exports the most commonly used
SDK surface so user code can do::

    from agentkit import workflow, agent, judge, IRBuilder, Event, AgentContext

For specialized internals (Bus / LLM / Notifier / Guardrail …)
import from the corresponding sub-package directly.

We deliberately keep top-level imports lightweight — pulling in
heavy adapters (e.g. ``aiokafka``, ``redis``) only when their
respective sub-package is imported.
"""

from __future__ import annotations

__version__ = "0.1.0"


# ============================================================
# Lightweight, always-safe imports
# ============================================================

# Core data types — no I/O, no heavy deps.
from agentkit.models.envelope import Envelope
from agentkit.models.enums import AgentState, Role, RunStatus

# User-facing event shape (Doc03 §3.1) + handler context.
from agentkit.runtime.context import AgentContext, Event

# SDK ergonomics — workflow factory + decorators + builder.
from agentkit.sdk import (
    END,
    ERROR,
    START,
    Agent,
    IRBuilder,
    WorkflowDef,
    agent,
    judge,
    workflow,
)

# Optional control-plane client (requires httpx, already a dep).
from agentkit.client import AgentKitClient


__all__ = [
    "END",
    "ERROR",
    "START",
    "Agent",
    "AgentContext",
    "AgentState",
    "Envelope",
    "Event",
    "IRBuilder",
    "Role",
    "RunStatus",
    "WorkflowDef",
    "__version__",
    "agent",
    "AgentKitClient",
    "judge",
    "workflow",
]
