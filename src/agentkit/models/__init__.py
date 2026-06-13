"""Pydantic data models shared across all higher layers.

This package contains *only* data structures (Pydantic models, enums,
TypedDicts) — no I/O, no business logic, no side effects. Modules at
this layer may import from :mod:`agentkit.common` and from each
other, but MUST NOT import from any higher-layer package.
"""

from agentkit.models.enums import AgentState, Role, RunStatus
from agentkit.models.envelope import (
    AgentRef,
    Envelope,
    EnvelopeHeaders,
    ToFilter,
)

__all__ = [
    # enums
    "AgentState",
    "Role",
    "RunStatus",
    # envelope
    "AgentRef",
    "Envelope",
    "EnvelopeHeaders",
    "ToFilter",
]
