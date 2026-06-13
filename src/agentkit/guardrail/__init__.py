"""Guardrail — token / cycle quota enforcement (Doc07).

Public surface::

    from agentkit.guardrail import (
        # data
        AgentGuardrail,
        RunGuardrail,
        GuardrailContext,
        OverrideRecord,
        Reservation,
        RunUsage,
        # resolver
        FrameworkDefaults,
        resolve_guardrail_context,
        # backend
        RedisGuardrail,
        GuardrailSettings,
        # errors
        QuotaExceeded,
        GuardrailUnavailable,
    )

The :class:`Reservation` and :class:`GuardrailHandle` Protocol live
in :mod:`agentkit.llm.guardrail_iface` for layering reasons (LLM
Gateway in L3 needs them); :class:`RedisGuardrail` here implements
that Protocol.
"""

from agentkit.guardrail.errors import (
    GuardrailError,
    GuardrailUnavailable,
    QuotaExceeded,
)
from agentkit.guardrail.models import (
    AgentGuardrail,
    GuardrailContext,
    OverrideRecord,
    QuotaLayer,
    QuotaSource,
    RunGuardrail,
    RunUsage,
)
from agentkit.guardrail.redis_backend import (
    GuardrailSettings,
    RedisGuardrail,
)
from agentkit.guardrail.resolver import (
    FRAMEWORK_DEFAULT_AGENT,
    FRAMEWORK_DEFAULT_RUN,
    FrameworkDefaults,
    resolve_guardrail_context,
)

# Re-export Reservation from the iface module so all Guardrail
# concepts are available under one roof.
from agentkit.llm.guardrail_iface import GuardrailHandle, Reservation

__all__ = [
    "FRAMEWORK_DEFAULT_AGENT",
    "FRAMEWORK_DEFAULT_RUN",
    "AgentGuardrail",
    "FrameworkDefaults",
    "GuardrailContext",
    "GuardrailError",
    "GuardrailHandle",
    "GuardrailSettings",
    "GuardrailUnavailable",
    "OverrideRecord",
    "QuotaExceeded",
    "QuotaLayer",
    "QuotaSource",
    "RedisGuardrail",
    "Reservation",
    "RunGuardrail",
    "RunUsage",
    "resolve_guardrail_context",
]
