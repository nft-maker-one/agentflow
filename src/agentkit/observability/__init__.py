"""Observability — metrics, tracing, audit, health probes (Doc10).

Public surface::

    from agentkit.observability import (
        # metrics
        metrics,                         # the global registry façade
        # tracing
        configure_tracing, get_tracer, traced,
        # audit
        AuditEntry, AuditWriter, InMemoryAuditWriter,
        # health
        ProbeServer, HealthCheck, HealthRegistry,
        # config
        ObservabilitySettings,
    )

Phase 1 deliberately *defines* every metric / span name / audit
field listed in Doc10 §4-§5 even when no production caller emits
yet. That way Phase 2 wiring is "find the call site, add one line"
rather than "decide what the metric name should be".
"""

from agentkit.observability.audit import (
    AuditEntry,
    AuditWriter,
    InMemoryAuditWriter,
)
from agentkit.observability.config import ObservabilitySettings
from agentkit.observability.health import (
    HealthCheck,
    HealthRegistry,
    ProbeServer,
    health_registry,
)
from agentkit.observability.metrics import metrics
from agentkit.observability.tracing import (
    configure_tracing,
    get_tracer,
    traced,
)

__all__ = [
    "AuditEntry",
    "AuditWriter",
    "HealthCheck",
    "HealthRegistry",
    "InMemoryAuditWriter",
    "ObservabilitySettings",
    "ProbeServer",
    "configure_tracing",
    "get_tracer",
    "health_registry",
    "metrics",
    "traced",
]
