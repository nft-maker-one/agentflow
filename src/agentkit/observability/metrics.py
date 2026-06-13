"""Centralized Prometheus metrics — Doc10 §4.

Every metric in §4.1-§4.7 is registered here exactly once. Callers
across modules access them via the :data:`metrics` singleton::

    from agentkit.observability import metrics

    metrics.llm_request_total.labels(
        provider="openai", model="gpt-4o", result="ok",
    ).inc()
    metrics.llm_request_duration_seconds.labels(
        provider="openai", model="gpt-4o",
    ).observe(0.842)

Why one central module:

* Avoids duplicate-registration errors when modules are imported
  in arbitrary order.
* Gives a single grep-friendly source of truth for "what metrics
  do we expose".
* Makes the ``/metrics`` Prometheus endpoint trivial — just expose
  ``REGISTRY``.

We use the **default global Prometheus registry** so the standard
``prometheus_client.start_http_server`` (and our :class:`ProbeServer`)
exposes everything automatically.
"""

from __future__ import annotations

from typing import Any

from prometheus_client import (
    REGISTRY,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
)

# Default histogram buckets — kept aggressive so we capture both
# sub-ms metric fast paths and minute-long LLM stream latencies.
_DURATION_BUCKETS = (
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
    1, 2.5, 5, 10, 30, 60, 120, 300, 600,
)


class _MetricsRegistry:
    """Holds every metric AgentKit emits (Doc10 §4).

    Constructed exactly once at import time. Pass a custom Prometheus
    ``CollectorRegistry`` to :func:`reset_for_test` if a test wants
    isolated counters — by default everything lands in the global
    ``prometheus_client.REGISTRY``.
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        # Allow callers (mostly tests) to scope metrics to their own
        # registry without touching the global one.
        self._registry: CollectorRegistry = registry or REGISTRY
        self._build()

    # ------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------

    def _c(self, name: str, doc: str, labels: list[str]) -> Counter:
        return Counter(name, doc, labels, registry=self._registry)

    def _g(self, name: str, doc: str, labels: list[str]) -> Gauge:
        return Gauge(name, doc, labels, registry=self._registry)

    def _h(
        self, name: str, doc: str, labels: list[str],
        *, buckets: tuple[float, ...] = _DURATION_BUCKETS,
    ) -> Histogram:
        return Histogram(
            name, doc, labels, buckets=buckets, registry=self._registry,
        )

    def _build(self) -> None:
        # ---- Doc10 §4.1 EventBus ----
        self.bus_publish_total = self._c(
            "bus_publish_total", "Total Bus publishes",
            ["topic", "result"],
        )
        self.bus_publish_bytes = self._c(
            "bus_publish_bytes_total", "Total Bus publish payload bytes",
            ["topic"],
        )
        self.bus_consume_total = self._c(
            "bus_consume_total", "Total Bus consume events",
            ["topic", "group", "result"],
        )
        self.bus_consumer_lag = self._g(
            "bus_consumer_lag", "Bus consumer lag (messages)",
            ["topic", "group", "partition"],
        )
        self.bus_inflight = self._g(
            "bus_inflight", "Bus in-flight messages",
            ["topic", "group"],
        )
        self.bus_dlq_total = self._c(
            "bus_dlq_total", "Total Bus DLQ events",
            ["topic", "reason"],
        )
        self.bus_replay_total = self._c(
            "bus_replay_total", "Total Bus replay events",
            ["topic", "mode"],
        )
        self.bus_broker_health = self._g(
            "bus_broker_health", "Broker health status (1=healthy, 0=down)",
            ["endpoint"],
        )

        # ---- Doc10 §4.2 Agent Runtime ----
        self.agent_state = self._g(
            "agent_state", "Agent FSM state (gauge per state combination)",
            ["agent_id", "template_key", "state"],
        )
        self.agent_event_processed_total = self._c(
            "agent_event_processed_total", "Events processed by agents",
            ["template_key", "result"],
        )
        self.agent_event_duration_seconds = self._h(
            "agent_event_duration_seconds", "Agent event handler latency",
            ["template_key"],
        )
        self.agent_inflight = self._g(
            "agent_inflight", "Agent inflight events",
            ["template_key", "agent_id"],
        )
        self.agent_retry_total = self._c(
            "agent_retry_total", "Agent retries",
            ["template_key", "reason"],
        )
        self.agent_fallback_total = self._c(
            "agent_fallback_total", "Agent fallback strategy fires",
            ["template_key", "strategy"],
        )
        self.agent_state_transition_total = self._c(
            "agent_state_transition_total", "FSM transitions",
            ["from", "to", "reason"],
        )
        self.gating_block_total = self._c(
            "gating_block_total", "Gating drops",
            ["gate", "template_key", "reason"],
        )

        # ---- Doc10 §4.3 Workflow & Compiler ----
        self.compile_total = self._c(
            "compile_total", "Workflow compile attempts",
            ["result"],
        )
        self.validate_violation_total = self._c(
            "validate_violation_total", "Compiler validation violations",
            ["rule"],
        )

        # ---- Doc10 §4.4 Orchestrator ----
        self.run_active = self._g(
            "run_active", "Active runs",
            ["workflow_id", "status"],
        )
        self.run_started_total = self._c(
            "run_started_total", "Runs started",
            ["workflow_id", "trigger"],
        )
        self.run_completed_total = self._c(
            "run_completed_total", "Runs completed",
            ["workflow_id", "terminal_status"],
        )
        self.run_duration_seconds = self._h(
            "run_duration_seconds", "Run wall-clock duration",
            ["workflow_id"],
        )
        self.branch_decision_total = self._c(
            "branch_decision_total", "Switch / branch routing decisions",
            ["edge_id", "by"],
        )
        self.orch_leader = self._g(
            "orch_leader", "Whether this Orchestrator instance holds the leader lock",
            ["instance"],
        )

        # ---- Doc10 §4.5 LLM Gateway ----
        self.llm_request_total = self._c(
            "llm_request_total", "LLM requests",
            ["provider", "model", "result"],
        )
        self.llm_request_duration_seconds = self._h(
            "llm_request_duration_seconds", "LLM request latency",
            ["provider", "model"],
        )
        self.llm_request_attempts = self._h(
            "llm_request_attempts",
            "LLM request retry attempt count",
            ["provider", "model"],
            buckets=(1, 2, 3, 4, 5, 8, 13),
        )
        self.llm_tokens_total = self._c(
            "llm_tokens_total", "LLM tokens consumed",
            ["provider", "model", "kind"],
        )
        self.llm_cost_usd_total = self._c(
            "llm_cost_usd_total", "LLM cost (informational)",
            ["provider", "model"],
        )
        self.llm_error_total = self._c(
            "llm_error_total", "LLM errors by class",
            ["provider", "model", "klass"],
        )
        self.llm_fallback_total = self._c(
            "llm_fallback_total", "LLM provider fallbacks",
            ["from_provider", "to_provider", "klass"],
        )
        self.llm_rate_limited_total = self._c(
            "llm_rate_limited_total", "LLM rate-limit decisions",
            ["provider", "model", "action"],
        )
        self.llm_provider_health = self._g(
            "llm_provider_health", "LLM provider health (1=ok, 0=down)",
            ["provider"],
        )

        # ---- Doc10 §4.6 Guardrail ----
        self.guardrail_precheck_total = self._c(
            "guardrail_precheck_total", "Guardrail precheck decisions",
            ["result", "layer", "dim"],
        )
        self.guardrail_consume_total = self._c(
            "guardrail_consume_total", "Guardrail consume calls",
            ["overshoot"],
        )
        self.guardrail_reservation_active = self._g(
            "guardrail_reservation_active", "Active guardrail reservations",
            ["workflow_id"],
        )
        self.guardrail_run_tokens_used = self._g(
            "guardrail_run_tokens_used", "Run-level tokens used",
            ["run_id", "workflow_id"],
        )
        self.guardrail_run_cycles_used = self._g(
            "guardrail_run_cycles_used", "Run-level cycles used",
            ["run_id", "workflow_id"],
        )
        self.guardrail_alert_total = self._c(
            "guardrail_alert_total", "Guardrail quota-exceeded alerts",
            ["layer", "dim"],
        )
        self.guardrail_redis_unavailable_total = self._c(
            "guardrail_redis_unavailable_total",
            "Guardrail Redis unavailability events",
            ["mode"],
        )

        # ---- Doc10 §4.7 Notifier ----
        self.notifier_rule_match_total = self._c(
            "notifier_rule_match_total", "Notifier rule match outcomes",
            ["rule_id", "result"],
        )
        self.notifier_sent_total = self._c(
            "notifier_sent_total", "Notifier dispatches",
            ["channel", "severity", "result"],
        )
        self.notifier_send_duration_seconds = self._h(
            "notifier_send_duration_seconds", "Notifier dispatch latency",
            ["channel"],
        )
        self.notifier_dedup_collapsed_total = self._c(
            "notifier_dedup_collapsed_total", "Notifier deduplications",
            ["rule_id"],
        )
        self.notifier_rate_limited_total = self._c(
            "notifier_rate_limited_total", "Notifier rate-limit drops",
            ["scope"],
        )

    # ------------------------------------------------------------
    # Test helpers
    # ------------------------------------------------------------

    def reset_for_test(self) -> None:
        """Drop+rebuild every metric. Useful between tests to get clean counters.

        WARNING: this only works if metrics live in a *test-scoped* registry —
        the global ``REGISTRY`` doesn't allow re-registration of the same name.
        """
        # Unregister any known collectors.
        registered = list(self._registry._names_to_collectors.values())  # type: ignore[attr-defined]
        for c in registered:
            try:
                self._registry.unregister(c)
            except KeyError:
                pass
        self._build()


# Single instance — every module imports this.
metrics = _MetricsRegistry()


__all__ = ["metrics"]
