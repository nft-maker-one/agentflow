"""Tests for the centralized metrics façade."""

from __future__ import annotations

from agentkit.observability import metrics


class TestMetricsAvailable:
    """Smoke checks — every metric in Doc10 §4 should be addressable."""

    def test_all_doc10_metrics_present(self) -> None:
        # Spot-check one metric from each module to confirm the
        # registry build path created them.
        for attr in [
            # §4.1 EventBus
            "bus_publish_total", "bus_consume_total", "bus_dlq_total",
            # §4.2 Runtime
            "agent_state", "agent_event_processed_total",
            "gating_block_total", "agent_state_transition_total",
            # §4.3 Compiler
            "compile_total", "validate_violation_total",
            # §4.4 Orchestrator
            "run_active", "run_started_total", "run_completed_total",
            "branch_decision_total",
            # §4.5 LLM
            "llm_request_total", "llm_request_duration_seconds",
            "llm_tokens_total", "llm_cost_usd_total", "llm_fallback_total",
            # §4.6 Guardrail
            "guardrail_precheck_total", "guardrail_run_tokens_used",
            "guardrail_alert_total",
            # §4.7 Notifier
            "notifier_rule_match_total", "notifier_sent_total",
            "notifier_dedup_collapsed_total",
        ]:
            assert hasattr(metrics, attr), f"missing metric: {attr}"


class TestMetricsIncrement:
    def test_counter_increments(self) -> None:
        before = _sample_counter("llm_request_total", {"provider": "p", "model": "m", "result": "ok"})
        metrics.llm_request_total.labels(
            provider="p", model="m", result="ok",
        ).inc()
        after = _sample_counter("llm_request_total", {"provider": "p", "model": "m", "result": "ok"})
        assert after == before + 1

    def test_histogram_observes(self) -> None:
        # Observation just returns None — we just verify no crash
        # and the count metric exists in the underlying collector.
        metrics.llm_request_duration_seconds.labels(
            provider="p", model="m",
        ).observe(0.5)
        # Confirm count went up.
        count = _sample_counter(
            "llm_request_duration_seconds_count",
            {"provider": "p", "model": "m"},
        )
        assert count >= 1

    def test_gauge_set(self) -> None:
        metrics.llm_provider_health.labels(provider="p").set(1)
        v = _sample_gauge("llm_provider_health", {"provider": "p"})
        assert v == 1.0

        metrics.llm_provider_health.labels(provider="p").set(0)
        v = _sample_gauge("llm_provider_health", {"provider": "p"})
        assert v == 0.0


class TestMetricsLabels:
    def test_labels_isolated(self) -> None:
        # Two distinct label combinations should not interfere.
        a = metrics.llm_tokens_total.labels(
            provider="openai", model="gpt-4o", kind="prompt",
        )
        b = metrics.llm_tokens_total.labels(
            provider="openai", model="gpt-4o", kind="completion",
        )
        a.inc(10)
        b.inc(5)

        assert _sample_counter(
            "llm_tokens_total",
            {"provider": "openai", "model": "gpt-4o", "kind": "prompt"},
        ) >= 10
        assert _sample_counter(
            "llm_tokens_total",
            {"provider": "openai", "model": "gpt-4o", "kind": "completion"},
        ) >= 5


# ----------------------------------------------------------------
# Helpers — read raw values from the prometheus_client registry
# ----------------------------------------------------------------


def _sample_counter(name: str, labels: dict[str, str]) -> float:
    """Return the current value of a counter+labels pair."""
    from prometheus_client import REGISTRY  # noqa: PLC0415
    # prometheus_client exposes ``get_sample_value`` for exactly this.
    val = REGISTRY.get_sample_value(name, labels=labels)
    if val is None:
        # Counter samples may report under name + ``_total`` suffix.
        val = REGISTRY.get_sample_value(name + "_total", labels=labels)
    return val if val is not None else 0.0


def _sample_gauge(name: str, labels: dict[str, str]) -> float:
    from prometheus_client import REGISTRY  # noqa: PLC0415
    val = REGISTRY.get_sample_value(name, labels=labels)
    return val if val is not None else 0.0
