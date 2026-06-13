"""Tests for the OpenTelemetry tracing façade."""

from __future__ import annotations

import pytest

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from agentkit.bus.builder import build_envelope
from agentkit.observability.tracing import (
    configure_tracing,
    get_tracer,
    span_from_envelope,
    traced,
)


# OTel allows setting the global tracer provider only once. Install
# it for the whole test module up front; individual tests clear the
# in-memory exporter to get a fresh view of spans.
_EXPORTER = InMemorySpanExporter()


@pytest.fixture(scope="module", autouse=True)
def _install_provider() -> None:
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(_EXPORTER))
    # ``set_tracer_provider`` is a no-op if a provider was already
    # installed earlier in the process. We always *try* — if it
    # silently fails, the existing provider may still let us see
    # spans through our exporter (since OTel will use whatever
    # provider is registered + ignore our extra processor).
    try:
        trace.set_tracer_provider(provider)
    except Exception:
        pass


@pytest.fixture(autouse=True)
def _reset_exporter() -> None:
    _EXPORTER.clear()


def _spans():
    return list(_EXPORTER.get_finished_spans())


# ----------------------------------------------------------------
# get_tracer + start_span
# ----------------------------------------------------------------


class TestBasicTracing:
    def test_start_span_records(self) -> None:
        tracer = get_tracer("test")
        with tracer.start_as_current_span("test.unit") as span:
            span.set_attribute("foo", "bar")

        assert any(s.name == "test.unit" for s in _spans())


# ----------------------------------------------------------------
# @traced decorator
# ----------------------------------------------------------------


class TestTracedDecorator:
    def test_sync_function_traced(self) -> None:
        @traced("compile.test_fn")
        def sync_fn(x: int) -> int:
            return x * 2

        assert sync_fn(5) == 10
        assert any(s.name == "compile.test_fn" for s in _spans())

    async def test_async_function_traced(self) -> None:
        @traced("agent.dispatch")
        async def async_fn(x: int) -> int:
            return x + 1

        result = await async_fn(7)
        assert result == 8
        assert any(s.name == "agent.dispatch" for s in _spans())

    def test_exception_marks_span_error(self) -> None:
        @traced("test.fail")
        def bomb() -> None:
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            bomb()

        bad = next(s for s in _spans() if s.name == "test.fail")
        from opentelemetry.trace import StatusCode  # noqa: PLC0415
        assert bad.status.status_code is StatusCode.ERROR


# ----------------------------------------------------------------
# span_from_envelope — propagate trace_id from Bus
# ----------------------------------------------------------------


class TestSpanFromEnvelope:
    def test_envelope_attrs_attached_to_span(self) -> None:
        env = build_envelope(
            topic="agent.x.in.q",
            payload={"a": 1},
            workflow_id="wf_demo",
            run_id="run_test",
            trace_id="trc_test",
        )

        with span_from_envelope(env, "agent.dispatch") as span:
            span.set_attribute("custom", "yes")

        ad = next(s for s in _spans() if s.name == "agent.dispatch")
        attrs = dict(ad.attributes)
        assert attrs["agentkit.run_id"] == "run_test"
        assert attrs["agentkit.workflow_id"] == "wf_demo"
        assert attrs["agentkit.topic"] == "agent.x.in.q"
        assert attrs["custom"] == "yes"


# ----------------------------------------------------------------
# Configure tracing — public API smoke
# ----------------------------------------------------------------


class TestConfigure:
    def test_configure_no_endpoint_no_crash(self) -> None:
        # Should be a no-op-ish provider; no exception even if OTel
        # rejects the override (we still keep going).
        configure_tracing(service_name="agentkit-test", sample_rate=1.0)

        @traced("smoke.after_configure")
        def dummy() -> int:
            return 1

        assert dummy() == 1
