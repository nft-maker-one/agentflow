"""OpenTelemetry tracing façade — Doc10 §5.

Two design constraints:

1. **Default no-op**. Importing AgentKit must not require an OTel
   collector. We use the default ``NoOpTracerProvider`` until
   :func:`configure_tracing` is called.

2. **trace_id propagation from Envelope**. Doc10 §3.2 mandates that
   any module touching a Run uses the *same* trace_id for the
   entire Run. Our Bus envelopes already carry ``trace_id`` /
   ``span_id`` strings; helpers in this module turn them into a
   live :class:`SpanContext` so child spans link correctly.

Public API::

    configure_tracing(endpoint=..., service_name=..., sample_rate=...)
    tracer = get_tracer("agent_runtime")
    with tracer.start_as_current_span("agent.dispatch", attributes={...}):
        ...

    # Or as a decorator:
    @traced("llm.complete")
    async def complete(...):
        ...

    # Bind an envelope's trace_id as the parent span:
    with span_from_envelope(envelope, "agent.dispatch") as span:
        ...
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, ParamSpec, TypeVar

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from opentelemetry.sdk.trace.sampling import (
    ALWAYS_OFF,
    ALWAYS_ON,
    ParentBased,
    TraceIdRatioBased,
)
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    SpanKind,
    Status,
    StatusCode,
    TraceFlags,
)

from agentkit.common.logging import get_logger
from agentkit.models.envelope import Envelope

log = get_logger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


# ============================================================
# Provider configuration
# ============================================================


_CONFIGURED: bool = False


def configure_tracing(
    *,
    endpoint: str | None = None,
    service_name: str = "agentkit",
    sample_rate: float = 1.0,
    console: bool = False,
) -> None:
    """Install a real :class:`TracerProvider` with the given exporter.

    * ``endpoint=None`` and ``console=False`` → keep the default
      no-op tracer (cheapest path; spans simply don't get exported).
    * ``console=True`` → useful for local dev: spans print to stderr.
    * ``endpoint=<url>`` → set up an OTLP gRPC exporter (Phase 2 will
      auto-detect ``opentelemetry-exporter-otlp`` from extras).

    Idempotent — calling twice replaces the previous provider.
    """
    global _CONFIGURED

    sampler = (
        ALWAYS_ON if sample_rate >= 1.0
        else ALWAYS_OFF if sample_rate <= 0.0
        else ParentBased(TraceIdRatioBased(sample_rate))
    )
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource, sampler=sampler)

    if console:
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    if endpoint:
        # Lazy import — opentelemetry-exporter-otlp is an optional dep.
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (  # noqa: PLC0415
                OTLPSpanExporter,
            )
            provider.add_span_processor(
                BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)),
            )
        except ImportError:
            log.warning(
                "tracing.otlp_exporter_missing",
                hint="install opentelemetry-exporter-otlp for OTLP export",
                endpoint=endpoint,
            )

    trace.set_tracer_provider(provider)
    _CONFIGURED = True
    log.info(
        "tracing.configured",
        service_name=service_name, sample_rate=sample_rate,
        endpoint=endpoint, console=console,
    )


def get_tracer(name: str) -> trace.Tracer:
    """Return a tracer for ``name`` (typically the calling module)."""
    return trace.get_tracer(name)


# ============================================================
# Decorator + context-manager helpers
# ============================================================


def traced(
    span_name: str | None = None,
    *,
    kind: SpanKind = SpanKind.INTERNAL,
    tracer_name: str = "agentkit",
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Wrap a function to be traced as a single span.

    Works on both sync and async functions::

        @traced("llm.complete")
        async def complete(req): ...

        @traced("compile.validate")
        def validate(ir): ...

    On exception the span is marked ``Status(ERROR)`` and re-raised.
    """

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        name = span_name or f"{fn.__module__}.{fn.__qualname__}"
        tracer = get_tracer(tracer_name)

        if _is_async(fn):
            @functools.wraps(fn)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                with tracer.start_as_current_span(name, kind=kind) as span:
                    try:
                        return await fn(*args, **kwargs)  # type: ignore[no-any-return,misc]
                    except Exception as exc:
                        span.set_status(Status(StatusCode.ERROR, str(exc)))
                        span.record_exception(exc)
                        raise
            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(fn)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            with tracer.start_as_current_span(name, kind=kind) as span:
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:
                    span.set_status(Status(StatusCode.ERROR, str(exc)))
                    span.record_exception(exc)
                    raise
        return sync_wrapper

    return decorator


@contextmanager
def span_from_envelope(
    envelope: Envelope,
    span_name: str,
    *,
    tracer_name: str = "agentkit",
    attributes: dict[str, Any] | None = None,
) -> Iterator[trace.Span]:
    """Open a child span linked to the envelope's trace_id.

    The Envelope's ``trace_id`` is a string (e.g. ``"trc_01HXZ..."``);
    OTel expects a 16-byte int trace ID. We hash the string via the
    same ULID tail every time so the same envelope produces the same
    OTel trace ID in repeated runs (deterministic mapping).
    """
    tracer = get_tracer(tracer_name)
    parent_ctx = _envelope_to_span_context(envelope)
    if parent_ctx is None:
        ctx_mgr = tracer.start_as_current_span(span_name, attributes=attributes)
    else:
        # Bind the parent context manually.
        token = trace.set_span_in_context(NonRecordingSpan(parent_ctx))
        ctx_mgr = tracer.start_as_current_span(
            span_name, context=token, attributes=attributes,
        )
    with ctx_mgr as span:
        # Always re-attach the framework-side IDs as span attributes
        # so trace backends can surface them as searchable fields.
        span.set_attribute("agentkit.run_id", envelope.run_id or "")
        span.set_attribute("agentkit.workflow_id", envelope.workflow_id or "")
        span.set_attribute("agentkit.event_id", envelope.event_id or "")
        span.set_attribute("agentkit.topic", envelope.topic)
        try:
            yield span
        except Exception as exc:
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            span.record_exception(exc)
            raise


# ============================================================
# Internals
# ============================================================


def _is_async(fn: Callable[..., Any]) -> bool:
    return hasattr(fn, "__code__") and bool(
        fn.__code__.co_flags & 0x80,  # CO_COROUTINE
    )


def _envelope_to_span_context(envelope: Envelope) -> SpanContext | None:
    """Synthesize a SpanContext from the envelope's trace_id string.

    OTel trace IDs are 128-bit ints; ours are ULID strings. We hash
    deterministically so the same envelope always maps to the same
    OTel trace context — this keeps cross-process spans linked even
    though our wire format predates OTel's ``traceparent``.
    """
    if not envelope.trace_id:
        return None
    trace_id_int = int.from_bytes(
        _stable_hash_128(envelope.trace_id), "big",
    )
    span_id_int = (
        int.from_bytes(_stable_hash_64(envelope.event_id or "evt"), "big")
        if envelope.event_id else int.from_bytes(_stable_hash_64(envelope.trace_id), "big")
    )
    if trace_id_int == 0 or span_id_int == 0:
        return None
    return SpanContext(
        trace_id=trace_id_int,
        span_id=span_id_int,
        is_remote=True,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )


def _stable_hash_128(s: str) -> bytes:
    import hashlib  # noqa: PLC0415
    return hashlib.sha256(s.encode("utf-8")).digest()[:16]


def _stable_hash_64(s: str) -> bytes:
    import hashlib  # noqa: PLC0415
    return hashlib.sha256(s.encode("utf-8")).digest()[:8]
