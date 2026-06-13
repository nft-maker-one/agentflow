"""Runtime-specific exceptions used across handler invocation, gating, and FSM."""

from __future__ import annotations

from agentkit.common.errors import (
    AgentKitError,
    PermanentError,
    TransientError,
)


class RuntimeError(AgentKitError):  # noqa: N818 — module-scoped name shadows stdlib intentionally
    """Base for all runtime-layer errors."""


# ---- Handler-side exceptions (raised inside or around user code) ----


class HandlerRecoverable(TransientError, RuntimeError):
    """User handler failed in a way that *may* succeed on retry.

    Maps to :class:`FailureClass.RECOVERABLE` and ultimately drives
    the FSM's ``Processing → Retry`` transition.
    """


class HandlerFatal(PermanentError, RuntimeError):
    """User handler failed irrecoverably (e.g. invalid prompt template).

    The Runtime sends the event to DLQ and pushes the FSM to ``Down``.
    """


class HandlerTimeout(HandlerRecoverable):
    """The handler exceeded its per-attempt timeout."""


class GuardrailExceeded(PermanentError, RuntimeError):
    """Token / cycle quota touched a hard limit — never retry.

    Mapped to :class:`FailureClass.GUARDRAIL_EXCEEDED`. The FSM
    routes straight to ``Failure`` (not ``Retry``) per Doc03 §4.1.
    """


# ---- Lifecycle / dispatcher errors ----


class GatingRejected(RuntimeError):
    """An event was dropped by one of the 4 gates.

    This is *not* an error in the strict sense — it's a normal
    runtime outcome. The Runtime acks the message and moves on.
    Gating callers raise this to short-circuit the dispatch loop.
    """

    def __init__(self, gate: str, reason: str) -> None:
        super().__init__(f"{gate}: {reason}")
        self.gate = gate
        self.reason = reason


class PermissionDenied(PermanentError, RuntimeError):
    """User code tried to publish to a non-whitelisted topic, or used a
    forbidden header field. The PublishPipeline raises this verbatim.
    """
