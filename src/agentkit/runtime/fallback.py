"""Failure classification + retry-decision helpers.

Mirrors the LLM module's pattern: keep policy in a small, pure-data
module so unit tests can exercise it deterministically without
spinning up a Runtime.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from enum import StrEnum

from jinja2 import TemplateError

from agentkit.common.errors import PermanentError, TransientError
from agentkit.llm.errors import LLMError, LLMErrorClass


class FailureClass(StrEnum):
    """Coarse-grained classification consumed by the FSM."""

    RECOVERABLE = "recoverable"        # → Retry / FAILURE per budget
    FATAL = "fatal"                    # → Down (no retry, no fallback)
    GUARDRAIL_EXCEEDED = "guardrail"   # → Failure (NOT Retry, never)


def classify_handler_exception(exc: BaseException) -> FailureClass:
    """Translate any exception coming out of a handler into a FailureClass.

    Hierarchy:

    * :class:`agentkit.runtime.errors.GuardrailExceeded` → ``GUARDRAIL_EXCEEDED``
    * :class:`agentkit.llm.LLMError` → use its ``retryable`` property
    * :class:`jinja2.TemplateError` (incl. ``UndefinedError`` from a
      prompt referencing a missing payload field) → ``FATAL`` — the
      template + event payload are fixed, so every retry fails
      identically; fail fast with a clear reason instead of burning
      retries and DLQ'ing.
    * :class:`agentkit.common.errors.PermanentError` → ``FATAL``
    * :class:`agentkit.common.errors.TransientError` → ``RECOVERABLE``
    * Anything else (vanilla ``Exception``) → ``RECOVERABLE`` (defensive default)
    """
    # Local import to break what would otherwise be a circular dep
    # (errors.py imports from this module via FailureClass).
    from agentkit.runtime.errors import GuardrailExceeded  # noqa: PLC0415

    if isinstance(exc, GuardrailExceeded):
        return FailureClass.GUARDRAIL_EXCEEDED
    if isinstance(exc, TemplateError):
        # Deterministic prompt-render failure — retrying is futile.
        return FailureClass.FATAL
    if isinstance(exc, LLMError):
        if exc.klass in (
            LLMErrorClass.AUTH,
            LLMErrorClass.INVALID_REQUEST,
            LLMErrorClass.CONTENT_FILTER,
        ):
            return FailureClass.FATAL
        if exc.retryable:
            return FailureClass.RECOVERABLE
        # QUOTA_EXCEEDED / PROVIDER_DOWN — Gateway absorbs via fallback;
        # if they bubble up here it means every provider failed.
        return FailureClass.RECOVERABLE
    if isinstance(exc, PermanentError):
        return FailureClass.FATAL
    if isinstance(exc, TransientError):
        return FailureClass.RECOVERABLE
    return FailureClass.RECOVERABLE


# ============================================================
# Retry timing
# ============================================================


@dataclass(frozen=True)
class RetryTimingPolicy:
    """Backoff policy for the Retry state's ``next_retry_at``."""

    base_ms: int = 500
    factor: float = 2.0
    max_ms: int = 8_000
    jitter: bool = True


def compute_next_retry_ms(
    *, attempt: int, policy: RetryTimingPolicy | None = None,
) -> int:
    """Return milliseconds to wait before next retry attempt.

    ``attempt`` is the count of *retries already performed* (0 means
    no retries yet — use the base delay).
    """
    p = policy or RetryTimingPolicy()
    if attempt < 0:
        attempt = 0
    delay = min(int(p.base_ms * (p.factor ** attempt)), p.max_ms)
    if p.jitter and delay > 0:
        delay = random.randint(delay // 2, delay)  # noqa: S311 — non-crypto
    return delay
