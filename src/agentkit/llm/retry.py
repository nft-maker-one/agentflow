"""Retry decision logic for the LLM Gateway.

Pure function: given an :class:`LLMError` and the current attempt
count, return a :class:`RetryDecision` (whether to retry, and how
long to wait first).

The Gateway calls this in its 9-step pipeline — we keep the policy
in a separate module so it's easy to unit-test deterministically
without spinning up providers.

Policy table mirrors ``Doc06 §4.2``.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from agentkit.llm.errors import LLMError, LLMErrorClass


@dataclass(frozen=True)
class RetryDecision:
    """Outcome of :func:`decide_retry`."""

    should_retry: bool
    wait_ms: int
    reason: str


@dataclass(frozen=True)
class RetryPolicy:
    """Per-error-class retry budget + backoff parameters.

    Defaults reflect ``Doc06 §4.2`` recommendations.
    """

    max_attempts_5xx: int = 3
    max_attempts_429: int = 3
    max_attempts_timeout: int = 1  # one extra try
    max_attempts_unknown: int = 2

    base_backoff_ms_5xx: int = 500
    base_backoff_ms_429: int = 2_000
    base_backoff_ms_unknown: int = 500

    max_backoff_ms: int = 8_000
    backoff_factor: float = 2.0
    jitter: bool = True


def _exp_backoff(*, base_ms: int, attempt: int, factor: float, max_ms: int, jitter: bool) -> int:
    """Compute exponential backoff with optional jitter.

    ``attempt`` is the *next* attempt number (1-indexed). The first
    retry waits ``base_ms``, the second ``base_ms * factor``, etc.,
    capped at ``max_ms``.
    """
    if attempt < 1:
        attempt = 1
    delay = min(int(base_ms * (factor ** (attempt - 1))), max_ms)
    if jitter and delay > 0:
        # Equal jitter — between 50% and 100% of the computed delay.
        delay = random.randint(delay // 2, delay)  # noqa: S311 - not security-sensitive
    return delay


def decide_retry(
    err: LLMError,
    *,
    attempt: int,
    policy: RetryPolicy | None = None,
) -> RetryDecision:
    """Decide whether to retry after ``err`` on attempt #``attempt``.

    ``attempt`` is the count of *attempts already made* (i.e. how
    many times we've called the provider). ``attempt=1`` means we
    just had our first failure and are deciding on attempt #2.

    Returns a :class:`RetryDecision`. The Gateway is responsible
    for actually awaiting ``wait_ms`` and re-issuing the call.
    """
    p = policy or RetryPolicy()

    # Errors that are never retried regardless of attempt count.
    if not err.retryable:
        return RetryDecision(False, 0, f"non-retryable class {err.klass.value}")

    # Special-case: provider-supplied ``Retry-After`` header is the
    # single most authoritative wait value we can have. We do NOT
    # apply our exponential-backoff math or our ``max_backoff_ms``
    # cap to it — those are policies for waits we generate ourselves;
    # a server explicitly telling us "wait 30s" should be honored.
    if err.klass is LLMErrorClass.RATE_LIMIT_429 and err.retry_after_ms is not None:
        if attempt >= p.max_attempts_429:
            return RetryDecision(
                False,
                0,
                f"attempt {attempt} >= max {p.max_attempts_429} for "
                f"{err.klass.value}",
            )
        return RetryDecision(
            True,
            err.retry_after_ms,
            f"retry on {err.klass.value} honoring Retry-After "
            f"(attempt {attempt + 1})",
        )

    # Pick the per-class budget + base backoff.
    if err.klass is LLMErrorClass.TRANSIENT_5XX:
        max_attempts = p.max_attempts_5xx
        base_ms = p.base_backoff_ms_5xx
    elif err.klass is LLMErrorClass.RATE_LIMIT_429:
        max_attempts = p.max_attempts_429
        base_ms = p.base_backoff_ms_429
    elif err.klass is LLMErrorClass.TIMEOUT:
        max_attempts = p.max_attempts_timeout
        base_ms = 0  # immediate single retry
    else:  # UNKNOWN
        max_attempts = p.max_attempts_unknown
        base_ms = p.base_backoff_ms_unknown

    if attempt >= max_attempts:
        return RetryDecision(
            False, 0, f"attempt {attempt} >= max {max_attempts} for {err.klass.value}",
        )

    wait = _exp_backoff(
        base_ms=base_ms,
        attempt=attempt,
        factor=p.backoff_factor,
        max_ms=p.max_backoff_ms,
        jitter=p.jitter,
    )
    return RetryDecision(True, wait, f"retry on {err.klass.value} (attempt {attempt + 1})")
