"""LLM error taxonomy.

The framework's contract with provider adapters: every failure must
be classified into one of :class:`LLMErrorClass` so the Gateway's
retry / fallback decision logic is provider-agnostic.

See ``Doc06 §2.3``.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from agentkit.common.errors import AgentKitError


class LLMErrorClass(StrEnum):
    """Provider-agnostic error classification.

    Decision matrix (per ``Doc06 §4.2`` retry policy):

    +------------------+--------+----------+-----------+
    | class            | retry? | fallback?| Runtime?  |
    +==================+========+==========+===========+
    | TRANSIENT_5XX    | yes    | yes      | RETRY     |
    | RATE_LIMIT_429   | yes    | yes      | RETRY     |
    | TIMEOUT          | once   | yes      | RETRY     |
    | UNKNOWN          | yes    | yes      | RETRY     |
    | CONTENT_FILTER   | no     | no       | FAILURE   |
    | AUTH             | no     | no       | FAILURE   |
    | INVALID_REQUEST  | no     | no       | FAILURE   |
    | QUOTA_EXCEEDED   | no     | yes      | FAILURE * |
    | PROVIDER_DOWN    | no     | yes      | FAILURE * |
    +------------------+--------+----------+-----------+

    \\* the Gateway absorbs these via Provider Fallback; Runtime only
    sees them if every provider in the chain has failed.
    """

    TRANSIENT_5XX = "transient_5xx"
    RATE_LIMIT_429 = "rate_limit_429"
    TIMEOUT = "timeout"
    CONTENT_FILTER = "content_filter"
    AUTH = "auth"
    INVALID_REQUEST = "invalid_request"
    QUOTA_EXCEEDED = "quota_exceeded"
    PROVIDER_DOWN = "provider_down"
    UNKNOWN = "unknown"


# Sets used by the retry / fallback policies. Defining them as
# frozensets keeps the policy tables in one place rather than
# scattered string comparisons across the codebase.

RETRYABLE_CLASSES: frozenset[LLMErrorClass] = frozenset(
    {
        LLMErrorClass.TRANSIENT_5XX,
        LLMErrorClass.RATE_LIMIT_429,
        LLMErrorClass.TIMEOUT,
        LLMErrorClass.UNKNOWN,
    },
)

FALLBACK_ADVISED_CLASSES: frozenset[LLMErrorClass] = frozenset(
    {
        LLMErrorClass.TRANSIENT_5XX,
        LLMErrorClass.RATE_LIMIT_429,
        LLMErrorClass.TIMEOUT,
        LLMErrorClass.UNKNOWN,
        LLMErrorClass.QUOTA_EXCEEDED,
        LLMErrorClass.PROVIDER_DOWN,
    },
)


class LLMError(AgentKitError):
    """Carrier for a classified provider error.

    Adapters MUST raise this (not raw HTTP / SDK exceptions). The
    ``klass`` attribute is the single source of truth for retry /
    fallback decisions in the Gateway.
    """

    def __init__(
        self,
        klass: LLMErrorClass,
        message: str,
        *,
        provider: str = "",
        model: str = "",
        http_status: int | None = None,
        raw: dict[str, Any] | None = None,
        retry_after_ms: int | None = None,
    ) -> None:
        super().__init__(message)
        self.klass = klass
        self.provider = provider
        self.model = model
        self.http_status = http_status
        self.raw = raw or {}
        self.retry_after_ms = retry_after_ms

    @property
    def retryable(self) -> bool:
        """True iff this error is in the retry-allowed set."""
        return self.klass in RETRYABLE_CLASSES

    @property
    def advise_fallback(self) -> bool:
        """True iff the Gateway should try the next Provider in the chain."""
        return self.klass in FALLBACK_ADVISED_CLASSES

    def __repr__(self) -> str:
        return (
            f"LLMError(klass={self.klass.value}, provider={self.provider!r}, "
            f"model={self.model!r}, http_status={self.http_status}, "
            f"message={str(self)!r})"
        )
