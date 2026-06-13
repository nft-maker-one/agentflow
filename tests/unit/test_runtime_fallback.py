"""Unit tests for failure classification and retry timing."""

from __future__ import annotations

from agentkit.common.errors import PermanentError, TransientError
from agentkit.llm.errors import LLMError, LLMErrorClass
from agentkit.runtime.errors import GuardrailExceeded
from agentkit.runtime.fallback import (
    FailureClass,
    RetryTimingPolicy,
    classify_handler_exception,
    compute_next_retry_ms,
)


class TestClassify:
    def test_guardrail_exceeded(self) -> None:
        assert (
            classify_handler_exception(GuardrailExceeded("run.tokens"))
            is FailureClass.GUARDRAIL_EXCEEDED
        )

    def test_llm_auth_is_fatal(self) -> None:
        err = LLMError(LLMErrorClass.AUTH, "401")
        assert classify_handler_exception(err) is FailureClass.FATAL

    def test_llm_invalid_request_is_fatal(self) -> None:
        err = LLMError(LLMErrorClass.INVALID_REQUEST, "400")
        assert classify_handler_exception(err) is FailureClass.FATAL

    def test_llm_content_filter_is_fatal(self) -> None:
        err = LLMError(LLMErrorClass.CONTENT_FILTER, "403")
        assert classify_handler_exception(err) is FailureClass.FATAL

    def test_llm_5xx_is_recoverable(self) -> None:
        err = LLMError(LLMErrorClass.TRANSIENT_5XX, "503")
        assert classify_handler_exception(err) is FailureClass.RECOVERABLE

    def test_permanent_error_is_fatal(self) -> None:
        assert classify_handler_exception(PermanentError("nope")) is FailureClass.FATAL

    def test_jinja_template_error_is_fatal(self) -> None:
        # A prompt referencing a missing field is deterministic — must NOT
        # be retried (it would fail identically every attempt).
        from jinja2 import UndefinedError
        assert (
            classify_handler_exception(UndefinedError("'dict object' has no attribute 'topic'"))
            is FailureClass.FATAL
        )

    def test_transient_error_is_recoverable(self) -> None:
        assert (
            classify_handler_exception(TransientError("try again"))
            is FailureClass.RECOVERABLE
        )

    def test_vanilla_exception_defaults_to_recoverable(self) -> None:
        assert (
            classify_handler_exception(RuntimeError("idk"))
            is FailureClass.RECOVERABLE
        )


class TestRetryTiming:
    def test_first_retry_uses_base(self) -> None:
        p = RetryTimingPolicy(base_ms=100, factor=2.0, max_ms=2_000, jitter=False)
        assert compute_next_retry_ms(attempt=0, policy=p) == 100

    def test_exponential_growth(self) -> None:
        p = RetryTimingPolicy(base_ms=100, factor=2.0, max_ms=10_000, jitter=False)
        assert compute_next_retry_ms(attempt=0, policy=p) == 100
        assert compute_next_retry_ms(attempt=1, policy=p) == 200
        assert compute_next_retry_ms(attempt=2, policy=p) == 400

    def test_max_cap(self) -> None:
        p = RetryTimingPolicy(base_ms=100, factor=2.0, max_ms=300, jitter=False)
        assert compute_next_retry_ms(attempt=10, policy=p) == 300

    def test_jitter_keeps_within_bounds(self) -> None:
        p = RetryTimingPolicy(base_ms=200, factor=1.0, max_ms=200, jitter=True)
        # All samples must lie in [base/2, base].
        samples = [compute_next_retry_ms(attempt=0, policy=p) for _ in range(50)]
        assert all(100 <= s <= 200 for s in samples)
