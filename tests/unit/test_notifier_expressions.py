"""Unit tests for the sandboxed when-expression evaluator (Doc08 §3.2)."""

from __future__ import annotations

import pytest

from agentkit.notifier.errors import RuleEvalError
from agentkit.notifier.expressions import evaluate_when


# ----------------------------------------------------------------
# Boolean / null / empty paths
# ----------------------------------------------------------------


class TestTrivialPaths:
    def test_none_expression_always_true(self) -> None:
        assert evaluate_when(None, {}) is True

    def test_empty_expression_always_true(self) -> None:
        assert evaluate_when("", {}) is True
        assert evaluate_when("   ", {}) is True

    def test_constant_true_false(self) -> None:
        assert evaluate_when("True", {}) is True
        assert evaluate_when("False", {}) is False


# ----------------------------------------------------------------
# Variable + comparison
# ----------------------------------------------------------------


class TestComparison:
    def test_eq(self) -> None:
        assert evaluate_when("topic == 'workflow.x.failed'", {"topic": "workflow.x.failed"})
        assert not evaluate_when("topic == 'foo'", {"topic": "bar"})

    def test_lt_lte_gt_gte(self) -> None:
        ctx = {"payload": {"score": 0.85}}
        assert evaluate_when("payload.score < 1.0", ctx)
        assert evaluate_when("payload.score >= 0.85", ctx)
        assert not evaluate_when("payload.score > 1.0", ctx)

    def test_chained_comparison(self) -> None:
        ctx = {"x": 5}
        assert evaluate_when("0 < x < 10", ctx)
        assert not evaluate_when("0 < x < 3", ctx)

    def test_in_membership(self) -> None:
        ctx = {"role": "judge"}
        assert evaluate_when("role in ['judge', 'writer']", ctx)
        assert not evaluate_when("role in ['runner']", ctx)


# ----------------------------------------------------------------
# Boolean ops
# ----------------------------------------------------------------


class TestBooleanOps:
    def test_and(self) -> None:
        ctx = {"payload": {"score": 0.9, "lang": "en"}}
        assert evaluate_when("payload.score >= 0.8 and payload.lang == 'en'", ctx)
        assert not evaluate_when("payload.score >= 0.8 and payload.lang == 'zh'", ctx)

    def test_or(self) -> None:
        # Either condition truthy → expression truthy.
        ctx = {"payload": {"flag": False, "score": 5}}
        assert evaluate_when("payload.flag or payload.score == 5", ctx)
        assert evaluate_when("payload.flag or True", {"payload": {"flag": False}})
        assert not evaluate_when(
            "payload.flag or payload.other == 5",
            {"payload": {"flag": False}},
        )

    def test_not(self) -> None:
        ctx = {"payload": {"failed": False}}
        assert evaluate_when("not payload.failed", ctx)


# ----------------------------------------------------------------
# JSONPath-style attribute access
# ----------------------------------------------------------------


class TestAttributeAccess:
    def test_nested_attribute(self) -> None:
        ctx = {"payload": {"a": {"b": {"c": 42}}}}
        assert evaluate_when("payload.a.b.c == 42", ctx)

    def test_missing_path_is_falsy(self) -> None:
        ctx = {"payload": {"a": 1}}
        # Comparisons against None never raise.
        assert not evaluate_when("payload.b.c == 42", ctx)
        # Truthiness:
        assert evaluate_when("not payload.b", ctx)

    def test_subscript_access(self) -> None:
        ctx = {"payload": {"items": [10, 20, 30]}}
        assert evaluate_when("payload.items[1] == 20", ctx)

    def test_subscript_missing(self) -> None:
        ctx = {"payload": {"items": [10]}}
        assert not evaluate_when("payload.items[5] == 1", ctx)


# ----------------------------------------------------------------
# Security — anything outside the whitelist is rejected
# ----------------------------------------------------------------


class TestSecurity:
    def test_function_call_rejected(self) -> None:
        with pytest.raises(RuleEvalError):
            evaluate_when("len(payload)", {"payload": [1, 2]})

    def test_lambda_rejected(self) -> None:
        with pytest.raises(RuleEvalError):
            evaluate_when("(lambda: 1)()", {})

    def test_import_rejected(self) -> None:
        with pytest.raises(RuleEvalError):
            evaluate_when("__import__('os')", {})

    def test_attribute_on_non_dict_rejected(self) -> None:
        # Even though 'topic' is a str, .upper would be a bound method;
        # we refuse to walk it to avoid leaking class-internals.
        with pytest.raises(RuleEvalError):
            evaluate_when("topic.upper", {"topic": "x"})

    def test_dict_comprehension_rejected(self) -> None:
        with pytest.raises(RuleEvalError):
            evaluate_when("{k: v for k, v in payload.items()}", {"payload": {}})

    def test_arithmetic_function_rejected(self) -> None:
        with pytest.raises(RuleEvalError):
            evaluate_when("abs(payload.x)", {"payload": {"x": -1}})


# ----------------------------------------------------------------
# Real-world rule snippets
# ----------------------------------------------------------------


class TestRealistic:
    def test_high_quality_summary(self) -> None:
        env = {
            "topic": "agent.research.out.summary",
            "payload": {"score": 0.92, "summary": "..."},
        }
        rule_when = (
            "topic == 'agent.research.out.summary' and payload.score >= 0.9"
        )
        assert evaluate_when(rule_when, env)

    def test_role_down_filter(self) -> None:
        env = {
            "payload": {"template_key": "researcher", "down_ratio": 0.6},
        }
        assert evaluate_when("payload.down_ratio > 0.5", env)
