"""Unit tests for the sandboxed switch-expression evaluator."""

from __future__ import annotations

import pytest

from agentkit.orchestrator.errors import SwitchEvalError
from agentkit.orchestrator.switch_eval import (
    evaluate_switch_expression,
    pick_case,
)


# ----------------------------------------------------------------
# Path extraction
# ----------------------------------------------------------------


class TestExtraction:
    def test_top_level_field(self) -> None:
        assert evaluate_switch_expression("$.choice", {"choice": "x"}) == "x"

    def test_nested_field(self) -> None:
        payload = {"a": {"b": {"c": 42}}}
        assert evaluate_switch_expression("$.a.b.c", payload) == 42

    def test_missing_field_returns_none(self) -> None:
        assert evaluate_switch_expression("$.nope", {"x": 1}) is None

    def test_path_through_non_dict_returns_none(self) -> None:
        # ``a`` is a list, but ``$.a.b`` tries dict access — None.
        assert evaluate_switch_expression("$.a.b", {"a": [1, 2]}) is None

    def test_array_index(self) -> None:
        payload = {"items": ["zero", "one", "two"]}
        assert evaluate_switch_expression("$.items[1]", payload) == "one"

    def test_negative_array_index(self) -> None:
        payload = {"items": [10, 20, 30]}
        assert evaluate_switch_expression("$.items[-1]", payload) == 30

    def test_index_out_of_range_returns_none(self) -> None:
        payload = {"items": [10]}
        assert evaluate_switch_expression("$.items[5]", payload) is None

    def test_chained_index_and_key(self) -> None:
        payload = {"items": [{"name": "a"}, {"name": "b"}]}
        assert evaluate_switch_expression("$.items[1].name", payload) == "b"


# ----------------------------------------------------------------
# Grammar enforcement (the security-critical part)
# ----------------------------------------------------------------


class TestSecurity:
    def test_must_start_with_dollar_dot(self) -> None:
        with pytest.raises(SwitchEvalError):
            evaluate_switch_expression("choice", {})
        with pytest.raises(SwitchEvalError):
            evaluate_switch_expression("$choice", {})

    def test_empty_expression_rejected(self) -> None:
        with pytest.raises(SwitchEvalError):
            evaluate_switch_expression("", {})
        with pytest.raises(SwitchEvalError):
            evaluate_switch_expression("$.", {})

    def test_trailing_dot_rejected(self) -> None:
        with pytest.raises(SwitchEvalError):
            evaluate_switch_expression("$.a.", {"a": "x"})

    def test_invalid_segment_chars_rejected(self) -> None:
        # Spaces, parens, special chars must NOT be accepted.
        with pytest.raises(SwitchEvalError):
            evaluate_switch_expression("$.a b", {"a b": 1})
        with pytest.raises(SwitchEvalError):
            evaluate_switch_expression("$.func()", {})
        with pytest.raises(SwitchEvalError):
            evaluate_switch_expression("$.x|y", {})

    def test_unbalanced_index_rejected(self) -> None:
        with pytest.raises(SwitchEvalError):
            evaluate_switch_expression("$.a[", {"a": [1]})
        with pytest.raises(SwitchEvalError):
            evaluate_switch_expression("$.a[1", {"a": [1]})

    def test_non_integer_index_rejected(self) -> None:
        with pytest.raises(SwitchEvalError):
            evaluate_switch_expression("$.a[abc]", {"a": [1]})

    def test_no_eval_of_python_code(self) -> None:
        """Sanity check: feeding Python code should NOT execute it.

        The whole point of this evaluator is that ``__import__('os')``
        and friends produce a SwitchEvalError, not a side effect.
        """
        for malicious in [
            "$.__import__('os').system('echo pwn')",
            "$.foo or 1==1",
            "$.x; print('hi')",
        ]:
            with pytest.raises(SwitchEvalError):
                evaluate_switch_expression(malicious, {})

    def test_dunder_keys_are_just_dict_lookups(self) -> None:
        """``$.x.__class__`` is *grammatically* valid — but it does plain
        ``dict.get('__class__')``, NOT Python attribute access. Confirm
        this stays safe even when fed an attacker-controlled payload.
        """
        # No __class__ key in the dict — returns None, no exception.
        assert evaluate_switch_expression("$.x.__class__", {"x": {}}) is None
        # Dunder *as a key* returns the value, not the underlying type.
        assert (
            evaluate_switch_expression(
                "$.x.__class__", {"x": {"__class__": "spoofed"}},
            )
            == "spoofed"
        )


# ----------------------------------------------------------------
# Case picking
# ----------------------------------------------------------------


class TestPickCase:
    def test_string_match(self) -> None:
        cases = {"approve": object(), "reject": object()}
        assert pick_case(cases, "approve") == "approve"

    def test_no_match_returns_default(self) -> None:
        cases = {"approve": 1}
        assert pick_case(cases, "other", default_key="def") == "def"

    def test_no_match_no_default_returns_none(self) -> None:
        cases = {"approve": 1}
        assert pick_case(cases, "other") is None

    def test_int_extracted_stringified(self) -> None:
        cases = {"42": "match"}
        assert pick_case(cases, 42) == "42"

    def test_bool_normalized_to_string(self) -> None:
        cases = {"true": "match", "false": "no-match"}
        assert pick_case(cases, True) == "true"
        assert pick_case(cases, False) == "false"

    def test_none_extracted_uses_default(self) -> None:
        cases = {"x": 1}
        assert pick_case(cases, None, default_key="fallback") == "fallback"
