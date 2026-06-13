"""Unit tests for the Agent FSM."""

from __future__ import annotations

import pytest

from agentkit.models.enums import AgentState
from agentkit.runtime.fallback import FailureClass
from agentkit.runtime.fsm import (
    FSMTransition,
    InvalidTransition,
    apply_transition,
    initial_snapshot,
    transition_for_failure,
)


@pytest.fixture
def initial():
    return initial_snapshot(
        agent_id="agt_test",
        template_key="echo",
        workflow_id="wf_x",
        description="echo agent",
        max_retries=3,
    )


class TestEnvCheck:
    def test_init_to_active_on_pass(self, initial) -> None:
        s = apply_transition(initial, FSMTransition.ENV_CHECK_PASS)
        assert s.state is AgentState.ACTIVE

    def test_init_to_down_on_fail(self, initial) -> None:
        s = apply_transition(
            initial, FSMTransition.ENV_CHECK_FAIL, reason="mcp.unreachable",
        )
        assert s.state is AgentState.DOWN
        assert s.state_meta.reason == "mcp.unreachable"


class TestActiveProcessing:
    def test_active_to_processing_on_event(self, initial) -> None:
        s = apply_transition(initial, FSMTransition.ENV_CHECK_PASS)
        s = apply_transition(
            s, FSMTransition.EVENT_ARRIVED, event_id="evt_1",
        )
        assert s.state is AgentState.PROCESSING
        assert s.state_meta.current_event_id == "evt_1"

    def test_processing_to_active_on_ok(self, initial) -> None:
        s = apply_transition(initial, FSMTransition.ENV_CHECK_PASS)
        s = apply_transition(s, FSMTransition.EVENT_ARRIVED, event_id="evt_1")
        s = apply_transition(s, FSMTransition.HANDLER_OK)
        assert s.state is AgentState.ACTIVE
        assert s.state_meta.current_event_id is None
        assert s.state_meta.retry_count == 0


class TestRetryFlow:
    def test_recoverable_under_budget_goes_to_retry(self, initial) -> None:
        s = apply_transition(initial, FSMTransition.ENV_CHECK_PASS)
        s = apply_transition(s, FSMTransition.EVENT_ARRIVED, event_id="evt_1")

        s = apply_transition(
            s, FSMTransition.RECOVERABLE_ERROR, reason="net glitch",
        )
        assert s.state is AgentState.RETRY
        assert s.state_meta.retry_count == 1
        assert s.state_meta.reason == "net glitch"

    def test_recoverable_at_budget_goes_to_failure(self, initial) -> None:
        s = apply_transition(initial, FSMTransition.ENV_CHECK_PASS)
        s = apply_transition(s, FSMTransition.EVENT_ARRIVED, event_id="evt_1")

        # Walk through retries 1, 2, 3 (max_retries=3).
        for _ in range(3):
            s = apply_transition(s, FSMTransition.RECOVERABLE_ERROR, reason="boom")
            assert s.state is AgentState.RETRY
            s = apply_transition(s, FSMTransition.RETRY_DUE)
            assert s.state is AgentState.PROCESSING

        # 4th failure exhausts budget → Failure.
        s = apply_transition(s, FSMTransition.RECOVERABLE_ERROR, reason="boom")
        assert s.state is AgentState.FAILURE
        assert s.state_meta.retry_count == 3

    def test_retry_due_returns_to_processing(self, initial) -> None:
        s = apply_transition(initial, FSMTransition.ENV_CHECK_PASS)
        s = apply_transition(s, FSMTransition.EVENT_ARRIVED, event_id="evt_1")
        s = apply_transition(s, FSMTransition.RECOVERABLE_ERROR, reason="glitch")
        assert s.state is AgentState.RETRY

        s = apply_transition(s, FSMTransition.RETRY_DUE)
        assert s.state is AgentState.PROCESSING


class TestGuardrailNeverRetries:
    def test_guardrail_exceeded_jumps_directly_to_failure(self, initial) -> None:
        s = apply_transition(initial, FSMTransition.ENV_CHECK_PASS)
        s = apply_transition(s, FSMTransition.EVENT_ARRIVED, event_id="evt_1")
        s = apply_transition(
            s, FSMTransition.GUARDRAIL_EXCEEDED, reason="run.tokens",
        )
        assert s.state is AgentState.FAILURE
        # Guardrail never goes through Retry, regardless of budget.
        assert s.state_meta.retry_count == 0


class TestFatalError:
    def test_fatal_to_down(self, initial) -> None:
        s = apply_transition(initial, FSMTransition.ENV_CHECK_PASS)
        s = apply_transition(s, FSMTransition.EVENT_ARRIVED, event_id="evt_1")
        s = apply_transition(
            s, FSMTransition.FATAL_ERROR, reason="schema corrupt",
        )
        assert s.state is AgentState.DOWN
        assert s.state_meta.reason == "schema corrupt"


class TestInvalidTransitions:
    def test_invalid_transition_raises(self, initial) -> None:
        with pytest.raises(InvalidTransition):
            apply_transition(initial, FSMTransition.HANDLER_OK)

    def test_complete_is_terminal(self, initial) -> None:
        s = apply_transition(initial, FSMTransition.ENV_CHECK_PASS)
        s = apply_transition(s, FSMTransition.EVENT_ARRIVED, event_id="evt_1")
        s = apply_transition(
            s, FSMTransition.WORKFLOW_DONE, result_topic="agent.x.out.final",
        )
        assert s.state is AgentState.COMPLETE
        assert s.state_meta.result_topic == "agent.x.out.final"


class TestFailureClassMapping:
    @pytest.mark.parametrize(
        ("klass", "expected"),
        [
            (FailureClass.RECOVERABLE, FSMTransition.RECOVERABLE_ERROR),
            (FailureClass.FATAL, FSMTransition.FATAL_ERROR),
            (FailureClass.GUARDRAIL_EXCEEDED, FSMTransition.GUARDRAIL_EXCEEDED),
        ],
    )
    def test_class_to_transition(self, klass, expected) -> None:
        assert transition_for_failure(klass) is expected
