"""Unit tests for the validate step.

Each test exercises one violation rule in isolation.
"""

from __future__ import annotations

import pytest

from agentkit.workflow.compiler.parse import parse_dict
from agentkit.workflow.compiler.validate import validate_ir
from tests.helpers.workflow_fixtures import (
    deep_copy,
    fanout_workflow,
    linear_three_node_workflow,
    minimal_workflow,
)


def _violations_for(spec: dict) -> list[str]:
    """Parse + validate; return violation list."""
    ir = parse_dict(spec)
    return validate_ir(ir)


# ----------------------------------------------------------------
# Happy-path baselines
# ----------------------------------------------------------------


class TestHappyPaths:
    def test_minimal_workflow_passes(self) -> None:
        assert _violations_for(minimal_workflow()) == []

    def test_linear_workflow_passes(self) -> None:
        assert _violations_for(linear_three_node_workflow()) == []

    def test_fanout_workflow_passes(self) -> None:
        assert _violations_for(fanout_workflow()) == []


# ----------------------------------------------------------------
# Topic whitelist + subscribe matching
# ----------------------------------------------------------------


class TestTopicWhitelist:
    def test_via_not_in_publish_whitelist_fails(self) -> None:
        spec = deep_copy(minimal_workflow())
        # Drop the publish whitelist entirely
        spec["agents"]["echo"]["publish"] = []
        violations = _violations_for(spec)
        assert any("publish whitelist" in v for v in violations)

    def test_via_not_in_target_subscribe_fails(self) -> None:
        spec = deep_copy(minimal_workflow())
        # Sender publishes to some-topic, but target doesn't subscribe to it.
        # We add a second agent that should consume the output edge but doesn't.
        spec["edges"]["e_out"]["via"] = "agent.echo.out.NOT_SUBSCRIBED"
        spec["agents"]["echo"]["publish"].append(
            {"topic": "agent.echo.out.NOT_SUBSCRIBED"},
        )
        # We make the consumer a real agent that doesn't subscribe.
        spec["edges"]["e_out"]["to"] = "consumer"
        spec["agents"]["consumer"] = {
            "role": "thinking",
            "subscribe": [{"topic": "agent.consumer.in.something_else"}],
            "publish": [{"topic": "agent.consumer.out.t"}],
        }
        spec["edges"]["e_consumer_out"] = {
            "from": "consumer",
            "to": "__end__",
            "via": "agent.consumer.out.t",
        }
        violations = _violations_for(spec)
        assert any(
            "no matching subscription" in v
            for v in violations
        )

    def test_publish_to_dlq_topic_rejected(self) -> None:
        spec = deep_copy(minimal_workflow())
        spec["agents"]["echo"]["publish"].append({"topic": "agent.echo.out.summary.dlq"})
        violations = _violations_for(spec)
        assert any(".dlq" in v and "reserved" in v for v in violations)


# ----------------------------------------------------------------
# Topology
# ----------------------------------------------------------------


class TestTopology:
    def test_unreachable_agent_flagged(self) -> None:
        spec = deep_copy(linear_three_node_workflow())
        # Add an orphan agent
        spec["agents"]["orphan"] = {
            "role": "thinking",
            "subscribe": [{"topic": "agent.orphan.in.q1"}],
            "publish": [{"topic": "agent.orphan.out.t"}],
        }
        violations = _violations_for(spec)
        assert any("orphan" in v and "unreachable" in v for v in violations)

    def test_no_path_to_terminal_flagged(self) -> None:
        spec = deep_copy(minimal_workflow())
        # Remove the outgoing edge so echo is a sink.
        del spec["edges"]["e_out"]
        violations = _violations_for(spec)
        assert any(
            "echo" in v and "no outgoing path" in v for v in violations
        )

    def test_dangling_target_flagged(self) -> None:
        spec = deep_copy(minimal_workflow())
        spec["edges"]["e_out"]["to"] = "ghost_agent"
        violations = _violations_for(spec)
        assert any("ghost_agent" in v and "unknown" in v for v in violations)


# ----------------------------------------------------------------
# Switch / branches
# ----------------------------------------------------------------


class TestSwitchValidation:
    def test_switch_branch_to_real_agent_requires_via(self) -> None:
        spec = deep_copy(linear_three_node_workflow())
        # Strip the via from the writer branch — should be flagged.
        spec["edges"]["e_judge_routing"]["cases"]["writer"].pop("via")
        violations = _violations_for(spec)
        assert any(
            "writer" in v and "requires a `via`" in v for v in violations
        )

    def test_switch_branch_to_terminal_no_via_ok(self) -> None:
        # Already in fixture: end branch goes to __end__ without via.
        assert _violations_for(linear_three_node_workflow()) == []


# ----------------------------------------------------------------
# Fallback alt_template
# ----------------------------------------------------------------


class TestFallbackAltTemplate:
    def test_alt_template_must_exist(self) -> None:
        spec = deep_copy(minimal_workflow())
        spec["agents"]["echo"]["fallback"] = {
            "strategy": "alt_template",
            "alt_template": "ghost",
            "on": ["recoverable_error"],
        }
        violations = _violations_for(spec)
        assert any("alt_template references unknown" in v for v in violations)

    def test_alt_template_present(self) -> None:
        spec = deep_copy(minimal_workflow())
        spec["agents"]["echo_lite"] = {
            "role": "thinking",
            "subscribe": [{"topic": "agent.echo.in.q1"}],
            "publish": [{"topic": "agent.echo.out.summary"}],
        }
        spec["agents"]["echo"]["fallback"] = {
            "strategy": "alt_template",
            "alt_template": "echo_lite",
            "on": ["recoverable_error"],
        }
        violations = _violations_for(spec)
        # Now echo_lite is present and reachable iff some edge points to
        # it OR we accept it can be unreachable. Compiler currently flags
        # unreachable agents — let's verify this is the only violation.
        assert all("alt_template references unknown" not in v for v in violations)


# ----------------------------------------------------------------
# Edge cases
# ----------------------------------------------------------------


class TestEdgeCases:
    def test_empty_agents_rejected(self) -> None:
        spec = {"id": "wf_empty", "version": 1, "agents": {}, "edges": {}}
        violations = _violations_for(spec)
        assert any("no agents" in v for v in violations)
