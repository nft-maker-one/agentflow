"""Unit tests for the Workflow IR Pydantic models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from agentkit.workflow.ir import (
    AgentTemplate,
    EdgeBranch,
    EdgeSpec,
    FallbackSpec,
    PublishSpec,
    ReplicaSpec,
    Subscription,
    Switch,
    WorkflowIR,
)
from agentkit.workflow.ir.workflow import (
    END_NODE,
    ERROR_NODE,
    START_NODE,
    VIRTUAL_NODES,
)


class TestAgentTemplate:
    def test_minimal(self) -> None:
        a = AgentTemplate(role="thinking", description="x")
        assert a.role.value == "thinking"
        assert a.subscribe == []
        assert a.publish == []
        assert a.replicas.min == 1 and a.replicas.max == 1

    def test_role_must_be_known(self) -> None:
        with pytest.raises(ValidationError):
            AgentTemplate(role="boss", description="x")  # type: ignore[arg-type]

    def test_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            AgentTemplate(role="thinking", bogus=1)  # type: ignore[call-arg]


class TestReplicaSpec:
    def test_max_below_min_rejected(self) -> None:
        with pytest.raises(ValueError, match="replicas.max"):
            ReplicaSpec(min=5, max=2)

    def test_zero_min_allowed(self) -> None:
        # Useful when the user wants the Autoscaler to fully drain.
        rs = ReplicaSpec(min=0, max=2)
        assert rs.min == 0


class TestFallbackSpec:
    def test_alt_template_required_when_strategy_alt_template(self) -> None:
        with pytest.raises(ValueError, match="alt_template"):
            FallbackSpec(strategy="alt_template")

    def test_retry_strategy_no_alt_required(self) -> None:
        f = FallbackSpec(strategy="retry")
        assert f.alt_template is None


class TestSubscriptionAndPublish:
    def test_subscription_with_tag_filter(self) -> None:
        s = Subscription(topic="agent.research.in.*", tag_filter={"language": "zh"})
        assert s.tag_filter["language"] == "zh"

    def test_publish_topic_required(self) -> None:
        with pytest.raises(ValidationError):
            PublishSpec(topic="")


class TestEdgeSpec:
    def test_direct_edge(self) -> None:
        e = EdgeSpec(**{"from": "a", "to": "b", "via": "t"})
        assert e.is_direct
        assert e.all_targets() == ["b"]
        assert e.all_vias() == ["t"]

    def test_fanout_edge(self) -> None:
        e = EdgeSpec(**{"from": "a", "to": ["b", "c"], "via": "t"})
        assert e.is_fanout
        assert sorted(e.all_targets()) == ["b", "c"]
        assert e.all_vias() == ["t"]

    def test_switch_edge_requires_cases(self) -> None:
        with pytest.raises(ValidationError):
            EdgeSpec(**{"from": "a", "to": {"switch": "$.x"}})

    def test_switch_edge_must_not_have_top_level_via(self) -> None:
        with pytest.raises(ValidationError):
            EdgeSpec(
                **{
                    "from": "a",
                    "to": {"switch": "$.x"},
                    "via": "t",
                    "cases": {"y": {"to": "b", "via": "t"}},
                },
            )

    def test_direct_edge_with_cases_rejected(self) -> None:
        with pytest.raises(ValidationError):
            EdgeSpec(
                **{
                    "from": "a",
                    "to": "b",
                    "via": "t",
                    "cases": {"y": {"to": "c", "via": "t"}},
                },
            )

    def test_switch_edge_all_targets_includes_default(self) -> None:
        e = EdgeSpec(
            **{
                "from": "a",
                "to": {"switch": "$.x"},
                "cases": {
                    "ok": {"to": "b", "via": "t1"},
                    "no": {"to": "__end__"},
                },
                "default": {"to": "__error__"},
            },
        )
        assert sorted(e.all_targets()) == ["__end__", "__error__", "b"]
        assert sorted(e.all_vias()) == ["t1"]

    def test_fanout_must_be_non_empty(self) -> None:
        with pytest.raises(ValidationError):
            EdgeSpec(**{"from": "a", "to": [], "via": "t"})


class TestEdgeBranch:
    def test_branch_to_end_no_via_required(self) -> None:
        b = EdgeBranch(to="__end__")
        assert b.to == "__end__"
        assert b.via is None


class TestSwitchModel:
    def test_switch_string_required_non_empty(self) -> None:
        with pytest.raises(ValidationError):
            Switch(switch="")


class TestWorkflowIR:
    def test_virtual_nodes_constants(self) -> None:
        assert START_NODE == "__start__"
        assert END_NODE == "__end__"
        assert ERROR_NODE == "__error__"
        assert {START_NODE, END_NODE, ERROR_NODE} == VIRTUAL_NODES

    def test_basic_construction(self) -> None:
        ir = WorkflowIR(
            id="wf_x",
            version=1,
            agents={"a": AgentTemplate(role="thinking")},
        )
        assert ir.id == "wf_x"
        assert "a" in ir.agents
        assert ir.is_virtual("__start__")
        assert ir.is_virtual("__end__")

    def test_compute_hash_deterministic(self) -> None:
        ir = WorkflowIR(
            id="wf_x",
            agents={"a": AgentTemplate(role="thinking")},
            edges={
                "e1": EdgeSpec(**{"from": "__start__", "to": "a", "via": "t"}),
            },
        )
        h1 = ir.compute_hash()
        h2 = ir.compute_hash()
        assert h1 == h2
        assert len(h1) == 12

    def test_compute_hash_changes_when_topology_changes(self) -> None:
        a = WorkflowIR(
            id="wf_x", agents={"a": AgentTemplate(role="thinking")},
        )
        b = WorkflowIR(
            id="wf_x",
            agents={"a": AgentTemplate(role="thinking", description="changed")},
        )
        assert a.compute_hash() != b.compute_hash()

    def test_compute_hash_excludes_meta(self) -> None:
        from agentkit.workflow.ir import IRMeta

        ir1 = WorkflowIR(
            id="wf_x", agents={"a": AgentTemplate(role="thinking")},
        )
        ir2 = ir1.with_meta(
            IRMeta(ir_hash="DEADBEEFCAFE", compiled_at="2026-01-01T00:00:00Z"),
        )
        # Meta differences must NOT change the content-addressable hash.
        assert ir1.compute_hash() == ir2.compute_hash()

    def test_all_topics_collected(self) -> None:
        ir = WorkflowIR(
            id="wf_x",
            agents={"a": AgentTemplate(role="thinking")},
            edges={
                "e1": EdgeSpec(**{"from": "__start__", "to": "a", "via": "t1"}),
                "e2": EdgeSpec(**{"from": "a", "to": "__end__", "via": "t2"}),
            },
        )
        assert ir.all_topics == ["t1", "t2"]
