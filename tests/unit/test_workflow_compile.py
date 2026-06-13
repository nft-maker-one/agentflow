"""Unit tests for the end-to-end Compiler pipeline."""

from __future__ import annotations

from pathlib import Path

import pytest

from agentkit.workflow import (
    CompileError,
    IRValidationError,
    compile_from_dict,
    compile_workflow,
)
from agentkit.workflow.ir.workflow import ERROR_NODE
from tests.helpers.workflow_fixtures import (
    deep_copy,
    fanout_workflow,
    linear_three_node_workflow,
    minimal_workflow,
)


# ----------------------------------------------------------------
# Happy path
# ----------------------------------------------------------------


class TestCompileFromDict:
    def test_minimal_compiles(self) -> None:
        ir, plan = compile_from_dict(minimal_workflow())
        assert ir.id == "wf_minimal"
        assert ir.meta.ir_hash != ""
        assert ir.meta.compiled_at != ""
        # One Agent, one consumer group derived
        assert "echo" in plan.agents
        assert plan.agents["echo"].consumer_group.startswith("grp.")

    def test_linear_workflow_compiles(self) -> None:
        ir, plan = compile_from_dict(linear_three_node_workflow())
        # 3 agents → 3 plans
        assert set(plan.agents) == {"researcher", "judge", "writer"}
        # LLM bindings carried through
        assert plan.llm_bindings["researcher"].provider == "openai"
        # Run-level guardrail caps from IR honoured
        assert plan.run_max_tokens == 200_000
        assert plan.run_max_cycles == 200

    def test_fanout_workflow_compiles(self) -> None:
        _, plan = compile_from_dict(fanout_workflow())
        # All 3 agents present
        assert set(plan.agents) == {"dispatcher", "worker_a", "worker_b"}
        # Both workers subscribe to the same topic
        wa = plan.agents["worker_a"]
        wb = plan.agents["worker_b"]
        assert "agent.work.in.task" in wa.subscribe_topics
        assert "agent.work.in.task" in wb.subscribe_topics


# ----------------------------------------------------------------
# Inject step — auto error edges
# ----------------------------------------------------------------


class TestInjectErrorEdges:
    def test_default_error_edge_added_for_each_agent(self) -> None:
        ir, _ = compile_from_dict(minimal_workflow())
        # The injector adds one auto-error edge for each agent that
        # didn't have one. The minimal workflow has 1 agent.
        auto_edges = [
            (eid, e) for eid, e in ir.edges.items() if eid.startswith("_auto_error_")
        ]
        assert len(auto_edges) == 1
        eid, edge = auto_edges[0]
        assert eid == "_auto_error_echo"
        assert ERROR_NODE in edge.all_targets()


# ----------------------------------------------------------------
# Lower step — hash determinism
# ----------------------------------------------------------------


class TestHashDeterminism:
    def test_compiling_same_input_yields_same_hash(self) -> None:
        spec = minimal_workflow()
        ir1, _ = compile_from_dict(spec)
        ir2, _ = compile_from_dict(deep_copy(spec))
        assert ir1.meta.ir_hash == ir2.meta.ir_hash

    def test_changing_a_field_changes_hash(self) -> None:
        s1 = minimal_workflow()
        s2 = deep_copy(s1)
        s2["agents"]["echo"]["description"] = "different"
        ir1, _ = compile_from_dict(s1)
        ir2, _ = compile_from_dict(s2)
        assert ir1.meta.ir_hash != ir2.meta.ir_hash


# ----------------------------------------------------------------
# Plan step — Bus topic provisioning
# ----------------------------------------------------------------


class TestBusTopicPlan:
    def test_business_topics_listed(self) -> None:
        _, plan = compile_from_dict(minimal_workflow())
        biz = [t for t in plan.bus_topics.topics if not t.is_dlq]
        assert {t.topic for t in biz} == {
            "agent.echo.in.q1",
            "agent.echo.out.summary",
        }

    def test_dlq_topics_auto_derived(self) -> None:
        _, plan = compile_from_dict(minimal_workflow())
        dlq = [t for t in plan.bus_topics.topics if t.is_dlq]
        assert {t.topic for t in dlq} == {
            "agent.echo.in.q1.dlq",
            "agent.echo.out.summary.dlq",
        }
        assert all(t.retention_hours == 7 * 24 for t in dlq)

    def test_partition_count_floor_six(self) -> None:
        _, plan = compile_from_dict(minimal_workflow())
        for t in plan.bus_topics.topics:
            assert t.partitions >= 6

    def test_partition_count_scales_with_replicas(self) -> None:
        spec = deep_copy(minimal_workflow())
        spec["agents"]["echo"]["replicas"] = {"min": 1, "max": 12}
        _, plan = compile_from_dict(spec)
        biz = [t for t in plan.bus_topics.topics if not t.is_dlq]
        assert all(t.partitions == 12 for t in biz)

    def test_topic_overrides_honored(self) -> None:
        spec = deep_copy(minimal_workflow())
        spec["bus"] = {
            "ordering": "per_run",
            "topic_overrides": {
                "agent.echo.in.q1": {"partitions": 24, "retention_hours": 48},
            },
        }
        _, plan = compile_from_dict(spec)
        target = next(
            t for t in plan.bus_topics.topics if t.topic == "agent.echo.in.q1"
        )
        assert target.partitions == 24
        assert target.retention_hours == 48


# ----------------------------------------------------------------
# Failure paths
# ----------------------------------------------------------------


class TestFailures:
    def test_validation_error_propagates(self) -> None:
        spec = deep_copy(minimal_workflow())
        spec["edges"]["e_out"]["to"] = "ghost"
        with pytest.raises(IRValidationError) as ei:
            compile_from_dict(spec)
        assert any("ghost" in v for v in ei.value.violations)

    def test_validate_can_be_skipped(self) -> None:
        # Skip validate — the rest of the pipeline should still work.
        spec = deep_copy(minimal_workflow())
        spec["edges"]["e_out"]["to"] = "ghost"
        ir, plan = compile_from_dict(spec, validate=False)
        assert ir.id == "wf_minimal"

    def test_pydantic_error_becomes_compile_error(self) -> None:
        spec = {
            "id": "",  # min_length=1 violation
            "agents": {},
            "edges": {},
        }
        with pytest.raises(CompileError):
            compile_from_dict(spec)


# ----------------------------------------------------------------
# YAML entry point
# ----------------------------------------------------------------


class TestCompileFromYaml:
    def test_loads_and_compiles(self, tmp_path: Path) -> None:
        import yaml

        f = tmp_path / "wf.yaml"
        f.write_text(yaml.safe_dump({"workflow": minimal_workflow()}))
        ir, plan = compile_workflow(f)
        assert ir.id == "wf_minimal"
        assert "echo" in plan.agents
