"""End-to-end Orchestrator tests with MockBus.

These exercise the full pipeline: deploy IR → create Run → events
flow through agents (here mocked as direct bus publishers) → switch
routing → terminal detection → Run.Succeeded.
"""

from __future__ import annotations

import asyncio

import pytest

from agentkit.bus.builder import build_envelope
from agentkit.models.envelope import AgentRef
from agentkit.models.enums import Role, RunStatus
from agentkit.orchestrator import (
    InMemoryRunStore,
    Orchestrator,
    UnknownWorkflow,
)
from agentkit.workflow import compile_from_dict
from tests.helpers.mock_bus import MockEventBus
from tests.helpers.workflow_fixtures import (
    deep_copy,
    fanout_workflow,
    linear_three_node_workflow,
    minimal_workflow,
)


# ----------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------


@pytest.fixture
async def bus() -> MockEventBus:
    b = MockEventBus()
    await b.start()
    return b


@pytest.fixture
async def orchestrator(bus):
    orch = Orchestrator(bus=bus, store=InMemoryRunStore())
    await orch.start()
    yield orch
    await orch.stop()


# ----------------------------------------------------------------
# deploy / create_run basics
# ----------------------------------------------------------------


class TestDeployAndCreate:
    async def test_unknown_workflow_rejected(self, orchestrator) -> None:
        with pytest.raises(UnknownWorkflow):
            await orchestrator.create_run(workflow_id="ghost")

    async def test_deploy_then_create_run(self, orchestrator) -> None:
        ir, _ = compile_from_dict(minimal_workflow())
        await orchestrator.deploy(ir)

        run = await orchestrator.create_run(
            workflow_id="wf_minimal", input={"text": "hi"},
        )
        assert run.status is RunStatus.RUNNING
        assert run.workflow_id == "wf_minimal"
        assert run.input == {"text": "hi"}

    async def test_initial_event_published(self, bus, orchestrator) -> None:
        ir, _ = compile_from_dict(minimal_workflow())
        await orchestrator.deploy(ir)
        await orchestrator.create_run(workflow_id="wf_minimal", input={"x": 1})
        # The start edge has via=agent.echo.in.q1
        published = bus.published_for_topic("agent.echo.in.q1")
        assert len(published) == 1
        assert published[0].payload == {"x": 1}


# ----------------------------------------------------------------
# Terminal detection — direct __end__ edge
# ----------------------------------------------------------------


class TestTerminalDirect:
    async def test_run_succeeds_when_event_reaches_terminal_via(
        self, bus, orchestrator,
    ) -> None:
        ir, _ = compile_from_dict(minimal_workflow())
        await orchestrator.deploy(ir)
        run = await orchestrator.create_run(workflow_id="wf_minimal")

        # Simulate the echo agent's output by publishing on the
        # terminal edge's via topic. The TerminalDetector should
        # observe and finalize the Run.
        await bus.publish(
            build_envelope(
                topic="agent.echo.out.summary",
                payload={"echoed": "hi"},
                workflow_id="wf_minimal",
                run_id=run.run_id,
                trace_id=run.trace_id,
                from_=AgentRef(role=Role.THINKING, agent_id="agt_echo"),
            ),
        )

        final = await orchestrator.wait_for_completion(run.run_id, timeout=2.0)
        assert final.status is RunStatus.SUCCEEDED
        assert final.ended_at is not None
        # The payload that reached __end__ is captured as the run output.
        assert final.output == {"echoed": "hi"}


# ----------------------------------------------------------------
# Switch routing — JudgeNode resolves into a real branch + a __end__ branch
# ----------------------------------------------------------------


class TestSwitchRouting:
    async def test_switch_routes_to_writer_branch(
        self, bus, orchestrator,
    ) -> None:
        ir, _ = compile_from_dict(linear_three_node_workflow())
        await orchestrator.deploy(ir)
        run = await orchestrator.create_run(
            workflow_id="wf_research_to_report",
        )

        # Researcher publishes its output (consumed by Judge).
        await bus.publish(
            build_envelope(
                topic="agent.research.out.summary",
                payload={"summary": "x"},
                workflow_id="wf_research_to_report",
                run_id=run.run_id,
                trace_id=run.trace_id,
                from_=AgentRef(
                    role=Role.THINKING, agent_id="agt_researcher",
                ),
            ),
        )

        # Judge publishes a decision payload — the switch fires here.
        await bus.publish(
            build_envelope(
                topic="agent.judge.out.choice",
                payload={"choice": "writer"},
                workflow_id="wf_research_to_report",
                run_id=run.run_id,
                trace_id=run.trace_id,
                from_=AgentRef(role=Role.JUDGE, agent_id="agt_judge"),
            ),
        )

        # The orchestrator should re-publish to the writer's via topic.
        for _ in range(40):
            await asyncio.sleep(0.05)
            if bus.published_for_topic("agent.writer.in.draft"):
                break
        published = bus.published_for_topic("agent.writer.in.draft")
        assert len(published) >= 1
        # The re-publish carries the same run_id / trace_id.
        assert published[0].run_id == run.run_id
        assert published[0].trace_id == run.trace_id

        # Now writer produces final output → terminal __end__.
        await bus.publish(
            build_envelope(
                topic="agent.writer.out.report",
                payload={"report": "ok"},
                workflow_id="wf_research_to_report",
                run_id=run.run_id,
                trace_id=run.trace_id,
                from_=AgentRef(role=Role.THINKING, agent_id="agt_writer"),
            ),
        )
        final = await orchestrator.wait_for_completion(run.run_id, timeout=2.0)
        assert final.status is RunStatus.SUCCEEDED

    async def test_switch_to_end_branch_finalizes_run(
        self, bus, orchestrator,
    ) -> None:
        ir, _ = compile_from_dict(linear_three_node_workflow())
        await orchestrator.deploy(ir)
        run = await orchestrator.create_run(
            workflow_id="wf_research_to_report",
        )

        # Publish researcher output then judge output choosing 'end'.
        await bus.publish(
            build_envelope(
                topic="agent.research.out.summary",
                payload={"summary": "x"},
                workflow_id="wf_research_to_report",
                run_id=run.run_id,
                trace_id=run.trace_id,
                from_=AgentRef(role=Role.THINKING, agent_id="agt_researcher"),
            ),
        )
        await bus.publish(
            build_envelope(
                topic="agent.judge.out.choice",
                payload={"choice": "end"},
                workflow_id="wf_research_to_report",
                run_id=run.run_id,
                trace_id=run.trace_id,
                from_=AgentRef(role=Role.JUDGE, agent_id="agt_judge"),
            ),
        )

        final = await orchestrator.wait_for_completion(run.run_id, timeout=2.0)
        assert final.status is RunStatus.SUCCEEDED

    async def test_switch_no_match_routes_to_error(
        self, bus, orchestrator,
    ) -> None:
        ir, _ = compile_from_dict(linear_three_node_workflow())
        await orchestrator.deploy(ir)
        run = await orchestrator.create_run(
            workflow_id="wf_research_to_report",
        )

        # Judge picks an unknown case — neither cases nor default match.
        await bus.publish(
            build_envelope(
                topic="agent.research.out.summary",
                payload={"summary": "x"},
                workflow_id="wf_research_to_report",
                run_id=run.run_id,
                trace_id=run.trace_id,
                from_=AgentRef(role=Role.THINKING, agent_id="agt_researcher"),
            ),
        )
        await bus.publish(
            build_envelope(
                topic="agent.judge.out.choice",
                payload={"choice": "unknown_value"},
                workflow_id="wf_research_to_report",
                run_id=run.run_id,
                trace_id=run.trace_id,
                from_=AgentRef(role=Role.JUDGE, agent_id="agt_judge"),
            ),
        )

        final = await orchestrator.wait_for_completion(run.run_id, timeout=2.0)
        assert final.status is RunStatus.FAILED
        assert "switch_no_match" in (final.failure_reason or "")


# ----------------------------------------------------------------
# Cancel
# ----------------------------------------------------------------


class TestCancel:
    async def test_cancel_run_marks_cancelled(
        self, orchestrator,
    ) -> None:
        ir, _ = compile_from_dict(minimal_workflow())
        await orchestrator.deploy(ir)
        run = await orchestrator.create_run(workflow_id="wf_minimal")

        cancelled = await orchestrator.cancel_run(run.run_id, reason="user")
        assert cancelled.status is RunStatus.CANCELLED
        assert cancelled.failure_reason == "user"

    async def test_cancel_terminal_run_is_idempotent(
        self, orchestrator,
    ) -> None:
        ir, _ = compile_from_dict(minimal_workflow())
        await orchestrator.deploy(ir)
        run = await orchestrator.create_run(workflow_id="wf_minimal")
        await orchestrator.cancel_run(run.run_id)
        # Second cancel doesn't change state.
        again = await orchestrator.cancel_run(run.run_id)
        assert again.status is RunStatus.CANCELLED


# ----------------------------------------------------------------
# Branch log
# ----------------------------------------------------------------


class TestBranchLog:
    async def test_branch_event_recorded_on_switch_resolve(
        self, bus, orchestrator,
    ) -> None:
        ir, _ = compile_from_dict(linear_three_node_workflow())
        await orchestrator.deploy(ir)
        run = await orchestrator.create_run(
            workflow_id="wf_research_to_report",
        )

        await bus.publish(
            build_envelope(
                topic="agent.research.out.summary",
                payload={"summary": "x"},
                workflow_id="wf_research_to_report",
                run_id=run.run_id,
                trace_id=run.trace_id,
                from_=AgentRef(role=Role.THINKING, agent_id="agt_researcher"),
            ),
        )
        await bus.publish(
            build_envelope(
                topic="agent.judge.out.choice",
                payload={"choice": "writer"},
                workflow_id="wf_research_to_report",
                run_id=run.run_id,
                trace_id=run.trace_id,
                from_=AgentRef(role=Role.JUDGE, agent_id="agt_judge"),
            ),
        )

        # Give the SwitchRouter time to record.
        await asyncio.sleep(0.5)

        latest = await orchestrator.get_run(run.run_id)
        edge_ids = [ev.edge_id for ev in latest.cursor.branch_log]
        # Must include the start-edge entry AND the switch routing entry.
        assert "e_judge_routing" in edge_ids


# ----------------------------------------------------------------
# Fanout — direct edge with multiple targets reaching __end__
# ----------------------------------------------------------------


class TestFanoutTermination:
    async def test_fanout_run_finalizes_on_terminal_via(
        self, bus, orchestrator,
    ) -> None:
        ir, _ = compile_from_dict(fanout_workflow())
        await orchestrator.deploy(ir)
        run = await orchestrator.create_run(workflow_id="wf_fanout")

        # Worker_a publishes its terminal — Run finalizes on first hit.
        await bus.publish(
            build_envelope(
                topic="agent.work.out.result",
                payload={"src": "a"},
                workflow_id="wf_fanout",
                run_id=run.run_id,
                trace_id=run.trace_id,
                from_=AgentRef(role=Role.THINKING, agent_id="agt_w_a"),
            ),
        )
        final = await orchestrator.wait_for_completion(run.run_id, timeout=2.0)
        assert final.status is RunStatus.SUCCEEDED
