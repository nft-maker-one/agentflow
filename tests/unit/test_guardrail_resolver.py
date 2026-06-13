"""Unit tests for the quota resolver — Doc07 §2.3."""

from __future__ import annotations

from agentkit.guardrail.models import AgentGuardrail, RunGuardrail
from agentkit.guardrail.resolver import (
    FRAMEWORK_DEFAULT_AGENT,
    FRAMEWORK_DEFAULT_RUN,
    FrameworkDefaults,
    resolve_guardrail_context,
)


class TestFrameworkOnly:
    def test_no_overrides_uses_framework(self) -> None:
        ctx = resolve_guardrail_context(
            run_id="r", workflow_id="w",
        )
        assert ctx.agent.max_tokens_per_call == FRAMEWORK_DEFAULT_AGENT.max_tokens_per_call
        assert ctx.run.max_total_tokens == FRAMEWORK_DEFAULT_RUN.max_total_tokens
        assert all(o.source == "framework_default" for o in ctx.overrides)


class TestNarrowingOnly:
    def test_workflow_can_shrink_run_tokens(self) -> None:
        ctx = resolve_guardrail_context(
            run_id="r", workflow_id="w",
            workflow_run=RunGuardrail(
                max_total_tokens=50_000,
                max_cycles_per_run=200,
            ),
        )
        # Narrowed by workflow:
        token_record = next(
            o for o in ctx.overrides if o.field == "max_total_tokens"
        )
        assert ctx.run.max_total_tokens == 50_000
        assert token_record.source == "workflow"

    def test_workflow_cannot_widen(self) -> None:
        # Workflow tries 500k but framework caps at 200k → 200k wins.
        ctx = resolve_guardrail_context(
            run_id="r", workflow_id="w",
            workflow_run=RunGuardrail(
                max_total_tokens=500_000,
                max_cycles_per_run=1000,
            ),
        )
        assert ctx.run.max_total_tokens == 200_000
        record = next(o for o in ctx.overrides if o.field == "max_total_tokens")
        assert record.source == "framework_default"

    def test_overlay_cannot_widen_workflow(self) -> None:
        # Workflow says 50k, overlay says 100k → overlay can't widen.
        ctx = resolve_guardrail_context(
            run_id="r", workflow_id="w",
            workflow_run=RunGuardrail(
                max_total_tokens=50_000,
                max_cycles_per_run=200,
            ),
            overlay_run=RunGuardrail(
                max_total_tokens=100_000,
                max_cycles_per_run=200,
            ),
        )
        assert ctx.run.max_total_tokens == 50_000
        record = next(o for o in ctx.overrides if o.field == "max_total_tokens")
        assert record.source == "workflow"

    def test_overlay_can_shrink(self) -> None:
        ctx = resolve_guardrail_context(
            run_id="r", workflow_id="w",
            workflow_run=RunGuardrail(
                max_total_tokens=100_000,
                max_cycles_per_run=200,
            ),
            overlay_run=RunGuardrail(
                max_total_tokens=20_000,
                max_cycles_per_run=200,
            ),
        )
        assert ctx.run.max_total_tokens == 20_000
        record = next(o for o in ctx.overrides if o.field == "max_total_tokens")
        assert record.source == "run_overlay"


class TestProjectDefault:
    def test_project_overrides_framework(self) -> None:
        ctx = resolve_guardrail_context(
            run_id="r", workflow_id="w",
            project_agent=AgentGuardrail(max_tokens_per_call=2_000, max_cycles=2),
        )
        rec = next(o for o in ctx.overrides if o.field == "max_tokens_per_call")
        assert rec.source == "project_default"
        assert ctx.agent.max_tokens_per_call == 2_000


class TestTieBreaking:
    def test_equal_values_prefer_narrower_scope(self) -> None:
        # All four levels declare exactly the same number — narrower
        # scope wins so audit shows the most-specific source.
        ctx = resolve_guardrail_context(
            run_id="r", workflow_id="w",
            project_run=RunGuardrail(max_total_tokens=200_000, max_cycles_per_run=200),
            workflow_run=RunGuardrail(max_total_tokens=200_000, max_cycles_per_run=200),
            overlay_run=RunGuardrail(max_total_tokens=200_000, max_cycles_per_run=200),
        )
        rec = next(o for o in ctx.overrides if o.field == "max_total_tokens")
        assert rec.source == "run_overlay"


class TestSwappableFrameworkDefaults:
    def test_can_inject_tighter_framework(self) -> None:
        fw = FrameworkDefaults(
            agent=AgentGuardrail(max_tokens_per_call=100, max_cycles=1),
            run=RunGuardrail(max_total_tokens=500, max_cycles_per_run=5),
        )
        ctx = resolve_guardrail_context(
            run_id="r", workflow_id="w", framework=fw,
        )
        assert ctx.agent.max_tokens_per_call == 100
        assert ctx.run.max_total_tokens == 500
