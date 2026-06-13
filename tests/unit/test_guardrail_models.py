"""Unit tests for Guardrail data models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from agentkit.guardrail.models import (
    AgentGuardrail,
    GuardrailContext,
    OverrideRecord,
    RunGuardrail,
    RunUsage,
)


class TestAgentGuardrail:
    def test_defaults_doc07_2_5(self) -> None:
        g = AgentGuardrail()
        assert g.max_tokens_per_call == 8_000
        assert g.max_cycles == 5

    def test_min_one_token(self) -> None:
        with pytest.raises(ValidationError):
            AgentGuardrail(max_tokens_per_call=0)

    def test_min_one_cycle(self) -> None:
        with pytest.raises(ValidationError):
            AgentGuardrail(max_cycles=0)


class TestRunGuardrail:
    def test_defaults_doc07_2_5(self) -> None:
        g = RunGuardrail()
        assert g.max_total_tokens == 200_000
        assert g.max_cycles_per_run == 200


class TestGuardrailContext:
    def test_minimum_construction(self) -> None:
        ctx = GuardrailContext(
            run_id="r", workflow_id="w",
            agent=AgentGuardrail(),
            run=RunGuardrail(),
        )
        assert ctx.owner is None
        assert ctx.project is None
        assert ctx.overrides == []

    def test_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            GuardrailContext(
                run_id="r", workflow_id="w",
                agent=AgentGuardrail(),
                run=RunGuardrail(),
                extra_field=1,  # type: ignore[call-arg]
            )


class TestRunUsage:
    def test_pct_helpers(self) -> None:
        u = RunUsage(
            run_id="r",
            used_tokens=8_000,
            used_cycles=20,
            limits_tokens=10_000,
            limits_cycles=100,
        )
        assert u.tokens_pct == 0.8
        assert u.cycles_pct == 0.2

    def test_pct_zero_safe(self) -> None:
        u = RunUsage(run_id="r", limits_tokens=0, limits_cycles=0)
        assert u.tokens_pct == 0.0
        assert u.cycles_pct == 0.0


class TestOverrideRecord:
    def test_layer_dim_round_trip(self) -> None:
        o = OverrideRecord(
            layer="run", field="max_total_tokens",
            source="workflow", value=50_000,
        )
        assert o.value == 50_000
        # Pydantic serializes Literal values back as plain strings.
        dumped = o.model_dump()
        assert dumped["layer"] == "run"
        assert dumped["source"] == "workflow"
