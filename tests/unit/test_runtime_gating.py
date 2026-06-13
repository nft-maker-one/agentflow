"""Unit tests for the 4-gate ingress pipeline."""

from __future__ import annotations

import pytest

from agentkit.bus.builder import build_envelope
from agentkit.llm.guardrail_iface import NoOpGuardrail
from agentkit.models.envelope import ToFilter
from agentkit.runtime.dedup import DedupStore
from agentkit.runtime.gating import (
    GatingVerdict,
    gate_dedup,
    gate_schema,
    gate_tag,
    run_gating,
)


# ----------------------------------------------------------------
# Gate 1 — Tag filter
# ----------------------------------------------------------------


class TestGateTag:
    def test_no_required_tags_passes(self) -> None:
        env = build_envelope(topic="t", payload={})
        result = gate_tag(env, agent_tags={"language": "zh"})
        assert result.verdict is GatingVerdict.PASS

    def test_all_required_tags_present_passes(self) -> None:
        env = build_envelope(
            topic="t", payload={},
            to_filter=ToFilter(tags={"language": "zh"}),
        )
        result = gate_tag(env, agent_tags={"language": "zh", "region": "cn"})
        assert result.verdict is GatingVerdict.PASS

    def test_missing_required_tag_fails(self) -> None:
        env = build_envelope(
            topic="t", payload={},
            to_filter=ToFilter(tags={"language": "zh"}),
        )
        result = gate_tag(env, agent_tags={"language": "en"})
        assert result.verdict is GatingVerdict.DROP_TAG
        assert "language" in result.reason


# ----------------------------------------------------------------
# Gate 2 — Schema check
# ----------------------------------------------------------------


class TestGateSchema:
    def test_no_schema_passes(self) -> None:
        env = build_envelope(topic="t", payload={"q": "hi"})
        assert gate_schema(env, schema_in=None).verdict is GatingVerdict.PASS

    def test_valid_payload_passes(self) -> None:
        env = build_envelope(topic="t", payload={"q": "hi"})
        schema = {
            "type": "object",
            "properties": {"q": {"type": "string"}},
            "required": ["q"],
        }
        assert gate_schema(env, schema_in=schema).verdict is GatingVerdict.PASS

    def test_missing_required_field_dlq(self) -> None:
        env = build_envelope(topic="t", payload={})
        schema = {
            "type": "object",
            "properties": {"q": {"type": "string"}},
            "required": ["q"],
        }
        result = gate_schema(env, schema_in=schema)
        assert result.verdict is GatingVerdict.DLQ_SCHEMA

    def test_wrong_type_dlq(self) -> None:
        env = build_envelope(topic="t", payload={"q": 42})
        schema = {
            "type": "object",
            "properties": {"q": {"type": "string"}},
        }
        assert gate_schema(env, schema_in=schema).verdict is GatingVerdict.DLQ_SCHEMA


# ----------------------------------------------------------------
# Gate 4 — Dedup
# ----------------------------------------------------------------


class TestGateDedup:
    def test_first_seen_passes(self) -> None:
        env = build_envelope(topic="t", payload={})
        store = DedupStore(window_ms=10_000)
        assert gate_dedup(env, store=store).verdict is GatingVerdict.PASS

    def test_replay_dropped(self) -> None:
        env = build_envelope(topic="t", payload={})
        store = DedupStore(window_ms=10_000)
        store.seen(env.event_id)
        result = gate_dedup(env, store=store)
        assert result.verdict is GatingVerdict.DROP_DEDUP


# ----------------------------------------------------------------
# Top-level pipeline
# ----------------------------------------------------------------


class TestPipeline:
    async def test_full_pass(self) -> None:
        env = build_envelope(topic="t", payload={"q": "hi"})
        result = await run_gating(
            env,
            agent_tags={},
            schema_in=None,
            guardrail=NoOpGuardrail(),
            agent_id="agt_x",
            est_tokens=100,
            dedup_store=DedupStore(),
        )
        assert result.passed
        assert result.reservation is not None

    async def test_short_circuits_on_first_failure(self) -> None:
        # Schema fails before dedup is touched.
        env = build_envelope(topic="t", payload={})
        schema = {"type": "object", "required": ["x"]}
        store = DedupStore()
        result = await run_gating(
            env,
            agent_tags={},
            schema_in=schema,
            guardrail=NoOpGuardrail(),
            agent_id="agt_x",
            est_tokens=100,
            dedup_store=store,
        )
        assert result.verdict is GatingVerdict.DLQ_SCHEMA
        # Dedup wasn't touched.
        assert len(store) == 0

    async def test_tag_then_schema_order(self) -> None:
        env = build_envelope(
            topic="t", payload={"x": "ok"},
            to_filter=ToFilter(tags={"language": "zh"}),
        )
        # Tag mismatch → drop_tag, schema isn't even consulted.
        result = await run_gating(
            env,
            agent_tags={"language": "en"},
            schema_in={"type": "object", "required": ["q"]},  # would fail
            guardrail=NoOpGuardrail(),
            agent_id="agt_x",
            est_tokens=100,
            dedup_store=DedupStore(),
        )
        assert result.verdict is GatingVerdict.DROP_TAG
