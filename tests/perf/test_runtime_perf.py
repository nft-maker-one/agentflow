"""Microbenchmarks for runtime hot paths (FSM, dedup, gating)."""

from __future__ import annotations

import asyncio

import pytest

from agentkit.bus.builder import build_envelope
from agentkit.llm.guardrail_iface import NoOpGuardrail
from agentkit.models.enums import AgentState
from agentkit.runtime.dedup import DedupStore
from agentkit.runtime.fsm import (
    FSMTransition,
    apply_transition,
    initial_snapshot,
)
from agentkit.runtime.gating import gate_dedup, gate_schema, gate_tag, run_gating


# ----------------------------------------------------------------
# FSM
# ----------------------------------------------------------------


@pytest.mark.perf
def test_perf_fsm_transition(benchmark) -> None:
    s = initial_snapshot(
        agent_id="a", template_key="t", workflow_id="w",
    )
    s = apply_transition(s, FSMTransition.ENV_CHECK_PASS)

    def cycle() -> None:
        s2 = apply_transition(s, FSMTransition.EVENT_ARRIVED, event_id="evt_x")
        apply_transition(s2, FSMTransition.HANDLER_OK)

    benchmark(cycle)


# ----------------------------------------------------------------
# Dedup
# ----------------------------------------------------------------


@pytest.mark.perf
def test_perf_dedup_seen(benchmark) -> None:
    store = DedupStore(window_ms=60_000, max_entries=10_000)
    counter = {"n": 0}

    def call() -> None:
        counter["n"] += 1
        store.seen(f"evt_{counter['n']}")

    benchmark(call)


# ----------------------------------------------------------------
# Gating
# ----------------------------------------------------------------


@pytest.mark.perf
def test_perf_gate_tag(benchmark) -> None:
    from agentkit.models.envelope import ToFilter

    env = build_envelope(
        topic="t", payload={},
        to_filter=ToFilter(tags={"language": "zh", "region": "cn"}),
    )
    benchmark(gate_tag, env, agent_tags={"language": "zh", "region": "cn"})


@pytest.mark.perf
def test_perf_gate_schema(benchmark) -> None:
    env = build_envelope(topic="t", payload={"x": "y", "n": 42})
    schema = {
        "type": "object",
        "properties": {
            "x": {"type": "string"},
            "n": {"type": "integer"},
        },
        "required": ["x"],
    }
    benchmark(gate_schema, env, schema_in=schema)


@pytest.mark.perf
def test_perf_gate_dedup(benchmark) -> None:
    store = DedupStore(window_ms=60_000)
    env = build_envelope(topic="t", payload={})
    # First call seeds the store; subsequent calls hit the dedup-fast path.
    store.seen(env.event_id)
    benchmark(gate_dedup, env, store=store)


@pytest.mark.perf
def test_perf_run_gating_full_pass(benchmark) -> None:
    env = build_envelope(topic="t", payload={"q": "hi"})
    schema = {"type": "object", "properties": {"q": {"type": "string"}}}
    store = DedupStore(window_ms=60_000)
    guard = NoOpGuardrail()

    counter = {"n": 0}

    def call() -> None:
        # New event each call so dedup doesn't reject.
        counter["n"] += 1
        e = build_envelope(topic="t", payload={"q": f"hi {counter['n']}"})
        asyncio.run(
            run_gating(
                e,
                agent_tags={},
                schema_in=schema,
                guardrail=guard,
                agent_id="a",
                est_tokens=100,
                dedup_store=store,
            ),
        )

    # benchmark returns mean — async setup makes this slower than the
    # individual gates, which is the point.
    benchmark(call)
