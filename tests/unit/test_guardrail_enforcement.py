"""Run-level token guardrail is actually ENFORCED end-to-end.

Regression for the wiring gap where the per-run token quota was never
enforced because the Orchestrator (LocalRuntime) / AgentWorker (serve)
weren't handed the guardrail instance — so the run quota was never
registered and Gate-3 ``Guardrail.precheck`` used a NoOp.
"""

from __future__ import annotations

from agentkit import END, START, Agent, workflow
from agentkit.guardrail.inprocess import InProcessGuardrail
from agentkit.models.enums import RunStatus
from agentkit.testing import LocalRuntime, MockLLMGateway


def _writer_wf(*, max_total_tokens: int):
    wf = workflow(
        "wf_guard",
        guardrails={"per_run": {
            "max_total_tokens": max_total_tokens,
            "max_cycles_per_run": 500,
        }},
    )
    a = Agent(
        template_key="writer", role="thinking",
        subscribe=["w.in"], publish=["w.out"],
        llm="mock/mock", prompt="write: {{ payload.q }}", output_field="text",
    )
    wf.add(a)
    wf.connect(START, a, via="w.in")
    wf.connect(a, END, via="w.out")
    return wf


async def _run(wf, *, guardrail):
    """Return ``(run_status, n_agent_outputs)``."""
    async with LocalRuntime(wf, llm=MockLLMGateway(), guardrail=guardrail) as rt:
        status = None
        try:
            run = await rt.run(input={"q": "hi"}, timeout=2.0)
            status = run.status
        except TimeoutError:
            status = "TIMEOUT"
        return status, len(rt.bus.published_for_topic("w.out"))


class TestRunTokenGuardrail:
    async def test_tiny_cap_blocks_and_fails_the_run(self) -> None:
        # Per-call estimate (~1k tokens) alone exceeds a cap of 10 → the
        # gate blocks the event before the handler runs → no output, AND
        # the run is terminated as Failed (not left hanging in Running).
        status, outputs = await _run(
            _writer_wf(max_total_tokens=10), guardrail=InProcessGuardrail(),
        )
        assert outputs == 0
        assert status is RunStatus.FAILED   # guardrail block → terminal, not TIMEOUT

    async def test_ample_cap_runs_normally(self) -> None:
        status, outputs = await _run(
            _writer_wf(max_total_tokens=100_000), guardrail=InProcessGuardrail(),
        )
        assert outputs == 1
        assert status is RunStatus.SUCCEEDED

    async def test_run_quota_is_registered_with_the_configured_cap(self) -> None:
        gr = InProcessGuardrail()
        wf = _writer_wf(max_total_tokens=12_345)
        async with LocalRuntime(wf, llm=MockLLMGateway(), guardrail=gr) as rt:
            try:
                await rt.run(input={"q": "hi"}, timeout=2.0)
            except TimeoutError:
                pass
            # The orchestrator registered the run with the workflow's cap.
            assert gr._runs, "no run was registered with the guardrail"  # noqa: SLF001
            caps = {rs.ctx.run.max_total_tokens for rs in gr._runs.values()}  # noqa: SLF001
            assert 12_345 in caps


def _agent_wf(*, per_agent_guardrail: dict | None):
    """Single agent with an optional PER-AGENT ('this agent only') cap."""
    wf = workflow("wf_agentcap")   # no workflow-level guardrail
    kw = dict(
        template_key="agent1", role="thinking",
        subscribe=["w.in"], publish=["w.out"],
        llm="mock/mock", prompt="x: {{ payload.q }}", output_field="t",
    )
    if per_agent_guardrail is not None:
        kw["guardrail"] = per_agent_guardrail
    a = Agent(**kw)
    wf.add(a)
    wf.connect(START, a, via="w.in")
    wf.connect(a, END, via="w.out")
    return wf


class TestPerAgentGuardrail:
    async def test_per_agent_token_cap_is_enforced(self) -> None:
        # "this agent only" → max_tokens_per_call=1. The gate estimate
        # (~1k) exceeds it → agent1 is blocked → run Failed.
        status, outputs = await _run(
            _agent_wf(per_agent_guardrail={"max_tokens_per_call": 1, "max_cycles": 5}),
            guardrail=InProcessGuardrail(),
        )
        assert outputs == 0
        assert status is RunStatus.FAILED

    async def test_no_per_agent_cap_inherits_default(self) -> None:
        status, outputs = await _run(
            _agent_wf(per_agent_guardrail=None), guardrail=InProcessGuardrail(),
        )
        assert outputs == 1
        assert status is RunStatus.SUCCEEDED

    async def test_override_is_keyed_by_template_in_context(self) -> None:
        gr = InProcessGuardrail()
        wf = _agent_wf(per_agent_guardrail={"max_tokens_per_call": 42, "max_cycles": 3})
        async with LocalRuntime(wf, llm=MockLLMGateway(), guardrail=gr) as rt:
            try:
                await rt.run(input={"q": "hi"}, timeout=2.0)
            except TimeoutError:
                pass
            ctxs = [rs.ctx for rs in gr._runs.values()]  # noqa: SLF001
            assert any(
                c.agent_overrides.get("agent1")
                and c.agent_overrides["agent1"].max_tokens_per_call == 42
                for c in ctxs
            )
