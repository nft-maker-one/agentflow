"""End-node fan-in (``end_join`` FlowControl) — TerminalDetector join.

Default (``end_join=False``): the run is declared terminal on the FIRST
end-edge signal. With ``end_join=True`` and >1 direct edge into
``__end__``, the run only completes once EVERY end via-topic has fired.
"""

from __future__ import annotations

from agentkit import Agent, END, START, workflow
from agentkit.bus.builder import build_envelope
from agentkit.models.enums import RunStatus
from agentkit.orchestrator.routing import TerminalDetector
from agentkit.testing import LocalRuntime, MockLLMGateway


# ----------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------


def _fanin_wf(*, end_join: bool):
    """Two agents, each START→agent→END on its own via-topic."""
    wf = workflow("wf_join", end_join=end_join)
    a = Agent(template_key="a", role="thinking", subscribe=["a.in"], publish=["a.out"])
    b = Agent(template_key="b", role="thinking", subscribe=["b.in"], publish=["b.out"])
    wf.add(a)
    wf.add(b)
    wf.connect(START, a, via="a.in")
    wf.connect(START, b, via="b.in")
    wf.connect(a, END, via="a.out")
    wf.connect(b, END, via="b.out")
    return wf


def _detector(ir, calls):
    async def rec(run_id, kind, reason, payload=None):
        calls.append((run_id, kind))
    return TerminalDetector(bus=None, ir=ir, on_terminal=rec)


def _end_env(topic, run_id="r1"):
    return build_envelope(topic=topic, payload={}, workflow_id="wf_join", run_id=run_id)


# ----------------------------------------------------------------
# Config plumbing
# ----------------------------------------------------------------


class TestEndJoinPlumbing:
    def test_flag_flows_sdk_to_ir(self) -> None:
        ir, _ = _fanin_wf(end_join=True).compile()
        assert ir.end_join is True
        ir2, _ = _fanin_wf(end_join=False).compile()
        assert ir2.end_join is False

    def test_join_active_only_when_flag_and_multiple_end_topics(self) -> None:
        ir, _ = _fanin_wf(end_join=True).compile()
        det = _detector(ir, [])
        assert sorted(det.end_topics) == ["a.out", "b.out"]
        assert det._end_join is True  # noqa: SLF001

    def test_join_inactive_with_single_end_topic(self) -> None:
        wf = workflow("wf_one", end_join=True)
        a = Agent(template_key="a", role="thinking", subscribe=["a.in"], publish=["a.out"])
        wf.add(a)
        wf.connect(START, a, via="a.in")
        wf.connect(a, END, via="a.out")
        ir, _ = wf.compile()
        det = _detector(ir, [])
        assert det._end_join is False  # only one end-topic → nothing to join  # noqa: SLF001


# ----------------------------------------------------------------
# Join gate semantics (drive _notify directly — deterministic)
# ----------------------------------------------------------------


class TestJoinGate:
    async def test_holds_first_fires_on_all(self) -> None:
        ir, _ = _fanin_wf(end_join=True).compile()
        calls: list = []
        det = _detector(ir, calls)
        await det._notify(_end_env("a.out"), terminal_kind="__end__")  # noqa: SLF001
        assert calls == []  # held — still waiting for b.out
        await det._notify(_end_env("b.out"), terminal_kind="__end__")  # noqa: SLF001
        assert calls == [("r1", "__end__")]  # fired once, all collected

    async def test_duplicate_signal_does_not_satisfy_join(self) -> None:
        ir, _ = _fanin_wf(end_join=True).compile()
        calls: list = []
        det = _detector(ir, calls)
        await det._notify(_end_env("a.out"), terminal_kind="__end__")  # noqa: SLF001
        await det._notify(_end_env("a.out"), terminal_kind="__end__")  # dup  # noqa: SLF001
        assert calls == []  # a.out twice ≠ both topics
        await det._notify(_end_env("b.out"), terminal_kind="__end__")  # noqa: SLF001
        assert calls == [("r1", "__end__")]

    async def test_runs_are_independent(self) -> None:
        ir, _ = _fanin_wf(end_join=True).compile()
        calls: list = []
        det = _detector(ir, calls)
        await det._notify(_end_env("a.out", "rX"), terminal_kind="__end__")  # noqa: SLF001
        await det._notify(_end_env("a.out", "rY"), terminal_kind="__end__")  # noqa: SLF001
        assert calls == []
        await det._notify(_end_env("b.out", "rX"), terminal_kind="__end__")  # noqa: SLF001
        assert calls == [("rX", "__end__")]  # only rX completed

    async def test_error_bypasses_join(self) -> None:
        ir, _ = _fanin_wf(end_join=True).compile()
        calls: list = []
        det = _detector(ir, calls)
        # An error mid-join terminates the run immediately (no waiting).
        await det._notify(_end_env("anything"), terminal_kind="__error__")  # noqa: SLF001
        assert calls == [("r1", "__error__")]

    async def test_switch_synthetic_marker_bypasses_join(self) -> None:
        ir, _ = _fanin_wf(end_join=True).compile()
        calls: list = []
        det = _detector(ir, calls)
        # A switch resolving to __end__ emits system.run.<id>.end — not a
        # direct end via-topic → fires immediately.
        env = _end_env("system.run.r1.end")
        await det._notify(env, terminal_kind="__end__")  # noqa: SLF001
        assert calls == [("r1", "__end__")]

    async def test_default_fires_on_first_signal(self) -> None:
        ir, _ = _fanin_wf(end_join=False).compile()
        calls: list = []
        det = _detector(ir, calls)
        await det._notify(_end_env("a.out"), terminal_kind="__end__")  # noqa: SLF001
        assert calls == [("r1", "__end__")]  # no join → first wins


# ----------------------------------------------------------------
# End-to-end through the real runtime
# ----------------------------------------------------------------


class TestEndJoinE2E:
    async def test_run_completes_after_both_branches(self) -> None:
        wf = _fanin_wf(end_join=True)
        async with LocalRuntime(wf, llm=MockLLMGateway()) as rt:
            run = await rt.run(input={"q": "hi"}, timeout=5.0)
        # Both branches fired; run reached terminal exactly once, Succeeded.
        assert run.status is RunStatus.SUCCEEDED
        terminal_events = [
            e for e in run.cursor.branch_log if e.edge_id == "__terminal__"
        ]
        assert len(terminal_events) == 1
