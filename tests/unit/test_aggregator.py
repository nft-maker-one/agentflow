"""End-to-end tests for the Aggregator (fan-in) pattern.

Verifies the runtime AgentInstance buffer + gate works as designed:

* threshold gating — handler fires only when N distinct topics arrived
* required gating — handler waits for specified topics regardless of count
* merged payload — all upstream payloads union into one dict + ``_inputs``
"""

from __future__ import annotations

import asyncio

import pytest

from agentkit import END, START, Agent, Event, workflow
from agentkit.models.enums import RunStatus
from agentkit.testing import LocalRuntime, MockLLMGateway


class TestAggregator:
    async def test_threshold_all_topics(self) -> None:
        """Default threshold = len(subscribe). Aggregator fires once
        ALL three upstream agents have published."""

        # Three upstream agents, each tags a different field on the input.
        class A(Agent):
            template_key = "a"
            subscribe = ["agent.start.in"]
            publish = ["agent.a.out"]
            async def handle(self, ctx, event):
                return [Event("agent.a.out", {**event.payload, "from_a": "AAA"})]

        class B(Agent):
            template_key = "b"
            subscribe = ["agent.start.in"]
            publish = ["agent.b.out"]
            async def handle(self, ctx, event):
                return [Event("agent.b.out", {**event.payload, "from_b": "BBB"})]

        class C(Agent):
            template_key = "c"
            subscribe = ["agent.start.in"]
            publish = ["agent.c.out"]
            async def handle(self, ctx, event):
                return [Event("agent.c.out", {**event.payload, "from_c": "CCC"})]

        # Aggregator sees three topics; default threshold = 3 = all.
        captured_payloads: list[dict] = []

        class Combiner(Agent):
            template_key = "combiner"
            role = "aggregator"
            subscribe = ["agent.a.out", "agent.b.out", "agent.c.out"]
            publish = ["agent.combiner.out"]
            aggregate = {"threshold": 0}  # 0 → all subscribe topics

            async def handle(self, ctx, event):
                captured_payloads.append(dict(event.payload))
                return [Event("agent.combiner.out", {
                    "joined": "+".join([
                        event.payload.get("from_a", "?"),
                        event.payload.get("from_b", "?"),
                        event.payload.get("from_c", "?"),
                    ]),
                })]

        wf = workflow("wf_agg_threshold")
        a, b, c, combiner = A(), B(), C(), Combiner()
        for ag in (a, b, c, combiner):
            wf.add(ag)
        wf.connect(START, a, via="agent.start.in")
        wf.connect(START, b, via="agent.start.in")
        wf.connect(START, c, via="agent.start.in")
        wf.connect(a, combiner, via="agent.a.out")
        wf.connect(b, combiner, via="agent.b.out")
        wf.connect(c, combiner, via="agent.c.out")
        wf.connect(combiner, END, via="agent.combiner.out")

        async with LocalRuntime(wf, llm=MockLLMGateway()) as rt:
            run = await rt.run(input={"q": "hello"}, timeout=5.0)

        assert run.status is RunStatus.SUCCEEDED
        # Exactly ONE invocation of combiner — proof the gate held
        # back A and B until C also arrived.
        assert len(captured_payloads) == 1
        merged = captured_payloads[0]
        # Merged payload contains every upstream's contribution.
        assert merged["from_a"] == "AAA"
        assert merged["from_b"] == "BBB"
        assert merged["from_c"] == "CCC"
        assert merged["q"] == "hello"  # original input preserved
        # _inputs map is keyed by topic.
        assert "_inputs" in merged
        assert set(merged["_inputs"].keys()) == {
            "agent.a.out", "agent.b.out", "agent.c.out",
        }

    async def test_required_topics_block_until_present(self) -> None:
        """Two of three topics arrive but the required one doesn't —
        handler must NOT fire even if threshold would be met."""

        class A(Agent):
            template_key = "a"
            subscribe = ["agent.start.in"]
            publish = ["agent.a.out"]
            async def handle(self, ctx, event):
                return [Event("agent.a.out", {**event.payload, "from_a": "AAA"})]

        class B(Agent):
            template_key = "b"
            subscribe = ["agent.start.in"]
            publish = ["agent.b.out"]
            async def handle(self, ctx, event):
                return [Event("agent.b.out", {**event.payload, "from_b": "BBB"})]

        # C never fires — its subscribe topic isn't reached.
        # We simulate by simply not adding C and not wiring its topic.
        captured: list[dict] = []

        class Combiner(Agent):
            template_key = "combiner"
            role = "aggregator"
            subscribe = ["agent.a.out", "agent.b.out", "agent.c.out"]
            publish = ["agent.combiner.out"]
            # threshold = 2 (would be met by A+B)
            # but required = c.out forces wait for C
            aggregate = {
                "threshold": 2,
                "required": ["agent.c.out"],
            }

            async def handle(self, ctx, event):
                captured.append(dict(event.payload))
                return [Event("agent.combiner.out", event.payload)]

        wf = workflow("wf_agg_required")
        for ag in (A(), B(), Combiner()):
            wf.add(ag)
        wf.connect(START, "a", via="agent.start.in")
        wf.connect(START, "b", via="agent.start.in")
        wf.connect("a", "combiner", via="agent.a.out")
        wf.connect("b", "combiner", via="agent.b.out")
        # We don't wire combiner→END: the run won't terminate, but
        # we'll cancel it after a brief wait.

        async with LocalRuntime(wf, llm=MockLLMGateway()) as rt:
            # Fire and let A+B emit; combiner should not fire.
            run = await rt.orchestrator.create_run(
                workflow_id="wf_agg_required", input={"q": "x"},
            )
            await asyncio.sleep(0.5)
            # Combiner never fired — required topic absent.
            assert len(captured) == 0
            # Cancel for clean teardown.
            await rt.orchestrator.cancel_run(run.run_id, reason="test_cleanup")
            await asyncio.sleep(0.1)
