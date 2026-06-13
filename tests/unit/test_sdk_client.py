"""Smoke tests for the SDK ↔ API round-trip via ``AgentKitClient``.

Spins up an in-memory ``AppState`` + FastAPI app via ``ASGITransport``
(no real socket), exercises the ``AgentKitClient`` against it.

We're checking that every UI-exposed capability has a working SDK
on-ramp, mirroring the parity audit in ``docs/sdk_ui_parity.md``.
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator

import httpx
import pytest

from agentkit import AgentKitClient, Event, workflow
from agentkit.api import AppState, create_app
from agentkit.sdk.agent_class import Agent
from agentkit.testing import MockLLMGateway


class _PingAgent(Agent):
    """Pure-spec agent: handler logic lives in ``python_script`` so it
    survives JSON serialization to the server (this matches the way
    the React UI lets users author handlers in the textarea)."""
    role = "thinking"
    template_key = "ping"
    subscribe = ["agent.ping.in"]
    publish = ["agent.ping.out"]
    output_field = "text"
    python_script = (
        "def handle(payload, event=None):\n"
        "    return {\"text\": payload.get(\"q\", \"\")}\n"
    )

    async def handle(self, ctx, event):  # local-mode fallback
        return [Event("agent.ping.out", {"text": event.payload.get("q", "")})]


@pytest.fixture
async def state() -> AsyncIterator[AppState]:
    s = AppState()
    await s.start()
    s.set_llm_gateway(MockLLMGateway())
    try:
        yield s
    finally:
        await s.stop()


@pytest.fixture
async def client(state: AppState) -> AsyncIterator[AgentKitClient]:
    """An ``AgentKitClient`` whose underlying httpx talks to the
    in-memory FastAPI app via ``ASGITransport``."""
    app = create_app(state)
    transport = httpx.ASGITransport(app=app)
    c = AgentKitClient("http://testserver")
    # Re-bind the internal httpx client to the in-memory transport.
    await c._client.aclose()  # noqa: SLF001
    c._client = httpx.AsyncClient(  # noqa: SLF001
        transport=transport, base_url="http://testserver", timeout=30.0,
    )
    try:
        yield c
    finally:
        await c.close()


class TestSdkClientParity:
    """Each test corresponds to a row of the SDK ↔ UI parity table."""

    async def test_health(self, client: AgentKitClient) -> None:
        h = await client.health()
        assert h["ok"] is True

    async def test_deploy_from_spec(
        self, client: AgentKitClient,
    ) -> None:
        wf = workflow("wf_sdk_smoke")
        a = _PingAgent()
        wf.add(a)
        wf.connect("__start__", a)
        wf.connect(a, "__end__")

        detail = await client.deploy(wf)
        assert detail["id"] == "wf_sdk_smoke"
        assert any(ag["template_key"] == "ping" for ag in detail["agents"])

    async def test_event_driven_and_external_io(
        self, client: AgentKitClient,
    ) -> None:
        # Build a workflow with ext source + sink declared at SDK time.
        wf = workflow(
            "wf_ext_sdk",
            event_driven=True,
            start_input_fields=["msg"],
        )
        a = _PingAgent()
        wf.add(a)
        wf.connect("__start__", a)
        wf.connect(a, "__end__")
        wf.add_source(
            name="py_in", kind="python_script", topic="ext.in",
            config={"script": (
                "async def stream(ctx):\n"
                "    if False:\n"
                "        yield {}\n"
            )},
        )
        wf.add_sink(
            name="py_out", kind="python_script", topic="agent.ping.out",
            config={"script": (
                "async def handle(ctx, payload):\n"
                "    pass\n"
            )},
        )
        await client.deploy(wf)

        ext = await client.list_external("wf_ext_sdk")
        assert {s["name"] for s in ext["sources"]} == {"py_in"}
        assert {s["name"] for s in ext["sinks"]} == {"py_out"}

        # Mode flipped to event_driven by deploy().
        detail = await client.get_workflow("wf_ext_sdk")
        assert detail["mode"] == "event_driven"

    async def test_run_lifecycle(self, client: AgentKitClient) -> None:
        wf = workflow("wf_run_smoke")
        a = _PingAgent()
        wf.add(a)
        wf.connect("__start__", a)
        wf.connect(a, "__end__")
        await client.deploy(wf)
        # Let the redeployed worker settle its bus subscription before
        # we publish the start envelope — without this the InProcess
        # bus may swallow the first publish (subscriber registration
        # is async and races with create_run).
        await asyncio.sleep(0.3)

        run = await client.create_run("wf_run_smoke", input={"q": "hi"})
        assert run["status"] in {"Running", "Succeeded"}

        # Wait for terminal.
        for _ in range(50):
            r = await client.get_run(run["run_id"])
            if r["status"] != "Running":
                break
            await asyncio.sleep(0.1)
        assert r["status"] == "Succeeded"

        # Snapshot has at least one envelope.
        events = await client.run_events_snapshot(run["run_id"])
        assert any("agent.ping" in e["topic"] for e in events)

    async def test_inbox(self, client: AgentKitClient) -> None:
        wf = workflow("wf_inbox_smoke")
        a = _PingAgent()
        wf.add(a)
        wf.connect("__start__", a)
        wf.connect(a, "__end__")
        await client.deploy(wf)
        await asyncio.sleep(0.3)

        run = await client.create_run("wf_inbox_smoke", input={"q": "x"})
        # Let the terminal envelope flow.
        for _ in range(50):
            await asyncio.sleep(0.1)
            r = await client.get_run(run["run_id"])
            if r["status"] != "Running":
                break

        await asyncio.sleep(0.2)  # tap loop catch-up
        inbox = await client.list_inbox(workflow_id="wf_inbox_smoke")
        assert any(
            it["category"] == "run_succeeded" for it in inbox["items"]
        ), f"no run_succeeded in {inbox['items']}"

    async def test_guardrail_setter(self, client: AgentKitClient) -> None:
        wf = workflow("wf_guard_smoke")
        a = _PingAgent()
        wf.add(a)
        wf.connect("__start__", a)
        wf.connect(a, "__end__")
        await client.deploy(wf)

        await client.set_workflow_guardrail(
            "wf_guard_smoke",
            max_total_tokens=50_000,
            max_cycles_per_run=20,
        )
        d = await client.get_workflow("wf_guard_smoke")
        g = d["workflow_guardrail"]
        assert g["max_total_tokens"] == 50_000
        assert g["max_cycles_per_run"] == 20
