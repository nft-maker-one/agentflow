"""Tests for the FastAPI control-plane API.

Uses ``httpx.AsyncClient`` against an in-memory ``AppState`` — no real
HTTP socket needed. Verifies the routes that the React UI depends on.
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator

import httpx
import pytest

from agentkit import END, START, Agent, Event, workflow
from agentkit.api import AppState, create_app
from agentkit.runtime.executor import _FunctionExecutor
from agentkit.testing import MockLLMGateway


# ----------------------------------------------------------------
# Fixture — start an AppState + deploy two demo workflows
# ----------------------------------------------------------------


class _Echo(Agent):
    role = "thinking"
    template_key = "echo"
    subscribe = ["agent.echo.in.q"]
    publish = ["agent.echo.out.r"]

    async def handle(self, ctx, event):
        return [Event("agent.echo.out.r", {"text": event.payload.get("q", "")})]


@pytest.fixture
async def deployed_state() -> AsyncIterator[AppState]:
    state = AppState()
    await state.start()
    try:
        # Deploy a single one-agent workflow so we have something to query.
        echo = _Echo()
        wf = workflow("wf_test_api")
        wf.add(echo)
        wf.connect(START, echo)
        wf.connect(echo, END)
        ir, plan = wf.compile()

        state.handler_registry.register(
            workflow_id=ir.id,
            template_key=echo.key,
            executor=_FunctionExecutor(echo.handler),
            replace=True,
        )
        # Track on agents_by_key so PATCH endpoints can find it.
        state.agents_by_key[(ir.id, echo.key)] = echo
        await state.deploy_workflow(ir, plan, llm_gateway=MockLLMGateway())
        # Settle subscriber.
        await asyncio.sleep(0.05)
        yield state
    finally:
        await state.stop()


@pytest.fixture
async def client(deployed_state: AppState) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(deployed_state)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver",
    ) as c:
        yield c


# ================================================================
# /api/health
# ================================================================


class TestHealth:
    async def test_health_ok(self, client) -> None:
        r = await client.get("/api/health")
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is True
        assert "wf_test_api" in body["deployed_workflows"]


# ================================================================
# /api/workflows
# ================================================================


class TestWorkflows:
    async def test_list_workflows(self, client) -> None:
        r = await client.get("/api/workflows")
        assert r.status_code == 200
        data = r.json()
        assert any(w["id"] == "wf_test_api" for w in data)
        wf = next(w for w in data if w["id"] == "wf_test_api")
        assert wf["n_agents"] == 1
        assert "ir_hash" in wf

    async def test_get_workflow_detail_includes_graph(self, client) -> None:
        r = await client.get("/api/workflows/wf_test_api")
        assert r.status_code == 200
        wf = r.json()
        assert wf["id"] == "wf_test_api"
        # 1 agent + __start__ + __end__ + __error__ = 4 nodes
        node_ids = {n["id"] for n in wf["nodes"]}
        assert "echo" in node_ids
        assert "__start__" in node_ids
        assert "__end__" in node_ids
        # Edges should include start→echo and echo→end
        srcs_targets = {(e["source"], e["target"]) for e in wf["edges"]}
        assert ("__start__", "echo") in srcs_targets
        assert ("echo", "__end__") in srcs_targets

    async def test_get_workflow_404(self, client) -> None:
        r = await client.get("/api/workflows/nonexistent")
        assert r.status_code == 404


# ================================================================
# /api/runs
# ================================================================


class TestRuns:
    async def test_create_then_get_run(self, client) -> None:
        r = await client.post(
            "/api/runs",
            json={"workflow_id": "wf_test_api", "input": {"q": "hello"}},
        )
        assert r.status_code == 201, r.text
        run = r.json()
        run_id = run["run_id"]
        assert run["workflow_id"] == "wf_test_api"

        # Wait for completion (worker handles it in-process).
        for _ in range(40):
            await asyncio.sleep(0.05)
            r2 = await client.get(f"/api/runs/{run_id}")
            if r2.json()["status"] in ("Succeeded", "Failed"):
                break

        detail = r2.json()
        assert detail["status"] == "Succeeded"
        assert detail["run_id"] == run_id
        assert detail["input"] == {"q": "hello"}

    async def test_list_runs_filters(self, client) -> None:
        # Create 2 runs, filter by workflow_id.
        run_ids: list[str] = []
        for _ in range(2):
            r = await client.post(
                "/api/runs",
                json={"workflow_id": "wf_test_api", "input": {"q": "x"}},
            )
            run_ids.append(r.json()["run_id"])

        # Wait for both to terminate so teardown is fast (otherwise the
        # worker drain timeout fires).
        for rid in run_ids:
            for _ in range(40):
                await asyncio.sleep(0.05)
                r = await client.get(f"/api/runs/{rid}")
                if r.json()["status"] in ("Succeeded", "Failed"):
                    break

        r = await client.get(
            "/api/runs",
            params={"workflow_id": "wf_test_api", "limit": 10},
        )
        assert r.status_code == 200
        runs = r.json()
        assert len(runs) >= 2
        assert all(r["workflow_id"] == "wf_test_api" for r in runs)

    async def test_create_run_unknown_workflow(self, client) -> None:
        r = await client.post(
            "/api/runs",
            json={"workflow_id": "nope", "input": {}},
        )
        assert r.status_code == 404


# ================================================================
# /api/agents
# ================================================================


class TestAgents:
    async def test_list_agents(self, client) -> None:
        r = await client.get("/api/agents")
        assert r.status_code == 200
        data = r.json()
        # template_key is namespaced as "<workflow>/<key>"
        assert any(a["template_key"] == "wf_test_api/echo" for a in data)


# ================================================================
# /api/runs/{id}/events  (SSE)
# ================================================================


class TestEvents:
    async def test_run_events_recorded_in_buffer(
        self, client, deployed_state,
    ) -> None:
        """The wildcard tap captures every run-tagged envelope into the
        per-run buffer that powers SSE. Verify directly — testing the
        SSE wire format is left for the live ``agentkit serve`` smoke
        test (httpx ASGITransport's stream handling is flaky)."""
        r = await client.post(
            "/api/runs",
            json={"workflow_id": "wf_test_api", "input": {"q": "buf-test"}},
        )
        run_id = r.json()["run_id"]

        # Wait for completion.
        for _ in range(40):
            await asyncio.sleep(0.05)
            r2 = await client.get(f"/api/runs/{run_id}")
            if r2.json()["status"] == "Succeeded":
                break

        envelopes = deployed_state.snapshot_events(run_id)
        assert envelopes, "tap subscriber didn't capture any events"
        topics = {e.topic for e in envelopes}
        # The pipeline is: __start__ → echo (in.q) → echo emits out.r → __end__.
        assert "agent.echo.in.q" in topics
        assert "agent.echo.out.r" in topics
        # Every envelope should be tagged with our run_id.
        assert all(e.run_id == run_id for e in envelopes)


# ================================================================
# /api/workflows/{wf}/agents/{key}  — live edit
# ================================================================


class TestAgentEdit:
    async def test_get_config(self, client) -> None:
        r = await client.get("/api/workflows/wf_test_api/agents/echo/config")
        assert r.status_code == 200
        cfg = r.json()
        assert cfg["template_key"] == "echo"
        assert cfg["max_retries"] == 0

    async def test_patch_prompt(self, client) -> None:
        r = await client.patch(
            "/api/workflows/wf_test_api/agents/echo",
            json={
                "prompt": "Summarize: {{ payload.text }}",
                "max_retries": 3,
            },
        )
        assert r.status_code == 200
        cfg = r.json()
        assert cfg["prompt"] == "Summarize: {{ payload.text }}"
        assert cfg["max_retries"] == 3

        # Read back is consistent.
        r2 = await client.get("/api/workflows/wf_test_api/agents/echo/config")
        assert r2.json()["prompt"] == "Summarize: {{ payload.text }}"

    async def test_patch_prompt_recompiles_template(self, client, deployed_state) -> None:
        # Regression: patching the prompt must recompile the agent's cached
        # Jinja template (`_compiled_prompt`), not just `cfg["prompt"]` —
        # otherwise the runtime keeps rendering the OLD prompt (the editor
        # showed the new field while runs still failed on the old one).
        from agentkit.runtime.context import Event
        r = await client.patch(
            "/api/workflows/wf_test_api/agents/echo",
            json={"prompt": "subject={{ payload.intent }}"},
        )
        assert r.status_code == 200
        agent = deployed_state.agents_by_key[("wf_test_api", "echo")]
        rendered = agent._render_compiled(Event("agent.x.in", {"intent": "ok"}))  # noqa: SLF001
        assert rendered == "subject=ok"

    async def test_patch_unknown_workflow_404(self, client) -> None:
        r = await client.patch(
            "/api/workflows/nope/agents/echo",
            json={"prompt": "x"},
        )
        assert r.status_code == 404

    async def test_patch_unknown_agent_404(self, client) -> None:
        r = await client.patch(
            "/api/workflows/wf_test_api/agents/nope",
            json={"prompt": "x"},
        )
        assert r.status_code == 404

    async def test_patch_empty_body_400(self, client) -> None:
        r = await client.patch(
            "/api/workflows/wf_test_api/agents/echo",
            json={},
        )
        assert r.status_code == 400


# ================================================================
# /api/workflows/{wf}/external + bridge edges
# ================================================================


class TestExtStartBridge:
    """Regression guard for the ``connect_to_start ghost-checked``
    bug — see docs/bugs.md.

    Invariants asserted:
      1. Synthetic bridge edges have id-prefix ``e_ext_start_``.
      2. The helper is idempotent: repeated update_agent does NOT
         multiply bridge edges.
      3. When the user adds an explicit ``__start__ → agent`` edge,
         no synthetic bridge is created (avoiding "ghost-check").
    """

    async def _create_test_wf(self, client, wf_id: str) -> None:
        """Create a fresh editable workflow via the public API so
        ``raw_spec_by_id`` is populated for add_agent / update_agent
        endpoints."""
        r = await client.post(
            "/api/workflows",
            json={"id": wf_id, "description": "ext-bridge regression test"},
        )
        assert r.status_code == 201, r.text

    async def test_ext_start_bridge_id_prefix_and_idempotent(
        self, client,
    ) -> None:
        wf_id = "wf_bridge_test"
        await self._create_test_wf(client, wf_id)

        # Switch to event_driven (gate for ext IO).
        r = await client.put(
            f"/api/workflows/{wf_id}/mode", json={"mode": "event_driven"},
        )
        assert r.status_code == 200

        # Register an external source that will feed the agent.
        r = await client.post(
            f"/api/workflows/{wf_id}/external/sources",
            json={
                "name": "fake_src",
                "kind": "python_script",
                "topic": "ext.fake.in",
                "config": {"script": (
                    "async def stream(ctx):\n"
                    "    if False:\n"
                    "        yield {}\n"
                )},
            },
        )
        assert r.status_code == 200, r.text

        # Add an agent whose only subscribe is the ext source topic.
        # Without the bridge it would fail IR validation as
        # unreachable from __start__.
        body = {
            "template_key": "ext_listener",
            "role": "thinking",
            "subscribe_topics": ["ext.fake.in"],
            "publish_topics": ["agent.ext_listener.out"],
            "output_field": "result",
            "max_retries": 0,
            "connect_to_start": False,
            "connect_to_end": True,
            "python_script": (
                "async def handle(ctx, event):\n"
                "    return []\n"
            ),
        }
        r = await client.post(
            f"/api/workflows/{wf_id}/agents", json=body,
        )
        assert r.status_code == 201, r.text

        # Inspect graph: exactly one e_ext_start_* edge should exist.
        r = await client.get(f"/api/workflows/{wf_id}")
        assert r.status_code == 200
        edges = r.json()["edges"]
        bridges = [
            e for e in edges
            if e["id"].startswith("e_ext_start_")
            and e["target"] == "ext_listener"
        ]
        assert len(bridges) == 1, f"expected exactly 1 bridge, got {bridges}"
        assert bridges[0]["source"] == "__start__"
        assert bridges[0]["via"] == "ext.fake.in"

        # Re-PUT the same agent twice — bridge count stays at 1
        # (idempotency invariant). PUT uses ``UpdateAgentRequest``
        # which doesn't accept ``template_key`` in the body.
        update_body = {k: v for k, v in body.items() if k != "template_key"}
        for _ in range(2):
            r = await client.put(
                f"/api/workflows/{wf_id}/agents/ext_listener",
                json=update_body,
            )
            assert r.status_code == 200, r.text

        r = await client.get(f"/api/workflows/{wf_id}")
        edges = r.json()["edges"]
        bridges = [
            e for e in edges
            if e["id"].startswith("e_ext_start_")
            and e["target"] == "ext_listener"
        ]
        assert len(bridges) == 1, (
            "bridge edges multiplied across updates — "
            "_auto_wire_external_start_edges is no longer idempotent. "
            "See docs/bugs.md §connect_to_start ghost-checked."
        )

    async def test_explicit_start_wire_suppresses_bridge(
        self, client,
    ) -> None:
        """When user ticks 'Wire from __start__', no synthetic bridge
        should be added — the explicit edge already makes the agent
        reachable, so synthesising another would re-trigger the
        ghost-checked behaviour from docs/bugs.md."""
        wf_id = "wf_bridge_test2"
        await self._create_test_wf(client, wf_id)
        await client.put(
            f"/api/workflows/{wf_id}/mode", json={"mode": "event_driven"},
        )
        await client.post(
            f"/api/workflows/{wf_id}/external/sources",
            json={
                "name": "fake_src2", "kind": "python_script",
                "topic": "ext.fake2.in",
                "config": {"script": (
                    "async def stream(ctx):\n"
                    "    if False:\n"
                    "        yield {}\n"
                )},
            },
        )
        r = await client.post(
            f"/api/workflows/{wf_id}/agents",
            json={
                "template_key": "explicit_start",
                "role": "thinking",
                "subscribe_topics": ["ext.fake2.in"],
                "publish_topics": ["agent.explicit_start.out"],
                "output_field": "result",
                "max_retries": 0,
                # User explicitly wired this from __start__:
                "connect_to_start": True,
                "connect_to_end": True,
                "python_script": (
                    "async def handle(ctx, event):\n"
                    "    return []\n"
                ),
            },
        )
        assert r.status_code == 201, r.text

        r = await client.get(f"/api/workflows/{wf_id}")
        edges = r.json()["edges"]
        bridges = [
            e for e in edges
            if e["id"].startswith("e_ext_start_")
            and e["target"] == "explicit_start"
        ]
        assert len(bridges) == 0, (
            f"explicit __start__ wiring should suppress synthetic "
            f"bridges, got {bridges}"
        )
        # And the explicit edge IS there.
        explicit = [
            e for e in edges
            if e["source"] == "__start__"
            and e["target"] == "explicit_start"
            and not e["id"].startswith("e_ext_start_")
        ]
        assert len(explicit) >= 1
