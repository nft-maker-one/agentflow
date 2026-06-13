"""``AgentKitClient`` — programmatic access to a running control-plane.

Mirrors the React UI's API surface so SDK users can:
  * deploy a Python-defined :class:`~agentkit.WorkflowDef` to a server
  * trigger / cancel runs, stream events
  * manage external sources / sinks
  * flip workflow mode (normal ↔ event_driven)
  * read inbox notifications

This is a **client**: it does no I/O of its own beyond HTTP — it talks
to the FastAPI server defined in :mod:`agentkit.api`. Use it from
scripts, notebooks, integration tests.

Example::

    import asyncio
    from agentkit import workflow, Agent
    from agentkit.client import AgentKitClient

    class Echo(Agent):
        subscribe = ["echo.in"]; publish = ["echo.out"]
        async def handle(self, ctx, ev):
            return [Event("echo.out", {"text": ev.payload.get("text")})]

    wf = workflow("demo").add(Echo()).connect("__start__", Echo()).connect(Echo(), "__end__")

    async def main():
        async with AgentKitClient("http://localhost:8765") as c:
            await c.deploy(wf)
            run = await c.create_run("demo", input={"text": "hi"})
            print(run["status"])

    asyncio.run(main())
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from agentkit.sdk.workflow import WorkflowDef


class AgentKitClient:
    """Thin async HTTP wrapper around the control-plane API."""

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        *,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(timeout),
        )

    async def __aenter__(self) -> "AgentKitClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------------
    # System / health
    # ------------------------------------------------------------------

    async def health(self) -> dict[str, Any]:
        return await self._get("/api/health")

    async def llm_models(self) -> dict[str, list[str]]:
        return await self._get("/api/system/llm-models")

    # ------------------------------------------------------------------
    # Workflow lifecycle
    # ------------------------------------------------------------------

    async def list_workflows(self) -> list[dict[str, Any]]:
        return await self._get("/api/workflows")

    async def get_workflow(self, wf_id: str) -> dict[str, Any]:
        return await self._get(f"/api/workflows/{wf_id}")

    async def deploy(
        self,
        wf: WorkflowDef,
        *,
        project_id: str = "default",
    ) -> dict[str, Any]:
        """Deploy a SDK-built :class:`WorkflowDef` to the server.

        Multi-step orchestration that mirrors the UI's create-and-edit
        flow so handler-level fields (``python_script``, ``prompt``,
        ``output_field``, …) — which live OUTSIDE the IR — survive the
        round-trip.

        Steps:

        1. ``POST /api/workflows``   — bootstrap with placeholder agent
        2. Remove the placeholder
        3. For each SDK agent: ``POST /api/workflows/{id}/agents``
           passing every handler field (including python_script)
        4. ``PUT /workflows/{id}/start-input`` if customised
        5. ``PUT /workflows/{id}/mode``       if event_driven=True
        6. ``POST /external/{sources|sinks}`` for each ext I/O spec
        """
        # 1) Bootstrap a fresh workflow.
        r = await self._client.post(
            "/api/workflows",
            json={
                "id": wf.id,
                "description": wf._builder._description or "",  # noqa: SLF001
                "project_id": project_id,
            },
        )
        r.raise_for_status()

        # 2) Add each SDK agent FIRST (must come before deleting the
        #    bootstrap echo — IR validator rejects empty workflows).
        # 3) Add each SDK agent through live-edit so all handler-level
        #    fields (python_script, prompt, …) flow through.
        ir_dict = wf.to_dict()
        agents_dict = ir_dict.get("agents", {})
        # Pull edges so we can decide whether to wire connect_to_start /
        # connect_to_end on the FIRST/LAST agent.
        edges = ir_dict.get("edges", {})
        for key, adef in agents_dict.items():
            agent_obj = wf._agents_by_key.get(key)  # noqa: SLF001
            connect_start = any(
                e.get("from") == "__start__" and e.get("to") == key
                for e in edges.values()
            )
            connect_end = any(
                e.get("from") == key and e.get("to") == "__end__"
                for e in edges.values()
            )

            body: dict[str, Any] = {
                "template_key": key,
                "role": adef.get("role", "thinking"),
                "description": adef.get("description", ""),
                "subscribe_topics": [t["topic"] for t in adef.get("subscribe", [])],
                "publish_topics":   [t["topic"] for t in adef.get("publish", [])],
                "max_retries": adef.get("max_retries", 0),
                "connect_to_start": connect_start,
                "connect_to_end": connect_end,
            }
            # IR-level llm field (already a dict).
            if adef.get("llm"):
                body["llm"] = adef["llm"]
            # Handler-level fields live on the SDK Agent instance, not
            # the IR. Pull them from the original Agent object so they
            # reach the server through AddAgentRequest.
            if agent_obj is not None:
                for fld in (
                    "prompt", "system_prompt", "python_script",
                    "output_field",
                ):
                    val = getattr(agent_obj, fld, None)
                    if val is not None:
                        body[fld] = val
                # Output_field has a default of "result" — pass through.
                if "output_field" not in body:
                    body["output_field"] = getattr(
                        agent_obj, "output_field", "result",
                    )
            # Aggregate / guardrail (also outside IR for the API but
            # in the IR dict — pass through if present).
            if adef.get("aggregate"):
                body["aggregate_threshold"] = adef["aggregate"].get("threshold", 0)
                body["aggregate_required_topics"] = list(
                    adef["aggregate"].get("required", [])
                )
            if adef.get("guardrail"):
                body["agent_guardrail"] = adef["guardrail"]

            r = await self._client.post(
                f"/api/workflows/{wf.id}/agents", json=body,
            )
            if r.status_code not in (200, 201):
                r.raise_for_status()

        # 3.5) NOW it's safe to delete the bootstrap echo agent —
        #      we've added at least one real agent so the workflow
        #      isn't empty when echo leaves.
        if "echo" not in agents_dict:
            try:
                r = await self._client.delete(
                    f"/api/workflows/{wf.id}/agents/echo",
                )
                if r.status_code not in (200, 204):
                    pass  # already gone
            except httpx.HTTPStatusError:
                pass

        # 4) start-input fields.
        if wf.start_input_fields and wf.start_input_fields != ["q"]:
            await self._put_json(
                f"/api/workflows/{wf.id}/start-input",
                {"fields": list(wf.start_input_fields)},
            )

        # 5) Mode flip.
        if wf.event_driven:
            await self._put_json(
                f"/api/workflows/{wf.id}/mode",
                {"mode": "event_driven"},
            )

        # 6) External I/O.
        for spec_io in wf.external_specs:
            await self.add_external(
                wf.id,
                direction=spec_io["direction"],
                name=spec_io["name"],
                kind=spec_io["kind"],
                topic=spec_io["topic"],
                config=spec_io.get("config", {}),
            )

        return await self.get_workflow(wf.id)

    async def delete_workflow(self, wf_id: str) -> None:
        r = await self._client.delete(f"/api/workflows/{wf_id}")
        r.raise_for_status()

    async def undo_workflow(self, wf_id: str) -> dict[str, Any]:
        r = await self._client.post(f"/api/workflows/{wf_id}/undo")
        r.raise_for_status()
        return r.json()

    async def set_mode(
        self, wf_id: str, mode: str,
    ) -> dict[str, Any]:
        return await self._put_json(
            f"/api/workflows/{wf_id}/mode", {"mode": mode},
        )

    async def set_workflow_guardrail(
        self,
        wf_id: str,
        *,
        max_total_tokens: int | None = None,
        max_cycles_per_run: int | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if max_total_tokens is not None:
            body["max_total_tokens"] = max_total_tokens
        if max_cycles_per_run is not None:
            body["max_cycles_per_run"] = max_cycles_per_run
        return await self._put_json(
            f"/api/workflows/{wf_id}/guardrail", body,
        )

    # ------------------------------------------------------------------
    # Runs
    # ------------------------------------------------------------------

    async def create_run(
        self,
        workflow_id: str,
        *,
        input: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self._post_json(
            "/api/runs",
            {"workflow_id": workflow_id, "input": input or {}},
        )

    async def list_runs(
        self,
        *,
        workflow_id: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if workflow_id:
            params["workflow_id"] = workflow_id
        return await self._get("/api/runs", params=params)

    async def get_run(self, run_id: str) -> dict[str, Any]:
        return await self._get(f"/api/runs/{run_id}")

    async def cancel_run(self, run_id: str) -> dict[str, Any]:
        r = await self._client.post(f"/api/runs/{run_id}/cancel")
        r.raise_for_status()
        return r.json()

    async def run_events_snapshot(self, run_id: str) -> list[dict[str, Any]]:
        return await self._get(f"/api/runs/{run_id}/events/snapshot")

    async def stream_run_events(self, run_id: str):  # type: ignore[no-untyped-def]
        """Async-iterate every envelope on this run via SSE."""
        url = f"{self.base_url}/api/runs/{run_id}/events"
        async with self._client.stream("GET", url) as r:
            r.raise_for_status()
            buf: list[str] = []
            async for line in r.aiter_lines():
                if line == "":
                    if buf:
                        # parse the SSE block
                        ev_type = ""
                        data = ""
                        for ln in buf:
                            if ln.startswith("event:"):
                                ev_type = ln[len("event:"):].strip()
                            elif ln.startswith("data:"):
                                data = ln[len("data:"):].strip()
                        buf = []
                        if ev_type == "envelope" and data:
                            try:
                                yield json.loads(data)
                            except json.JSONDecodeError:
                                continue
                else:
                    buf.append(line)

    # ------------------------------------------------------------------
    # External I/O
    # ------------------------------------------------------------------

    async def list_external_kinds(self) -> list[dict[str, Any]]:
        return await self._get("/api/external/kinds")

    async def list_external(self, wf_id: str) -> dict[str, Any]:
        return await self._get(f"/api/workflows/{wf_id}/external")

    async def add_external(
        self,
        wf_id: str,
        *,
        direction: str,
        name: str,
        kind: str,
        topic: str,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        endpoint = (
            f"/api/workflows/{wf_id}/external/sources"
            if direction == "source"
            else f"/api/workflows/{wf_id}/external/sinks"
        )
        return await self._post_json(
            endpoint,
            {
                "name": name, "kind": kind, "topic": topic,
                "config": dict(config or {}),
            },
        )

    async def remove_external(
        self, wf_id: str, *, direction: str, name: str,
    ) -> None:
        endpoint = (
            f"/api/workflows/{wf_id}/external/sources/{name}"
            if direction == "source"
            else f"/api/workflows/{wf_id}/external/sinks/{name}"
        )
        r = await self._client.delete(endpoint)
        if r.status_code not in (200, 204):
            r.raise_for_status()

    # ------------------------------------------------------------------
    # Inbox
    # ------------------------------------------------------------------

    async def list_inbox(
        self,
        *,
        workflow_id: str | None = None,
        include_archived: bool = False,
        unread_only: bool = False,
        limit: int = 100,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": limit}
        if workflow_id:
            params["workflow_id"] = workflow_id
        if include_archived:
            params["include_archived"] = "true"
        if unread_only:
            params["unread_only"] = "true"
        return await self._get("/api/inbox", params=params)

    async def inbox_mark_read(self, item_id: str) -> None:
        r = await self._client.post(f"/api/inbox/{item_id}/read")
        if r.status_code not in (200, 204):
            r.raise_for_status()

    async def inbox_mark_all_read(
        self, *, workflow_id: str | None = None,
    ) -> dict[str, int]:
        params = {"workflow_id": workflow_id} if workflow_id else None
        r = await self._client.post("/api/inbox/read-all", params=params)
        r.raise_for_status()
        return r.json()

    async def inbox_archive(self, item_id: str) -> None:
        r = await self._client.post(f"/api/inbox/{item_id}/archive")
        if r.status_code not in (200, 204):
            r.raise_for_status()

    async def inbox_delete(self, item_id: str) -> None:
        r = await self._client.delete(f"/api/inbox/{item_id}")
        if r.status_code not in (200, 204):
            r.raise_for_status()

    async def inbox_clear(
        self,
        *,
        workflow_id: str | None = None,
        archived_only: bool = False,
    ) -> dict[str, int]:
        params: dict[str, Any] = {}
        if workflow_id:
            params["workflow_id"] = workflow_id
        if archived_only:
            params["archived_only"] = "true"
        r = await self._client.post("/api/inbox/clear", params=params)
        r.raise_for_status()
        return r.json()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _get(self, path: str, *, params: dict | None = None) -> Any:
        r = await self._client.get(path, params=params)
        r.raise_for_status()
        return r.json()

    async def _post_json(self, path: str, body: dict) -> Any:
        r = await self._client.post(path, json=body)
        r.raise_for_status()
        return r.json()

    async def _put_json(self, path: str, body: dict) -> Any:
        r = await self._client.put(path, json=body)
        r.raise_for_status()
        return r.json()
