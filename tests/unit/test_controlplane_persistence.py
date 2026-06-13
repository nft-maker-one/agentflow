"""Control-plane persistence — restore wiring (backend-agnostic).

Uses an in-memory fake (``enabled=True``) so the AppState save/restore
path is exercised in CI without a real Postgres.
"""

from __future__ import annotations

from typing import Any

from agentkit import Agent, START, END, workflow
from agentkit.api.persistence import NoOpControlPlane
from agentkit.api.state import AppState
from agentkit.runtime.executor import _FunctionExecutor
from agentkit.testing import MockLLMGateway


class FakePersistence:
    """In-memory ControlPlanePersistence stand-in for Postgres."""

    enabled = True

    def __init__(self) -> None:
        self.projects: dict[str, dict] = {}
        self.workflows: dict[str, dict] = {}
        self.inbox: dict[str, dict] = {}
        self.run_events: dict[str, list] = {}

    async def start(self) -> None: ...
    async def stop(self) -> None: ...

    async def load_projects(self):
        return list(self.projects.values())

    async def load_workflows(self):
        # Mirror Postgres: only live (non-tombstoned) rows are restored.
        return [w for w in self.workflows.values() if not w.get("deleted_at")]

    async def load_inbox(self, *, limit: int = 500):
        return list(self.inbox.values())

    async def upsert_project(self, p):
        self.projects[p["id"]] = {"project_id": p["id"], **p}

    async def delete_project(self, pid):
        self.projects.pop(pid, None)

    async def upsert_workflow(self, w):
        # Resurrect on re-deploy (clears any tombstone), mirroring Postgres.
        self.workflows[w["workflow_id"]] = {**dict(w), "deleted_at": None}

    async def delete_workflow(self, wid):
        # Soft delete: tombstone, don't drop the row.
        if wid in self.workflows:
            self.workflows[wid]["deleted_at"] = "deleted"

    async def upsert_inbox(self, item):
        self.inbox[item["id"]] = {"item_id": item["id"], **item}

    async def update_inbox(self, item_id, *, read=None, archived=None):
        it = self.inbox.get(item_id)
        if it and read is not None:
            it["read"] = read
        if it and archived is not None:
            it["archived"] = archived

    async def mark_all_inbox_read(self, *, workflow_id=None):
        for it in self.inbox.values():
            it["read"] = True

    async def delete_inbox(self, item_id):
        self.inbox.pop(item_id, None)

    async def delete_inbox_for_workflow(self, workflow_id):
        for k in [k for k, v in self.inbox.items() if v.get("workflow_id") == workflow_id]:
            self.inbox.pop(k, None)

    async def persist_run_events(self, run_id, workflow_id, envelopes):
        self.run_events[run_id] = list(envelopes)

    async def load_run_events(self, run_id, *, limit=2000):
        return list(self.run_events.get(run_id, []))[:limit]


def _wf():
    wf = workflow("wf_cp")
    a = Agent(template_key="writer", role="thinking", subscribe=["w.in"],
              publish=["w.out"], llm="mock/mock", prompt="x: {{ payload.q }}",
              output_field="t")
    wf.add(a)
    wf.connect(START, a, via="w.in")
    wf.connect(a, END, via="w.out")
    return wf, a


class TestNoOp:
    async def test_noop_disabled_and_empty(self) -> None:
        n = NoOpControlPlane()
        assert n.enabled is False
        assert await n.load_workflows() == []
        assert await n.load_projects() == []


class TestRestore:
    async def test_deploy_then_restore_in_fresh_appstate(self) -> None:
        store: dict[str, Any] = {}
        fake = FakePersistence()

        # ── AppState #1: deploy workflow + project + inbox ──
        st1 = AppState(persistence=fake)
        await st1.start()
        st1.set_llm_gateway(MockLLMGateway())
        wf, a = _wf()
        st1.agents_by_key[("wf_cp", "writer")] = a
        st1.handler_registry.register(
            workflow_id="wf_cp", template_key="writer",
            executor=_FunctionExecutor(a.handler), replace=True,
        )
        ir, plan = wf.compile()
        await st1.deploy_workflow(ir, plan, raw_spec=wf.to_dict())
        proj = await st1.create_project("P1")
        await st1.push_inbox(workflow_id="wf_cp", category="run_succeeded", title="hi")
        await st1.stop()
        store["proj"] = proj.id

        # The fake captured everything.
        assert "wf_cp" in fake.workflows
        assert fake.workflows["wf_cp"]["agent_kwargs"].get("writer")  # handler config
        assert any(p["name"] == "P1" for p in fake.projects.values())
        assert any(i["title"] == "hi" for i in fake.inbox.values())

        # ── AppState #2 (fresh): restore from the same fake ──
        st2 = AppState(persistence=fake)
        await st2.start()
        st2.set_llm_gateway(MockLLMGateway())
        await st2.restore_from_persistence()
        assert "wf_cp" in st2.ir_by_id                       # workflow redeployed
        assert ("wf_cp", "writer") in st2.agents_by_key      # agent handler rebuilt
        assert any(p.name == "P1" for p in st2.projects.values())
        assert any(i.title == "hi" for i in st2.inbox._items)  # noqa: SLF001

        # Re-run the restored workflow — handler must work. Give the
        # freshly-deployed worker a beat to bind its bus subscription.
        import asyncio
        await asyncio.sleep(0.2)
        run = await st2.orchestrator.create_run(workflow_id="wf_cp", input={"q": "hi"})
        try:
            await st2.orchestrator.wait_for_completion(run.run_id, timeout=5.0)
        except Exception:
            pass
        got = await st2.store.get(run.run_id)
        from agentkit.models.enums import RunStatus
        assert got.status is RunStatus.SUCCEEDED
        await st2.stop()

    async def test_run_events_persisted_on_completion_and_reloadable(self) -> None:
        import asyncio
        fake = FakePersistence()
        st = AppState(persistence=fake)
        await st.start()
        st.set_llm_gateway(MockLLMGateway())
        wf, a = _wf()
        st.agents_by_key[("wf_cp", "writer")] = a
        st.handler_registry.register(
            workflow_id="wf_cp", template_key="writer",
            executor=_FunctionExecutor(a.handler), replace=True,
        )
        ir, plan = wf.compile()
        await st.deploy_workflow(ir, plan, raw_spec=wf.to_dict())
        await asyncio.sleep(0.2)  # let the worker bind its subscription
        run = await st.orchestrator.create_run(workflow_id="wf_cp", input={"q": "hi"})
        try:
            await st.orchestrator.wait_for_completion(run.run_id, timeout=5.0)
        except Exception:
            pass
        # The tap loop sees `*.completed` and async-flushes — give it a beat.
        for _ in range(40):
            await asyncio.sleep(0.1)
            if run.run_id in fake.run_events:
                break
        assert fake.run_events.get(run.run_id), "run events were not persisted"
        # Reload from durable storage (as the SSE fallback does for an
        # archived run) and confirm an intermediate agent event is there.
        loaded = await st.load_persisted_events(run.run_id)
        assert len(loaded) > 0
        topics = {e.topic for e in loaded}
        assert "w.out" in topics   # the writer's published output (intermediate)
        await st.stop()

    async def test_deleting_workflow_removes_its_inbox_items(self) -> None:
        fake = FakePersistence()
        st = AppState(persistence=fake)
        await st.start()
        st.set_llm_gateway(MockLLMGateway())
        wf, a = _wf()
        st.agents_by_key[("wf_cp", "writer")] = a
        st.handler_registry.register(
            workflow_id="wf_cp", template_key="writer",
            executor=_FunctionExecutor(a.handler), replace=True,
        )
        ir, plan = wf.compile()
        await st.deploy_workflow(ir, plan, raw_spec=wf.to_dict())
        await st.push_inbox(workflow_id="wf_cp", category="run_succeeded", title="x")
        assert any(i.workflow_id == "wf_cp" for i in st.inbox._items)  # noqa: SLF001
        assert any(i["workflow_id"] == "wf_cp" for i in fake.inbox.values())
        # Orchestrator routers (SwitchRouter + TerminalDetector) are live.
        assert "wf_cp" in st.orchestrator._workflows           # noqa: SLF001
        assert "wf_cp" in st.orchestrator._terminal_detectors  # noqa: SLF001

        await st.undeploy_workflow("wf_cp")
        # Orchestrator routers torn down — no leaked bus consumers.
        assert "wf_cp" not in st.orchestrator._workflows           # noqa: SLF001
        assert "wf_cp" not in st.orchestrator._terminal_detectors  # noqa: SLF001
        assert "wf_cp" not in st.orchestrator._switch_routers      # noqa: SLF001
        # Inbox is HARD-deleted — no dangling notifications (memory + persisted).
        assert not any(i.workflow_id == "wf_cp" for i in st.inbox._items)  # noqa: SLF001
        assert not any(i["workflow_id"] == "wf_cp" for i in fake.inbox.values())
        # Workflow is SOFT-deleted — row tombstoned, excluded from restore.
        assert fake.workflows["wf_cp"]["deleted_at"]          # tombstone stamped
        assert not any(w["workflow_id"] == "wf_cp" for w in await fake.load_workflows())
        await st.stop()

    async def test_soft_deleted_workflow_resurrects_on_redeploy(self) -> None:
        fake = FakePersistence()
        st = AppState(persistence=fake)
        await st.start()
        st.set_llm_gateway(MockLLMGateway())

        def _deploy():
            wf, a = _wf()
            st.agents_by_key[("wf_cp", "writer")] = a
            st.handler_registry.register(
                workflow_id="wf_cp", template_key="writer",
                executor=_FunctionExecutor(a.handler), replace=True,
            )
            ir, plan = wf.compile()
            return ir, plan, wf

        ir, plan, wf = _deploy()
        await st.deploy_workflow(ir, plan, raw_spec=wf.to_dict())
        await st.undeploy_workflow("wf_cp")
        assert fake.workflows["wf_cp"]["deleted_at"]           # tombstoned

        # Re-deploying the same id clears the tombstone.
        ir, plan, wf = _deploy()
        await st.deploy_workflow(ir, plan, raw_spec=wf.to_dict())
        assert fake.workflows["wf_cp"]["deleted_at"] is None   # resurrected
        assert any(w["workflow_id"] == "wf_cp" for w in await fake.load_workflows())
        await st.stop()
