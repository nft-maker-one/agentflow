"""Shared in-process state for the control-plane HTTP API.

Tracks Bus + Orchestrator + Worker + Workflow inventory + per-run event
ring buffers + Projects + raw workflow specs (for live IR mutation).

Key extension points for the editable Web UI (Phase 2.2):

* ``raw_spec_by_id`` — the editable dict form of every deployed
  workflow. Mutations rewrite this then call :meth:`redeploy_workflow`.
* ``projects``       — in-memory project model. Each workflow has
  ``project_id`` pointing into this map.
* :meth:`redeploy_workflow` — drains the old worker and starts a fresh
  one for the same ``workflow_id`` after the spec changed.
"""

from __future__ import annotations

import asyncio
import copy
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agentkit.api.persistence import ControlPlanePersistence
    from agentkit.llm.guardrail_iface import GuardrailHandle

from agentkit.bus.inprocess import InProcessEventBus
from agentkit.bus.interface import EventBus, SubscribeSpec, Subscriber
from agentkit.common.logging import get_logger
from agentkit.api.inbox import Inbox
from agentkit.guardrail.inprocess import InProcessGuardrail
from agentkit.external_io import ExternalIOManager
from agentkit.models.envelope import Envelope
from agentkit.orchestrator import InMemoryRunStore, Orchestrator, RunStore
from agentkit.runtime import AgentWorker, HandlerRegistry
from agentkit.workflow import compile_from_dict
from agentkit.workflow.ir import WorkflowIR
from agentkit.workflow.plan import RuntimePlan

log = get_logger(__name__)

EVENT_BUFFER_PER_RUN: int = 500
TAP_PATTERN: str = "#"

DEFAULT_PROJECT_ID = "default"

#: Maximum number of topology snapshots kept in the undo stack per
#: workflow. Snapshots are small (deep-copy of raw spec + per-agent
#: constructor kwargs) — 30 covers a long edit session without
#: bloating memory. Older entries are evicted FIFO.
MAX_UNDO_HISTORY: int = 30


@dataclass
class _PerRunBuffer:
    events: deque[Envelope] = field(
        default_factory=lambda: deque(maxlen=EVENT_BUFFER_PER_RUN),
    )
    cond: asyncio.Condition = field(default_factory=asyncio.Condition)


@dataclass
class Project:
    """Lightweight in-memory project record.

    Workflows reference projects by id. The ``default`` project is
    always present.
    """

    id: str
    name: str
    description: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class AppState:
    """Container for everything HTTP handlers + the React UI need."""

    def __init__(
        self,
        *,
        bus: EventBus | None = None,
        store: RunStore | None = None,
        guardrail: "InProcessGuardrail | GuardrailHandle | None" = None,
        persistence: "ControlPlanePersistence | None" = None,
    ) -> None:
        # Injectable backends (selected by ``--mq`` / ``--store`` /
        # ``--guardrail`` at the serve boundary). All default to the
        # zero-dependency in-process implementations so ``AppState()``
        # keeps working out-of-the-box. Explicit ``is None`` checks —
        # these objects may define ``__len__`` / ``__bool__`` and read
        # falsy when empty.
        self.bus: EventBus = bus if bus is not None else InProcessEventBus()
        self.store: RunStore = store if store is not None else InMemoryRunStore()
        # Quota guardrail — must be created BEFORE the Orchestrator so it
        # can be passed in. May be in-process or Redis-backed; both
        # satisfy the GuardrailHandle Protocol.
        self.guardrail: "InProcessGuardrail | GuardrailHandle" = (
            guardrail if guardrail is not None else InProcessGuardrail()
        )
        # Control-plane metadata persistence (projects / workflows /
        # inbox). NoOp in memory mode; Postgres when ``--store pg``. The
        # in-memory dicts below remain the live working copy; this mirrors
        # them durably and reloads on boot. See ``api/persistence.py``.
        from agentkit.api.persistence import NoOpControlPlane  # noqa: PLC0415
        self.persistence: "ControlPlanePersistence" = (
            persistence if persistence is not None else NoOpControlPlane()
        )
        # Inbox surface — workflow lifecycle + ext IO + errors
        # are pushed here for the UI to render. Bounded ring
        # buffer; oldest entries evict.
        self.inbox: Inbox = Inbox()
        # External IO manager — pluggable Telegram / IMAP / SMTP / Python
        # adapters that bridge bus topics to the outside world.
        self.external_io: ExternalIOManager = ExternalIOManager(bus=self.bus)
        self.orchestrator: Orchestrator = Orchestrator(
            bus=self.bus, store=self.store, guardrail=self.guardrail,
        )
        # Late-bind orchestrator + mode_getter into the IO manager
        # so external sources can auto-spawn Runs in event-driven mode.
        self.external_io.orchestrator = self.orchestrator
        self.external_io.mode_getter = self._get_mode
        self.external_io.session_run_getter = self._get_session_run
        self.handler_registry: HandlerRegistry = HandlerRegistry()
        self.workers: dict[str, AgentWorker] = {}

        # Workflow inventory (compiled).
        self.ir_by_id: dict[str, WorkflowIR] = {}
        self.plan_by_id: dict[str, RuntimePlan] = {}

        # NEW: editable raw spec (dict) per workflow_id. The UI mutates
        # this to add/remove agents; we recompile + redeploy.
        self.raw_spec_by_id: dict[str, dict[str, Any]] = {}

        # NEW: per-workflow trigger-input schema (the ``payload``
        # field names the user supplies when starting a Run). Persisted
        # so the prompt-builder UI can suggest the right variables to
        # downstream agents instead of hardcoding ``q``. Defaults to
        # ``["q"]`` to match the historical convention. Saved via the
        # workflow_edit router's ``PUT .../start-input`` endpoint.
        self.start_input_fields_by_workflow: dict[str, list[str]] = {}

        # NEW: workflow execution mode — "normal" | "event_driven".
        # Normal = one Run per /api/runs POST, external IO disallowed.
        # Event-driven = external sources auto-spawn Runs forever
        # until the user toggles back to normal (which removes IO).
        self.workflow_modes: dict[str, str] = {}

        # Per-workflow event-driven session run_id. Created on
        # mode→event_driven, cancelled on mode→normal. Every
        # external event in this session shares this run_id so
        # the UI shows ONE long-running activity instead of one
        # Run per message.
        self.event_session_run_by_workflow: dict[str, str] = {}

        # NEW: undo stack for topology edits (add / remove / update
        # agent). Each snapshot is::
        #
        #     {
        #         "spec": deep_copy_of_raw_spec,
        #         "agent_kwargs": {key: agent_constructor_kwargs, ...},
        #     }
        #
        # so we can fully restore both the IR-shaping spec AND the
        # handler-level config (prompts etc) on undo. Capped per
        # workflow to ``MAX_UNDO_HISTORY`` to bound memory.
        self.spec_history_by_workflow: dict[str, list[dict[str, Any]]] = {}

        # NEW: workflow_id -> project_id mapping.
        self.workflow_to_project: dict[str, str] = {}

        # NEW: project inventory. Includes a built-in "default" project.
        self.projects: dict[str, Project] = {
            DEFAULT_PROJECT_ID: Project(
                id=DEFAULT_PROJECT_ID,
                name="Default",
                description="Workflows loaded from --workflows dir at boot.",
            ),
        }

        # Live :class:`Agent` instances keyed by (workflow_id, template_key).
        self.agents_by_key: dict[tuple[str, str], object] = {}

        # LLM gateway — set by the boot sequence.
        self._llm_gateway: object | None = None

        # Per-run event ring buffers.
        self._buffers: dict[str, _PerRunBuffer] = defaultdict(_PerRunBuffer)
        self._tap_sub: Subscriber | None = None
        self._tap_task: asyncio.Task[None] | None = None
        self._started: bool = False

    # ------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        await self.bus.start()
        # Persistent stores (e.g. PostgresRunStore) expose an async
        # ``start`` that opens the pool + migrates the schema. The
        # in-memory store has no lifecycle, so we probe for the method.
        store_start = getattr(self.store, "start", None)
        if callable(store_start):
            await store_start()
        # Redis-backed guardrail opens its connection + verifies
        # connectivity in ``start``; InProcessGuardrail has none.
        guardrail_start = getattr(self.guardrail, "start", None)
        if callable(guardrail_start):
            await guardrail_start()
        await self.persistence.start()
        await self.orchestrator.start()
        await self._start_tap()
        self._started = True
        log.info("api.state.started")

    async def restore_from_persistence(self) -> None:
        """Reload projects / workflows / inbox from durable storage and
        re-deploy the workflows. No-op in memory mode.

        Called by ``serve`` AFTER the LLM gateway is wired (workflow
        deployment needs it). Workflows are deployed with ``persist=False``
        so reloading doesn't immediately re-write what we just read.
        """
        if not getattr(self.persistence, "enabled", False):
            return
        # ── projects ──
        for p in await self.persistence.load_projects():
            self.projects[p["id"] if "id" in p else p["project_id"]] = Project(
                id=p.get("project_id") or p.get("id"),
                name=p["name"],
                description=p.get("description", ""),
                created_at=p.get("created_at") or datetime.now(timezone.utc),
            )
        # ── workflows (re-deploy) ──
        from agentkit.workflow import compile_from_dict  # noqa: PLC0415
        n_wf = 0
        for w in await self.persistence.load_workflows():
            try:
                ir, plan = compile_from_dict(w["raw_spec"])
            except Exception:  # noqa: BLE001
                log.exception("api.restore.compile_failed", workflow_id=w["workflow_id"])
                continue
            self.start_input_fields_by_workflow[ir.id] = w.get("start_input_fields", ["q"])
            # Rebuild each agent's handler from its persisted constructor
            # kwargs and register it BEFORE deploying the worker (so the
            # worker finds an executor for every agent — same path the
            # undo/redeploy flow uses).
            from agentkit.sdk.agent_class import Agent  # noqa: PLC0415
            from agentkit.runtime.executor import _FunctionExecutor  # noqa: PLC0415
            for key, kwargs in (w.get("agent_kwargs") or {}).items():
                try:
                    agent = Agent(template_key=key, **kwargs)
                except Exception:  # noqa: BLE001
                    log.exception(
                        "api.restore.rebuild_agent_failed",
                        workflow_id=ir.id, template_key=key,
                    )
                    continue
                self.handler_registry.register(
                    workflow_id=ir.id, template_key=key,
                    executor=_FunctionExecutor(agent.handler), replace=True,
                )
                self.agents_by_key[(ir.id, key)] = agent
            await self.deploy_workflow(
                ir, plan, raw_spec=w["raw_spec"],
                project_id=w.get("project_id", "default"),
                persist=False,
            )
            mode = w.get("mode", "normal")
            if mode and mode != "normal":
                self.workflow_modes[ir.id] = mode
            n_wf += 1
        # Ensure the always-present default project exists in storage.
        dft = self.projects.get(DEFAULT_PROJECT_ID)
        if dft is not None:
            await self.persistence.upsert_project(
                {"id": dft.id, "name": dft.name, "description": dft.description},
            )
        # ── inbox ──
        self.inbox.restore_many(await self.persistence.load_inbox())
        log.info("api.state.restored", projects=len(self.projects), workflows=n_wf)

    async def stop(self) -> None:
        if not self._started:
            return
        if self._tap_task is not None:
            self._tap_task.cancel()
            try:
                await self._tap_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        if self._tap_sub is not None:
            await self._tap_sub.close()
        for w in list(self.workers.values()):
            try:
                await w.stop()
            except Exception:  # noqa: BLE001
                log.exception("api.state.worker_stop_failed")
        await self.orchestrator.stop()
        await self.external_io.stop_all()
        await self.bus.stop()
        # Tear down persistent backends last (after everything that
        # might still write to them has stopped).
        store_stop = getattr(self.store, "stop", None)
        if callable(store_stop):
            try:
                await store_stop()
            except Exception:  # noqa: BLE001
                log.exception("api.state.store_stop_failed")
        guardrail_stop = getattr(self.guardrail, "stop", None)
        if callable(guardrail_stop):
            try:
                await guardrail_stop()
            except Exception:  # noqa: BLE001
                log.exception("api.state.guardrail_stop_failed")
        try:
            await self.persistence.stop()
        except Exception:  # noqa: BLE001
            log.exception("api.state.persistence_stop_failed")
        self._started = False
        log.info("api.state.stopped")

    # ------------------------------------------------------------
    # LLM gateway accessor
    # ------------------------------------------------------------

    def set_llm_gateway(self, gateway: object) -> None:
        """Set the gateway after boot. Used by the deploy / redeploy paths."""
        self._llm_gateway = gateway

    @property
    def llm_gateway(self) -> object:
        if self._llm_gateway is None:
            raise RuntimeError("LLM gateway not set on AppState")
        return self._llm_gateway

    # ------------------------------------------------------------
    # Workflow registration
    # ------------------------------------------------------------

    async def deploy_workflow(
        self,
        ir: WorkflowIR,
        plan: RuntimePlan,
        *,
        raw_spec: dict[str, Any] | None = None,
        project_id: str = DEFAULT_PROJECT_ID,
        llm_gateway: object | None = None,
        persist: bool = True,
    ) -> None:
        """Wire IR + plan + start a dedicated worker for this workflow.

        ``persist=False`` skips mirroring to durable storage — used when
        re-deploying workflows we just LOADED from persistence on boot.
        """
        if not self._started:
            raise RuntimeError("AppState.start() must be called first")

        if llm_gateway is not None and self._llm_gateway is None:
            self._llm_gateway = llm_gateway
        gw = llm_gateway or self._llm_gateway
        if gw is None:
            raise RuntimeError("no LLM gateway configured")

        self.ir_by_id[ir.id] = ir
        self.plan_by_id[ir.id] = plan
        if raw_spec is not None:
            self.raw_spec_by_id[ir.id] = copy.deepcopy(raw_spec)
        self.workflow_to_project.setdefault(ir.id, project_id)

        await self.orchestrator.deploy(ir)
        worker = AgentWorker(
            plan=plan, bus=self.bus, llm=gw,
            handlers=self.handler_registry,
            guardrail=self.guardrail,   # so Gate-3 enforces run/agent quotas
        )
        await worker.start()
        self.workers[ir.id] = worker
        if persist:
            await self.persist_workflow(ir.id)
        log.info(
            "api.state.workflow_deployed",
            workflow_id=ir.id, agents=list(plan.agents),
            project_id=project_id,
        )

    async def persist_workflow(self, workflow_id: str) -> None:
        """Mirror a workflow's current metadata to durable storage.

        Reads the live in-memory state (raw_spec / project / mode /
        start-input) so any mutation path can call this after editing.
        No-op in memory mode.
        """
        if not getattr(self.persistence, "enabled", False):
            return
        raw_spec = self.raw_spec_by_id.get(workflow_id)
        if raw_spec is None:
            return
        ir = self.ir_by_id.get(workflow_id)
        # Per-agent handler config (prompt / python_script / output_field /
        # llm …) lives on the live Agent instances, NOT in the IR raw_spec.
        # Capture each agent's constructor kwargs so restore can rebuild
        # the handlers without a Python handler module.
        agent_kwargs: dict[str, Any] = {}
        for (wf, key), agent in self.agents_by_key.items():
            if wf != workflow_id:
                continue
            meta = getattr(agent, "meta", None)
            raw = getattr(meta, "raw_kwargs", None)
            if isinstance(raw, dict):
                agent_kwargs[key] = raw
        try:
            await self.persistence.upsert_workflow({
                "workflow_id": workflow_id,
                "project_id": self.workflow_to_project.get(workflow_id, DEFAULT_PROJECT_ID),
                "description": (ir.description if ir else "") or "",
                "raw_spec": raw_spec,
                "agent_kwargs": agent_kwargs,
                "mode": self.workflow_modes.get(workflow_id, "normal"),
                "start_input_fields": self.start_input_fields_by_workflow.get(workflow_id, ["q"]),
            })
        except Exception:  # noqa: BLE001
            log.exception("api.persist_workflow_failed", workflow_id=workflow_id)

    async def undeploy_workflow(self, workflow_id: str) -> None:
        """Tear down a workflow — stops worker, drops IR/plan."""
        worker = self.workers.pop(workflow_id, None)
        if worker is not None:
            try:
                await worker.stop()
            except Exception:  # noqa: BLE001
                log.exception("api.state.worker_stop_failed", workflow_id=workflow_id)
        # Stop the orchestrator's routers (SwitchRouter + TerminalDetector)
        # for this workflow. Without this they keep running — their bus
        # consumers leak (orphaned, still-active consumer groups) for every
        # deleted workflow. (redeploy is fine: orchestrator.deploy tears the
        # old ones down itself.)
        try:
            await self.orchestrator.undeploy(workflow_id)
        except Exception:  # noqa: BLE001
            log.exception("api.state.orch_undeploy_failed", workflow_id=workflow_id)
        # Drop registry entries for this workflow's agents.
        for (wf, key) in list(self.agents_by_key.keys()):
            if wf == workflow_id:
                self.agents_by_key.pop((wf, key), None)
        self.ir_by_id.pop(workflow_id, None)
        self.plan_by_id.pop(workflow_id, None)
        self.raw_spec_by_id.pop(workflow_id, None)
        self.workflow_to_project.pop(workflow_id, None)
        self.start_input_fields_by_workflow.pop(workflow_id, None)
        self.spec_history_by_workflow.pop(workflow_id, None)
        self.workflow_modes.pop(workflow_id, None)
        self.event_session_run_by_workflow.pop(workflow_id, None)
        await self.external_io.remove_all_for_workflow(workflow_id)
        # Drop this workflow's inbox notifications so they don't dangle to
        # a gone workflow (broken links in the UI).
        self.inbox.remove_for_workflow(workflow_id)
        if getattr(self.persistence, "enabled", False):
            try:
                await self.persistence.delete_workflow(workflow_id)
                await self.persistence.delete_inbox_for_workflow(workflow_id)
            except Exception:  # noqa: BLE001
                log.exception("api.persist_delete_workflow_failed", workflow_id=workflow_id)
        log.info("api.state.workflow_undeployed", workflow_id=workflow_id)

    async def redeploy_workflow(self, workflow_id: str) -> tuple[WorkflowIR, RuntimePlan]:
        """Recompile the raw spec and restart the worker.

        Stops the in-flight worker, recompiles ``raw_spec_by_id``, and
        starts a fresh worker. In-flight Runs continue draining on the
        old worker; new Runs use the new IR.
        """
        if workflow_id not in self.raw_spec_by_id:
            raise RuntimeError(
                f"workflow {workflow_id!r} has no raw spec — cannot redeploy",
            )
        spec = self.raw_spec_by_id[workflow_id]
        project_id = self.workflow_to_project.get(workflow_id, DEFAULT_PROJECT_ID)

        # Stop existing worker first so the new one binds the same
        # subscribers cleanly.
        worker = self.workers.pop(workflow_id, None)
        if worker is not None:
            try:
                await worker.stop()
            except Exception:  # noqa: BLE001
                log.exception("api.state.redeploy_worker_stop_failed",
                              workflow_id=workflow_id)

        ir, plan = compile_from_dict(spec)
        self.ir_by_id[workflow_id] = ir
        self.plan_by_id[workflow_id] = plan

        # Re-deploy on the orchestrator (it overwrites prior IR for this id).
        await self.orchestrator.deploy(ir)

        worker = AgentWorker(
            plan=plan, bus=self.bus, llm=self._llm_gateway,
            handlers=self.handler_registry,
            guardrail=self.guardrail,   # so Gate-3 enforces run/agent quotas
        )
        await worker.start()
        self.workers[workflow_id] = worker
        # Spec / mode / guardrail edits all flow through redeploy — mirror
        # the new metadata to durable storage.
        await self.persist_workflow(workflow_id)
        log.info(
            "api.state.workflow_redeployed",
            workflow_id=workflow_id,
            agents=list(plan.agents),
            project_id=project_id,
        )
        return ir, plan

    # ------------------------------------------------------------
    # Project ops
    # ------------------------------------------------------------

    async def create_project(self, name: str, description: str = "") -> Project:
        pid = f"prj_{uuid.uuid4().hex[:10]}"
        proj = Project(id=pid, name=name, description=description)
        self.projects[pid] = proj
        if getattr(self.persistence, "enabled", False):
            await self.persistence.upsert_project(
                {"id": pid, "name": name, "description": description},
            )
        log.info("api.project.created", project_id=pid, name=name)
        return proj

    async def delete_project(self, project_id: str) -> None:
        if project_id == DEFAULT_PROJECT_ID:
            raise RuntimeError("cannot delete the default project")
        if project_id not in self.projects:
            raise KeyError(project_id)
        # Reassign workflows in this project to default.
        for wf_id, p in list(self.workflow_to_project.items()):
            if p == project_id:
                self.workflow_to_project[wf_id] = DEFAULT_PROJECT_ID
                await self.persist_workflow(wf_id)
        self.projects.pop(project_id, None)
        if getattr(self.persistence, "enabled", False):
            await self.persistence.delete_project(project_id)
        log.info("api.project.deleted", project_id=project_id)

    # ------------------------------------------------------------
    # SSE event log
    # ------------------------------------------------------------

    def _get_mode(self, workflow_id: str) -> str:
        """Return the workflow's execution mode (``normal`` default)."""
        return self.workflow_modes.get(workflow_id, "normal")

    def _get_session_run(self, workflow_id: str) -> str:
        """Active event-driven session run_id (empty if none)."""
        return self.event_session_run_by_workflow.get(workflow_id, "")

    async def _start_tap(self) -> None:
        spec = SubscribeSpec(
            topic_pattern=TAP_PATTERN,
            group="grp.api.eventtap",
            starting_position="latest",
        )
        self._tap_sub = await self.bus.subscribe(spec)
        self._tap_task = asyncio.create_task(self._tap_loop())

    async def _tap_loop(self) -> None:
        assert self._tap_sub is not None
        try:
            async for msg in self._tap_sub.messages():
                env = msg.envelope
                # ── Inbox: surface ext.source / ext.sink events ──
                # These are tap-driven (no per-run filter required) so
                # event_driven workflows that share a session run also
                # show in the inbox without having to subscribe to a
                # specific run.
                await self._maybe_push_inbox(env)
                if not env.run_id:
                    continue
                buf = self._buffers[env.run_id]
                async with buf.cond:
                    buf.events.append(env)
                    buf.cond.notify_all()
                # On run completion, async-flush the buffered timeline to
                # durable storage so an archived run can be replayed after
                # the in-memory buffer is gone (events aren't otherwise
                # persisted). The `.completed` marker is published last, so
                # the buffer already holds every event by now.
                if env.topic.startswith("system.run.") and env.topic.endswith(".completed"):
                    asyncio.create_task(self._flush_run_events(env.run_id))
        except asyncio.CancelledError:
            pass

    async def _flush_run_events(self, run_id: str) -> None:
        """Persist a completed run's buffered event timeline (best-effort)."""
        if not getattr(self.persistence, "enabled", False):
            return
        buf = self._buffers.get(run_id)
        if buf is None or not buf.events:
            return
        envs = list(buf.events)
        workflow_id = envs[0].workflow_id if envs else ""
        payload = [e.model_dump(mode="json", by_alias=True) for e in envs]
        try:
            await self.persistence.persist_run_events(run_id, workflow_id, payload)
        except Exception:  # noqa: BLE001
            log.exception("api.persist_run_events_failed", run_id=run_id)

    async def load_persisted_events(self, run_id: str) -> list[Envelope]:
        """Reconstruct a run's event timeline from durable storage — used
        when the in-memory buffer is gone (archived run / after restart)."""
        if not getattr(self.persistence, "enabled", False):
            return []
        try:
            rows = await self.persistence.load_run_events(run_id)
        except Exception:  # noqa: BLE001
            log.exception("api.load_run_events_failed", run_id=run_id)
            return []
        out: list[Envelope] = []
        for d in rows:
            try:
                out.append(Envelope.model_validate(d))
            except Exception:  # noqa: BLE001
                continue
        return out

    async def push_inbox(self, **kwargs: Any) -> None:
        """Push an inbox item AND mirror it to durable storage."""
        item = self.inbox.push(**kwargs)
        if getattr(self.persistence, "enabled", False):
            try:
                await self.persistence.upsert_inbox(item.to_dict())
            except Exception:  # noqa: BLE001
                log.exception("api.persist_inbox_failed")

    async def _maybe_push_inbox(self, env: Any) -> None:
        """Inspect a tap-observed envelope and push a notification if
        it matches one of our inbox-worthy categories. Best-effort —
        any error is swallowed so the tap loop keeps running."""
        try:
            topic = env.topic
            if topic.startswith("system.ext.source.") and topic.endswith(".received"):
                p = dict(env.payload or {})
                await self.push_inbox(
                    workflow_id=env.workflow_id or "external",
                    category="ext_source",
                    title=f"🔌 {p.get('source_name', 'source')} received",
                    body=p.get("preview", "") or f"fields: {p.get('fields', [])}",
                    payload=p,
                )
            elif topic.startswith("system.ext.sink.") and topic.endswith(".delivered"):
                p = dict(env.payload or {})
                ok = bool(p.get("ok", True))
                await self.push_inbox(
                    workflow_id=env.workflow_id or "external",
                    category="ext_sink_ok" if ok else "ext_sink_error",
                    title=(
                        f"📤 {p.get('sink_name', 'sink')} → {p.get('target', '?')}"
                        if ok else
                        f"⚠ {p.get('sink_name', 'sink')} delivery failed"
                    ),
                    body=(p.get("preview") or "") if ok else (p.get("error") or "delivery failed"),
                    payload=p,
                )
            elif topic.startswith("system.run.") and topic.endswith(".completed"):
                p = dict(env.payload or {})
                status = p.get("status", "Succeeded")
                cat = (
                    "run_succeeded" if status == "Succeeded"
                    else "run_cancelled" if status == "Cancelled"
                    else "run_failed"
                )
                # Skip the long-lived event-driven session run — it
                # only "completes" when the user flips back to normal
                # mode, and seeing 1× run_succeeded for the whole
                # session of inbound messages would be misleading.
                if env.run_id == self.event_session_run_by_workflow.get(env.workflow_id):
                    return
                await self.push_inbox(
                    workflow_id=env.workflow_id or "",
                    category=cat,  # type: ignore[arg-type]
                    title=(
                        f"✓ Run succeeded" if status == "Succeeded"
                        else f"✗ Run {status.lower()}"
                    ),
                    body=p.get("reason") or f"run_id: {env.run_id}",
                    payload=p,
                )
            elif topic.startswith("system.run.") and topic.endswith(".error"):
                p = dict(env.payload or {})
                await self.push_inbox(
                    workflow_id=env.workflow_id or "",
                    category="error",
                    title=f"⚠ Agent failure: {p.get('template_key', '?')}",
                    body=p.get("reason", "") or p.get("klass", "error"),
                    payload=p,
                )
        except Exception:  # noqa: BLE001
            log.exception("inbox.push_failed")

    def snapshot_events(self, run_id: str) -> list[Envelope]:
        if run_id not in self._buffers:
            return []
        return list(self._buffers[run_id].events)

    def buffer(self, run_id: str) -> _PerRunBuffer:
        return self._buffers[run_id]
