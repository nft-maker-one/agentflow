"""Workflow lifecycle CRUD — create / delete / add-agent / remove-agent.

These endpoints mutate the **raw workflow spec** stored on
:class:`AppState`, then call ``redeploy_workflow`` to recompile the
IR and restart the worker. In-flight Runs continue on the old worker
until they drain; new Runs use the updated IR.
"""

from __future__ import annotations

import copy
import re
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from agentkit.api.models import WorkflowSummary
from agentkit.api.state import DEFAULT_PROJECT_ID
from agentkit.common.logging import get_logger
from agentkit.models.enums import Role
from agentkit.runtime.executor import _FunctionExecutor
from agentkit.sdk.agent_class import Agent, _compile_python_handler
from agentkit.workflow.errors import CompileError, IRValidationError

log = get_logger(__name__)

# Topic name format. Rationale:
#   - Lowercase keeps Kafka/Redpanda happy (they accept upper but
#     mixed case is a portability minefield).
#   - Allowed: letters, digits, dot, underscore, hyphen.
#   - Must START with [a-z0-9] (no leading punctuation).
TOPIC_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")

# Reserved virtual node names — NOT valid topic names.
RESERVED_NODE_NAMES = frozenset({"__start__", "__end__", "__error__"})

# Reserved suffixes managed by the framework — users can't claim them.
RESERVED_SUFFIXES: tuple[str, ...] = (".dlq",)

router = APIRouter(tags=["workflow-edit"])


def _validate_topology(
    *,
    template_key: str,
    subscribe: list[str],
    publish: list[str],
    other_agents: dict[str, Any],
) -> None:
    """Enforce topic naming + structural rules.

    Raises
    ------
    HTTPException(400)
        On the first rule violation, with a specific message.

    Rules
    -----
    1. Non-empty, no whitespace.
    2. Not a reserved virtual node name (``__start__`` etc.).
    3. No reserved suffix (``.dlq``).
    4. Matches the topic format regex.
    5. No duplicates within a list.
    6. No overlap between subscribe and publish (self-loop).

    Allowed (intentionally not flagged):
    * Multiple agents publishing the same topic (legitimate fanout).
    * Subscribing to a topic no other agent publishes (legitimate
      ingestion point — gets fed by ``__start__`` or external).
    """
    for label, topics in (("subscribe", subscribe), ("publish", publish)):
        seen: set[str] = set()
        for t in topics:
            if not isinstance(t, str) or not t.strip():
                raise HTTPException(
                    status_code=400,
                    detail=f"{label}: empty topic name",
                )
            if any(ch.isspace() for ch in t):
                raise HTTPException(
                    status_code=400,
                    detail=f"{label}: topic {t!r} contains whitespace",
                )
            if t in RESERVED_NODE_NAMES:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"{label}: {t!r} is a reserved virtual node name. "
                        f"Use a regular topic like 'agent.{template_key}.in' "
                        f"and the 'Wire from __start__' / 'Wire to __end__' "
                        f"checkboxes to attach it to virtual nodes."
                    ),
                )
            for suffix in RESERVED_SUFFIXES:
                if t.endswith(suffix):
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"{label}: topic {t!r} ends with reserved "
                            f"suffix {suffix!r}. The framework auto-derives "
                            f"these (e.g. DLQ topics) — pick a different name."
                        ),
                    )
            if not TOPIC_RE.match(t):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"{label}: topic {t!r} must match "
                        f"^[a-z0-9][a-z0-9._-]*$ — lowercase letters, "
                        f"digits, dot, underscore, hyphen only."
                    ),
                )
            if t in seen:
                raise HTTPException(
                    status_code=400,
                    detail=f"{label}: duplicate topic {t!r}",
                )
            seen.add(t)

    sub_set = set(subscribe)
    pub_set = set(publish)
    self_loop = sub_set & pub_set
    if self_loop:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Self-loop forbidden: topic(s) {sorted(self_loop)} appear "
                f"in BOTH subscribe and publish of {template_key!r}. "
                f"This would make the agent re-trigger itself on every "
                f"emit. Use a different output topic."
            ),
        )

    # Inform (don't reject) on multi-publish overlap so the user can
    # decide. Currently we just attach metadata to the response — for
    # MVP we silently allow.  See Doc02 §3 for fanout semantics.
    _ = other_agents  # reserved for future warnings


def _validate_llm_for_prompt(
    *,
    prompt: str | None,
    llm: dict | None,
    python_script: str | None = None,
) -> None:
    """An LLM-driven agent (``prompt`` set) needs both provider and model.

    Skipped entirely when ``python_script`` is set — script mode
    bypasses the LLM call.
    """
    if python_script and python_script.strip():
        return  # script mode: prompt/llm are irrelevant
    if not prompt:
        return
    provider = (llm or {}).get("provider")
    model = (llm or {}).get("model")
    if not provider or not model:
        missing = []
        if not provider:
            missing.append("LLM provider")
        if not model:
            missing.append("LLM model")
        raise HTTPException(
            status_code=400,
            detail=(
                f"Cannot save: prompt template is set but {', '.join(missing)} "
                f"is missing. The default LLM-driven handler invokes "
                f"ctx.llm.chat() and would fail on every event without "
                f"a configured provider+model. Either pick both, or "
                f"clear the prompt to use a pass-through agent."
            ),
        )


def _validate_python_script(*, python_script: str | None) -> None:
    """Compile the script eagerly so syntax / structural errors land
    on the user as HTTP 400 instead of crashing the agent at first
    event. ``_compile_python_handler`` raises ``ValueError`` with a
    user-readable message; we wrap it in HTTPException."""
    if not python_script:
        return
    try:
        _compile_python_handler(python_script)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


def _validate_agent_guardrail(g: dict[str, Any] | None) -> dict[str, int] | None:
    """Sanity-check a per-agent guardrail dict. Returns the cleaned
    dict or None. Raises HTTP 400 on bad shape."""
    if not g:
        return None
    allowed = {"max_tokens_per_call", "max_cycles"}
    extra = set(g) - allowed
    if extra:
        raise HTTPException(
            status_code=400,
            detail=f"agent_guardrail has unknown keys: {sorted(extra)} "
                   f"(allowed: {sorted(allowed)})",
        )
    out: dict[str, int] = {}
    for k, v in g.items():
        if not isinstance(v, int) or v < 1:
            raise HTTPException(
                status_code=400,
                detail=f"agent_guardrail.{k} must be a positive integer, got {v!r}",
            )
        out[k] = v
    return out


# ============================================================
# Transactional redeploy helper
# ============================================================


async def _safe_redeploy(
    state,                                      # type: ignore[no-untyped-def]
    wf_id: str,
    spec_backup: dict[str, Any],
    *,
    operation: str,
):
    """Try to redeploy ``wf_id``; on failure restore ``spec_backup`` and
    redeploy again so the worker doesn't get stuck in a half-applied
    state. The originating exception is converted to an HTTPException
    with the IR validation violations surfaced in the detail message.

    Caller is responsible for any handler-registry / agents_by_key
    cleanup that should accompany the rollback (e.g. add_agent rolls
    back its registry insertion before calling this helper).
    """
    try:
        return await state.redeploy_workflow(wf_id)
    except Exception as exc:
        # Surface as much detail as the compiler gave us.
        violations = getattr(exc, "violations", None) or []
        if isinstance(exc, (CompileError, IRValidationError)) and violations:
            detail = (
                f"{operation}: workflow recompile failed with "
                f"{len(violations)} violation(s):\n  - "
                + "\n  - ".join(violations)
            )
        else:
            detail = f"{operation}: workflow recompile failed: {exc}"

        # Restore the previous spec and try to bring the worker back.
        state.raw_spec_by_id[wf_id] = spec_backup
        try:
            await state.redeploy_workflow(wf_id)
        except Exception:
            log.exception(
                "api.safe_redeploy.rollback_failed",
                workflow_id=wf_id, operation=operation,
            )
            # We can't re-raise here — the user already has an error,
            # and a rollback failure is an internal anomaly to log.
        raise HTTPException(status_code=400, detail=detail) from exc


# ============================================================
# Request / Response models
# ============================================================


class CreateWorkflowRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., description="Globally unique workflow id (e.g. wf_my_pipeline)")
    description: str = ""
    project_id: str = DEFAULT_PROJECT_ID


class AddAgentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    template_key: str = Field(..., min_length=1)
    role: str = "thinking"
    description: str = ""
    subscribe_topics: list[str] = Field(..., min_length=1)
    publish_topics: list[str] = Field(default_factory=list)
    llm: dict[str, Any] | None = None
    prompt: str | None = None
    system_prompt: str | None = None
    python_script: str | None = None
    output_field: str = "result"
    max_retries: int = Field(default=0, ge=0, le=10)
    # If true and there is no edge from __start__ yet, wire one.
    connect_to_start: bool = False
    # If true and there is no edge to __end__ yet, wire one.
    connect_to_end: bool = False
    # Fan-in aggregator gating (only meaningful when subscribe_topics
    # has > 1 entry). threshold = 0 means "all".
    aggregate_threshold: int = Field(default=0, ge=0)
    aggregate_required_topics: list[str] = Field(default_factory=list)
    # Per-agent guardrail caps (None = inherit framework defaults).
    agent_guardrail: dict[str, int] | None = None


class UpdateAgentRequest(BaseModel):
    """Same shape as AddAgentRequest but template_key is in the path
    (and immutable — renaming an agent is not supported, delete+add
    instead)."""

    model_config = ConfigDict(extra="forbid")

    role: str = "thinking"
    description: str = ""
    subscribe_topics: list[str] = Field(..., min_length=1)
    publish_topics: list[str] = Field(default_factory=list)
    llm: dict[str, Any] | None = None
    prompt: str | None = None
    system_prompt: str | None = None
    python_script: str | None = None
    output_field: str = "result"
    max_retries: int = Field(default=0, ge=0, le=10)
    connect_to_start: bool = False
    connect_to_end: bool = False
    aggregate_threshold: int = Field(default=0, ge=0)
    aggregate_required_topics: list[str] = Field(default_factory=list)
    agent_guardrail: dict[str, int] | None = None


class WorkflowModeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["normal", "event_driven"] = "normal"


class WorkflowGuardrailRequest(BaseModel):
    """Body for ``PUT /workflows/{wf_id}/guardrail`` — workflow-level
    run quota caps."""

    model_config = ConfigDict(extra="forbid")

    max_total_tokens: int | None = Field(default=None, ge=1)
    max_cycles_per_run: int | None = Field(default=None, ge=1)


# ============================================================
# Undo history (per workflow_id)
# ============================================================


def _push_history(state, wf_id: str) -> None:  # type: ignore[no-untyped-def]
    """Capture a snapshot of the current spec + Agent constructor
    kwargs into the per-workflow undo stack. Called BEFORE any
    structural mutation (add / remove / update agent).

    The snapshot stores enough state to fully reconstruct the
    workflow at this point: the raw spec for the IR side, plus each
    Agent's ``_meta.raw_kwargs`` for the handler side (prompt /
    output_field / etc). On undo we restore the spec, drop current
    Agent instances, and rebuild every agent from the saved kwargs.
    """
    # Lazy import to keep this module decoupled from state's class.
    from agentkit.api.state import MAX_UNDO_HISTORY

    if wf_id not in state.raw_spec_by_id:
        return  # nothing to snapshot
    history = state.spec_history_by_workflow.setdefault(wf_id, [])
    agent_kwargs: dict[str, dict[str, Any]] = {}
    for (w, key), agent in state.agents_by_key.items():
        if w != wf_id:
            continue
        meta = getattr(agent, "_meta", None)
        raw = getattr(meta, "raw_kwargs", None) if meta else None
        if raw is None:
            continue
        agent_kwargs[key] = copy.deepcopy(raw)
    # Snapshot external I/O configs verbatim (raw, with secrets) so
    # an undo can rebuild the exact source/sink instances.
    ext_configs = copy.deepcopy(
        state.external_io._configs.get(wf_id, []),  # noqa: SLF001
    )
    mode = state.workflow_modes.get(wf_id, "normal")
    history.append({
        "spec": copy.deepcopy(state.raw_spec_by_id[wf_id]),
        "agent_kwargs": agent_kwargs,
        "external_io": ext_configs,
        "mode": mode,
    })
    while len(history) > MAX_UNDO_HISTORY:
        history.pop(0)


@router.post("/workflows/{wf_id}/undo")
async def undo_workflow(wf_id: str, req: Request) -> dict[str, Any]:
    """Restore the most recent topology snapshot.

    Topology mutations (add/remove/update agent) push a snapshot onto
    a per-workflow stack BEFORE applying their changes. This endpoint
    pops the last snapshot and restores everything: the raw spec,
    every Agent's full handler config, and the deployed worker.

    Prompt patches via ``PATCH /agents/{key}`` and trigger-input
    schema saves do NOT push history — they're explicitly outside the
    undo scope per design.
    """
    state = req.app.state.app_state
    if wf_id not in state.raw_spec_by_id:
        raise HTTPException(status_code=404, detail="workflow not found")

    history = state.spec_history_by_workflow.get(wf_id, [])
    if not history:
        raise HTTPException(
            status_code=409,
            detail="Nothing to undo — no topology changes recorded yet.",
        )

    # Pop the most recent snapshot.
    snap = history.pop()

    # Save the *current* state as a forward-rollback target in case
    # the restore + redeploy fails.
    rollback_spec = copy.deepcopy(state.raw_spec_by_id[wf_id])
    rollback_agents: dict[str, dict[str, Any]] = {}
    for (w, key), agent in list(state.agents_by_key.items()):
        if w != wf_id:
            continue
        meta = getattr(agent, "_meta", None)
        raw = getattr(meta, "raw_kwargs", None) if meta else None
        if raw is not None:
            rollback_agents[key] = copy.deepcopy(raw)

    # Apply the snapshot.
    state.raw_spec_by_id[wf_id] = copy.deepcopy(snap["spec"])

    # Drop every current Agent for this workflow.
    keys_to_drop = [k for (w, k) in list(state.agents_by_key) if w == wf_id]
    for k in keys_to_drop:
        state.agents_by_key.pop((wf_id, k), None)
        state.handler_registry._handlers.pop((wf_id, k), None)  # type: ignore[attr-defined]

    # Rebuild every Agent from the snapshot kwargs.
    snap_kwargs: dict[str, dict[str, Any]] = snap.get("agent_kwargs", {})
    rebuilt: list[str] = []
    for key, kwargs in snap_kwargs.items():
        try:
            agent = Agent(template_key=key, **kwargs)
        except Exception:
            log.exception(
                "api.undo.rebuild_agent_failed",
                workflow_id=wf_id, template_key=key,
            )
            continue
        state.handler_registry.register(
            workflow_id=wf_id,
            template_key=key,
            executor=_FunctionExecutor(agent.handler),
            replace=True,
        )
        state.agents_by_key[(wf_id, key)] = agent
        rebuilt.append(key)

    # Restore mode from the snapshot BEFORE rebuilding ext IO so the
    # emit() callback evaluates against the right mode.
    state.workflow_modes[wf_id] = snap.get("mode", "normal")

    # Tear down all current external IO and rebuild from snapshot.
    rollback_ext = copy.deepcopy(
        state.external_io._configs.get(wf_id, []),  # noqa: SLF001
    )
    rollback_mode = state.workflow_modes.get(wf_id, "normal")
    await state.external_io.remove_all_for_workflow(wf_id)
    snap_ext = snap.get("external_io", []) or []
    for cfg in snap_ext:
        try:
            await state.external_io.add(
                wf_id,
                direction=cfg["direction"],
                kind=cfg["kind"],
                name=cfg["name"],
                topic=cfg["topic"],
                config=cfg["config"],
            )
        except Exception:
            log.exception(
                "api.undo.rebuild_ext_io_failed",
                workflow_id=wf_id, name=cfg.get("name"),
            )

    # Redeploy. On failure, restore the pre-undo state.
    try:
        ir, _ = await state.redeploy_workflow(wf_id)
    except Exception as exc:
        # Roll forward: re-apply the state we just popped from.
        # First restore mode + ext IO for the pre-undo configuration.
        state.workflow_modes[wf_id] = rollback_mode
        await state.external_io.remove_all_for_workflow(wf_id)
        for cfg in rollback_ext:
            try:
                await state.external_io.add(
                    wf_id,
                    direction=cfg["direction"], kind=cfg["kind"],
                    name=cfg["name"], topic=cfg["topic"],
                    config=cfg["config"],
                )
            except Exception:
                log.exception("api.undo.rollback_ext_io_failed",
                              workflow_id=wf_id, name=cfg.get("name"))
        log.exception(
            "api.undo.redeploy_failed_rolling_forward",
            workflow_id=wf_id, error=str(exc),
        )
        state.raw_spec_by_id[wf_id] = rollback_spec
        for k in [k for (w, k) in list(state.agents_by_key) if w == wf_id]:
            state.agents_by_key.pop((wf_id, k), None)
            state.handler_registry._handlers.pop((wf_id, k), None)  # type: ignore[attr-defined]
        for key, kwargs in rollback_agents.items():
            agent = Agent(template_key=key, **kwargs)
            state.handler_registry.register(
                workflow_id=wf_id, template_key=key,
                executor=_FunctionExecutor(agent.handler), replace=True,
            )
            state.agents_by_key[(wf_id, key)] = agent
        try:
            await state.redeploy_workflow(wf_id)
        except Exception:
            log.exception("api.undo.rollforward_redeploy_failed", workflow_id=wf_id)
        # Push the popped snapshot back so the user can retry once
        # the underlying issue is fixed.
        history.append(snap)
        raise HTTPException(
            status_code=500,
            detail=f"Undo failed: {exc!s}",
        ) from exc

    log.info(
        "api.workflow.undone",
        workflow_id=wf_id,
        restored_agents=sorted(rebuilt),
        history_depth=len(history),
    )
    return {
        "workflow_id": wf_id,
        "history_depth": len(history),
        "restored_agents": sorted(rebuilt),
        "ir_hash": ir.meta.ir_hash,
    }


# ============================================================
# Workflow CRUD
# ============================================================


@router.post("/workflows", response_model=WorkflowSummary, status_code=201)
async def create_workflow(
    body: CreateWorkflowRequest, req: Request,
) -> WorkflowSummary:
    """Create a brand-new (empty-ish) workflow.

    The IR has no agents yet — only ``__start__`` and ``__end__``,
    which is invalid for compilation. So we add a single placeholder
    pass-through agent to make compile succeed; the user immediately
    deletes / replaces it via the UI.
    """
    state = req.app.state.app_state
    if body.id in state.ir_by_id:
        raise HTTPException(
            status_code=409, detail=f"workflow {body.id!r} already exists",
        )
    if body.project_id not in state.projects:
        raise HTTPException(
            status_code=400, detail=f"unknown project_id {body.project_id!r}",
        )

    placeholder_topic = f"agent.{body.id}.in"
    spec: dict[str, Any] = {
        "id": body.id,
        "version": 1,
        "description": body.description or "(new workflow)",
        "agents": {
            "echo": {
                "role": "thinking",
                "description": "Pass-through echo. Edit me or add more agents.",
                "subscribe": [{"topic": placeholder_topic}],
                "publish": [{"topic": f"agent.{body.id}.out"}],
            },
        },
        "edges": {
            "e_start": {
                "from": "__start__", "to": "echo",
                "via": placeholder_topic,
            },
            "e_end": {
                "from": "echo", "to": "__end__",
                "via": f"agent.{body.id}.out",
            },
        },
    }

    from agentkit.workflow import compile_from_dict   # noqa: PLC0415
    ir, plan = compile_from_dict(spec)
    # Build a real Agent instance so the UI can edit it like any other.
    echo_agent = Agent(
        template_key="echo",
        role="thinking",
        description="Pass-through echo. Edit me or add more agents.",
        subscribe=[placeholder_topic],
        publish=[f"agent.{body.id}.out"],
        # No prompt → default handler runs in pass-through mode
        # (forwards event.payload unchanged to publish[0]).
    )
    state.handler_registry.register(
        workflow_id=ir.id,
        template_key="echo",
        executor=_FunctionExecutor(echo_agent.handler),
        replace=True,
    )
    state.agents_by_key[(ir.id, "echo")] = echo_agent
    await state.deploy_workflow(
        ir, plan, raw_spec=spec, project_id=body.project_id,
    )

    return WorkflowSummary(
        id=ir.id, version=ir.version,
        description=ir.description or "",
        ir_hash=ir.meta.ir_hash,
        n_agents=len(ir.agents),
        n_edges=len(ir.edges),
        project_id=body.project_id,
    )


class FromSpecRequest(BaseModel):
    """Body of ``POST /api/workflows/from-spec`` — accepts a dict that
    matches the IR shape produced by ``WorkflowDef.to_dict()``."""

    model_config = ConfigDict(extra="allow")  # forward-compat


@router.post("/workflows/from-spec", response_model=WorkflowSummary, status_code=201)
async def create_workflow_from_spec(
    body: FromSpecRequest, req: Request,
) -> WorkflowSummary:
    """Deploy a fully-shaped workflow spec (no echo placeholder).

    Used by ``AgentKitClient.deploy()`` so an SDK-built ``WorkflowDef``
    survives the full round-trip without losing agents or edges.
    """
    state = req.app.state.app_state
    spec = body.model_dump(by_alias=True, exclude_none=True)
    wf_id = spec.get("id")
    if not wf_id:
        raise HTTPException(status_code=400, detail="spec missing 'id'")
    if wf_id in state.ir_by_id:
        raise HTTPException(
            status_code=409, detail=f"workflow {wf_id!r} already exists",
        )

    from agentkit.workflow import compile_from_dict   # noqa: PLC0415
    try:
        ir, plan = compile_from_dict(spec)
    except Exception as e:  # noqa: BLE001
        log.exception("api.workflow.from_spec_compile_failed", workflow_id=wf_id)
        raise HTTPException(status_code=400, detail=f"compile failed: {e}") from e

    # Rebuild Agent instances from the spec so live editing /
    # PATCH /agents/{key} works the same way. We propagate every
    # handler-level field the SDK supports so a python_script defined
    # in the SDK class survives the server round-trip.
    for key, adef in (spec.get("agents") or {}).items():
        kwargs: dict[str, Any] = {
            "template_key": key,
            "role": adef.get("role", "thinking"),
            "description": adef.get("description", ""),
            "subscribe": [t["topic"] for t in adef.get("subscribe", [])],
            "publish":   [t["topic"] for t in adef.get("publish", [])],
        }
        for f in (
            "llm", "prompt", "system_prompt", "output_field",
            "max_retries", "python_script", "aggregate", "guardrail",
        ):
            if adef.get(f) is not None:
                kwargs[f] = adef[f]
        agent = Agent(**kwargs)
        state.handler_registry.register(
            workflow_id=ir.id, template_key=key,
            executor=_FunctionExecutor(agent.handler),
            replace=True,
        )
        state.agents_by_key[(ir.id, key)] = agent

    await state.deploy_workflow(ir, plan, raw_spec=spec)
    return WorkflowSummary(
        id=ir.id, version=ir.version,
        description=ir.description or "",
        ir_hash=ir.meta.ir_hash,
        n_agents=len(ir.agents),
        n_edges=len(ir.edges),
        project_id=DEFAULT_PROJECT_ID,
    )


@router.delete("/workflows/{wf_id}", status_code=204)
async def delete_workflow(wf_id: str, req: Request) -> None:
    state = req.app.state.app_state
    if wf_id not in state.ir_by_id:
        raise HTTPException(
            status_code=404, detail=f"workflow {wf_id!r} not found",
        )
    await state.undeploy_workflow(wf_id)


# ============================================================
# Agent CRUD (within an existing workflow)
# ============================================================


@router.post(
    "/workflows/{wf_id}/agents",
    response_model=WorkflowSummary,
    status_code=201,
)
async def add_agent(
    wf_id: str, body: AddAgentRequest, req: Request,
) -> WorkflowSummary:
    """Add a new Agent to an existing workflow and hot-redeploy."""
    state = req.app.state.app_state
    if wf_id not in state.raw_spec_by_id:
        raise HTTPException(
            status_code=404,
            detail=f"workflow {wf_id!r} not found or not editable",
        )
    spec = state.raw_spec_by_id[wf_id]
    if body.template_key in spec["agents"]:
        raise HTTPException(
            status_code=409,
            detail=f"agent {body.template_key!r} already exists",
        )

    # Snapshot for undo BEFORE any mutation. Failed validation below
    # raises before _safe_redeploy fires, so we must push history
    # only AFTER all early checks pass to avoid polluting the stack
    # with no-op snapshots.
    _push_history(state, wf_id)

    # Validate role early so we surface a friendly error instead of a
    # deep ValueError from the IR builder.
    valid_roles = [r.value for r in Role]
    if body.role not in valid_roles:
        raise HTTPException(
            status_code=400,
            detail=f"invalid role {body.role!r} — must be one of {valid_roles}",
        )

    # An LLM-driven agent (prompt set) MUST have a publish topic — the
    # default handler emits its result to ``publish[0]`` and would
    # otherwise raise on every incoming event.
    if body.prompt and not body.publish_topics:
        raise HTTPException(
            status_code=400,
            detail=(
                "publish_topics is required when prompt is set: the "
                "default handler emits its LLM result to publish[0]. "
                "An agent with no publish would compute its output and "
                "drop it on the floor."
            ),
        )

    # Prompt without a configured LLM provider+model would crash at
    # first event — reject up front with a friendly error.
    _validate_llm_for_prompt(
        prompt=body.prompt, llm=body.llm,
        python_script=body.python_script,
    )

    # Eagerly compile the user Python script (if any) so syntax /
    # structural errors land here as HTTP 400, not at first event.
    _validate_python_script(python_script=body.python_script)

    # Topic-naming + structural validation.
    _validate_topology(
        template_key=body.template_key,
        subscribe=body.subscribe_topics,
        publish=body.publish_topics,
        other_agents={
            k: v for k, v in spec["agents"].items() if k != body.template_key
        },
    )

    # Aggregate validation. Required must be a subset of subscribe.
    extra_required = [
        t for t in body.aggregate_required_topics
        if t not in body.subscribe_topics
    ]
    if extra_required:
        raise HTTPException(
            status_code=400,
            detail=(
                f"aggregate.required topics not in subscribe: "
                f"{extra_required}. Required topics must be a subset of "
                f"subscribe topics."
            ),
        )
    if body.aggregate_threshold > len(body.subscribe_topics):
        raise HTTPException(
            status_code=400,
            detail=(
                f"aggregate.threshold ({body.aggregate_threshold}) > "
                f"len(subscribe) ({len(body.subscribe_topics)})."
            ),
        )
    # Aggregator is enabled whenever there are multiple subscribe
    # topics. ``threshold = 0`` means "all subscribe topics" — the
    # runtime reads it as ``len(plan.subscribe_topics)``. This is
    # the natural default for fan-in agents: wait for every upstream
    # to emit before firing the merged-payload handler. Users can
    # opt into per-event dispatch by setting threshold=1.
    use_aggregate = len(body.subscribe_topics) >= 2

    # Snapshot before mutation so we can roll back atomically.
    spec_backup = copy.deepcopy(spec)

    # Mutate the spec.
    agent_dict: dict[str, Any] = {
        "role": body.role,
        "description": body.description,
        "subscribe": [{"topic": t} for t in body.subscribe_topics],
        "publish":   [{"topic": t} for t in body.publish_topics],
    }
    if body.llm:
        agent_dict["llm"] = body.llm
    if use_aggregate:
        agent_dict["aggregate"] = {
            "threshold": body.aggregate_threshold,
            "required":  list(body.aggregate_required_topics),
        }
    cleaned_guardrail = _validate_agent_guardrail(body.agent_guardrail)
    if cleaned_guardrail:
        agent_dict["guardrail"] = cleaned_guardrail
    spec["agents"][body.template_key] = agent_dict

    _auto_wire_edges(spec, body.template_key, body.subscribe_topics, body.publish_topics)

    if body.connect_to_start and body.subscribe_topics:
        conventional = f"agent.{body.template_key}.in"
        start_via = (
            conventional if conventional in body.subscribe_topics
            else body.subscribe_topics[0]
        )
        spec["edges"][f"e_start_{body.template_key}"] = {
            "from": "__start__",
            "to": body.template_key,
            "via": start_via,
        }
    if body.connect_to_end and body.publish_topics:
        spec["edges"][f"e_end_{body.template_key}"] = {
            "from": body.template_key,
            "to": "__end__",
            "via": body.publish_topics[0],
        }
    # IMPORTANT: bridge synthesis runs LAST so it can see the
    # explicit edges (if any) and skip itself when not needed.
    # See docs/bugs.md §"connect_to_start ghost-checked".
    _auto_wire_external_start_edges(
        spec, body.template_key, body.subscribe_topics,
        state=state, workflow_id=wf_id,
    )

    # Build a runtime Agent instance so the handler runs the prompt.
    agent_obj = Agent(
        template_key=body.template_key,
        role=body.role,
        description=body.description,
        subscribe=list(body.subscribe_topics),
        publish=list(body.publish_topics),
        llm=_normalize_llm_field(body.llm),
        prompt=body.prompt,
        system_prompt=body.system_prompt,
        python_script=body.python_script,
        output_field=body.output_field,
        max_retries=body.max_retries,
        aggregate=(
            {
                "threshold": body.aggregate_threshold,
                "required":  list(body.aggregate_required_topics),
            } if use_aggregate else None
        ),
    )
    state.handler_registry.register(
        workflow_id=wf_id,
        template_key=body.template_key,
        executor=_FunctionExecutor(agent_obj.handler),
        replace=True,
    )
    state.agents_by_key[(wf_id, body.template_key)] = agent_obj

    try:
        ir, _ = await _safe_redeploy(
            state, wf_id, spec_backup, operation=f"add agent {body.template_key!r}",
        )
    except HTTPException:
        # _safe_redeploy already restored the spec — also clean up the
        # handler registry / agents_by_key entries we added above.
        state.handler_registry._handlers.pop(  # type: ignore[attr-defined]
            (wf_id, body.template_key), None,
        )
        state.agents_by_key.pop((wf_id, body.template_key), None)
        raise

    return WorkflowSummary(
        id=ir.id, version=ir.version,
        description=ir.description or "",
        ir_hash=ir.meta.ir_hash,
        n_agents=len(ir.agents),
        n_edges=len(ir.edges),
        project_id=state.workflow_to_project.get(wf_id, DEFAULT_PROJECT_ID),
    )


@router.delete(
    "/workflows/{wf_id}/agents/{key}",
    response_model=WorkflowSummary,
)
async def remove_agent(
    wf_id: str, key: str, req: Request,
) -> WorkflowSummary:
    state = req.app.state.app_state
    if wf_id not in state.raw_spec_by_id:
        raise HTTPException(status_code=404, detail="workflow not found")
    spec = state.raw_spec_by_id[wf_id]
    if key not in spec["agents"]:
        raise HTTPException(status_code=404, detail=f"agent {key!r} not found")

    # Snapshot for undo BEFORE any mutation.
    _push_history(state, wf_id)

    # Snapshot before mutation for atomic rollback.
    spec_backup = copy.deepcopy(spec)

    spec["agents"].pop(key, None)
    # Drop edges that reference this agent.
    for ekey, edge in list(spec["edges"].items()):
        # Edges in the raw IR can be direct ({to: str}) or switch ({cases: ...}).
        froms = [edge.get("from")]
        tos: list[str] = []
        if isinstance(edge.get("to"), str):
            tos.append(edge["to"])
        if isinstance(edge.get("to"), list):
            tos.extend(edge["to"])
        for case in (edge.get("cases") or {}).values():
            if isinstance(case, dict) and "to" in case:
                tos.append(case["to"])
        if key in froms or key in tos:
            spec["edges"].pop(ekey, None)

    ir, _ = await _safe_redeploy(
        state, wf_id, spec_backup, operation=f"remove agent {key!r}",
    )

    # Only clean up registries AFTER redeploy succeeded — otherwise
    # the worker rollback wouldn't have access to the handler.
    state.handler_registry._handlers.pop(  # type: ignore[attr-defined]
        (wf_id, key), None,
    )
    state.agents_by_key.pop((wf_id, key), None)

    return WorkflowSummary(
        id=ir.id, version=ir.version,
        description=ir.description or "",
        ir_hash=ir.meta.ir_hash,
        n_agents=len(ir.agents),
        n_edges=len(ir.edges),
        project_id=state.workflow_to_project.get(wf_id, DEFAULT_PROJECT_ID),
    )


# ============================================================
# Full Agent edit — subscribe / publish / role / llm / prompt / aggregate
# ============================================================


@router.put(
    "/workflows/{wf_id}/agents/{key}",
    response_model=WorkflowSummary,
)
async def update_agent(
    wf_id: str, key: str, body: UpdateAgentRequest, req: Request,
) -> WorkflowSummary:
    """Replace ``agents[key]`` (subscribe / publish / LLM / aggregate /
    handler) and hot-redeploy."""
    state = req.app.state.app_state
    if wf_id not in state.raw_spec_by_id:
        raise HTTPException(
            status_code=404,
            detail=f"workflow {wf_id!r} not found",
        )
    spec = state.raw_spec_by_id[wf_id]
    if key not in spec["agents"]:
        raise HTTPException(status_code=404, detail=f"agent {key!r} not found")

    # Snapshot for undo BEFORE any mutation.
    _push_history(state, wf_id)

    valid_roles = [r.value for r in Role]
    if body.role not in valid_roles:
        raise HTTPException(
            status_code=400,
            detail=f"invalid role {body.role!r} — must be one of {valid_roles}",
        )

    if body.prompt and not body.publish_topics:
        raise HTTPException(
            status_code=400,
            detail=(
                "publish_topics is required when prompt is set: the "
                "default handler emits its LLM result to publish[0]."
            ),
        )

    _validate_llm_for_prompt(
        prompt=body.prompt, llm=body.llm,
        python_script=body.python_script,
    )
    _validate_python_script(python_script=body.python_script)

    # Topic-naming + structural validation.
    _validate_topology(
        template_key=key,
        subscribe=body.subscribe_topics,
        publish=body.publish_topics,
        other_agents={k: v for k, v in spec["agents"].items() if k != key},
    )

    extra_required = [
        t for t in body.aggregate_required_topics
        if t not in body.subscribe_topics
    ]
    if extra_required:
        raise HTTPException(
            status_code=400,
            detail=f"aggregate.required topics not in subscribe: {extra_required}",
        )
    if body.aggregate_threshold > len(body.subscribe_topics):
        raise HTTPException(
            status_code=400,
            detail=(
                f"aggregate.threshold ({body.aggregate_threshold}) > "
                f"len(subscribe) ({len(body.subscribe_topics)})."
            ),
        )

    use_aggregate = len(body.subscribe_topics) >= 2

    # Snapshot for rollback.
    spec_backup = copy.deepcopy(spec)

    # Drop edges that reference this agent — they'll be re-wired
    # below based on the new topology.
    for eid, edge in list(spec["edges"].items()):
        froms = [edge.get("from")]
        tos: list[str] = []
        if isinstance(edge.get("to"), str):
            tos.append(edge["to"])
        if isinstance(edge.get("to"), list):
            tos.extend(edge["to"])
        for case in (edge.get("cases") or {}).values():
            if isinstance(case, dict) and "to" in case:
                tos.append(case["to"])
        if key in froms or key in tos:
            spec["edges"].pop(eid, None)

    # Replace the agent dict.
    agent_dict: dict[str, Any] = {
        "role": body.role,
        "description": body.description,
        "subscribe": [{"topic": t} for t in body.subscribe_topics],
        "publish":   [{"topic": t} for t in body.publish_topics],
    }
    if body.llm:
        agent_dict["llm"] = body.llm
    if use_aggregate:
        agent_dict["aggregate"] = {
            "threshold": body.aggregate_threshold,
            "required":  list(body.aggregate_required_topics),
        }
    cleaned_guardrail = _validate_agent_guardrail(body.agent_guardrail)
    if cleaned_guardrail:
        agent_dict["guardrail"] = cleaned_guardrail
    spec["agents"][key] = agent_dict

    # Re-wire via topic intersection.
    _auto_wire_edges(spec, key, body.subscribe_topics, body.publish_topics)
    if body.connect_to_start and body.subscribe_topics:
        conventional = f"agent.{key}.in"
        start_via = (
            conventional if conventional in body.subscribe_topics
            else body.subscribe_topics[0]
        )
        spec["edges"][f"e_start_{key}"] = {
            "from": "__start__", "to": key, "via": start_via,
        }
    if body.connect_to_end and body.publish_topics:
        spec["edges"][f"e_end_{key}"] = {
            "from": key, "to": "__end__", "via": body.publish_topics[0],
        }
    # IMPORTANT: bridge synthesis runs LAST so it can see the
    # explicit edges (if any) and skip itself when not needed.
    # See docs/bugs.md §"connect_to_start ghost-checked".
    _auto_wire_external_start_edges(
        spec, key, body.subscribe_topics,
        state=state, workflow_id=wf_id,
    )

    # Build the fresh Agent instance.
    agent_obj = Agent(
        template_key=key,
        role=body.role,
        description=body.description,
        subscribe=list(body.subscribe_topics),
        publish=list(body.publish_topics),
        llm=_normalize_llm_field(body.llm),
        prompt=body.prompt,
        system_prompt=body.system_prompt,
        python_script=body.python_script,
        output_field=body.output_field,
        max_retries=body.max_retries,
        aggregate=(
            {
                "threshold": body.aggregate_threshold,
                "required":  list(body.aggregate_required_topics),
            } if use_aggregate else None
        ),
    )
    state.handler_registry.register(
        workflow_id=wf_id,
        template_key=key,
        executor=_FunctionExecutor(agent_obj.handler),
        replace=True,
    )
    state.agents_by_key[(wf_id, key)] = agent_obj

    ir, _ = await _safe_redeploy(
        state, wf_id, spec_backup, operation=f"update agent {key!r}",
    )

    return WorkflowSummary(
        id=ir.id, version=ir.version,
        description=ir.description or "",
        ir_hash=ir.meta.ir_hash,
        n_agents=len(ir.agents),
        n_edges=len(ir.edges),
        project_id=state.workflow_to_project.get(wf_id, DEFAULT_PROJECT_ID),
    )


# ============================================================
# Trigger-input schema
# ============================================================


class StartInputSchemaRequest(BaseModel):
    """Body for ``PUT /workflows/{wf_id}/start-input``."""

    model_config = ConfigDict(extra="forbid")

    fields: list[str] = Field(min_length=1)


@router.put("/workflows/{wf_id}/start-input")
async def save_start_input_schema(
    wf_id: str, body: StartInputSchemaRequest, req: Request,
) -> dict[str, Any]:
    """Persist the conventional ``payload`` field names that __start__
    injects for this workflow. The PromptEditor's beginner-mode
    variable picker reads this back so it can suggest the right
    fields to downstream agents (instead of hardcoding ``q``).
    """
    state = req.app.state.app_state
    if wf_id not in state.raw_spec_by_id:
        raise HTTPException(status_code=404, detail="workflow not found")

    seen: set[str] = set()
    for f in body.fields:
        if not isinstance(f, str) or not f.strip():
            raise HTTPException(
                status_code=400,
                detail="field name cannot be empty or whitespace",
            )
        if any(ch.isspace() for ch in f):
            raise HTTPException(
                status_code=400,
                detail=f"field name {f!r} contains whitespace",
            )
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", f):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"field name {f!r} must match ^[a-zA-Z_][a-zA-Z0-9_]*$ "
                    f"so Jinja2 ``payload.<name>`` access works without "
                    f"bracket notation."
                ),
            )
        if f in seen:
            raise HTTPException(
                status_code=400,
                detail=f"duplicate field name {f!r}",
            )
        seen.add(f)

    state.start_input_fields_by_workflow[wf_id] = list(body.fields)
    await state.persist_workflow(wf_id)
    log.info(
        "api.workflow.start_input_saved",
        workflow_id=wf_id, fields=list(body.fields),
    )
    return {"workflow_id": wf_id, "start_input_fields": list(body.fields)}


@router.put("/workflows/{wf_id}/mode")
async def set_workflow_mode(
    wf_id: str, body: WorkflowModeRequest, req: Request,
):  # type: ignore[no-untyped-def]
    """Switch a workflow between normal and event-driven mode.

    Mode flip semantics:
    * ``→ event_driven``: allocate a per-workflow SESSION run_id (one
      run shared by all inbound external events), resume any
      previously-paused source/sink instances.
    * ``→ normal``: pause source/sink instances (configs preserved
      so the UI can show them greyed-out + the user can flip back),
      and mark the session run as Succeeded.
    Sources / sinks are NOT deleted on mode flip — only on explicit
    DELETE.
    """
    state = req.app.state.app_state
    if wf_id not in state.ir_by_id:
        raise HTTPException(status_code=404, detail="workflow not found")

    prior = state.workflow_modes.get(wf_id, "normal")
    # Snapshot for undo BEFORE flipping so the user can revert.
    _push_history(state, wf_id)

    if body.mode == prior:
        # Idempotent — no-op
        return {"workflow_id": wf_id, "mode": body.mode}

    state.workflow_modes[wf_id] = body.mode
    await state.persist_workflow(wf_id)

    if body.mode == "event_driven":
        # Allocate a session Run that lives for the duration of this
        # mode session. Reused for every inbound external envelope.
        run = await state.orchestrator.create_run_for_external(
            workflow_id=wf_id,
            initial_input={},
            source_name="event_session",
        )
        state.event_session_run_by_workflow[wf_id] = run.run_id
        # Resume any previously-paused source/sink instances.
        await state.external_io.resume_for_workflow(wf_id)
        log.info(
            "api.workflow.mode_event_driven_started",
            workflow_id=wf_id, session_run_id=run.run_id,
        )
    else:
        # Pause every IO instance but keep configs.
        await state.external_io.pause_for_workflow(wf_id)
        # Cancel the session run so the UI sees it terminate cleanly.
        sess_run_id = state.event_session_run_by_workflow.pop(wf_id, "")
        if sess_run_id:
            try:
                await state.orchestrator.mark_run_terminal_external(
                    sess_run_id, reason="event_driven_stopped",
                )
            except Exception:  # noqa: BLE001
                log.exception(
                    "api.workflow.session_run_close_failed",
                    workflow_id=wf_id, run_id=sess_run_id,
                )
        log.info("api.workflow.mode_normal_resumed", workflow_id=wf_id)

    return {"workflow_id": wf_id, "mode": body.mode}


@router.put("/workflows/{wf_id}/guardrail", response_model=WorkflowSummary)
async def save_workflow_guardrail(
    wf_id: str, body: WorkflowGuardrailRequest, req: Request,
) -> WorkflowSummary:
    """Set the workflow-level run quota caps. Both fields are optional;
    omit to clear the override and fall back to project / framework
    defaults. Triggers a recompile so the new caps land in the IR."""
    state = req.app.state.app_state
    if wf_id not in state.raw_spec_by_id:
        raise HTTPException(status_code=404, detail="workflow not found")

    _push_history(state, wf_id)
    spec = state.raw_spec_by_id[wf_id]
    spec_backup = copy.deepcopy(spec)

    guardrails = spec.setdefault("guardrails", {})
    per_run: dict[str, int] = {}
    if body.max_total_tokens is not None:
        per_run["max_total_tokens"] = body.max_total_tokens
    if body.max_cycles_per_run is not None:
        per_run["max_cycles_per_run"] = body.max_cycles_per_run
    if per_run:
        guardrails["per_run"] = per_run
    else:
        # Both fields omitted → clear the override entirely so we
        # fall back to project / framework defaults.
        guardrails.pop("per_run", None)
        if not guardrails:
            spec.pop("guardrails", None)

    ir, _ = await _safe_redeploy(
        state, wf_id, spec_backup, operation="save workflow guardrail",
    )

    log.info(
        "api.workflow.guardrail_saved",
        workflow_id=wf_id, per_run=per_run or None,
    )
    return WorkflowSummary(
        id=ir.id,
        version=ir.version,
        description=ir.description or "",
        ir_hash=ir.meta.ir_hash,
        n_agents=len(ir.agents),
        n_edges=len(ir.edges),
        project_id=state.workflow_to_project.get(wf_id, DEFAULT_PROJECT_ID),
    )


# ============================================================
# Helpers
# ============================================================


def _normalize_llm_field(llm: dict[str, Any] | None):  # type: ignore[no-untyped-def]
    """Convert UI-supplied dict into a string Agent() accepts."""
    if not llm:
        return None
    provider = llm.get("provider")
    model = llm.get("model")
    if provider and model:
        return f"{provider}/{model}"
    return llm  # already a binding-shaped dict


def _auto_wire_edges(
    spec: dict[str, Any],
    new_key: str,
    subscribe_topics: list[str],
    publish_topics: list[str],
) -> None:
    """Add edges based on topic overlap with existing agents.

    * For each ``subscribe_topic`` of the new agent: if any *other*
      agent in ``spec`` publishes that topic, add an edge from that
      agent to the new one.
    * Symmetric for ``publish_topics`` — connects new agent → others.

    This makes the new agent immediately reachable in the graph,
    which is required for compile-time validation.
    """
    agents = spec.get("agents", {})
    edges = spec.setdefault("edges", {})

    def _topics(side: str, agent_def: dict[str, Any]) -> set[str]:
        return {t["topic"] for t in agent_def.get(side, []) if "topic" in t}

    for sub_topic in subscribe_topics:
        for other_key, other_def in agents.items():
            if other_key == new_key:
                continue
            if sub_topic in _topics("publish", other_def):
                eid = f"e_{other_key}_to_{new_key}"
                if eid not in edges:
                    edges[eid] = {
                        "from": other_key, "to": new_key, "via": sub_topic,
                    }

    for pub_topic in publish_topics:
        for other_key, other_def in agents.items():
            if other_key == new_key:
                continue
            if pub_topic in _topics("subscribe", other_def):
                eid = f"e_{new_key}_to_{other_key}"
                if eid not in edges:
                    edges[eid] = {
                        "from": new_key, "to": other_key, "via": pub_topic,
                    }


def _auto_wire_external_start_edges(
    spec: dict[str, Any],
    new_key: str,
    subscribe_topics: list[str],
    *,
    state: Any,
    workflow_id: str,
) -> None:
    """Synthesise the ``__start__ → agent`` IR-bridge edge **iff**
    needed: agent has at least one subscribe topic fed by a registered
    external source AND no explicit ``__start__ → agent`` edge already
    exists.

    Background — external sources are intentionally outside the IR
    (they're hot-pluggable, no recompile on add/remove). But the IR's
    reachability validator demands every agent be reachable from
    ``__start__``. Without this helper, an agent whose only subscribe
    topic is an external source's publish topic would be rejected as
    unreachable.

    Idempotency rules — every call is responsible for **first removing**
    any prior synthetic edges from this same agent, then re-creating
    only what's still warranted by the current spec. This prevents:

    1. Stale synthetic edges hanging around after the user removes the
       triggering external source.
    2. The frontend mis-detecting them as user-initiated start wiring
       (see bugs.md §"connect_to_start ghost-checked for ext-source
       agents") — which would otherwise force ``connectStart=true`` on
       Edit modal open and refuse to honour user un-checking.
    3. Duplicate synthetic edges accumulating across edits.

    See bugs.md §"connect_to_start ghost-checked for ext-source agents"
    for the full incident write-up.
    """
    edges = spec.setdefault("edges", {})

    # Step 1 — wipe every prior synthetic edge for this agent.
    for eid in list(edges.keys()):
        edge = edges.get(eid) or {}
        if (
            eid.startswith("e_ext_start_")
            and edge.get("to") == new_key
            and edge.get("from") == "__start__"
        ):
            edges.pop(eid, None)

    if not subscribe_topics:
        return

    # Step 2 — early-exit if there is already an explicit
    # ``__start__ → agent`` edge from the user. The agent is reachable
    # without our help; injecting another edge would only confuse the
    # frontend's ``connectStart`` checkbox detection.
    for edge in edges.values():
        if (
            edge.get("from") == "__start__"
            and edge.get("to") == new_key
        ):
            return

    # Step 3 — build the set of external source topics this workflow
    # currently has registered.
    ext_topics: set[str] = set()
    try:
        ext_view = state.external_io.list_for_workflow(workflow_id)
    except Exception:  # noqa: BLE001
        return
    for s in ext_view.get("sources", []):
        ext_topics.add(s["topic"])
    if not ext_topics:
        return

    # Step 4 — synthesise a bridge edge per matching subscribe topic.
    for sub_topic in subscribe_topics:
        if sub_topic not in ext_topics:
            continue
        eid = f"e_ext_start_{new_key}_{_safe_eid_suffix(sub_topic)}"
        edges[eid] = {
            "from": "__start__", "to": new_key, "via": sub_topic,
        }


def _safe_eid_suffix(topic: str) -> str:
    """Render a topic as a stable, alnum-only edge-id suffix."""
    return re.sub(r"[^a-zA-Z0-9]+", "_", topic).strip("_")

