"""Live Agent configuration edits.

Currently supports mutating the **handler config** of class-based
:class:`Agent` instances at runtime — prompt / system_prompt /
max_retries / etc. The Agent's IR template (subscribe / publish
topics, role, etc.) is **not** mutable here because that would
require recompiling the workflow IR and re-deploying it.

Endpoints:

* ``PATCH /api/workflows/{wf_id}/agents/{key}`` — update prompt etc.
* ``GET   /api/workflows/{wf_id}/agents/{key}/config`` — read current config
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict

from agentkit.common.logging import get_logger
from agentkit.sdk.agent_class import _JINJA_ENV, Agent, _compile_python_handler

log = get_logger(__name__)

# Note: this router is mounted under /api so the actual path is
# /api/workflows/{wf}/agents/{key}.
router = APIRouter(tags=["agent-edit"])


class AgentConfig(BaseModel):
    """The set of fields a UI can read for a live Agent instance."""

    model_config = ConfigDict(extra="forbid")

    template_key: str
    prompt: str | None = None
    system_prompt: str | None = None
    output_field: str = "result"
    max_retries: int = 0
    retry_backoff_s: float = 0.5
    fallback_response: dict[str, Any] | None = None
    json_output: bool = False
    json_unwrap: bool = False
    python_script: str | None = None


class AgentConfigPatch(BaseModel):
    """The set of fields a UI is allowed to mutate."""

    model_config = ConfigDict(extra="forbid")

    prompt: str | None = None
    system_prompt: str | None = None
    output_field: str | None = None
    max_retries: int | None = None
    retry_backoff_s: float | None = None
    fallback_response: dict[str, Any] | None = None
    json_output: bool | None = None
    json_unwrap: bool | None = None
    python_script: str | None = None


# -----------------------------------------------------------------------


@router.get(
    "/workflows/{wf_id}/agents/{key}/config",
    response_model=AgentConfig,
)
async def get_agent_config(
    wf_id: str, key: str, req: Request,
) -> AgentConfig:
    state = req.app.state.app_state
    agent = _find_agent_or_404(state, wf_id, key)
    cfg = agent._handler_cfg  # type: ignore[attr-defined]
    return AgentConfig(
        template_key=key,
        prompt=cfg.get("prompt"),
        system_prompt=cfg.get("system_prompt"),
        output_field=cfg.get("output_field", "result"),
        max_retries=cfg.get("max_retries", 0),
        retry_backoff_s=cfg.get("retry_backoff_s", 0.5),
        fallback_response=cfg.get("fallback_response"),
        json_output=cfg.get("json_output", False),
        json_unwrap=cfg.get("json_unwrap", False),
        python_script=cfg.get("python_script"),
    )


@router.patch(
    "/workflows/{wf_id}/agents/{key}",
    response_model=AgentConfig,
)
async def patch_agent(
    wf_id: str, key: str, body: AgentConfigPatch, req: Request,
) -> AgentConfig:
    """Mutate an Agent instance's handler config at runtime.

    Only fields explicitly provided in the request body are touched.
    The mutation takes effect on the **next** event the Agent
    consumes — there is no draining of in-flight events.
    """
    state = req.app.state.app_state
    agent = _find_agent_or_404(state, wf_id, key)
    cfg = agent._handler_cfg  # type: ignore[attr-defined]

    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="no fields to update")

    # If python_script is being patched, compile it first so syntax
    # errors fail fast (HTTP 400) and we update the cached compiled
    # handler atomically with the cfg.
    if "python_script" in updates:
        new_script = updates["python_script"] or ""
        if new_script.strip():
            try:
                fn = _compile_python_handler(new_script)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e)) from e
            agent._compiled_python_handler = fn  # type: ignore[attr-defined]
        else:
            # Clearing the script — fall back to prompt/pass-through mode.
            agent._compiled_python_handler = None  # type: ignore[attr-defined]
            updates["python_script"] = None

    # Recompile the cached Jinja template when the prompt changes — the
    # handler renders `agent._compiled_prompt`, NOT `cfg["prompt"]`, so
    # updating only the cfg left the runtime rendering the OLD template
    # (the editor showed the new prompt while runs still failed on the
    # old `{{ payload.<oldfield> }}`).
    if "prompt" in updates:
        new_prompt = updates["prompt"]
        agent._compiled_prompt = (  # type: ignore[attr-defined]
            _JINJA_ENV.from_string(new_prompt) if new_prompt else None
        )

    for k, v in updates.items():
        cfg[k] = v
    log.info(
        "api.agent.config_patched",
        workflow_id=wf_id, template_key=key,
        fields=sorted(updates.keys()),
    )

    return await get_agent_config(wf_id, key, req)


# -----------------------------------------------------------------------


def _find_agent_or_404(state, wf_id: str, key: str) -> Agent:  # type: ignore[no-untyped-def]
    if wf_id not in state.ir_by_id:
        raise HTTPException(status_code=404, detail=f"workflow {wf_id!r} not found")
    agent = state.agents_by_key.get((wf_id, key))
    if agent is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Agent {key!r} in workflow {wf_id!r} is not editable: "
                "either it doesn't exist, or it's not a class-based Agent "
                "instance (only Agent subclasses / direct instantiations are "
                "live-editable)."
            ),
        )
    if not isinstance(agent, Agent):
        raise HTTPException(
            status_code=400,
            detail="Agent is not a class-based instance — not live-editable.",
        )
    return agent
