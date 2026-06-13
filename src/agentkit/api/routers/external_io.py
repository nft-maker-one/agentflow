"""HTTP endpoints for external I/O management.

* ``GET    /external/kinds``                            — list available adapter kinds
* ``GET    /workflows/{wf_id}/external``                — list configured sources + sinks
* ``POST   /workflows/{wf_id}/external/sources``        — add (upsert) a source
* ``POST   /workflows/{wf_id}/external/sinks``          — add (upsert) a sink
* ``DELETE /workflows/{wf_id}/external/sources/{name}`` — remove a source
* ``DELETE /workflows/{wf_id}/external/sinks/{name}``   — remove a sink

Naming + upsert semantics: ``name`` is the user-supplied identifier,
unique per (workflow, direction).  POSTing the same name replaces the
prior config (after stopping the old instance), so the UI can offer a
clean "Edit" without juggling delete-then-create.
"""

from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from agentkit.common.logging import get_logger
from agentkit.external_io.registry import KIND_REGISTRY, list_kinds, lookup

log = get_logger(__name__)

router = APIRouter(tags=["external-io"])

NAME_RE = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9._-]*$")


class AddSourceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=64)
    kind: str = Field(..., min_length=1)
    topic: str = Field(..., min_length=1)
    config: dict[str, Any] = Field(default_factory=dict)


class AddSinkRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=64)
    kind: str = Field(..., min_length=1)
    topic: str = Field(..., min_length=1)
    config: dict[str, Any] = Field(default_factory=dict)


# --- queries -------------------------------------------------------------

@router.get("/external/kinds")
async def get_kinds():  # type: ignore[no-untyped-def]
    """List every registered adapter kind + its config schema.

    Used by the UI to render the dynamic form when the user picks a
    kind from the dropdown.
    """
    return [
        {
            "kind": m.kind,
            "direction": m.direction,
            "label": m.label,
            "description": m.description,
            "fields": m.fields,
        }
        for m in list_kinds()
    ]


@router.get("/workflows/{wf_id}/external")
async def list_external(wf_id: str, req: Request):  # type: ignore[no-untyped-def]
    state = req.app.state.app_state
    if wf_id not in state.ir_by_id:
        raise HTTPException(status_code=404, detail="workflow not found")
    return state.external_io.list_for_workflow(wf_id)


# --- mutations -----------------------------------------------------------

@router.post("/workflows/{wf_id}/external/sources")
async def add_source(wf_id: str, body: AddSourceRequest, req: Request):  # type: ignore[no-untyped-def]
    state = req.app.state.app_state
    if wf_id not in state.ir_by_id:
        raise HTTPException(status_code=404, detail="workflow not found")
    _ensure_event_driven(state, wf_id)
    _validate_name(body.name)
    _ensure_kind(body.kind, "source")
    try:
        await state.external_io.add(
            wf_id,
            direction="source", kind=body.kind, name=body.name,
            topic=body.topic, config=body.config,
        )
    except Exception as e:  # noqa: BLE001
        log.exception("api.ext.add_source_failed", workflow_id=wf_id, name=body.name)
        raise HTTPException(status_code=400, detail=str(e)) from e
    return state.external_io.list_for_workflow(wf_id)


@router.post("/workflows/{wf_id}/external/sinks")
async def add_sink(wf_id: str, body: AddSinkRequest, req: Request):  # type: ignore[no-untyped-def]
    state = req.app.state.app_state
    if wf_id not in state.ir_by_id:
        raise HTTPException(status_code=404, detail="workflow not found")
    _ensure_event_driven(state, wf_id)
    _validate_name(body.name)
    _ensure_kind(body.kind, "sink")
    try:
        await state.external_io.add(
            wf_id,
            direction="sink", kind=body.kind, name=body.name,
            topic=body.topic, config=body.config,
        )
    except Exception as e:  # noqa: BLE001
        log.exception("api.ext.add_sink_failed", workflow_id=wf_id, name=body.name)
        raise HTTPException(status_code=400, detail=str(e)) from e
    return state.external_io.list_for_workflow(wf_id)


@router.delete("/workflows/{wf_id}/external/sources/{name}", status_code=204)
async def remove_source(wf_id: str, name: str, req: Request):  # type: ignore[no-untyped-def]
    state = req.app.state.app_state
    _ensure_event_driven(state, wf_id)
    removed = await state.external_io.remove(wf_id, direction="source", name=name)
    if not removed:
        raise HTTPException(status_code=404, detail="source not found")


@router.delete("/workflows/{wf_id}/external/sinks/{name}", status_code=204)
async def remove_sink(wf_id: str, name: str, req: Request):  # type: ignore[no-untyped-def]
    state = req.app.state.app_state
    _ensure_event_driven(state, wf_id)
    removed = await state.external_io.remove(wf_id, direction="sink", name=name)
    if not removed:
        raise HTTPException(status_code=404, detail="sink not found")


# --- helpers -------------------------------------------------------------

def _validate_name(name: str) -> None:
    if not NAME_RE.match(name):
        raise HTTPException(
            status_code=400,
            detail=f"name must match {NAME_RE.pattern!r} (got {name!r})",
        )


def _ensure_kind(kind: str, direction: str) -> None:
    try:
        lookup(kind, direction)
    except KeyError:
        kinds = sorted({m.kind for (_, m) in KIND_REGISTRY.values() if m.direction == direction})
        raise HTTPException(
            status_code=400,
            detail=f"unknown {direction} kind {kind!r}. Available: {kinds}",
        ) from None


def _ensure_event_driven(state, wf_id: str) -> None:
    """Refuse external IO mutations unless the workflow is in
    event-driven mode. Normal-mode workflows must run via POST /api/runs
    only — surfacing this as a 409 keeps the contract obvious to
    frontend / scripts."""
    mode = state.workflow_modes.get(wf_id, "normal")
    if mode != "event_driven":
        raise HTTPException(
            status_code=409,
            detail=(
                f"workflow {wf_id!r} is in mode {mode!r}; "
                f"switch it to 'event_driven' before adding external IO."
            ),
        )
