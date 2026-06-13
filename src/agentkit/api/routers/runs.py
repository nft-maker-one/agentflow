"""Run lifecycle + history endpoints."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response

from agentkit.api.models import (
    BranchEventOut,
    CreateRunRequest,
    EventOut,
    RunDetail,
    RunSummary,
)
from agentkit.orchestrator.errors import RunNotFound, UnknownWorkflow

router = APIRouter(prefix="/runs", tags=["runs"])


@router.post("", response_model=RunSummary, status_code=201)
async def create_run(req: Request, body: CreateRunRequest) -> RunSummary:
    state = req.app.state.app_state
    try:
        run = await state.orchestrator.create_run(
            workflow_id=body.workflow_id, input=body.input,
        )
    except UnknownWorkflow as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return _to_summary(run)


@router.get("", response_model=list[RunSummary])
async def list_runs(
    req: Request,
    workflow_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> list[RunSummary]:
    """List runs (most recent first) — works against any RunStore backend."""
    state = req.app.state.app_state

    # Use the store's public query (in-memory filters its dict; Postgres
    # pushes WHERE/ORDER/LIMIT into SQL) so this endpoint is backend-
    # agnostic — no reaching into store internals.
    all_runs = await state.store.list_recent(
        workflow_id=workflow_id, status=status, limit=limit,
    )
    return [_to_summary(r) for r in all_runs]


@router.get("/{run_id}", response_model=RunDetail)
async def get_run(run_id: str, req: Request) -> RunDetail:
    state = req.app.state.app_state
    try:
        run = await state.orchestrator.get_run(run_id)
    except RunNotFound as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return _to_detail(run)


@router.post("/{run_id}/cancel", response_model=RunSummary)
async def cancel_run(
    run_id: str, req: Request, reason: str = "user_cancel",
) -> RunSummary:
    state = req.app.state.app_state
    try:
        run = await state.orchestrator.cancel_run(run_id, reason=reason)
    except RunNotFound as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return _to_summary(run)


@router.get("/{run_id}/export")
async def export_run(
    run_id: str, req: Request,
    format: str = Query(default="json", pattern="^(json|md)$"),
) -> Response:
    """Export a run + all its envelopes as a downloadable file.

    Two formats:
      - ``json`` — machine-readable: RunDetail + ordered event list.
      - ``md``   — human-readable Markdown timeline.

    The events come from the same in-memory ring buffer that backs
    the SSE stream, so a freshly-completed run is fully exportable
    immediately. Older runs may have had their buffer evicted; in
    that case ``events`` is empty (the run summary is still returned).
    """
    state = req.app.state.app_state
    try:
        run = await state.orchestrator.get_run(run_id)
    except RunNotFound as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    detail = _to_detail(run)
    events: list[EventOut] = []
    for env in state.snapshot_events(run_id):
        events.append(EventOut(
            event_id=env.event_id,
            topic=env.topic,
            payload=dict(env.payload or {}),
            from_template=(
                env.from_.role if env.from_ and env.from_.role else None
            ),
            from_agent=env.from_.agent_id if env.from_ else None,
            causation_id=env.causation_id,
            created_at=env.ts,
        ))

    exported_at = datetime.now(timezone.utc).isoformat()

    if format == "json":
        body = {
            "run":         detail.model_dump(mode="json"),
            "events":      [e.model_dump(mode="json") for e in events],
            "exported_at": exported_at,
        }
        content = json.dumps(body, indent=2, ensure_ascii=False)
        return Response(
            content=content,
            media_type="application/json",
            headers={
                "Content-Disposition":
                    f'attachment; filename="run_{run_id}.json"',
            },
        )

    # Markdown
    md = _format_run_markdown(detail, events, exported_at)
    return Response(
        content=md,
        media_type="text/markdown; charset=utf-8",
        headers={
            "Content-Disposition":
                f'attachment; filename="run_{run_id}.md"',
        },
    )


# ============================================================
# Internals — Run → response model
# ============================================================


def _to_summary(run) -> RunSummary:  # type: ignore[no-untyped-def]
    return RunSummary(
        run_id=run.run_id,
        workflow_id=run.workflow_id,
        status=run.status.value,
        started_at=run.started_at,
        ended_at=run.ended_at,
        failure_reason=run.failure_reason,
        trace_id=run.trace_id,
    )


def _snapshot_graph(snapshot: dict | None):  # type: ignore[no-untyped-def]
    """Build (nodes, edges, agents) from a run's stored workflow_snapshot
    (a serialized WorkflowIR), so the run view shows the topology AS RUN."""
    if not snapshot:
        return [], [], []
    from agentkit.api.models import AgentSummary  # noqa: PLC0415
    from agentkit.api.routers.workflows import _ir_to_graph  # noqa: PLC0415
    from agentkit.workflow.ir import WorkflowIR  # noqa: PLC0415
    try:
        ir = WorkflowIR.model_validate(snapshot)
    except Exception:  # noqa: BLE001
        return [], [], []
    nodes, edges = _ir_to_graph(ir)
    agents = [
        AgentSummary(
            template_key=key,
            role=str(t.role),
            description=t.description or "",
            subscribe=[s.topic for s in t.subscribe],
            publish=[p.topic for p in t.publish],
            llm=t.llm.model_dump() if t.llm else None,
            output_field=None,
            agent_guardrail=t.guardrail.model_dump() if t.guardrail else None,
            aggregate=t.aggregate.model_dump() if t.aggregate else None,
        )
        for key, t in ir.agents.items()
    ]
    return nodes, edges, agents


def _to_detail(run) -> RunDetail:  # type: ignore[no-untyped-def]
    s = _to_summary(run)
    last_node: str | None = None
    branch_log = run.cursor.branch_log if run.cursor else []
    if branch_log:
        last_node = branch_log[-1].chosen
    snap_nodes, snap_edges, snap_agents = _snapshot_graph(
        getattr(run, "workflow_snapshot", None),
    )
    return RunDetail(
        **s.model_dump(),
        input=dict(run.input or {}),
        output=run.output,
        current_node=last_node,
        branch_log=[
            BranchEventOut(
                edge_id=b.edge_id,
                chosen=b.chosen,
                by=b.by,
                reason=b.reason,
            )
            for b in branch_log
        ],
        snapshot_nodes=snap_nodes,
        snapshot_edges=snap_edges,
        snapshot_agents=snap_agents,
    )


def _format_run_markdown(
    run: RunDetail, events: list[EventOut], exported_at: str,
) -> str:
    """Render a run + its event timeline as Markdown."""
    lines: list[str] = []
    lines.append(f"# Run `{run.run_id}`")
    lines.append("")
    lines.append(f"- **Workflow**: `{run.workflow_id}`")
    lines.append(f"- **Status**: {run.status}")
    if run.trace_id:
        lines.append(f"- **Trace ID**: `{run.trace_id}`")
    if run.started_at:
        lines.append(f"- **Started**: {run.started_at.isoformat()}")
    if run.ended_at:
        lines.append(f"- **Ended**: {run.ended_at.isoformat()}")
    if run.started_at and run.ended_at:
        dur = (run.ended_at - run.started_at).total_seconds()
        lines.append(f"- **Duration**: {dur:.2f}s")
    if run.failure_reason:
        lines.append(f"- **Failure reason**: {run.failure_reason}")
    if run.current_node:
        lines.append(f"- **Last node**: `{run.current_node}`")
    lines.append(f"- **Exported at**: {exported_at}")
    lines.append("")

    lines.append("## Input")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(run.input, indent=2, ensure_ascii=False))
    lines.append("```")
    lines.append("")

    if run.output is not None:
        lines.append("## Output")
        lines.append("")
        lines.append("```json")
        lines.append(json.dumps(run.output, indent=2, ensure_ascii=False))
        lines.append("```")
        lines.append("")

    if run.branch_log:
        lines.append("## Branch decisions")
        lines.append("")
        lines.append("| edge | chosen | decided by | reason |")
        lines.append("|------|--------|------------|--------|")
        for b in run.branch_log:
            lines.append(
                f"| `{b.edge_id}` | `{b.chosen}` | {b.by} | "
                f"{b.reason or ''} |"
            )
        lines.append("")

    lines.append(f"## Event timeline ({len(events)} events)")
    lines.append("")
    if not events:
        lines.append("_No events captured (the run's event buffer may have been evicted)._")
        lines.append("")
    for i, ev in enumerate(events, 1):
        ts = ev.created_at.isoformat() if ev.created_at else ""
        lines.append(f"### {i}. `{ev.topic}`")
        lines.append("")
        meta_bits = [f"_{ts}_"] if ts else []
        if ev.from_template:
            meta_bits.append(f"emitted by **{ev.from_template}**")
        if ev.from_agent:
            meta_bits.append(f"agent `{ev.from_agent}`")
        if ev.causation_id:
            meta_bits.append(f"causation `{ev.causation_id}`")
        if meta_bits:
            lines.append(" · ".join(meta_bits))
            lines.append("")
        lines.append("```json")
        lines.append(json.dumps(ev.payload, indent=2, ensure_ascii=False))
        lines.append("```")
        lines.append("")

    return "\n".join(lines)
