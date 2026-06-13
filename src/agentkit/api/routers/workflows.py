"""Workflow inventory + DAG endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from agentkit.api.models import (
    AgentSummary,
    GraphEdge,
    GraphNode,
    WorkflowDetail,
    WorkflowSummary,
)
from agentkit.workflow.ir import (
    END_NODE,
    ERROR_NODE,
    START_NODE,
    EdgeBranch,
    EdgeSpec,
    WorkflowIR,
)

router = APIRouter(prefix="/workflows", tags=["workflows"])


@router.get("", response_model=list[WorkflowSummary])
async def list_workflows(req: Request) -> list[WorkflowSummary]:
    """List every workflow currently deployed in this process."""
    state = req.app.state.app_state
    return [
        WorkflowSummary(
            id=ir.id,
            version=ir.version,
            description=ir.description or "",
            ir_hash=ir.meta.ir_hash,
            n_agents=len(ir.agents),
            n_edges=len(ir.edges),
            project_id=state.workflow_to_project.get(ir.id, "default"),
        )
        for ir in state.ir_by_id.values()
    ]


@router.get("/{wf_id}", response_model=WorkflowDetail)
async def get_workflow(wf_id: str, req: Request) -> WorkflowDetail:
    state = req.app.state.app_state
    ir = state.ir_by_id.get(wf_id)
    if ir is None:
        raise HTTPException(status_code=404, detail=f"workflow {wf_id!r} not found")

    agents = []
    for key, tmpl in ir.agents.items():
        # Pull output_field from the live Agent instance if available.
        live = state.agents_by_key.get((wf_id, key))
        output_field: str | None = None
        if live is not None and hasattr(live, "_handler_cfg"):
            output_field = getattr(live, "_handler_cfg", {}).get("output_field")
        agent_guardrail: dict[str, int] | None = None
        if tmpl.guardrail is not None:
            agent_guardrail = tmpl.guardrail.model_dump()
        aggregate: dict | None = None
        if tmpl.aggregate is not None:
            aggregate = tmpl.aggregate.model_dump()
        agents.append(AgentSummary(
            template_key=key,
            role=str(tmpl.role),
            description=tmpl.description or "",
            subscribe=[s.topic for s in tmpl.subscribe],
            publish=[p.topic for p in tmpl.publish],
            llm=tmpl.llm.model_dump() if tmpl.llm else None,
            output_field=output_field,
            agent_guardrail=agent_guardrail,
            aggregate=aggregate,
        ))

    nodes, edges = _ir_to_graph(ir)
    workflow_guardrail: dict[str, int] | None = None
    if ir.guardrails is not None and ir.guardrails.per_run is not None:
        workflow_guardrail = ir.guardrails.per_run.model_dump()
    return WorkflowDetail(
        id=ir.id,
        version=ir.version,
        description=ir.description or "",
        ir_hash=ir.meta.ir_hash,
        agents=agents,
        nodes=nodes,
        edges=edges,
        project_id=state.workflow_to_project.get(ir.id, "default"),
        start_input_fields=state.start_input_fields_by_workflow.get(ir.id, ["q"]),
        undo_depth=len(state.spec_history_by_workflow.get(ir.id, [])),
        workflow_guardrail=workflow_guardrail,
        mode=state.workflow_modes.get(ir.id, "normal"),
        session_run_id=state.event_session_run_by_workflow.get(ir.id, ""),
    )


# ============================================================
# IR → React Flow graph
# ============================================================


def _ir_to_graph(ir: WorkflowIR) -> tuple[list[GraphNode], list[GraphEdge]]:
    nodes: list[GraphNode] = []
    seen_node_ids: set[str] = set()

    def _ensure_node(node_id: str) -> None:
        if node_id in seen_node_ids:
            return
        seen_node_ids.add(node_id)
        if node_id == START_NODE:
            nodes.append(GraphNode(id=node_id, label="Start", kind="start"))
        elif node_id == END_NODE:
            nodes.append(GraphNode(id=node_id, label="End", kind="end"))
        elif node_id == ERROR_NODE:
            nodes.append(GraphNode(id=node_id, label="Error", kind="error"))
        else:
            tmpl = ir.agents.get(node_id)
            role = str(tmpl.role) if tmpl else "?"
            label = node_id
            nodes.append(GraphNode(id=node_id, label=label, kind="agent", role=role))

    # Always include the three virtual nodes — even if no edge currently
    # references them. They're conceptual workflow boundaries (start /
    # end / error) and shouldn't disappear when the user temporarily
    # deletes the only agent wired to them.
    for vnode in (START_NODE, END_NODE, ERROR_NODE):
        _ensure_node(vnode)

    edges: list[GraphEdge] = []
    for eid, e in ir.edges.items():
        _ensure_node(e.from_)
        edges.extend(_edge_to_graph_edges(eid, e, _ensure_node))

    return nodes, edges


def _edge_to_graph_edges(
    edge_id: str,
    e: EdgeSpec,
    ensure: callable,  # type: ignore[valid-type]
) -> list[GraphEdge]:
    if e.is_direct:
        assert isinstance(e.to, str)
        ensure(e.to)
        return [GraphEdge(
            id=edge_id, source=e.from_, target=e.to,
            via=e.via, is_switch=False,
        )]
    if e.is_fanout:
        assert isinstance(e.to, list)
        out = []
        for tgt in e.to:
            ensure(tgt)
            out.append(GraphEdge(
                id=f"{edge_id}::{tgt}", source=e.from_, target=tgt,
                via=e.via, is_switch=False,
            ))
        return out
    # switch
    out = []
    for case_name, branch in (e.cases or {}).items():
        assert isinstance(branch, EdgeBranch)
        ensure(branch.to)
        out.append(GraphEdge(
            id=f"{edge_id}::{case_name}",
            source=e.from_, target=branch.to,
            via=branch.via, is_switch=True, case=case_name,
        ))
    if e.default is not None:
        ensure(e.default.to)
        out.append(GraphEdge(
            id=f"{edge_id}::__default__",
            source=e.from_, target=e.default.to,
            via=e.default.via, is_switch=True, case="__default__",
        ))
    return out
