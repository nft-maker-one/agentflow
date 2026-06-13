"""Agent template inventory — flat across all deployed workflows."""

from __future__ import annotations

from fastapi import APIRouter, Request

from agentkit.api.models import AgentSummary

router = APIRouter(prefix="/agents", tags=["agents"])


@router.get("", response_model=list[AgentSummary])
async def list_agents(req: Request) -> list[AgentSummary]:
    state = req.app.state.app_state
    out: list[AgentSummary] = []
    for ir in state.ir_by_id.values():
        for key, tmpl in ir.agents.items():
            out.append(AgentSummary(
                template_key=f"{ir.id}/{key}",
                role=str(tmpl.role),
                description=tmpl.description or "",
                subscribe=[s.topic for s in tmpl.subscribe],
                publish=[p.topic for p in tmpl.publish],
                llm=tmpl.llm.model_dump() if tmpl.llm else None,
            ))
    return out
