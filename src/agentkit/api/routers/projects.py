"""Project CRUD — group workflows for navigation.

Phase 2.x in-memory only; restart loses state. Persisting to Postgres
is part of the (deferred) Option C work.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict

router = APIRouter(prefix="/projects", tags=["projects"])


class ProjectOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    description: str = ""
    created_at: datetime
    n_workflows: int = 0


class CreateProjectRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    description: str = ""


@router.get("", response_model=list[ProjectOut])
async def list_projects(req: Request) -> list[ProjectOut]:
    state = req.app.state.app_state
    out: list[ProjectOut] = []
    for p in state.projects.values():
        n = sum(1 for v in state.workflow_to_project.values() if v == p.id)
        out.append(ProjectOut(
            id=p.id, name=p.name, description=p.description,
            created_at=p.created_at, n_workflows=n,
        ))
    out.sort(key=lambda x: (x.id != "default", x.created_at))
    return out


@router.post("", response_model=ProjectOut, status_code=201)
async def create_project(
    body: CreateProjectRequest, req: Request,
) -> ProjectOut:
    state = req.app.state.app_state
    p = await state.create_project(name=body.name, description=body.description)
    return ProjectOut(
        id=p.id, name=p.name, description=p.description,
        created_at=p.created_at, n_workflows=0,
    )


@router.delete("/{project_id}", status_code=204)
async def delete_project(project_id: str, req: Request) -> None:
    state = req.app.state.app_state
    if project_id not in state.projects:
        raise HTTPException(status_code=404, detail="project not found")
    try:
        await state.delete_project(project_id)
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
