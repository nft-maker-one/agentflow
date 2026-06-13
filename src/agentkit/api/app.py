"""FastAPI application factory.

Usage::

    from agentkit.api.app import create_app
    from agentkit.api.state import AppState

    state = AppState()
    app = create_app(state)
    # ... deploy workflows on `state` ...
    # then run with uvicorn

The module also serves the React UI's static bundle from
``agentflow/web/dist`` when present, so a single ``agentkit serve``
process is enough for both API + UI.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from agentkit import __version__
from agentkit.api.models import HealthResponse
from agentkit.api.routers import agents, events, runs, workflows
from agentkit.api.routers import agent_edit, external_io, inbox, projects, system, workflow_edit
from agentkit.api.state import AppState
from agentkit.common.logging import get_logger

log = get_logger(__name__)

# Where the Vite bundle ends up — relative to repo root.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_UI_DIST = _REPO_ROOT / "web" / "dist"


def create_app(state: AppState) -> FastAPI:
    """Build the FastAPI app, wiring it to a started :class:`AppState`."""

    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        # State is started by the caller (so they can deploy workflows
        # before any HTTP traffic). We only stop it on shutdown.
        log.info("api.lifespan.startup")
        try:
            yield
        finally:
            await state.stop()
            log.info("api.lifespan.shutdown")

    app = FastAPI(
        title="AgentKit Control Plane",
        version=__version__,
        lifespan=_lifespan,
    )
    # Stash on app.state for handlers.
    app.state.app_state = state

    # Permissive CORS so Vite dev server (5173) can hit the API on
    # 8080. In production same-origin makes this irrelevant.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---- API routes — all under /api/* ----
    app.include_router(workflows.router, prefix="/api")
    app.include_router(runs.router,      prefix="/api")
    app.include_router(agents.router,    prefix="/api")
    app.include_router(events.router,    prefix="/api")
    app.include_router(agent_edit.router, prefix="/api")
    app.include_router(workflow_edit.router, prefix="/api")
    app.include_router(projects.router,  prefix="/api")
    app.include_router(system.router,    prefix="/api")
    app.include_router(external_io.router, prefix="/api")
    app.include_router(inbox.router, prefix="/api")

    @app.get("/api/health", response_model=HealthResponse, tags=["health"])
    async def health() -> HealthResponse:
        return HealthResponse(
            ok=True,
            version=__version__,
            deployed_workflows=sorted(state.ir_by_id.keys()),
        )

    # ---- Static UI (if built) ----
    if _UI_DIST.exists() and (_UI_DIST / "index.html").exists():
        # Vite builds put hashed assets under /assets/ — serve them.
        app.mount(
            "/assets",
            StaticFiles(directory=str(_UI_DIST / "assets")),
            name="assets",
        )

        # Serve index.html for the SPA root + any client-side route.
        @app.get("/")
        async def _index() -> FileResponse:
            return FileResponse(_UI_DIST / "index.html")

        @app.get("/{path:path}")
        async def _spa_fallback(path: str) -> FileResponse:
            # If the file actually exists in dist, serve it.
            candidate = _UI_DIST / path
            if candidate.is_file():
                return FileResponse(candidate)
            # Otherwise fall through to index.html for SPA routing.
            return FileResponse(_UI_DIST / "index.html")
    else:
        @app.get("/")
        async def _no_ui() -> dict:
            return {
                "message": "AgentKit API is up. UI not built — run `make ui-build` for the Web UI.",
                "api": "/api/health",
            }

    return app
