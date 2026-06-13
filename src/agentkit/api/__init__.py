"""Control-plane HTTP API + Web UI server.

Public surface::

    from agentkit.api import AppState, create_app, serve

* ``AppState``  — process-wide state (Bus + Orch + Worker + IR registry)
* ``create_app(state)`` — FastAPI app factory
* ``serve(...)`` — uvicorn launcher used by the ``agentkit serve`` CLI
"""

from agentkit.api.app import create_app
from agentkit.api.server import serve
from agentkit.api.state import AppState

__all__ = ["AppState", "create_app", "serve"]
