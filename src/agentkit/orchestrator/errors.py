"""Orchestrator-specific exceptions."""

from __future__ import annotations

from agentkit.common.errors import (
    AgentKitError,
    PermanentError,
    ValidationError,
)


class OrchestratorError(AgentKitError):
    """Base for orchestrator-layer errors."""


class UnknownWorkflow(PermanentError, OrchestratorError):
    """Raised when ``create_run`` is called for an undeployed workflow_id."""


class RunNotFound(PermanentError, OrchestratorError):
    """Raised when a run_id can't be looked up."""


class SwitchEvalError(ValidationError, OrchestratorError):
    """Raised when a switch expression can't be parsed / evaluated.

    These do NOT crash the orchestrator — the surrounding loop
    catches and routes the run to ``__error__``.
    """
