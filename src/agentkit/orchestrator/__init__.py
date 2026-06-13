"""Orchestrator — control-plane Run lifecycle + switch routing.

Public surface::

    from agentkit.orchestrator import (
        Orchestrator,
        Run,
        RunCursor,
        BranchEvent,
        RunStore,
        InMemoryRunStore,
        evaluate_switch_expression,
        OrchestratorError,
        # Redis persistence
        RedisCompletionNotifier,
    )

See ``Doc05_Orchestrator.md``.
"""

from agentkit.orchestrator.completion_pubsub import RedisCompletionNotifier
from agentkit.orchestrator.errors import (
    OrchestratorError,
    RunNotFound,
    SwitchEvalError,
    UnknownWorkflow,
)
from agentkit.orchestrator.orchestrator import Orchestrator
from agentkit.orchestrator.run import (
    BranchEvent,
    Run,
    RunCursor,
    new_run,
)
from agentkit.orchestrator.store import (
    InMemoryRunStore,
    RunStore,
)
from agentkit.orchestrator.store_postgres import PostgresRunStore
from agentkit.orchestrator.switch_eval import (
    evaluate_switch_expression,
    pick_case,
)

__all__ = [
    "BranchEvent",
    "InMemoryRunStore",
    "Orchestrator",
    "OrchestratorError",
    "PostgresRunStore",
    "RedisCompletionNotifier",
    "Run",
    "RunCursor",
    "RunNotFound",
    "RunStore",
    "SwitchEvalError",
    "UnknownWorkflow",
    "evaluate_switch_expression",
    "new_run",
    "pick_case",
]
