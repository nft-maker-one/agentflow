"""Agent Runtime — the execution engine.

Public surface::

    from agentkit.runtime import (
        AgentInstance,
        AgentWorker,
        AgentContext,
        Event,
        AgentExecutor,
        agent_handler,
        HandlerRegistry,
        # FSM
        AgentSnapshot,
        AgentStateMeta,
        FSMTransition,
        FailureClass,
        # gating
        GatingResult,
        # errors
        RuntimeError as AgentKitRuntimeError,
        HandlerTimeout,
        HandlerFatal,
        HandlerRecoverable,
        GuardrailExceeded,
        # Redis persistence
        RedisDedupStore,
    )

See ``Doc03_AgentRuntime.md`` for the design.
"""

from agentkit.runtime.context import AgentContext, Event, EventBatch
from agentkit.runtime.dedup_redis import RedisDedupStore
from agentkit.runtime.errors import (
    GuardrailExceeded,
    HandlerFatal,
    HandlerRecoverable,
    HandlerTimeout,
)
from agentkit.runtime.errors import (
    RuntimeError as AgentKitRuntimeError,
)
from agentkit.runtime.executor import (
    AgentExecutor,
    HandlerFn,
    HandlerRegistry,
    agent_handler,
)
from agentkit.runtime.fallback import FailureClass, classify_handler_exception
from agentkit.runtime.fsm import (
    AgentSnapshot,
    AgentStateMeta,
    FSMTransition,
    apply_transition,
    initial_snapshot,
)
from agentkit.runtime.gating import GatingResult, GatingVerdict, run_gating
from agentkit.runtime.instance import AgentInstance
from agentkit.runtime.worker import AgentWorker

__all__ = [
    "AgentContext",
    "AgentExecutor",
    "AgentInstance",
    "AgentKitRuntimeError",
    "AgentSnapshot",
    "AgentStateMeta",
    "AgentWorker",
    "Event",
    "EventBatch",
    "FSMTransition",
    "FailureClass",
    "GatingResult",
    "GatingVerdict",
    "GuardrailExceeded",
    "HandlerFatal",
    "HandlerFn",
    "HandlerRecoverable",
    "HandlerRegistry",
    "HandlerTimeout",
    "RedisDedupStore",
    "agent_handler",
    "apply_transition",
    "classify_handler_exception",
    "initial_snapshot",
    "run_gating",
]
