"""Handler abstraction + registry + decorator.

A *handler* is the user code that processes one event. The framework
supports two equivalent shapes:

1. **Function** (most common)::

       @agent_handler(workflow_id="wf_x", template_key="researcher")
       async def researcher(ctx: AgentContext, event: Envelope) -> list[Event]:
           ...

2. **Class** implementing :class:`AgentExecutor` (advanced — useful when
   you need ``on_init`` / ``on_shutdown`` lifecycle hooks).

Both shapes get wrapped in a uniform call site by the AgentInstance
loop so the dispatcher only sees one interface.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

from agentkit.common.errors import AgentKitError
from agentkit.common.logging import get_logger
from agentkit.models.envelope import Envelope
from agentkit.runtime.context import AgentContext, Event

log = get_logger(__name__)


# ---- Type aliases ----

HandlerFn = Callable[[AgentContext, Envelope], Awaitable[list[Event]]]


# ============================================================
# Protocol — class-based handler shape
# ============================================================


@runtime_checkable
class AgentExecutor(Protocol):
    """Class-based handler. The decorator wraps function handlers
    into instances of an internal class implementing this Protocol.
    """

    async def on_event(self, ctx: AgentContext, event: Envelope) -> list[Event]:
        """Process one event; return any number of follow-up events."""
        ...

    async def on_init(self, ctx: AgentContext) -> None:
        """Lifecycle hook — called once per instance before first event."""
        ...

    async def on_shutdown(self, ctx: AgentContext) -> None:
        """Lifecycle hook — called once when the instance is draining."""
        ...


class _FunctionExecutor:
    """Wraps a plain ``async def handler(ctx, event) -> list[Event]``
    so it satisfies :class:`AgentExecutor`.
    """

    def __init__(self, fn: HandlerFn) -> None:
        self._fn = fn

    async def on_event(self, ctx: AgentContext, event: Envelope) -> list[Event]:
        result = await self._fn(ctx, event)
        if result is None:
            return []
        if not isinstance(result, list):
            raise AgentKitError(
                f"handler {self._fn.__qualname__} must return list[Event], "
                f"got {type(result).__name__}",
            )
        return result

    async def on_init(self, ctx: AgentContext) -> None:
        del ctx

    async def on_shutdown(self, ctx: AgentContext) -> None:
        del ctx


# ============================================================
# Handler registry
# ============================================================


class HandlerRegistry:
    """A scoped, mutable mapping of ``(workflow_id, template_key)`` → executor.

    Each :class:`AgentWorker` typically has its own registry instance
    so different processes can serve different subsets of the topology.

    The :func:`agent_handler` decorator pushes into a *default* global
    registry for SDK ergonomics — workers can opt in with
    ``HandlerRegistry.global_default()``.
    """

    _DEFAULT: HandlerRegistry | None = None

    def __init__(self) -> None:
        self._handlers: dict[tuple[str, str], AgentExecutor] = {}

    @classmethod
    def global_default(cls) -> HandlerRegistry:
        if cls._DEFAULT is None:
            cls._DEFAULT = cls()
        return cls._DEFAULT

    def register(
        self,
        *,
        workflow_id: str,
        template_key: str,
        executor: AgentExecutor,
        replace: bool = False,
    ) -> None:
        key = (workflow_id, template_key)
        if not replace and key in self._handlers:
            raise AgentKitError(
                f"handler already registered for {key}; "
                f"pass replace=True if intentional",
            )
        self._handlers[key] = executor

    def get(
        self, *, workflow_id: str, template_key: str,
    ) -> AgentExecutor | None:
        return self._handlers.get((workflow_id, template_key))

    def require(
        self, *, workflow_id: str, template_key: str,
    ) -> AgentExecutor:
        h = self.get(workflow_id=workflow_id, template_key=template_key)
        if h is None:
            raise AgentKitError(
                f"no handler registered for ({workflow_id!r}, {template_key!r}) — "
                f"did you import the module containing the @agent_handler?",
            )
        return h

    def __contains__(self, key: tuple[str, str]) -> bool:
        return key in self._handlers

    def __len__(self) -> int:
        return len(self._handlers)

    def keys(self):
        return list(self._handlers.keys())

    def clear(self) -> None:
        """Test convenience — drop all registered handlers."""
        self._handlers.clear()


# ============================================================
# Decorator — function shape
# ============================================================


def agent_handler(
    *,
    workflow_id: str,
    template_key: str,
    registry: HandlerRegistry | None = None,
    replace: bool = False,
) -> Callable[[HandlerFn], HandlerFn]:
    """Register an ``async def`` as the handler for ``(workflow_id, template_key)``.

    Returns the function unchanged so it remains directly testable
    (e.g. via :func:`tests.helpers.runtime.run_handler_locally`).
    """

    def decorator(fn: HandlerFn) -> HandlerFn:
        # NOTE: explicit None-check, not ``registry or default``.
        # ``HandlerRegistry`` defines ``__len__``, which makes empty
        # registries falsy — passing a freshly constructed registry
        # would silently fall through to the global default.
        target = registry if registry is not None else HandlerRegistry.global_default()
        target.register(
            workflow_id=workflow_id,
            template_key=template_key,
            executor=_FunctionExecutor(fn),
            replace=replace,
        )
        log.debug(
            "handler.registered",
            workflow_id=workflow_id,
            template_key=template_key,
            handler=fn.__qualname__,
        )
        return fn

    return decorator
