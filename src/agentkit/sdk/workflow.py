"""``workflow()`` factory + :class:`WorkflowDef` — the SDK's main user-facing class.

A :class:`WorkflowDef` aggregates @agent-decorated handlers + the
edges connecting them, then compiles to a :class:`WorkflowIR` +
:class:`RuntimePlan`. Pair with
:class:`agentkit.testing.LocalRuntime` to actually execute it.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from agentkit.runtime.executor import HandlerRegistry, _FunctionExecutor
from agentkit.sdk.builder import IRBuilder
from agentkit.sdk.decorators import HandlerFn, get_agent_meta
from agentkit.workflow.compiler.pipeline import compile_from_dict
from agentkit.workflow.ir import (
    END_NODE,
    ERROR_NODE,
    START_NODE,
    WorkflowIR,
)
from agentkit.workflow.plan import RuntimePlan

if TYPE_CHECKING:
    from agentkit.sdk.agent_class import Agent


# ============================================================
# Friendly node-name constants (re-exported at top level)
# ============================================================

#: The implicit "start" node of every workflow.
START: str = START_NODE         # "__start__"
#: The implicit "success" terminal node.
END: str = END_NODE             # "__end__"
#: The implicit "error" terminal node — auto-injected by the Compiler.
ERROR: str = ERROR_NODE         # "__error__"


# What ``add()`` and ``connect()`` accept.
NodeRef = "str | Agent"        # for docs only; real check is duck-typed
HandlerOrAgent = "HandlerFn | Agent"


class WorkflowDef:
    """SDK-side workflow handle.

    Two equivalent ways to populate it::

        # 1) Class-based (recommended)
        from agentkit import Agent, workflow, START, END

        class Researcher(Agent):
            role = "thinking"
            subscribe = ["agent.research.in.q"]
            publish   = ["agent.research.out.summary"]

            async def handle(self, ctx, event):
                ...

        researcher = Researcher()
        wf = workflow("wf_x")
        wf.add(researcher)
        wf.connect(START, researcher)         # via auto-derived
        wf.connect(researcher, END)           # via auto-derived

        # 2) Decorator-based (legacy / quick prototype)
        @agent(role="thinking",
               subscribe=["agent.research.in.q"],
               publish=["agent.research.out.summary"])
        async def researcher(ctx, event):
            ...

        wf = workflow("wf_x")
        wf.add(researcher)
        wf.connect("__start__", "researcher", via="agent.research.in.q")
        wf.connect("researcher", "__end__")
    """

    def __init__(
        self,
        *,
        id: str,
        version: int = 1,
        description: str = "",
        owner: str | None = None,
        project: str | None = None,
        guardrails: dict[str, dict[str, Any]] | None = None,
        registry: HandlerRegistry | None = None,
        event_driven: bool = False,
        start_input_fields: list[str] | None = None,
        end_join: bool = False,
    ) -> None:
        self._builder = IRBuilder(
            id=id,
            version=version,
            description=description,
            owner=owner,
            project=project,
            end_join=end_join,
        )
        if guardrails:
            self._builder.set_guardrails(
                per_agent=guardrails.get("per_agent"),
                per_run=guardrails.get("per_run"),
            )
        self._handlers: dict[str, HandlerFn] = {}
        # Track Agent instances so connect() can auto-derive `via` from
        # publish/subscribe topic intersections.
        self._agents_by_key: dict[str, Agent] = {}
        # Registry is only consulted at compile() time so the same
        # WorkflowDef can be compiled into multiple registries
        # (e.g. one per LocalRuntime instance in a test suite).
        self._registry = registry
        # ── Runtime-policy fields (NOT part of IR — kept here so the
        # CLI / Client can register them with the API server when
        # deploying). Mirrors what the React UI exposes through
        # ``PUT /workflows/{id}/mode`` and ``/start-input``. ──
        self.event_driven: bool = event_driven
        self.start_input_fields: list[str] = list(start_input_fields or ["q"])
        # External I/O specs (also outside IR; deploy-time only).
        # See ``add_source`` / ``add_sink``.
        self._external_specs: list[dict[str, Any]] = []

    # ------------------------------------------------------------
    # Add / connect
    # ------------------------------------------------------------

    def add(
        self,
        fn_or_agent: Any,
        *,
        template_key: str | None = None,
    ) -> WorkflowDef:
        """Register an Agent (class instance) or @agent-decorated function.

        For ``Agent`` instances we capture the object so :meth:`connect`
        can auto-derive ``via`` from its publish/subscribe topics.
        """
        # Lazy import to avoid a circular dep (agent_class -> decorators -> workflow).
        from agentkit.sdk.agent_class import Agent  # noqa: PLC0415

        if isinstance(fn_or_agent, Agent):
            agent = fn_or_agent
            key = template_key or agent.key
            self._builder.add_agent_template(key, agent.meta.template)
            self._handlers[key] = agent.handler
            self._agents_by_key[key] = agent
            return self

        # Function path (legacy decorator).
        meta = get_agent_meta(fn_or_agent)
        if meta is None:
            raise TypeError(
                f"{fn_or_agent!r} is not an @agent-decorated function "
                f"and not an Agent subclass instance — apply @agent(...) "
                f"or subclass Agent before passing to wf.add()",
            )
        key = template_key or meta.template_key
        self._builder.add_agent_template(key, meta.template)
        self._handlers[key] = meta.handler
        return self

    def connect(
        self,
        from_: Any,
        to: Any,
        *,
        via: str | None = None,
        edge_id: str | None = None,
    ) -> WorkflowDef:
        """Add a direct edge ``from_ → to``.

        Both endpoints can be:

        * An :class:`Agent` instance (recommended).
        * One of :data:`START` / :data:`END` / :data:`ERROR` constants.
        * A raw ``template_key`` string (legacy).

        ``via`` is auto-derived when possible:

        * If ``from_`` is an Agent with exactly one publish topic, use that.
        * If ``to`` is an Agent with exactly one subscribe topic, use that.
        * If both are agents and their topic sets share exactly one
          common entry, use that.
        * Otherwise raise — pass ``via=`` explicitly.
        """
        from_key, from_agent = self._resolve_node(from_)
        to_key, to_agent = self._resolve_node(to)

        if via is None:
            via = self._auto_via(
                from_agent=from_agent, to_agent=to_agent,
                from_key=from_key, to_key=to_key,
            )

        eid = edge_id or _auto_edge_id(from_key, to_key)
        self._builder.add_edge(eid, from_=from_key, to=to_key, via=via)
        return self

    def connect_switch(
        self,
        from_: Any,
        *,
        expr: str,
        cases: dict[str, dict[str, str]],
        default: dict[str, str] | None = None,
        edge_id: str | None = None,
    ) -> WorkflowDef:
        """Add a switch edge originating at ``from_``.

        ``from_`` accepts an :class:`Agent` instance OR a raw key string.
        ``cases`` values still use ``{"to": "<key>", "via": "<topic>"}``
        dicts — at the switch level we don't auto-derive topics because
        switch branches usually have arbitrary fan-out semantics.
        """
        from_key, _ = self._resolve_node(from_)
        eid = edge_id or f"e_switch_{from_key}"
        self._builder.add_switch(
            eid, from_=from_key, expr=expr, cases=cases, default=default,
        )
        return self

    # ------------------------------------------------------------
    # Internals — node + via resolution
    # ------------------------------------------------------------

    def add_source(
        self,
        *,
        name: str,
        kind: str,
        topic: str,
        config: dict[str, Any] | None = None,
    ) -> "WorkflowDef":
        """Register an external source (Telegram / IMAP / Python) that
        the deploy-time wiring will attach to the bus.

        Sources publish on ``topic`` so any agent subscribing receives
        events from the outside world. Implies ``event_driven=True``
        at deploy time.

        Returns ``self`` for chaining.
        """
        self._external_specs.append({
            "direction": "source",
            "name": name,
            "kind": kind,
            "topic": topic,
            "config": dict(config or {}),
        })
        return self

    def add_sink(
        self,
        *,
        name: str,
        kind: str,
        topic: str,
        config: dict[str, Any] | None = None,
    ) -> "WorkflowDef":
        """Register an external sink (Telegram / SMTP / Python) that
        consumes envelopes on ``topic`` and forwards to the outside.

        Returns ``self`` for chaining.
        """
        self._external_specs.append({
            "direction": "sink",
            "name": name,
            "kind": kind,
            "topic": topic,
            "config": dict(config or {}),
        })
        return self

    @property
    def external_specs(self) -> list[dict[str, Any]]:
        """Read-only snapshot of registered ext source / sink specs."""
        return list(self._external_specs)

    def _resolve_node(self, node: Any) -> tuple[str, Agent | None]:
        """Return ``(template_key, Agent|None)`` for ``node``.

        ``node`` may be:

        * An :class:`Agent` instance — return its registered key + the instance.
        * One of the ``__start__`` / ``__end__`` / ``__error__`` constants —
          return as-is, no Agent.
        * A plain string — treat as template_key; look up Agent if known.
        """
        # Lazy import (same circular-dep reason as in add()).
        from agentkit.sdk.agent_class import Agent  # noqa: PLC0415

        if isinstance(node, Agent):
            if node.key not in self._agents_by_key:
                raise ValueError(
                    f"Agent {node!r} was not added to this workflow — "
                    f"call wf.add({type(node).__name__}()) before connect()",
                )
            return node.key, node
        if isinstance(node, str):
            return node, self._agents_by_key.get(node)
        raise TypeError(
            f"connect() expects an Agent instance or str node name, got {type(node).__name__}",
        )

    def _auto_via(
        self,
        *,
        from_agent: Agent | None,
        to_agent: Agent | None,
        from_key: str,
        to_key: str,
    ) -> str:
        from_topics = from_agent.publish_topics if from_agent else []
        to_topics = to_agent.subscribe_topics if to_agent else []

        # Best signal: intersection.
        if from_topics and to_topics:
            common = [t for t in from_topics if t in to_topics]
            if len(common) == 1:
                return common[0]
            if len(common) > 1:
                raise ValueError(
                    f"Cannot auto-derive `via` for {from_key} → {to_key}: "
                    f"multiple matching topics {common} — pass via= explicitly",
                )
            # No common topic — fall through to single-side rules but warn
            # via the final error message if neither side disambiguates.

        # Single publisher.
        if len(from_topics) == 1:
            return from_topics[0]
        # Single subscriber (covers __start__ → agent with one subscribe).
        if len(to_topics) == 1:
            return to_topics[0]

        raise ValueError(
            f"Cannot auto-derive `via` for {from_key} → {to_key}: "
            f"from publishes {from_topics or '∅'}, "
            f"to subscribes {to_topics or '∅'} — pass via= explicitly",
        )

    # ------------------------------------------------------------
    # Compile / dump
    # ------------------------------------------------------------

    def compile(
        self,
        *,
        validate: bool = True,
        registry: HandlerRegistry | None = None,
    ) -> tuple[WorkflowIR, RuntimePlan]:
        """Compile the IR + RuntimePlan and register handlers.

        ``registry`` overrides the default registry for this call.
        Useful in tests where each test gets its own registry to
        avoid global-state pollution.
        """
        ir = self._builder.build()
        # Use the same path the YAML loader uses → identical IR
        # whether you came in through SDK or YAML.
        ir, plan = compile_from_dict(
            ir.model_dump(by_alias=True, exclude_none=True),
            validate=validate,
        )

        # Wire handlers into the registry.
        # NOTE: explicit None checks — HandlerRegistry defines __len__,
        # so an empty registry is falsy under ``or``. We hit this bug
        # twice already (Runtime, then SDK) — keep the pattern explicit.
        if registry is not None:
            target = registry
        elif self._registry is not None:
            target = self._registry
        else:
            target = HandlerRegistry.global_default()
        for template_key, fn in self._handlers.items():
            target.register(
                workflow_id=ir.id,
                template_key=template_key,
                executor=_FunctionExecutor(fn),
                replace=True,  # SDK users iterate; replace is friendlier
            )
        return ir, plan

    def to_dict(self) -> dict[str, Any]:
        """Return the IR as a plain dict (round-trippable to YAML)."""
        return self._builder.build().model_dump(
            by_alias=True, exclude_none=True, mode="json",
        )

    def dump_yaml(self, path: str | Path) -> None:
        """Write the workflow to a YAML file at ``path``."""
        Path(path).write_text(
            yaml.safe_dump(
                self.to_dict(),
                sort_keys=False,
                default_flow_style=False,
                allow_unicode=True,
            ),
            encoding="utf-8",
        )

    # ------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------

    @property
    def id(self) -> str:
        return self._builder.id

    @property
    def handlers(self) -> dict[str, HandlerFn]:
        return dict(self._handlers)

    def __repr__(self) -> str:  # pragma: no cover — debug only
        return (
            f"WorkflowDef(id={self.id!r}, "
            f"agents={list(self._builder.agents)}, "
            f"edges={list(self._builder.edges)})"
        )


# ============================================================
# Factory
# ============================================================


def workflow(
    id: str,
    *,
    version: int = 1,
    description: str = "",
    owner: str | None = None,
    project: str | None = None,
    guardrails: dict[str, dict[str, Any]] | None = None,
    event_driven: bool = False,
    start_input_fields: list[str] | None = None,
    end_join: bool = False,
) -> WorkflowDef:
    """Construct a fresh :class:`WorkflowDef`.

    Pass ``event_driven=True`` to put the workflow into continuous-
    listening mode at deploy time (each external event becomes part
    of one shared session run).  ``start_input_fields`` declares the
    payload field names the workflow expects from a manual ``POST
    /api/runs`` trigger — used by the React UI's prompt builder.

    Pass ``end_join=True`` to give the terminal node fan-in semantics:
    when several agents connect directly to ``__end__``, the run only
    completes once **all** of them have signalled — not on the first.
    """
    return WorkflowDef(
        id=id,
        version=version,
        description=description,
        owner=owner,
        project=project,
        guardrails=guardrails,
        event_driven=event_driven,
        start_input_fields=start_input_fields,
        end_join=end_join,
    )


# ============================================================
# Internals
# ============================================================


def _auto_edge_id(from_: str, to: str) -> str:
    """Stable derived id used when the user doesn't pass one."""
    safe = lambda s: s.replace("__", "").replace(".", "_")  # noqa: E731
    return f"e_{safe(from_)}_to_{safe(to)}"
