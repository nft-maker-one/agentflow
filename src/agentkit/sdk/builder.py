"""IRBuilder — explicit dynamic IR construction.

Useful when:

* You're writing a code generator that emits Workflows from data.
* You want pure data-side IR construction in CI / unit tests.
* You prefer the explicit form over decorator magic.

The Builder produces a :class:`WorkflowIR` directly; pair with
:class:`WorkflowDef` if you also want handler registration.
"""

from __future__ import annotations

from typing import Any

from agentkit.llm.models import LLMBinding
from agentkit.models.enums import Role
from agentkit.workflow.ir import (
    AgentTemplate,
    EdgeBranch,
    EdgeSpec,
    IRMeta,
    PublishSpec,
    RunGuardrail,
    Subscription,
    Switch,
    WorkflowGuardrail,
    WorkflowIR,
)
from agentkit.workflow.ir.agent import (
    AgentGuardrail,
    FallbackSpec,
)


class IRBuilder:
    """Builder for incrementally assembling a :class:`WorkflowIR`.

    All methods return ``self`` for chaining.
    """

    def __init__(
        self,
        *,
        id: str,
        version: int = 1,
        description: str = "",
        owner: str | None = None,
        project: str | None = None,
        end_join: bool = False,
    ) -> None:
        self._id = id
        self._version = version
        self._description = description
        self._owner = owner
        self._project = project
        self._end_join = end_join
        self._agents: dict[str, AgentTemplate] = {}
        self._edges: dict[str, EdgeSpec] = {}
        self._guardrails: WorkflowGuardrail | None = None

    # ------------------------------------------------------------
    # Agent additions
    # ------------------------------------------------------------

    def add_agent(
        self,
        template_key: str,
        *,
        role: Role | str = Role.THINKING,
        description: str = "",
        llm: LLMBinding | str | dict | None = None,
        subscribe: list[str | tuple[str, dict[str, str]]] | None = None,
        publish: list[str | tuple[str, str]] | None = None,
        tags: dict[str, str] | None = None,
        fallback: dict[str, Any] | None = None,
        guardrail: dict[str, Any] | None = None,
        schema_in: dict[str, Any] | None = None,
        schema_out: dict[str, Any] | None = None,
        topic_prefix: str | None = None,
        prompt_ref: str | None = None,
    ) -> IRBuilder:
        # Lazy-import to keep the SDK's decorator coercion in one place.
        from agentkit.sdk.decorators import (  # noqa: PLC0415
            _coerce_llm, _coerce_publish, _coerce_subscribe,
        )

        if template_key in self._agents:
            raise ValueError(f"agent template_key {template_key!r} already added")

        self._agents[template_key] = AgentTemplate(
            role=Role(role) if isinstance(role, str) else role,
            description=description,
            llm=_coerce_llm(llm),
            subscribe=[_coerce_subscribe(s) for s in (subscribe or [])],
            publish=[_coerce_publish(p) for p in (publish or [])],
            tags=dict(tags or {}),
            fallback=FallbackSpec(**fallback) if fallback else None,
            guardrail=AgentGuardrail(**guardrail) if guardrail else None,
            schema_in=schema_in,
            schema_out=schema_out,
            topic_prefix=topic_prefix,
            prompt_ref=prompt_ref,
        )
        return self

    def add_agent_template(
        self, template_key: str, template: AgentTemplate,
    ) -> IRBuilder:
        """Plug a pre-built :class:`AgentTemplate` (e.g. from `@agent`)."""
        if template_key in self._agents:
            raise ValueError(f"agent template_key {template_key!r} already added")
        self._agents[template_key] = template
        return self

    # ------------------------------------------------------------
    # Edge additions
    # ------------------------------------------------------------

    def add_edge(
        self,
        edge_id: str,
        *,
        from_: str,
        to: str | list[str],
        via: str | None = None,
    ) -> IRBuilder:
        """Add a direct or fanout edge.

        For switch edges, use :meth:`add_switch`.
        """
        if edge_id in self._edges:
            raise ValueError(f"edge_id {edge_id!r} already added")
        self._edges[edge_id] = EdgeSpec(
            from_=from_, to=to, via=via,
        )
        return self

    def add_switch(
        self,
        edge_id: str,
        *,
        from_: str,
        expr: str,
        cases: dict[str, dict[str, str]],
        default: dict[str, str] | None = None,
    ) -> IRBuilder:
        """Add a switch edge.

        ``cases`` keys are the matchable values; values are
        ``{"to": ..., "via": ...}`` dicts.
        """
        if edge_id in self._edges:
            raise ValueError(f"edge_id {edge_id!r} already added")
        self._edges[edge_id] = EdgeSpec(
            from_=from_,
            to=Switch(switch=expr),
            cases={k: EdgeBranch(**v) for k, v in cases.items()},
            default=EdgeBranch(**default) if default else None,
        )
        return self

    # ------------------------------------------------------------
    # Guardrails
    # ------------------------------------------------------------

    def set_guardrails(
        self,
        *,
        per_agent: dict[str, Any] | None = None,
        per_run: dict[str, Any] | None = None,
    ) -> IRBuilder:
        self._guardrails = WorkflowGuardrail(
            per_agent=AgentGuardrail(**per_agent) if per_agent else None,
            per_run=RunGuardrail(**per_run) if per_run else None,
        )
        return self

    # ------------------------------------------------------------
    # Build
    # ------------------------------------------------------------

    def build(self) -> WorkflowIR:
        return WorkflowIR(
            id=self._id,
            version=self._version,
            description=self._description,
            owner=self._owner,
            project=self._project,
            agents=dict(self._agents),
            edges=dict(self._edges),
            guardrails=self._guardrails,
            end_join=self._end_join,
            meta=IRMeta(),
        )

    # ------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------

    @property
    def id(self) -> str:
        return self._id

    @property
    def agents(self) -> dict[str, AgentTemplate]:
        return dict(self._agents)

    @property
    def edges(self) -> dict[str, EdgeSpec]:
        return dict(self._edges)
