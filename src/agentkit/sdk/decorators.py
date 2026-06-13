"""Function decorators for the SDK — :func:`@agent`, :func:`@judge`.

These attach an :class:`AgentTemplate`-shaped metadata blob to the
decorated handler function. The metadata is later consumed by
:class:`WorkflowDef.add` to build the IR. Handler registration
into the actual :class:`HandlerRegistry` happens at
``WorkflowDef.compile()`` time, when the workflow_id is known.

Why metadata-then-register instead of immediate register:

* The same handler function can be re-used across workflows
  (think shared library helpers). Binding to a workflow_id at
  decoration time would prevent that.
* It also keeps the decorator side-effect minimal — pytest
  importing the module shouldn't mutate global state.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from agentkit.llm.models import LLMBinding
from agentkit.models.enums import Role
from agentkit.runtime.context import AgentContext, Event
from agentkit.workflow.ir.agent import (
    AgentGuardrail,
    AgentTemplate,
    AggregateSpec,
    FallbackSpec,
    PublishSpec,
    Subscription,
)


# ---- Type aliases -----------------------------------------------

HandlerFn = Callable[[AgentContext, Any], Awaitable[list[Event]]]
SubscribeArg = str | tuple[str, dict[str, str]]
PublishArg = str | tuple[str, str]  # (topic) or (topic, schema_ref)


# Attribute name used to stash agent metadata onto a handler fn.
agent_meta_attr = "__agentkit_agent_meta__"


@dataclass(frozen=True)
class AgentMeta:
    """SDK-side bundle attached to a handler function."""

    template_key: str
    template: AgentTemplate
    handler: HandlerFn
    raw_kwargs: dict[str, Any] = field(default_factory=dict)


def get_agent_meta(fn: object) -> AgentMeta | None:
    """Retrieve the metadata attached by :func:`@agent`."""
    return getattr(fn, agent_meta_attr, None)


# ============================================================
# @agent decorator
# ============================================================


def agent(
    *,
    template_key: str | None = None,
    role: Role | str = Role.THINKING,
    description: str = "",
    llm: LLMBinding | str | dict | None = None,
    subscribe: list[SubscribeArg] | None = None,
    publish: list[PublishArg] | None = None,
    tags: dict[str, str] | None = None,
    fallback: dict[str, Any] | None = None,
    guardrail: dict[str, Any] | None = None,
    schema_in: dict[str, Any] | None = None,
    schema_out: dict[str, Any] | None = None,
    topic_prefix: str | None = None,
    prompt_ref: str | None = None,
    replicas_min: int = 1,
    replicas_max: int = 1,
) -> Callable[[HandlerFn], HandlerFn]:
    """Mark a function as an Agent handler + capture its template.

    The decorated function is returned unchanged (so it remains
    directly callable for tests). Add it to a :class:`WorkflowDef`
    via ``wf.add(fn)`` to actually register the handler.

    Most arguments map 1:1 to fields of :class:`AgentTemplate`;
    convenience translations:

    * ``llm="openai/gpt-4o"`` → ``LLMBinding(provider="openai", model="gpt-4o")``
    * ``subscribe=["agent.x.in.q"]`` → list of :class:`Subscription`
      with empty tag_filter; or pass ``("agent.x.in.q", {"lang": "zh"})``
    * ``publish=["agent.x.out.r"]`` → :class:`PublishSpec` (no schema_ref)
    * ``fallback={"strategy": "alt_template", "alt_template": "x"}``
    * ``guardrail={"max_tokens_per_call": 8000, "max_cycles": 5}``
    """

    def decorator(fn: HandlerFn) -> HandlerFn:
        key = template_key or fn.__name__
        template = _build_template(
            role=role,
            description=description,
            llm=llm,
            subscribe=subscribe or [],
            publish=publish or [],
            tags=tags or {},
            fallback=fallback,
            guardrail=guardrail,
            schema_in=schema_in,
            schema_out=schema_out,
            topic_prefix=topic_prefix,
            prompt_ref=prompt_ref,
            replicas_min=replicas_min,
            replicas_max=replicas_max,
        )
        meta = AgentMeta(
            template_key=key,
            template=template,
            handler=fn,
            raw_kwargs={
                "role": role,
                "subscribe": subscribe,
                "publish": publish,
            },
        )
        setattr(fn, agent_meta_attr, meta)
        return fn

    return decorator


# ============================================================
# @judge decorator — sugar for @agent(role="judge")
# ============================================================


def judge(
    *,
    template_key: str | None = None,
    description: str = "",
    llm: LLMBinding | str | dict | None = None,
    subscribe: list[SubscribeArg] | None = None,
    publish: list[PublishArg] | None = None,
    tags: dict[str, str] | None = None,
    schema_in: dict[str, Any] | None = None,
    schema_out: dict[str, Any] | None = None,
    topic_prefix: str | None = None,
    prompt_ref: str | None = None,
) -> Callable[[HandlerFn], HandlerFn]:
    """Sugar around :func:`@agent` with ``role="judge"`` pre-set."""
    return agent(
        template_key=template_key,
        role=Role.JUDGE,
        description=description,
        llm=llm,
        subscribe=subscribe,
        publish=publish,
        tags=tags,
        schema_in=schema_in,
        schema_out=schema_out,
        topic_prefix=topic_prefix,
        prompt_ref=prompt_ref,
    )


# ============================================================
# Internals
# ============================================================


def _build_template(
    *,
    role: Role | str,
    description: str,
    llm: LLMBinding | str | dict | None,
    subscribe: list[SubscribeArg],
    publish: list[PublishArg],
    tags: dict[str, str],
    fallback: dict[str, Any] | None,
    guardrail: dict[str, Any] | None,
    schema_in: dict[str, Any] | None,
    schema_out: dict[str, Any] | None,
    topic_prefix: str | None,
    prompt_ref: str | None,
    replicas_min: int,
    replicas_max: int,
    aggregate: dict[str, Any] | None = None,
) -> AgentTemplate:
    return AgentTemplate(
        role=Role(role) if isinstance(role, str) else role,
        description=description,
        llm=_coerce_llm(llm),
        subscribe=[_coerce_subscribe(s) for s in subscribe],
        publish=[_coerce_publish(p) for p in publish],
        tags=dict(tags),
        fallback=FallbackSpec(**fallback) if fallback else None,
        guardrail=AgentGuardrail(**guardrail) if guardrail else None,
        schema_in=schema_in,
        schema_out=schema_out,
        topic_prefix=topic_prefix,
        prompt_ref=prompt_ref,
        replicas={"min": replicas_min, "max": max(replicas_max, replicas_min)},
        aggregate=AggregateSpec(**aggregate) if aggregate else None,
    )


def _coerce_llm(v: LLMBinding | str | dict | None) -> LLMBinding | None:
    if v is None or isinstance(v, LLMBinding):
        return v
    if isinstance(v, str):
        # "openai/gpt-4o" → LLMBinding
        if "/" not in v:
            raise ValueError(f"llm string must be 'provider/model', got {v!r}")
        provider, _, model = v.partition("/")
        return LLMBinding(provider=provider, model=model)
    if isinstance(v, dict):
        return LLMBinding(**v)
    raise TypeError(f"unsupported llm type: {type(v).__name__}")


def _coerce_subscribe(v: SubscribeArg) -> Subscription:
    if isinstance(v, str):
        return Subscription(topic=v)
    if isinstance(v, tuple) and len(v) == 2:
        topic, tags = v
        return Subscription(topic=topic, tag_filter=dict(tags))
    raise TypeError(f"unsupported subscribe spec: {v!r}")


def _coerce_publish(v: PublishArg) -> PublishSpec:
    if isinstance(v, str):
        return PublishSpec(topic=v)
    if isinstance(v, tuple) and len(v) == 2:
        topic, schema_ref = v
        return PublishSpec(topic=topic, schema_ref=schema_ref)
    raise TypeError(f"unsupported publish spec: {v!r}")
