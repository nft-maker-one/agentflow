"""User-facing :class:`AgentContext` + sanctioned :class:`PublishPipeline`.

Strict architectural contract (Doc03 §3.2):

* User handlers receive an :class:`AgentContext` and an :class:`Envelope`.
* The ONLY way to publish events is :meth:`AgentContext.publish` —
  user code does not see the EventBus client.
* The PublishPipeline:
  - validates ``topic`` is in the publish whitelist;
  - validates ``payload`` against the agent's ``schema_out`` (if set);
  - injects ``trace_id``, ``run_id``, ``event_id``, ``from_``, ``ts`` —
    user-supplied values for these are *ignored*;
  - sanitizes user headers, dropping reserved keys;
  - sends to bus via ``EventBus.publish``.

The pipeline returns a :class:`PublishReceipt` so the caller / metrics
layer can inspect partition/offset, but the user's handler doesn't
need to.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from agentkit.bus.builder import build_envelope
from agentkit.bus.errors import BusError
from agentkit.bus.interface import EventBus, PublishReceipt
from agentkit.common.logging import get_logger
from agentkit.llm.gateway import LLMGatewayClient
from agentkit.llm.models import LLMBinding, LLMRequest, LLMResponse
from agentkit.models.envelope import AgentRef, Envelope, EnvelopeHeaders, ToFilter
from agentkit.models.enums import Role
from agentkit.runtime.errors import PermissionDenied

log = get_logger(__name__)


# ============================================================
# Event — user-facing emit shape
# ============================================================


class Event(BaseModel):
    """User-facing emit shape — what handlers return / accept.

    Distinct from :class:`Envelope` (the on-wire form) so users
    can't accidentally set system fields like ``trace_id``. The
    PublishPipeline converts ``Event → Envelope`` with proper
    metadata injection.

    Constructor accepts both positional and keyword forms::

        Event("agent.x.out.r", {"k": "v"})
        Event(topic="agent.x.out.r", payload={"k": "v"})
        Event("agent.x.out.r", {"k": "v"}, user_headers={"x-corr": "..."})
    """

    model_config = ConfigDict(extra="forbid")

    topic: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    # Only ``user_headers`` are user-writeable — control headers
    # like trace_id / runtime_token are filled by the Pipeline.
    user_headers: dict[str, str] = Field(default_factory=dict)
    # Optional tag filter routing hint (consumed by ToFilter).
    to_tags: dict[str, str] = Field(default_factory=dict)

    def __init__(
        self,
        topic: str | None = None,
        payload: dict[str, Any] | None = None,
        /,
        **kwargs: Any,
    ) -> None:
        # Accept positional shorthand without breaking the kwargs form
        # Pydantic ships natively. The two positional slots match the
        # two fields users care about most often.
        if topic is not None:
            kwargs.setdefault("topic", topic)
        if payload is not None:
            kwargs.setdefault("payload", payload)
        super().__init__(**kwargs)


class EventBatch(BaseModel):
    """Convenience wrapper when a handler emits multiple events."""

    model_config = ConfigDict(extra="forbid")

    events: list[Event] = Field(default_factory=list)


# ============================================================
# PublishPipeline — sanctioned publish path
# ============================================================


# Reserved header keys the user CANNOT set via Event.user_headers.
# The Pipeline strips these silently (or raises if strict=True).
_RESERVED_USER_HEADER_KEYS: frozenset[str] = frozenset(
    {
        "trace_id",
        "run_id",
        "event_id",
        "from",
        "from_",
        "ts",
        "schema_ver",
        "runtime_token",
        "retry_attempt",
        "deadline_ms",
    },
)


class PublishPipeline:
    """The single place where ``ctx.publish`` calls flow.

    Constructed once per :class:`AgentInstance`; the user-facing
    :class:`AgentContext` only holds a reference and forwards.
    """

    def __init__(
        self,
        *,
        bus: EventBus,
        agent_id: str,
        agent_role: Role,
        runtime_node: str,
        workflow_id: str,
        publish_whitelist: set[str],
        schema_out: dict[str, Any] | None = None,
    ) -> None:
        self._bus = bus
        self._agent_id = agent_id
        self._agent_role = agent_role
        self._runtime_node = runtime_node
        self._workflow_id = workflow_id
        self._whitelist = publish_whitelist
        self._schema_out = schema_out

    async def publish(
        self,
        event: Event,
        *,
        trace_id: str,
        run_id: str,
        causation_id: str | None = None,
    ) -> PublishReceipt:
        """Validate, enrich, and ship one user-emitted Event."""
        # ① Resolve possibly-short-form topic against the whitelist.
        # The Compiler expands ``"reply"`` → ``"agent.echo.out.reply"``;
        # if the user's @agent decorator uses the short form too, their
        # handler code naturally returns ``Event(topic="reply", ...)``
        # — accept that by suffix-matching the whitelist.
        resolved = self._resolve_topic(event.topic)
        if resolved not in self._whitelist:
            raise PermissionDenied(
                f"agent {self._agent_id} cannot publish to {event.topic!r}; "
                f"not in publish whitelist {sorted(self._whitelist)}",
            )

        # ② Schema check (optional).
        if self._schema_out is not None:
            self._validate_payload(event.payload)

        # ③ Sanitize user headers — drop reserved keys.
        clean_user_headers = {
            k: v
            for k, v in event.user_headers.items()
            if k not in _RESERVED_USER_HEADER_KEYS
        }

        # ④ Build envelope (system fields injected here, NOT user-controlled).
        envelope: Envelope = build_envelope(
            topic=resolved,
            payload=event.payload,
            workflow_id=self._workflow_id,
            run_id=run_id,
            trace_id=trace_id,
            from_=AgentRef(
                role=self._agent_role,
                agent_id=self._agent_id,
                runtime_node=self._runtime_node,
            ),
            to_filter=ToFilter(tags=dict(event.to_tags)),
            headers=EnvelopeHeaders(user_headers=clean_user_headers),
            causation_id=causation_id,
        )

        # ⑤ Bus publish — bubble up bus errors; gating wraps in handler-fail logic.
        try:
            return await self._bus.publish(envelope)
        except BusError:
            log.exception(
                "publish.bus_error",
                agent_id=self._agent_id,
                topic=event.topic,
            )
            raise

    # ----------------------------------------------------------------
    # Internals
    # ----------------------------------------------------------------

    def _resolve_topic(self, topic: str) -> str:
        """Expand a short-form topic against the whitelist.

        Rules:

        * If ``topic`` is already in the whitelist → as-is.
        * If ``topic`` has no dots AND exactly one whitelist entry
          ends with ``.<topic>`` → return that entry.
        * Otherwise → return ``topic`` unchanged (caller will reject).
        """
        if topic in self._whitelist:
            return topic
        if "." in topic:
            return topic
        candidates = [w for w in self._whitelist if w.endswith(f".{topic}")]
        if len(candidates) == 1:
            return candidates[0]
        # Zero or >1 → return original; caller handles.
        return topic

    def _validate_payload(self, payload: dict[str, Any]) -> None:
        # Local import to avoid pulling jsonschema into modules that
        # don't need it.
        import jsonschema  # noqa: PLC0415

        from agentkit.common.jsonschema_cache import validate_cached  # noqa: PLC0415

        try:
            # Cached validator — avoids recompiling schema_out per publish (B5).
            validate_cached(payload, self._schema_out)
        except jsonschema.ValidationError as e:
            raise PermissionDenied(
                f"agent {self._agent_id} emitted payload violating schema_out: {e.message}",
            ) from e


# ============================================================
# LLMHandle — agent-bound view of the gateway
# ============================================================


class LLMHandle:
    """Per-agent wrapper around :class:`LLMGatewayClient`.

    Handlers receive this as ``ctx.llm``. When the agent declares an
    :class:`LLMBinding` (via the ``llm`` class attr / decorator
    kwarg), every call here auto-injects ``provider`` / ``model``,
    so user code stays clean::

        class Researcher(Agent):
            llm = LLMBinding(provider="deepseek", model="deepseek-chat")
            async def handle(self, ctx, event):
                # No provider/model needed — picked up from class attr.
                text = await ctx.llm.chat("hello")

    Without a binding the handle still works as a pass-through —
    falls back to the gateway's ``default_provider`` / ``default_model``
    or to whatever the user passes explicitly.
    """

    def __init__(
        self,
        *,
        gateway: LLMGatewayClient,
        binding: LLMBinding | None,
        agent_id: str,
        run_id: str,
        trace_id: str,
        template_key: str = "",
    ) -> None:
        self._gateway = gateway
        self._binding = binding
        self._agent_id = agent_id
        self._run_id = run_id
        self._trace_id = trace_id
        self._template_key = template_key

    @property
    def gateway(self) -> LLMGatewayClient:
        """Escape hatch — direct access to the underlying gateway."""
        return self._gateway

    @property
    def binding(self) -> LLMBinding | None:
        return self._binding

    # ---- High-level shortcuts ----------------------------------

    async def chat(
        self,
        prompt: str,
        *,
        provider: str | None = None,
        model: str | None = None,
        system: str | None = None,
        **kwargs: Any,
    ) -> str:
        """Send a single user message, return assistant text.

        ``provider`` / ``model`` defaults come from the agent's
        binding. Passing them explicitly overrides the binding for
        this one call only.
        """
        eff_provider, eff_model = self._effective_pm(provider, model)
        return await self._gateway.chat(
            prompt,
            provider=eff_provider, model=eff_model,
            system=system,
            **self._with_correlation(kwargs),
        )

    async def complete(self, req: LLMRequest) -> LLMResponse:
        """Run the full Gateway pipeline. Auto-fills provider/model
        and correlation IDs on the request if missing."""
        # Pydantic models are immutable-ish — copy with overrides.
        merged: dict[str, Any] = {}
        if req.provider is None and self._binding is not None:
            merged["provider"] = self._binding.provider
        if req.model is None and self._binding is not None:
            merged["model"] = self._binding.model
        if not getattr(req, "agent_id_", None):
            merged["agent_id_"] = self._agent_id
        if not getattr(req, "run_id_", None):
            merged["run_id_"] = self._run_id
        if not getattr(req, "trace_id_", None):
            merged["trace_id_"] = self._trace_id
        if not getattr(req, "template_key_", None):
            merged["template_key_"] = self._template_key
        if merged:
            req = req.model_copy(update=merged)
        return await self._gateway.complete(req)

    # ---- Internals ---------------------------------------------

    def _effective_pm(
        self, provider: str | None, model: str | None,
    ) -> tuple[str | None, str | None]:
        if provider is None and self._binding is not None:
            provider = self._binding.provider
        if model is None and self._binding is not None:
            model = self._binding.model
        return provider, model

    def _with_correlation(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        # ``chat`` forwards **kwargs to LLMRequest — inject correlation
        # IDs unless the caller already set them.
        kwargs.setdefault("agent_id_", self._agent_id)
        kwargs.setdefault("run_id_", self._run_id)
        kwargs.setdefault("trace_id_", self._trace_id)
        kwargs.setdefault("template_key_", self._template_key)
        return kwargs


# ============================================================
# AgentContext — what user handlers see
# ============================================================


class AgentContext:
    """The capability surface for user handler code.

    The handler receives this as the first argument. It exposes:

    * ``ctx.llm`` — an :class:`LLMHandle` that pre-fills the agent's
      ``LLMBinding`` (provider/model) into every call. So user code can
      just write ``await ctx.llm.chat("hello")`` without re-stating
      provider/model.
    * ``ctx.publish(event)`` — sanctioned publish
    * ``ctx.logger`` — pre-bound structured logger
    * ``ctx.agent_id`` / ``ctx.template_key`` / ``ctx.workflow_id`` /
      ``ctx.run_id`` / ``ctx.trace_id`` — scoped identifiers

    User code MUST NOT reach into private attributes (``_bus`` etc).
    """

    def __init__(
        self,
        *,
        agent_id: str,
        template_key: str,
        workflow_id: str,
        run_id: str,
        trace_id: str,
        llm: LLMGatewayClient,
        publish_pipeline: PublishPipeline,
        causation_id: str | None = None,
        agent_tags: dict[str, str] | None = None,
        llm_binding: "LLMBinding | None" = None,
    ) -> None:
        self.agent_id = agent_id
        self.template_key = template_key
        self.workflow_id = workflow_id
        self.run_id = run_id
        self.trace_id = trace_id
        self.causation_id = causation_id
        self.agent_tags = dict(agent_tags or {})
        # Wrap the gateway so handlers don't have to repeat provider/model.
        # If no binding was supplied (e.g. tests), the handle still works
        # as a thin pass-through using the gateway's defaults.
        self.llm: LLMHandle = LLMHandle(
            gateway=llm, binding=llm_binding,
            agent_id=agent_id, run_id=run_id, trace_id=trace_id,
            template_key=template_key,
        )

        # NB: store as a private attribute — users shouldn't see the
        # pipeline directly. Forward via ``publish``.
        self._pipeline = publish_pipeline

        # Pre-bind log fields so every handler log line is correlated.
        self.logger = log.bind(
            agent_id=agent_id,
            template_key=template_key,
            workflow_id=workflow_id,
            run_id=run_id,
            trace_id=trace_id,
        )

    async def publish(self, event: Event) -> PublishReceipt:
        """Publish a user-emitted Event through the sanctioned pipeline."""
        return await self._pipeline.publish(
            event,
            trace_id=self.trace_id,
            run_id=self.run_id,
            causation_id=self.causation_id,
        )
