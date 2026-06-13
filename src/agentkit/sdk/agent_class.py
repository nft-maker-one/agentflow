"""Class-based Agent definition — the main user-facing SDK style.

Two ways to construct an Agent:

1) **Direct instantiation** (普通用户最常用 — 不需要继承)::

    summarizer = Agent(
        role="thinking",
        description="Summarize the input text in one sentence",
        subscribe=["agent.summarizer.in.q"],
        publish=["agent.summarizer.out.r"],
        llm="deepseek/deepseek-chat",
        prompt="Summarize this:\\n\\n{{ payload.text }}",
        output_field="summary",
        max_retries=2,
        fallback={"strategy": "static_response",
                  "response": {"summary": "(LLM unavailable)"}},
    )

   The default :meth:`handle` does:

   * if ``prompt`` is set → render the Jinja2 template against ``event``,
     call ``ctx.llm.chat()``, emit ``{output_field: <reply>, **payload}``;
   * otherwise → forward ``event.payload`` unchanged to the publish topic.
   * on transient failure → retry up to ``max_retries`` times (exp backoff);
   * on terminal failure → invoke ``fallback`` if configured, else re-raise.

2) **Subclass** (有自定义需求的开发者) — override :meth:`handle`::

    class Researcher(Agent):
        role = "thinking"
        subscribe = ["agent.researcher.in.q"]
        publish = ["agent.researcher.out.draft"]
        llm = "deepseek/deepseek-chat"

        async def handle(self, ctx, event):
            # Free-form custom logic — full control.
            ...

Subclassing also lets you set defaults at the class level for *every*
instance you create from that subclass.
"""

from __future__ import annotations

import asyncio
import inspect
import re
from typing import Any, ClassVar

import orjson
from jinja2 import (
    Environment,
    StrictUndefined,
    Template as JinjaTemplate,
    TemplateError,
    UndefinedError,
)

from agentkit.common.jsonschema_cache import validate_cached
from agentkit.common.offload import maybe_offload
from agentkit.llm.errors import LLMError
from agentkit.llm.models import (
    ChatMessage,
    LLMBinding,
    LLMRequest,
    LLMResponse,
    ResponseFormat,
)
from agentkit.common.errors import PermanentError
from agentkit.models.enums import Role
from agentkit.runtime.context import AgentContext, Event
from agentkit.sdk.decorators import (
    AgentMeta,
    HandlerFn,
    PublishArg,
    SubscribeArg,
    _build_template,
    agent_meta_attr,
)


# ============================================================
# Sentinel — distinguishes "user passed None" from "not passed"
# ============================================================


class _Unset:
    def __repr__(self) -> str:
        return "<unset>"


_UNSET: Any = _Unset()


# Names of class attrs that flow into the AgentTemplate (IR side).
_IR_FIELDS: tuple[str, ...] = (
    "role", "description", "llm",
    "subscribe", "publish", "tags",
    "fallback", "guardrail",
    "schema_in", "schema_out",
    "topic_prefix", "prompt_ref",
    "replicas_min", "replicas_max",
    "aggregate",
)

# Names of class attrs consumed by the *default handler* (instance side only —
# never passed to the IR template builder).
_HANDLER_FIELDS: tuple[str, ...] = (
    "prompt", "system_prompt",
    "max_retries", "retry_backoff_s",
    "output_field", "preserve_input",
    "fallback_response",
    "json_output", "json_schema", "json_unwrap",
    "python_script",
)


# ============================================================
# Agent
# ============================================================


class Agent:
    """Generic Agent — directly instantiable OR subclass-able.

    See module docstring for the two construction styles.
    """

    # ---- IR-bound fields (override on subclass or via __init__) ----
    role: ClassVar[Role | str] = Role.THINKING
    description: ClassVar[str] = ""
    template_key: ClassVar[str | None] = None
    llm: ClassVar[LLMBinding | str | dict | None] = None
    subscribe: ClassVar[list[SubscribeArg]] = []
    publish: ClassVar[list[PublishArg]] = []
    tags: ClassVar[dict[str, str]] = {}
    fallback: ClassVar[dict[str, Any] | None] = None
    guardrail: ClassVar[dict[str, Any] | None] = None
    schema_in: ClassVar[dict[str, Any] | None] = None
    schema_out: ClassVar[dict[str, Any] | None] = None
    topic_prefix: ClassVar[str | None] = None
    prompt_ref: ClassVar[str | None] = None
    replicas_min: ClassVar[int] = 1
    replicas_max: ClassVar[int] = 1
    #: Fan-in gating spec. When set, runtime buffers events per run_id
    #: and only invokes the handler once threshold + required topics
    #: are satisfied. Shape::
    #:
    #:     {"threshold": 2, "required": ["agent.x.out"]}
    #:
    #: ``threshold = 0`` means "all subscribe topics".
    aggregate: ClassVar[dict[str, Any] | None] = None

    # ---- Default-handler fields ----
    #: Jinja2 template rendered against ``{"payload": ..., "event": ...}``.
    #: When set together with ``llm``, the default handler runs an LLM call.
    prompt: ClassVar[str | None] = None
    #: System prompt for the LLM call (default-handler LLM mode).
    system_prompt: ClassVar[str | None] = None
    #: Default-handler retry count for transient errors. Default 0.
    max_retries: ClassVar[int] = 0
    #: Initial backoff (seconds); doubles each retry (exp backoff).
    retry_backoff_s: ClassVar[float] = 0.5
    #: Field name for the LLM output in the published payload.
    output_field: ClassVar[str] = "result"
    #: Whether to merge ``event.payload`` into the published payload.
    preserve_input: ClassVar[bool] = True
    #: Emitted (instead of an exception) when the default handler exhausts
    #: ``max_retries``. The dict is merged on top of the input payload.
    #: This is **handler-level** fallback — distinct from the IR-level
    #: ``fallback`` (FallbackSpec) which controls runtime retry / alt_template
    #: at the AgentInstance level.
    fallback_response: ClassVar[dict[str, Any] | None] = None
    #: Force the LLM to emit JSON, parse it, and emit a *dict* downstream.
    #: When ``True`` the default handler:
    #:
    #: 1. Asks the provider for ``response_format=json_object``
    #:    (DeepSeek / OpenAI / Qwen all honor this).
    #: 2. Parses the text as JSON; on parse failure raises a normal
    #:    error → ``max_retries`` retries → ``fallback_response``.
    #: 3. Optionally validates the parsed object against ``json_schema``.
    #: 4. Either replaces ``output_field`` (default) with the dict, OR if
    #:    ``json_unwrap=True``, merges the parsed dict directly into
    #:    the top-level payload.
    json_output: ClassVar[bool] = False
    #: Optional JSON Schema (Draft 2020-12) the parsed JSON must satisfy.
    #: When set, asks the provider for ``response_format=json_schema``
    #: AND validates locally with ``jsonschema.validate``.
    json_schema: ClassVar[dict[str, Any] | None] = None
    #: When ``json_output=True`` and this is ``True``, merge the parsed
    #: dict into the top-level published payload instead of nesting it
    #: under ``output_field``.
    json_unwrap: ClassVar[bool] = False
    #: Python script source replacing the LLM call. When set, the
    #: default handler executes this script (instead of rendering a
    #: prompt + calling the LLM) and uses the dict returned by its
    #: ``handle`` function as the output. The script must define::
    #:
    #:     def handle(payload, event=None) -> dict:
    #:         ...
    #:         return {...}
    #:
    #: ``handle`` may be ``async``; the runtime awaits coroutines.
    #: Takes priority over ``prompt`` and ``llm`` when set. The
    #: returned dict is merged with ``event.payload`` (when
    #: ``preserve_input=True``, the default).
    python_script: ClassVar[str | None] = None

    # Underscore-prefixed instance state — populated in __init__.
    _meta: AgentMeta
    _resolved_template_key: str
    _handler_cfg: dict[str, Any]
    _compiled_python_handler: Callable | None = None
    #: Pre-compiled Jinja2 template for ``prompt`` (None when no prompt).
    _compiled_prompt: Any = None

    # ------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------

    def __init__(self, **overrides: Any) -> None:
        cls = type(self)

        # Split kwargs into IR-bound vs handler-bound.
        ir_cfg: dict[str, Any] = {f: _copy_default(getattr(cls, f)) for f in _IR_FIELDS}
        handler_cfg: dict[str, Any] = {f: getattr(cls, f) for f in _HANDLER_FIELDS}

        # template_key is special: not an IR field on AgentTemplate (it
        # lives on the IR map key) and not a handler field either.
        explicit_key = overrides.pop("template_key", cls.template_key)

        unknown = set(overrides) - set(_IR_FIELDS) - set(_HANDLER_FIELDS)
        if unknown:
            raise TypeError(
                f"{cls.__name__}: unknown init kwargs: {sorted(unknown)} "
                f"(allowed: template_key + {sorted(_IR_FIELDS + _HANDLER_FIELDS)})",
            )

        for k, v in overrides.items():
            if k in _IR_FIELDS:
                ir_cfg[k] = v
            else:
                handler_cfg[k] = v

        self._resolved_template_key = explicit_key or _default_template_key(cls)
        self._handler_cfg = handler_cfg

        # Pre-compile the Python script handler (if any) so syntax
        # errors surface at construction time, not on the first
        # event. ``_compiled_python_handler`` is the extracted
        # ``handle`` callable; ``None`` means "no script mode".
        script_src: str | None = handler_cfg.get("python_script")
        if script_src:
            self._compiled_python_handler = _compile_python_handler(script_src)
        else:
            self._compiled_python_handler = None

        # Pre-compile the Jinja2 prompt once (B6). Constructing
        # ``Template(...)`` per event recompiles the template on the hot
        # path; the prompt string is fixed for the agent's lifetime.
        # A bad template now fails fast at construction (like scripts).
        prompt_src: str | None = handler_cfg.get("prompt")
        self._compiled_prompt = (
            _JINJA_ENV.from_string(prompt_src) if prompt_src else None
        )

        template = _build_template(**ir_cfg)

        # Build a plain function (not a bound method) for the handler so
        # we can attach metadata via setattr — bound methods reject this.
        # The closure captures `self` so subclass overrides of ``handle``
        # are honoured at call time.
        async def _handler(ctx: AgentContext, event: Event) -> list[Event]:
            result = await self.handle(ctx, event)
            return result or []

        self._handler_fn: HandlerFn = _handler

        self._meta = AgentMeta(
            template_key=self._resolved_template_key,
            template=template,
            handler=_handler,
            raw_kwargs={**ir_cfg, **handler_cfg},
        )
        setattr(_handler, agent_meta_attr, self._meta)

    # ============================================================
    # User-overridable handler
    # ============================================================

    async def handle(
        self, ctx: AgentContext, event: Event,
    ) -> list[Event] | None:
        """Process one event, return events to emit.

        Default implementation (when this method isn't overridden):

        * If ``prompt`` is configured → render template, call ``ctx.llm.chat()``,
          publish ``{output_field: <reply>, **payload}`` to ``publish[0]``.
        * Otherwise → forward ``event.payload`` unchanged to ``publish[0]``.
        * Retry transient failures up to ``max_retries`` (exp backoff).
        * On terminal failure invoke ``fallback`` if configured.

        Override this in a subclass for full control.
        """
        return await self._default_handle(ctx, event)

    # ============================================================
    # Default-handler implementation
    # ============================================================

    async def _default_handle(
        self, ctx: AgentContext, event: Event,
    ) -> list[Event]:
        """The auto-generated handler body — see :meth:`handle` for behavior."""
        if not self.publish_topics:
            raise RuntimeError(
                f"{type(self).__name__} default handler needs at least "
                f"one `publish` topic — pass publish=[...] or override handle()",
            )
        output_topic = self.publish_topics[0]

        max_retries: int = max(0, int(self._handler_cfg["max_retries"]))
        backoff: float = max(0.0, float(self._handler_cfg["retry_backoff_s"]))

        last_err: BaseException | None = None
        for attempt in range(max_retries + 1):
            try:
                payload = await self._compute_output_payload(ctx, event)
                return [Event(output_topic, payload)]
            except Exception as e:
                # Don't retry on programming errors that won't fix
                # themselves (TypeError / ValueError from prompt template).
                if isinstance(e, (TemplateError, UndefinedError, TypeError)):
                    last_err = e
                    break
                # LLMError exposes ``advise_fallback`` / class — only retry
                # transient classes.
                if isinstance(e, LLMError) and not getattr(e, "retryable", True):
                    last_err = e
                    break

                last_err = e
                if attempt < max_retries:
                    sleep_for = backoff * (2 ** attempt)
                    ctx.logger.warning(
                        "default_handler.retry",
                        attempt=attempt + 1, max_attempts=max_retries + 1,
                        sleep_s=round(sleep_for, 3), error=str(e),
                    )
                    await asyncio.sleep(sleep_for)
                else:
                    ctx.logger.warning(
                        "default_handler.exhausted",
                        attempts=attempt + 1, error=str(e),
                    )

        # Out of retries — try fallback.
        fallback_event = self._build_fallback_event(
            output_topic, event, last_err,
        )
        if fallback_event is not None:
            ctx.logger.info(
                "default_handler.fallback_emitted",
                error=str(last_err) if last_err else "",
            )
            return [fallback_event]

        assert last_err is not None
        raise last_err

    async def _compute_output_payload(
        self, ctx: AgentContext, event: Event,
    ) -> dict[str, Any]:
        """Produce the payload to publish for this event."""
        prompt: str | None = self._handler_cfg["prompt"]
        system_prompt: str | None = self._handler_cfg["system_prompt"]
        output_field: str = self._handler_cfg["output_field"]
        preserve: bool = bool(self._handler_cfg["preserve_input"])
        json_output: bool = bool(self._handler_cfg["json_output"])
        json_schema: dict | None = self._handler_cfg["json_schema"]
        json_unwrap: bool = bool(self._handler_cfg["json_unwrap"])

        # ---- Python script mode (highest priority) ----
        # User supplied a ``def handle(payload, event=None) -> dict``
        # function to replace the LLM call. We invoke it with the
        # current payload and merge the returned dict over the input
        # (when ``preserve_input``).
        if self._compiled_python_handler is not None:
            fn = self._compiled_python_handler
            payload_in: dict[str, Any] = dict(event.payload)
            # Inspect to support both 1-arg and 2-arg signatures.
            sig = inspect.signature(fn)
            pos_params = [
                p for p in sig.parameters.values()
                if p.kind in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                )
            ]
            call_args = (payload_in, event) if len(pos_params) >= 2 else (payload_in,)
            try:
                if inspect.iscoroutinefunction(fn):
                    # Async user code already cooperates with the loop.
                    result = await fn(*call_args)
                else:
                    # B2: offload synchronous user code to a thread so a
                    # blocking or CPU-heavy script can't freeze the whole
                    # event loop (and every other agent on it).
                    result = await asyncio.to_thread(fn, *call_args)
            except TypeError as e:
                raise TypeError(
                    f"python_script handle() invocation failed — "
                    f"check signature is `def handle(payload)` or "
                    f"`def handle(payload, event)`: {e}",
                ) from e

            # Safety net: a sync fn that returned a coroutine.
            if inspect.iscoroutine(result):
                result = await result

            if not isinstance(result, dict):
                raise TypeError(
                    f"python_script handle() must return a dict, got "
                    f"{type(result).__name__}",
                )

            out: dict[str, Any] = dict(event.payload) if preserve else {}
            out.update(result)
            return out

        # ---- LLM mode ----
        if prompt is not None:
            rendered = self._render_compiled(event)

            if json_output:
                # Ask the provider to emit JSON. We always use ``json_object``
                # mode (universally supported by OpenAI / DeepSeek / Qwen);
                # if a ``json_schema`` was supplied we still validate it
                # locally via jsonschema, which is more portable than the
                # OpenAI ``response_format=json_schema`` mode (whose wire
                # format varies across compat providers).
                response_format = ResponseFormat(kind="json_object")
                effective_system = (
                    (system_prompt or "")
                    + "\n\nReply with valid JSON only — no markdown fences, no commentary."
                ).strip()
                text = await ctx.llm.chat(
                    rendered, system=effective_system,
                    response_format=response_format,
                )
                # B7: parse large LLM JSON off the event loop.
                parsed = await maybe_offload(
                    _parse_json_strict, text, size_hint=len(text),
                )
                if json_schema is not None:
                    _validate_json_schema(parsed, json_schema)
                # Build the output payload.
                out: dict[str, Any] = dict(event.payload) if preserve else {}
                if json_unwrap:
                    # Spread the parsed JSON at the top level. Caller is
                    # responsible for not clobbering keys it cares about.
                    out.update(parsed)
                else:
                    out[output_field] = parsed
                return out

            # Plain-text LLM call.
            text = await ctx.llm.chat(rendered, system=system_prompt)
            out = dict(event.payload) if preserve else {}
            out[output_field] = text
            return out

        # ---- Pass-through mode ----
        return dict(event.payload) if preserve else {}

    def _build_fallback_event(
        self, output_topic: str, event: Event, err: BaseException | None,
    ) -> Event | None:
        """Realize the ``fallback_response`` static payload, if any."""
        canned = self._handler_cfg.get("fallback_response")
        if canned is None:
            return None
        out: dict[str, Any] = (
            dict(event.payload) if self._handler_cfg["preserve_input"] else {}
        )
        out.update(canned)
        if err is not None and "_fallback_reason" not in out:
            out["_fallback_reason"] = f"{type(err).__name__}: {err}"
        return Event(output_topic, out)

    def _render_compiled(self, event: Event) -> str:
        """Render the agent's pre-compiled Jinja2 prompt (B6).

        Exposes the same variables as :func:`_render_prompt`:
        ``payload`` / ``event`` / ``topic`` plus payload keys at the
        top level. Falls back to compiling on the fly only if the
        template wasn't precompiled (defensive — shouldn't happen).
        """
        tmpl = self._compiled_prompt
        if tmpl is None:
            tmpl = _JINJA_ENV.from_string(self._handler_cfg["prompt"])
        return _render_template(tmpl, event)

    # ============================================================
    # Read-only properties
    # ============================================================

    @property
    def meta(self) -> AgentMeta:
        """The captured :class:`AgentMeta` (template + handler)."""
        return self._meta

    @property
    def key(self) -> str:
        """The resolved template_key (== ``meta.template_key``)."""
        return self._resolved_template_key

    @property
    def handler(self) -> HandlerFn:
        """The handler callable (used by HandlerRegistry)."""
        return self._handler_fn

    @property
    def subscribe_topics(self) -> list[str]:
        """The (resolved) topic strings this agent subscribes to."""
        return [s.topic for s in self._meta.template.subscribe]

    @property
    def publish_topics(self) -> list[str]:
        """The (resolved) topic strings this agent publishes to."""
        return [p.topic for p in self._meta.template.publish]

    def __repr__(self) -> str:
        return f"{type(self).__name__}(template_key={self.key!r})"


# ============================================================
# Internals
# ============================================================


def _copy_default(value: Any) -> Any:
    """Shallow-copy mutable defaults so instances don't share state."""
    if isinstance(value, list):
        return list(value)
    if isinstance(value, dict):
        return dict(value)
    return value


def _compile_python_handler(script: str) -> Any:
    """Compile a user-provided Python script and return its ``handle``
    function. Raises ``ValueError`` early on syntax / structural
    errors so they surface at agent construction (the API rejects
    bad scripts with HTTP 400 instead of crashing on first event).

    The script runs in a fresh dict namespace with full builtins
    available — there is **no sandbox**. This is acceptable for a
    self-hosted dev framework where the operator already controls
    the Python source; never expose the API to untrusted users.
    """
    if not script or not script.strip():
        raise ValueError("python_script is empty")

    namespace: dict[str, Any] = {"__builtins__": __builtins__}
    try:
        code = compile(script, "<agent_python_script>", "exec")
    except SyntaxError as e:
        raise ValueError(f"python_script syntax error: {e}") from e
    try:
        exec(code, namespace)  # noqa: S102 — intentional, see docstring
    except Exception as e:
        raise ValueError(
            f"python_script raised at module load: {type(e).__name__}: {e}",
        ) from e

    fn = namespace.get("handle")
    if fn is None:
        raise ValueError(
            "python_script must define a top-level function named "
            "`handle` — e.g. `def handle(payload): return {'result': ...}`",
        )
    if not callable(fn):
        raise ValueError(
            f"`handle` is not callable — got {type(fn).__name__}",
        )
    return fn


def _default_template_key(cls: type) -> str:
    """``URLFetcher`` → ``url_fetcher``.   ``Fetcher`` → ``fetcher``."""
    name = cls.__name__
    if name == "Agent":
        # When the user does ``Agent(...)`` without subclassing, fall
        # back to a generic auto-key. Users are expected to pass
        # ``template_key=`` for direct instantiation in that case.
        return "agent"
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.lower()


# Shared Jinja2 environment. Prompts are pre-compiled per agent in
# ``Agent.__init__`` (see ``_compiled_prompt`` / ``_render_compiled``);
# this env is the single compiler. ``StrictUndefined`` makes missing
# variables fail loudly at render time instead of silently blanking.
_JINJA_ENV = Environment(undefined=StrictUndefined)


def _render_prompt(template: str, event: Event) -> str:
    """Render a Jinja2 prompt template (compiles on each call).

    Kept for direct/ad-hoc use and backward compatibility. The hot
    path uses the pre-compiled :meth:`Agent._render_compiled` instead.

    Variables exposed: ``payload`` / ``event`` / ``topic`` plus payload
    keys at the top level.
    """
    return _render_template(_JINJA_ENV.from_string(template), event)


def _render_template(tmpl: JinjaTemplate, event: Event) -> str:
    """Render ``tmpl`` against the event context, turning any Jinja
    ``TemplateError`` (e.g. ``{{ payload.topic }}`` when the payload has
    no ``topic`` field) into an actionable :class:`PermanentError`.

    Raising ``PermanentError`` (a) fails fast — the template + payload are
    fixed, so retrying is futile — and (b) gives the user the available
    payload fields instead of the opaque ``'dict object' has no attribute
    'topic'`` message.
    """
    try:
        return tmpl.render(**_render_context(event))
    except TemplateError as e:
        keys = (
            sorted(event.payload.keys())
            if isinstance(event.payload, dict) else []
        )
        raise PermanentError(
            f"prompt template render failed: {e}. "
            f"Available payload fields: {keys}. "
            f"Reference a field as {{{{ payload.<field> }}}} or {{{{ <field> }}}}."
        ) from e


def _render_context(event: Event) -> dict[str, object]:
    """Build the Jinja render context for a prompt template.

    Exposes the reserved ``payload`` / ``event`` / ``topic`` names plus
    payload keys at the top level for ergonomics. Building one merged
    dict (instead of ``render(topic=…, **payload)``) avoids the
    ``render() got multiple values for keyword argument`` ``TypeError``
    that crashed the handler when a payload field collided with a
    reserved name.

    Precedence: payload keys WIN at the top level, so a start-input
    field named ``topic`` is reachable as ``{{ topic }}`` (the common
    user intent). The reserved names remain available as fallbacks when
    not shadowed — and the envelope topic is always reachable via
    ``{{ event.topic }}`` regardless.
    """
    payload = event.payload if isinstance(event.payload, dict) else {}
    return {
        "payload": event.payload,
        "event": event,
        "topic": event.topic,
        **payload,
    }


# ============================================================
# JSON helpers (used by json_output mode)
# ============================================================


_JSON_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", flags=re.MULTILINE)


def _parse_json_strict(text: str) -> Any:
    """Parse ``text`` as JSON; tolerate provider-added markdown fences.

    Raises :class:`ValueError` on parse failure — the default handler
    treats this as a transient error and retries.
    """
    s = text.strip()
    s = _JSON_FENCE.sub("", s)
    try:
        # orjson is faster than stdlib json and is already a hard dep
        # (used by the Kafka serde + Postgres store).
        return orjson.loads(s)
    except orjson.JSONDecodeError as e:
        raise ValueError(
            f"json_output: LLM did not return valid JSON ({e}). "
            f"First 200 chars: {text[:200]!r}",
        ) from e


def _validate_json_schema(value: Any, schema: dict[str, Any]) -> None:
    """Validate ``value`` against a JSON Schema. Raises ``ValueError`` on fail."""
    import jsonschema  # noqa: PLC0415

    try:
        # Cached validator avoids recompiling the schema each call (B5).
        validate_cached(value, schema)
    except jsonschema.ValidationError as e:
        raise ValueError(
            f"json_output: parsed JSON failed schema validation: {e.message}",
        ) from e
