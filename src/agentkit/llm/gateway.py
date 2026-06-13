"""LLMGatewayClient — the 9-step request pipeline.

See ``Doc06 §4.1`` for the canonical pipeline. Each step is broken
into a small private method so unit tests can exercise individual
concerns (retry, fallback, rate-limit) in isolation.

The Gateway is intentionally **decoupled** from EventBus and from
the real Guardrail:

* It receives a :class:`GuardrailHandle` (Protocol) at construction.
  The default :class:`NoOpGuardrail` is fine for dev/tests; the real
  Guardrail (Doc07) plugs in transparently.
* It does NOT publish to the bus. Telemetry is structured logging
  for now; metrics integration arrives with the Observability
  module (Doc10).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any, Final

from agentkit.common.logging import get_logger
from agentkit.llm.errors import LLMError, LLMErrorClass
from agentkit.llm.guardrail_iface import GuardrailHandle, NoOpGuardrail, Reservation
from agentkit.llm.models import (
    ChatMessage,
    LLMBinding,
    LLMChunk,
    LLMRequest,
    LLMResponse,
    TokenUsage,
)
from agentkit.llm.provider import LLMProvider
from agentkit.llm.ratelimit import RateLimiter
from agentkit.llm.retry import RetryPolicy, decide_retry
from agentkit.llm.tokenizer import estimate_request_tokens
from agentkit.observability import metrics

log = get_logger(__name__)


# Padding we add to the prompt-token estimate to cover ``max_tokens``
# when the user didn't set one explicitly. Used for the Guardrail
# pre-reservation amount only — provider returns the truth later.
_DEFAULT_MAX_COMPLETION_TOKENS_FOR_ESTIMATE: Final[int] = 1_000

# Doc06 §4.1 Step 6 — Provider Fallback speculative racing.
#
# When the primary (or current) binding is still pending after this
# many milliseconds, the Gateway speculatively launches the *next*
# binding in the chain IN PARALLEL (Hedged-request / "tail at scale"
# pattern). Whichever attempt completes successfully first wins; the
# slower in-flight attempt(s) are cancelled.
#
# Rationale: a fast primary returns well under this threshold and
# pays zero extra cost (no second binding is ever launched). Only a
# slow primary's TAIL latency is hidden behind a parallel attempt,
# bounding total latency to ~max(primary, backup) instead of
# primary + backup.
#
# No dedicated settings field exists on the Gateway today (bindings /
# providers / retry-policy are the only tunables passed at
# construction); a module constant keeps this change scoped to this
# file per Doc06 guidance, while still being easy to override in
# tests via monkeypatch.
SPECULATIVE_THRESHOLD_MS: Final[int] = 1_000


@dataclass(frozen=True, slots=True)
class _Race:
    """Bookkeeping for one in-flight provider attempt in the chain race."""

    binding: LLMBinding
    index: int
    started_at: float


@dataclass(frozen=True, slots=True)
class _Outcome:
    """Result of collecting a batch of completed race attempts.

    Exactly one of ``response`` / ``fatal_err`` drives the caller's
    next move; ``last_err`` / ``errors`` feed metrics + the
    "all providers failed" fallback path.
    """

    response: LLMResponse | None
    fatal_err: LLMError | None
    last_err: LLMError | None
    errors: list[tuple[_Race, LLMError]] = field(default_factory=list)


class LLMGatewayClient:
    """Public LLM client used by Runtime / SDK.

    Constructor accepts:

    * ``providers`` — mapping of provider-name → :class:`LLMProvider`.
      The Gateway looks up the right adapter when dispatching a
      request based on ``LLMRequest.provider`` (or :class:`LLMBinding`).
    * ``bindings`` — registered :class:`LLMBinding` lookup table
      (populated by Workflow Compiler in later modules; empty for now).
    * ``default_provider`` / ``default_model`` — used when an
      :class:`LLMRequest` does NOT specify ``provider`` / ``model`` /
      ``binding``. Lets simple users call ``gateway.chat("hi")``.
    * ``guardrail`` — defaults to :class:`NoOpGuardrail`.
    * ``rate_limiter`` — shared :class:`RateLimiter`. Defaults are fine.
    * ``retry_policy`` — exposed for tests to use a deterministic policy.
    """

    def __init__(
        self,
        *,
        providers: dict[str, LLMProvider],
        bindings: dict[str, LLMBinding] | None = None,
        default_provider: str | None = None,
        default_model: str | None = None,
        guardrail: GuardrailHandle | None = None,
        rate_limiter: RateLimiter | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        if not providers:
            raise ValueError("at least one provider must be configured")
        if default_provider is not None and default_provider not in providers:
            raise ValueError(
                f"default_provider {default_provider!r} not in providers "
                f"({sorted(providers)})",
            )
        self._providers = providers
        self._bindings = bindings or {}
        self._default_provider = default_provider
        self._default_model = default_model
        self._guardrail: GuardrailHandle = guardrail or NoOpGuardrail()
        self._rate_limiter = rate_limiter or RateLimiter()
        self._retry_policy = retry_policy or RetryPolicy()

    @property
    def default_provider(self) -> str | None:
        return self._default_provider

    @property
    def default_model(self) -> str | None:
        return self._default_model

    @property
    def providers(self) -> dict[str, LLMProvider]:
        """Read-only view of registered providers (for tests / introspection)."""
        return dict(self._providers)

    # ------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------

    async def complete(self, req: LLMRequest) -> LLMResponse:
        """Run the full 9-step pipeline and return a :class:`LLMResponse`.

        Streaming variant is :meth:`stream`.
        """
        chain = self._resolve_chain(req)
        # Step 2: PreCount (uses *first* model in the chain — sufficient
        # for an estimate; provider has the truth after the call).
        primary = chain[0]
        est_tokens = self._estimate_tokens(req, primary)

        # Step 3: Guardrail PreCheck
        reservation = await self._guardrail.precheck(
            run_id=req.run_id_ or "",
            agent_id=req.agent_id_ or "",
            est_tokens=est_tokens,
            cycle_inc=1,
            dry_charge=False,
            template_key=req.template_key_ or "",
        )

        deadline_s = time.monotonic() + (req.timeout_ms / 1000.0)
        try:
            # Steps 4-6 — RateLimit, retrying provider call, and
            # speculative provider-fallback racing (see
            # :meth:`_run_chain_speculatively`).
            response = await self._run_chain_speculatively(
                chain=chain,
                req=req,
                est_tokens=est_tokens,
                deadline_s=deadline_s,
                reservation=reservation,
            )
            # Step 7-8: PostCount + Guardrail consume
            await self._guardrail.consume(
                reservation,
                actual_tokens=response.usage.total_tokens,
                actual_cost=response.cost_usd,
            )
            self._log_success(req, response)
            return response
        except BaseException:
            await self._guardrail.release(reservation, reason="exception")
            raise

    async def stream(self, req: LLMRequest) -> AsyncIterator[LLMChunk]:
        """Stream a chat completion as :class:`LLMChunk`s.

        v0.1 scope: streaming has no retry / fallback (Doc06 §5.2);
        the first provider attempt either streams successfully or
        we propagate the error. The user can retry at the request
        level if needed.
        """
        chain = self._resolve_chain(req)
        binding = chain[0]
        provider = self._providers[binding.provider]

        est_tokens = self._estimate_tokens(req, binding)
        reservation = await self._guardrail.precheck(
            run_id=req.run_id_ or "",
            agent_id=req.agent_id_ or "",
            est_tokens=est_tokens,
            cycle_inc=1,
            dry_charge=False,
            template_key=req.template_key_ or "",
        )

        await self._rate_limiter.acquire(
            binding.rate_limit,
            provider=binding.provider,
            model=binding.model,
            agent_tag=None,
            tokens_estimated=est_tokens,
            budget_ms=req.timeout_ms,
        )

        # Make a per-call request copy with the resolved provider/model,
        # so providers don't need to re-do binding resolution.
        sub_req = self._copy_with_binding(req, binding)

        usage_seen: TokenUsage | None = None
        try:
            async for chunk in provider.stream(sub_req):
                if chunk.usage is not None:
                    usage_seen = chunk.usage
                yield chunk
        except LLMError:
            await self._guardrail.release(reservation, reason="stream_failed")
            raise

        # Final accounting after stream end.
        if usage_seen is None:
            usage_seen = TokenUsage()
        cost = provider.price(binding.model, usage_seen)
        await self._guardrail.consume(
            reservation,
            actual_tokens=usage_seen.total_tokens,
            actual_cost=cost,
        )

    # ------------------------------------------------------------
    # Convenience: chat() — most common case in user code
    # ------------------------------------------------------------

    async def chat(
        self,
        prompt: str,
        *,
        provider: str | None = None,
        model: str | None = None,
        system: str | None = None,
        **kwargs: object,
    ) -> str:
        """Shortcut: send a single user message, return assistant text.

        ``provider`` / ``model`` are optional when the Gateway has
        ``default_provider`` / ``default_model`` configured. Pass
        either to override.
        """
        from agentkit.llm.models import ChatMessage  # noqa: PLC0415 — keeps top-level deps tidy

        req = LLMRequest(
            provider=provider,
            model=model,
            system=system,
            messages=[ChatMessage(role="user", content=prompt)],
            **kwargs,  # type: ignore[arg-type]
        )
        rsp = await self.complete(req)
        return rsp.text

    # ------------------------------------------------------------
    # Offline helpers
    # ------------------------------------------------------------

    def count_tokens(
        self,
        text_or_messages: object,
        *,
        provider: str,
        model: str,
    ) -> int:
        """Offline token estimate for a piece of text / message list."""
        prov = self._providers[provider]
        return prov.count_tokens(text_or_messages, model)  # type: ignore[arg-type]

    def estimate_cost(
        self, usage: TokenUsage, *, provider: str, model: str,
    ) -> float:
        """Informational cost estimate; never used for enforcement."""
        return self._providers[provider].price(model, usage)

    # ------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------

    def _resolve_chain(self, req: LLMRequest) -> list[LLMBinding]:
        """Step 1 — Bind Resolve.

        Priority:

        1. Explicit ``provider`` + ``model`` on the request → single
           binding (no fallback).
        2. ``binding`` name → lookup table → primary + fallback list.
        3. Gateway-level ``default_provider`` + ``default_model``
           (set at construction).

        Returns a non-empty list of bindings to try in order.
        """
        if req.provider and req.model:
            if req.provider not in self._providers:
                raise LLMError(
                    LLMErrorClass.INVALID_REQUEST,
                    f"unknown provider: {req.provider}",
                )
            return [
                LLMBinding(
                    provider=req.provider,
                    model=req.model,
                    timeout_ms=req.timeout_ms,
                ),
            ]

        if req.binding:
            try:
                primary = self._bindings[req.binding]
            except KeyError as e:
                raise LLMError(
                    LLMErrorClass.INVALID_REQUEST,
                    f"unknown binding: {req.binding}",
                ) from e
            chain = [primary, *primary.fallback]
            for b in chain:
                if b.provider not in self._providers:
                    raise LLMError(
                        LLMErrorClass.INVALID_REQUEST,
                        f"binding references unregistered provider: {b.provider}",
                    )
            return chain

        if self._default_provider and self._default_model:
            return [
                LLMBinding(
                    provider=self._default_provider,
                    model=self._default_model,
                    timeout_ms=req.timeout_ms,
                ),
            ]

        raise LLMError(
            LLMErrorClass.INVALID_REQUEST,
            "LLMRequest must specify provider+model or binding, "
            "or the Gateway must have default_provider+default_model configured",
        )

    def _estimate_tokens(self, req: LLMRequest, binding: LLMBinding) -> int:
        """Step 2 — PreCount."""
        prompt_est = estimate_request_tokens(
            messages=req.messages,
            model=binding.model,
            tools=req.tools or None,
            system=req.system,
        )
        completion_est = (
            req.max_tokens
            if req.max_tokens is not None
            else _DEFAULT_MAX_COMPLETION_TOKENS_FOR_ESTIMATE
        )
        return prompt_est + completion_est

    async def _call_one_provider(
        self,
        *,
        req: LLMRequest,
        binding: LLMBinding,
        est_tokens: int,
        deadline_s: float,
    ) -> LLMResponse:
        """Steps 4-5 — RateLimit + retrying provider call.

        Raises :class:`LLMError`. If ``advise_fallback`` is True the
        outer loop in :meth:`complete` will move to the next binding.
        """
        provider = self._providers[binding.provider]
        sub_req = self._copy_with_binding(req, binding)

        await self._rate_limiter.acquire(
            binding.rate_limit,
            provider=binding.provider,
            model=binding.model,
            agent_tag=None,
            tokens_estimated=est_tokens,
            budget_ms=max(1, int((deadline_s - time.monotonic()) * 1000)),
        )

        attempt = 0
        while True:
            attempt += 1
            remaining_ms = int((deadline_s - time.monotonic()) * 1000)
            if remaining_ms <= 0:
                raise LLMError(
                    LLMErrorClass.TIMEOUT,
                    f"timeout exhausted before attempt #{attempt}",
                    provider=binding.provider,
                    model=binding.model,
                )

            try:
                response = await asyncio.wait_for(
                    provider.complete(sub_req),
                    timeout=remaining_ms / 1000.0,
                )
            except asyncio.TimeoutError as e:
                err = LLMError(
                    LLMErrorClass.TIMEOUT,
                    f"per-attempt timeout (attempt {attempt})",
                    provider=binding.provider,
                    model=binding.model,
                )
                decision = decide_retry(err, attempt=attempt, policy=self._retry_policy)
                if not decision.should_retry:
                    raise err from e
                await asyncio.sleep(decision.wait_ms / 1000.0)
                continue
            except LLMError as e:
                decision = decide_retry(e, attempt=attempt, policy=self._retry_policy)
                if not decision.should_retry:
                    raise
                # Honour deadline: don't sleep past the budget.
                if time.monotonic() + decision.wait_ms / 1000.0 >= deadline_s:
                    raise LLMError(
                        LLMErrorClass.TIMEOUT,
                        f"retry backoff would exceed deadline; last err: {e}",
                        provider=binding.provider,
                        model=binding.model,
                    ) from e
                log.warning(
                    "llm.retry",
                    provider=binding.provider,
                    model=binding.model,
                    attempt=attempt,
                    wait_ms=decision.wait_ms,
                    klass=e.klass.value,
                    reason=decision.reason,
                )
                await asyncio.sleep(decision.wait_ms / 1000.0)
                continue

            response.attempts = attempt
            return response

    # ------------------------------------------------------------
    # Step 6 — Provider Fallback (speculative racing)
    # ------------------------------------------------------------

    async def _run_chain_speculatively(
        self,
        *,
        chain: list[LLMBinding],
        req: LLMRequest,
        est_tokens: int,
        deadline_s: float,
        reservation: Reservation,
    ) -> LLMResponse:
        """Run ``chain`` with hedged / speculative provider racing.

        Ordering semantics are preserved: the primary binding
        (``chain[0]``) is launched first and, if it answers before
        :data:`SPECULATIVE_THRESHOLD_MS`, nothing else ever runs — a
        fast primary incurs **zero** extra cost.

        If the current attempt is still pending after the threshold,
        the *next* binding in the chain is launched in parallel via
        :func:`asyncio.wait` (``return_when=FIRST_COMPLETED``). The
        first attempt to *succeed* wins; every other in-flight attempt
        is cancelled and drained. A failure with
        ``advise_fallback=False`` is a final failure — racing stops
        immediately and the error propagates (no further speculation).

        Raises :class:`LLMError` (the last error seen) when every
        binding in the chain has failed with ``advise_fallback=True``.
        """
        races: dict[asyncio.Task[LLMResponse], _Race] = {}
        all_errors: list[tuple[_Race, LLMError]] = []
        next_index = 0
        last_err: LLMError | None = None
        try:
            while True:
                if not races and next_index >= len(chain):
                    break  # nothing left to try and nothing in flight
                if next_index < len(chain) and (not races or self._should_speculate(races)):
                    self._launch_next(
                        races=races, chain=chain, index=next_index,
                        req=req, est_tokens=est_tokens, deadline_s=deadline_s,
                    )
                    next_index += 1

                timeout_s = self._race_wait_timeout_s(races, next_index, len(chain))
                done, _pending = await asyncio.wait(
                    races.keys(), timeout=timeout_s, return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    continue  # speculation threshold elapsed — loop launches the next binding

                outcome = await self._collect_outcome(done, races)
                all_errors.extend(outcome.errors)
                if outcome.response is not None:
                    await self._cancel_races(races)
                    self._record_fallback_metrics(chain, all_errors)
                    return outcome.response
                last_err = outcome.last_err or last_err
                if outcome.fatal_err is not None:
                    await self._cancel_races(races)
                    self._record_fallback_metrics(chain, all_errors)
                    raise outcome.fatal_err

            assert last_err is not None  # noqa: S101
            # All providers in the chain failed with advise_fallback=True.
            self._record_fallback_metrics(chain, all_errors)
            metrics.llm_request_total.labels(
                provider=chain[-1].provider, model=chain[-1].model, result="error",
            ).inc()
            await self._guardrail.release(reservation, reason="all_providers_failed")
            raise last_err
        finally:
            await self._cancel_races(races)

    @staticmethod
    def _should_speculate(races: dict[asyncio.Task[LLMResponse], _Race]) -> bool:
        """True once the *most recently launched* attempt is stale.

        Each newly launched speculative attempt gets its own full
        threshold window before we hedge again — otherwise a slow
        primary would cause every remaining binding in a 3+ chain to
        fire back-to-back instead of being staggered by the threshold.
        """
        newest_started = max(race.started_at for race in races.values())
        elapsed_ms = (time.monotonic() - newest_started) * 1000.0
        return elapsed_ms >= SPECULATIVE_THRESHOLD_MS

    @staticmethod
    def _race_wait_timeout_s(
        races: dict[asyncio.Task[LLMResponse], _Race],
        next_index: int,
        chain_len: int,
    ) -> float | None:
        """How long to wait before re-checking whether to speculate.

        Returns ``None`` (wait indefinitely) once every binding is
        already in flight — there is nothing left to speculate into.
        Otherwise bounds the wait by how much of the *newest* attempt's
        threshold window remains (mirrors :meth:`_should_speculate`).
        """
        if next_index >= chain_len or not races:
            return None
        newest_started = max(race.started_at for race in races.values())
        remaining_ms = SPECULATIVE_THRESHOLD_MS - (time.monotonic() - newest_started) * 1000.0
        return max(0.0, remaining_ms / 1000.0)

    def _launch_next(
        self,
        *,
        races: dict[asyncio.Task[LLMResponse], _Race],
        chain: list[LLMBinding],
        index: int,
        req: LLMRequest,
        est_tokens: int,
        deadline_s: float,
    ) -> None:
        """Start attempt #``index`` of ``chain`` and register it as a race."""
        binding = chain[index]
        if index > 0:
            log.info(
                "llm.speculative.launch",
                provider=binding.provider,
                model=binding.model,
                chain_index=index,
                threshold_ms=SPECULATIVE_THRESHOLD_MS,
                run_id=req.run_id_,
                agent_id=req.agent_id_,
                trace_id=req.trace_id_,
            )
        task = asyncio.ensure_future(
            self._call_one_provider(
                req=req, binding=binding, est_tokens=est_tokens, deadline_s=deadline_s,
            ),
        )
        races[task] = _Race(binding=binding, index=index, started_at=time.monotonic())

    async def _collect_outcome(
        self,
        done: set[asyncio.Task[LLMResponse]],
        races: dict[asyncio.Task[LLMResponse], _Race],
    ) -> _Outcome:
        """Pop completed tasks out of ``races`` and classify the result.

        Successes win outright. Failures are logged / counted; a
        ``advise_fallback=False`` failure is reported as ``fatal_err``
        so the caller stops racing immediately.
        """
        winner: LLMResponse | None = None
        winner_race: _Race | None = None
        fatal_err: LLMError | None = None
        last_err: LLMError | None = None
        errors: list[tuple[_Race, LLMError]] = []

        for task in done:
            race = races.pop(task)
            try:
                response = task.result()
            except LLMError as e:
                last_err = e
                errors.append((race, e))
                self._log_error(e, binding=race.binding)
                metrics.llm_error_total.labels(
                    provider=race.binding.provider,
                    model=race.binding.model,
                    klass=e.klass.value if hasattr(e.klass, "value") else str(e.klass),
                ).inc()
                if not e.advise_fallback and fatal_err is None:
                    metrics.llm_request_total.labels(
                        provider=race.binding.provider, model=race.binding.model, result="error",
                    ).inc()
                    fatal_err = e
            else:
                if winner_race is None or race.started_at < winner_race.started_at:
                    # First success, or — in the rare case two
                    # attempts complete in the same wait() wakeup —
                    # the one that started earliest wins (closest to
                    # the chain's original ordering semantics). The
                    # other success is simply slower, not an error;
                    # it is dropped silently.
                    winner = response
                    winner_race = race

        return _Outcome(
            response=winner, fatal_err=fatal_err, last_err=last_err, errors=errors,
        )

    @staticmethod
    def _record_fallback_metrics(
        chain: list[LLMBinding],
        errors: list[tuple[_Race, LLMError]],
    ) -> None:
        """Attribute ``llm_fallback_total`` for bindings that lost the race.

        Each binding can fail at most once across the whole chain run,
        so iterating ``errors`` here can never double-count: this is
        the single place fallback transitions are recorded, whether
        the next binding was launched serially (after a failure) or
        speculatively (after the threshold elapsed).
        """
        for race, err in errors:
            next_index = race.index + 1
            if next_index >= len(chain):
                continue
            to_binding = chain[next_index]
            metrics.llm_fallback_total.labels(
                from_provider=race.binding.provider,
                to_provider=to_binding.provider,
                klass=err.klass.value if hasattr(err.klass, "value") else str(err.klass),
            ).inc()

    @staticmethod
    async def _cancel_races(races: dict[asyncio.Task[LLMResponse], _Race]) -> None:
        """Cancel and drain every still-pending race so nothing leaks.

        Awaiting cancelled tasks (with exceptions suppressed) prevents
        ``Task was destroyed but it is pending`` warnings.
        """
        if not races:
            return
        pending = list(races.keys())
        races.clear()
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    def _log_success(self, req: LLMRequest, response: LLMResponse) -> None:
        """Doc10 §4.5 — record success metrics + structured log."""
        metrics.llm_request_total.labels(
            provider=response.provider, model=response.model, result="ok",
        ).inc()
        metrics.llm_request_duration_seconds.labels(
            provider=response.provider, model=response.model,
        ).observe((response.latency_ms or 0) / 1000.0)
        metrics.llm_request_attempts.labels(
            provider=response.provider, model=response.model,
        ).observe(response.attempts)
        metrics.llm_tokens_total.labels(
            provider=response.provider, model=response.model, kind="prompt",
        ).inc(response.usage.prompt_tokens)
        metrics.llm_tokens_total.labels(
            provider=response.provider, model=response.model, kind="completion",
        ).inc(response.usage.completion_tokens)
        if response.cost_usd:
            metrics.llm_cost_usd_total.labels(
                provider=response.provider, model=response.model,
            ).inc(response.cost_usd)
        log.info(
            "llm.complete.ok",
            provider=response.provider,
            model=response.model,
            attempts=response.attempts,
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
            cost_usd=response.cost_usd,
            latency_ms=response.latency_ms,
            run_id=req.run_id_,
            agent_id=req.agent_id_,
            trace_id=req.trace_id_,
        )

    @staticmethod
    def _copy_with_binding(req: LLMRequest, binding: LLMBinding) -> LLMRequest:
        """Return a copy of ``req`` with ``provider/model`` filled from ``binding``."""
        # Apply binding ``params`` (sampling overrides) when the
        # request didn't specify them itself. We only copy a small
        # whitelist to avoid surprising overrides.
        merged_extra = dict(req.extra)
        for k, v in binding.params.items():
            merged_extra.setdefault(k, v)

        return req.model_copy(
            update={
                "provider": binding.provider,
                "model": binding.model,
                "extra": merged_extra,
            },
        )

    def _log_error(self, err: LLMError, *, binding: LLMBinding) -> None:
        log.warning(
            "llm.provider.failed",
            provider=binding.provider,
            model=binding.model,
            klass=err.klass.value,
            http_status=err.http_status,
            advise_fallback=err.advise_fallback,
            message=str(err),
        )
