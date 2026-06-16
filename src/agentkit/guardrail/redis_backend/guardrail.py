"""Redis-backed Guardrail — the production implementation of the Doc07 contract.

Satisfies :class:`agentkit.llm.guardrail_iface.GuardrailHandle` so
the LLM Gateway can swap from :class:`NoOpGuardrail` to this class
with zero code changes::

    from agentkit.guardrail import RedisGuardrail, GuardrailSettings
    settings = GuardrailSettings()                # reads env vars
    guardrail = RedisGuardrail(settings=settings)
    await guardrail.start()
    gateway = LLMGatewayClient(..., guardrail=guardrail)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError

from agentkit.common.logging import get_logger
from agentkit.common.time import utcnow
from agentkit.guardrail.errors import GuardrailUnavailable, QuotaExceeded
from agentkit.guardrail.models import GuardrailContext, RunUsage
from agentkit.guardrail.redis_backend.config import GuardrailSettings
from agentkit.guardrail.redis_backend.scripts import (
    COST_SCALE,
    CONSUME_LUA,
    PRECHECK_LUA,
    RELEASE_LUA,
)
from agentkit.llm.guardrail_iface import Reservation

log = get_logger(__name__)


# ---- breach reasons returned by the precheck Lua ----

_PRECHECK_BREACH_TO_LAYER_DIM: dict[str, tuple[str, str]] = {
    "agent_tokens": ("agent", "tokens"),
    "agent_cycles": ("agent", "cycles"),
    "run_tokens":   ("run",   "tokens"),
    "run_cycles":   ("run",   "cycles"),
    "agent_exhausted": ("agent", "tokens"),  # post-consume sticky touch
    "run_exhausted":   ("run",   "tokens"),
}


def _to_str(v: Any) -> str:
    """Decode a possibly-bytes value into ``str``.

    redis-py's ``decode_responses=True`` is not honored uniformly
    by every backend (fakeredis ≤ 2.35 returns bytes from HGETALL
    even when configured otherwise). Production reads go through
    this helper so the rest of the module can safely treat values
    as ``str``.
    """
    if v is None:
        return ""
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return str(v)


def _decode_hash(d: dict[Any, Any] | None) -> dict[str, str]:
    if not d:
        return {}
    return {_to_str(k): _to_str(v) for k, v in d.items()}


class RedisGuardrail:
    """Production Guardrail backed by Redis Lua scripts.

    Lifecycle::

        guardrail = RedisGuardrail(settings)
        await guardrail.start()
        ...
        await guardrail.init_run_quota(ctx)         # at create_run
        # use guardrail.precheck / consume / release inline
        await guardrail.finalize_run(run_id)        # at run terminal
        ...
        await guardrail.stop()

    All write paths are idempotent: re-init / re-finalize are safe.
    """

    def __init__(
        self,
        *,
        settings: GuardrailSettings | None = None,
        redis: Redis | None = None,
    ) -> None:
        self._settings = settings or GuardrailSettings()
        self._redis: Redis | None = redis
        self._owns_redis = redis is None
        self._scripts_loaded: bool = False
        # Cache the per-run effective quotas — Redis only stores
        # *usage*, not limits. Limits are static for a Run's lifetime.
        self._contexts: dict[str, GuardrailContext] = {}

    # ------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------

    async def start(self) -> None:
        if self._redis is None:
            self._redis = Redis.from_url(
                self._settings.redis_url, decode_responses=True,
            )
        # Verify connectivity early so misconfig fails fast.
        try:
            await self._redis.ping()
        except RedisError as e:
            log.warning(
                "guardrail.redis.unreachable",
                fail_mode=self._settings.fail_mode,
                error=str(e),
            )
            if self._settings.is_strict:
                raise GuardrailUnavailable(str(e)) from e
        self._scripts_loaded = True

    async def stop(self) -> None:
        if self._owns_redis and self._redis is not None:
            try:
                await self._redis.aclose()
            except RedisError:
                log.exception("guardrail.redis.close_failed")

    # ------------------------------------------------------------
    # Run lifecycle
    # ------------------------------------------------------------

    async def init_run_quota(self, ctx: GuardrailContext) -> None:
        """Seed the run hash with metadata and TTL.

        Idempotent — if a hash already exists with the same run_id,
        we update meta but keep usage counters intact.
        """
        self._contexts[ctx.run_id] = ctx
        if self._redis is None:
            return  # Will fail-open at next call site.

        run_key = self._run_key(ctx.run_id)
        try:
            await self._redis.hset(
                run_key,
                mapping={
                    "workflow_id": ctx.workflow_id,
                    "owner": ctx.owner or "",
                    "project": ctx.project or "",
                    "limit_tokens": ctx.run.max_total_tokens,
                    "limit_cycles": ctx.run.max_cycles_per_run,
                    "limit_agent_tokens": ctx.agent.max_tokens_per_call,
                    "limit_agent_cycles": ctx.agent.max_cycles,
                    "started_at": utcnow().isoformat(),
                },
            )
            await self._redis.expire(
                run_key, self._settings.run_hash_ttl_seconds,
            )
        except RedisError as e:
            self._handle_redis_error("init_run_quota", e)

        log.info(
            "guardrail.run.init",
            run_id=ctx.run_id,
            workflow_id=ctx.workflow_id,
            limits={
                "run_tokens": ctx.run.max_total_tokens,
                "run_cycles": ctx.run.max_cycles_per_run,
                "agent_tokens": ctx.agent.max_tokens_per_call,
                "agent_cycles": ctx.agent.max_cycles,
            },
        )

    async def finalize_run(self, run_id: str) -> RunUsage:
        """Compute the final RunUsage snapshot.

        Phase 1: returns the snapshot in-memory. Phase 2 will also
        flush the audit row to Postgres (Doc10).
        """
        usage = await self.get_run_usage(run_id)
        # Drop the per-run context cache; Redis hash lingers per
        # ``run_hash_ttl_seconds`` so UIs can still query it.
        self._contexts.pop(run_id, None)
        log.info(
            "guardrail.run.finalize",
            run_id=run_id,
            used_tokens=usage.used_tokens,
            used_cycles=usage.used_cycles,
            used_cost_usd=usage.used_cost_usd,
            quota_exhausted=usage.quota_exhausted,
        )
        return usage

    # ------------------------------------------------------------
    # Pre/Post-charge — the GuardrailHandle Protocol
    # ------------------------------------------------------------

    async def precheck(
        self,
        *,
        run_id: str,
        agent_id: str,
        est_tokens: int,
        cycle_inc: int = 1,
        dry_charge: bool = False,
        timeout_ms: int | None = None,
        template_key: str = "",   # accepted for Protocol compat; per-agent
                                  # overrides are an InProcess feature for now
    ) -> Reservation:
        """Atomic two-layer pre-reservation.

        Raises :class:`QuotaExceeded` on cap touch — caller (LLM
        Gateway / Runtime Gate 3) maps to a Failure transition.

        Replay events bypass the Lua mutation entirely (``dry_charge=True``)
        — Doc07 §3.4.
        """
        ctx = self._contexts.get(run_id)
        if ctx is None:
            raise GuardrailUnavailable(
                f"guardrail.precheck called before init_run_quota for run_id={run_id!r}",
            )

        ttl_ms = self._reservation_ttl_ms(timeout_ms)
        reservation = Reservation(
            run_id=run_id,
            agent_id=agent_id,
            est_tokens=est_tokens,
            cycle_inc=cycle_inc,
            dry_charge=dry_charge,
            workflow_id=ctx.workflow_id,
            issued_at=utcnow(),
            expires_at=utcnow() + timedelta(milliseconds=ttl_ms),
        )

        if dry_charge:
            # Replay path — no Redis mutation.
            log.debug(
                "guardrail.precheck.dry_charge", run_id=run_id, agent_id=agent_id,
            )
            return reservation

        if self._redis is None:
            return self._fail_open_reservation(
                reservation, reason="redis_not_started",
            )

        run_key = self._run_key(run_id)
        agent_key = self._agent_key(run_id, agent_id)
        rsv_key = self._reservation_key(reservation.id)

        try:
            result = await self._redis.eval(
                PRECHECK_LUA,
                3,
                run_key, agent_key, rsv_key,
                est_tokens,
                cycle_inc,
                ctx.run.max_total_tokens,
                ctx.run.max_cycles_per_run,
                ctx.agent.max_tokens_per_call,
                ctx.agent.max_cycles,
                ttl_ms,
                reservation.id,
                self._settings.run_hash_ttl_seconds,
            )
        except RedisError as e:
            return self._fail_open_or_raise(
                "precheck", e, reservation,
            )

        ok, label = self._unpack_lua_pair(result)

        if ok == 1:
            log.debug(
                "guardrail.precheck.ok",
                run_id=run_id, agent_id=agent_id,
                est_tokens=est_tokens, reservation_id=reservation.id,
            )
            return reservation

        layer, dim = _PRECHECK_BREACH_TO_LAYER_DIM[label]
        used_after = await self._read_used(run_id, agent_id, layer, dim)
        limit = self._lookup_limit(ctx, layer, dim)
        log.warning(
            "guardrail.precheck.deny",
            run_id=run_id, agent_id=agent_id,
            layer=layer, dim=dim,
            used=used_after, limit=limit,
            est_tokens=est_tokens,
        )
        raise QuotaExceeded(
            layer=layer, dim=dim,
            used=used_after, limit=limit,
            increment=est_tokens if dim == "tokens" else cycle_inc,
        )

    async def consume(
        self,
        reservation: Reservation,
        *,
        actual_tokens: int,
        actual_cost: float = 0.0,
    ) -> None:
        """Adjust counters by ``actual − est`` and accumulate cost.

        ``actual_cost`` is informational only (Doc07 §3.2) — never
        compared against a limit; we just sum it for telemetry.
        """
        if reservation.dry_charge:
            return  # No Redis state to adjust.

        ctx = self._contexts.get(reservation.run_id)
        if ctx is None:
            log.warning(
                "guardrail.consume.no_context",
                run_id=reservation.run_id,
                reservation_id=reservation.id,
            )
            return

        if self._redis is None:
            log.warning(
                "guardrail.consume.no_redis",
                run_id=reservation.run_id,
                actual_tokens=actual_tokens,
            )
            return

        run_key = self._run_key(reservation.run_id)
        agent_key = self._agent_key(reservation.run_id, reservation.agent_id)
        rsv_key = self._reservation_key(reservation.id)
        cost_centi = int(round(actual_cost * COST_SCALE))

        try:
            result = await self._redis.eval(
                CONSUME_LUA,
                3,
                run_key, agent_key, rsv_key,
                actual_tokens,
                cost_centi,
                ctx.run.max_total_tokens,
                ctx.run.max_cycles_per_run,
                ctx.agent.max_tokens_per_call,
                ctx.agent.max_cycles,
                "0",
            )
        except RedisError as e:
            self._handle_redis_error("consume", e)
            return

        # result = [used_tokens, used_cycles, used_cost_centi, exhausted, late]
        used_tokens, used_cycles, used_cost_centi, exhausted, late = (
            int(result[0]),
            int(result[1]),
            int(result[2]),
            int(result[3]),
            int(result[4]),
        )
        log.info(
            "guardrail.consume.ok",
            run_id=reservation.run_id,
            agent_id=reservation.agent_id,
            actual_tokens=actual_tokens,
            actual_cost=actual_cost,
            used_tokens=used_tokens,
            used_cycles=used_cycles,
            used_cost_usd=used_cost_centi / COST_SCALE,
            exhausted=bool(exhausted),
            late_consume=bool(late),
        )

    async def release(
        self, reservation: Reservation, *, reason: str,
    ) -> None:
        """Roll back a reservation that didn't reach consume()."""
        if reservation.dry_charge or self._redis is None:
            return

        run_key = self._run_key(reservation.run_id)
        agent_key = self._agent_key(reservation.run_id, reservation.agent_id)
        rsv_key = self._reservation_key(reservation.id)

        try:
            result = await self._redis.eval(
                RELEASE_LUA, 3, run_key, agent_key, rsv_key,
            )
        except RedisError as e:
            self._handle_redis_error("release", e)
            return

        rolled_back = int(result[0]) if result else 0
        log.debug(
            "guardrail.release",
            run_id=reservation.run_id,
            agent_id=reservation.agent_id,
            reason=reason,
            rolled_back=bool(rolled_back),
            est_tokens=int(result[1]) if result and rolled_back else 0,
        )

    # ------------------------------------------------------------
    # Inspection — RunUsage
    # ------------------------------------------------------------

    async def get_run_usage(self, run_id: str) -> RunUsage:
        ctx = self._contexts.get(run_id)
        limits_tokens = ctx.run.max_total_tokens if ctx else 0
        limits_cycles = ctx.run.max_cycles_per_run if ctx else 0

        if self._redis is None:
            return RunUsage(
                run_id=run_id,
                limits_tokens=limits_tokens,
                limits_cycles=limits_cycles,
            )

        run_key = self._run_key(run_id)
        try:
            raw = await self._redis.hgetall(run_key)
        except RedisError as e:
            self._handle_redis_error("get_run_usage", e)
            return RunUsage(
                run_id=run_id,
                limits_tokens=limits_tokens,
                limits_cycles=limits_cycles,
            )

        data = _decode_hash(raw)
        return RunUsage(
            run_id=run_id,
            used_tokens=int(data.get("tokens_used", 0) or 0),
            used_cycles=int(data.get("cycles_used", 0) or 0),
            used_cost_usd=int(data.get("cost_used_centi", 0) or 0) / COST_SCALE,
            limits_tokens=limits_tokens,
            limits_cycles=limits_cycles,
            quota_exhausted=str(data.get("quota_exhausted", "0")) == "1",
            reservations_total=int(data.get("reservations_total", 0) or 0),
        )

    # ------------------------------------------------------------
    # Internals — keys
    # ------------------------------------------------------------

    def _run_key(self, run_id: str) -> str:
        return f"{self._settings.namespace_prefix}:run:{run_id}"

    def _agent_key(self, run_id: str, agent_id: str) -> str:
        # Scoped by run_id so the per-agent cycle/token ledger resets each
        # run (and expires with the run hash). Keying by agent_id alone made
        # the per-agent ``max_cycles`` a *lifetime* cap that never reset —
        # an instance was permanently denied after ``max_cycles`` events
        # across all runs. The InProcess backend was always per-run; this
        # aligns Redis with it.
        return f"{self._settings.namespace_prefix}:agent:{run_id}:{agent_id}"

    def _reservation_key(self, reservation_id: str) -> str:
        return f"{self._settings.namespace_prefix}:reservation:{reservation_id}"

    # ------------------------------------------------------------
    # Internals — TTL
    # ------------------------------------------------------------

    def _reservation_ttl_ms(self, request_timeout_ms: int | None) -> int:
        if request_timeout_ms is not None and request_timeout_ms > 0:
            return int(request_timeout_ms * self._settings.reservation_ttl_factor)
        return self._settings.default_reservation_ttl_ms

    # ------------------------------------------------------------
    # Internals — fail-mode handling
    # ------------------------------------------------------------

    def _handle_redis_error(self, op: str, e: RedisError) -> None:
        """Log + decide whether to re-raise based on fail_mode."""
        log.warning(
            "guardrail.redis.error",
            op=op, fail_mode=self._settings.fail_mode, error=str(e),
        )
        if self._settings.is_strict:
            raise GuardrailUnavailable(str(e)) from e

    def _fail_open_or_raise(
        self, op: str, e: RedisError, reservation: Reservation,
    ) -> Reservation:
        if self._settings.is_strict:
            log.warning("guardrail.fail_strict", op=op, error=str(e))
            raise GuardrailUnavailable(str(e)) from e
        return self._fail_open_reservation(reservation, reason=str(e))

    def _fail_open_reservation(
        self, reservation: Reservation, *, reason: str,
    ) -> Reservation:
        log.warning(
            "guardrail.fail_open",
            reservation_id=reservation.id, reason=reason,
        )
        # Mark via the pydantic ``extra="allow"`` field so audit
        # picks it up later.
        return reservation.model_copy(update={"bypassed_reason": reason})

    # ------------------------------------------------------------
    # Internals — Lua return parsing
    # ------------------------------------------------------------

    @staticmethod
    def _unpack_lua_pair(result: Any) -> tuple[int, str]:
        """Coerce ``[ok, label]`` from a Lua return."""
        if isinstance(result, list | tuple) and len(result) == 2:
            return int(result[0]), str(result[1])
        raise GuardrailUnavailable(
            f"unexpected Lua return shape: {result!r}",
        )

    # ------------------------------------------------------------
    # Internals — usage / limit lookup helpers
    # ------------------------------------------------------------

    async def _read_used(
        self, run_id: str, agent_id: str, layer: str, dim: str,
    ) -> int:
        if self._redis is None:
            return 0
        key = self._run_key(run_id) if layer == "run" else self._agent_key(run_id, agent_id)
        field = "tokens_used" if dim == "tokens" else "cycles_used"
        try:
            v = await self._redis.hget(key, field)
        except RedisError:
            return 0
        return int(_to_str(v) or 0)

    @staticmethod
    def _lookup_limit(ctx: GuardrailContext, layer: str, dim: str) -> int:
        if layer == "run":
            return (
                ctx.run.max_total_tokens
                if dim == "tokens"
                else ctx.run.max_cycles_per_run
            )
        return (
            ctx.agent.max_tokens_per_call
            if dim == "tokens"
            else ctx.agent.max_cycles
        )
