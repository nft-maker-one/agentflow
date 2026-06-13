"""In-process guardrail — enforces token / cycle quotas without Redis.

Designed for the API server (and local dev) where a Redis backend is
not available.  One shared singleton tracks all in-flight Runs.

Usage::

    guardrail = InProcessGuardrail()
    # At create_run:
    guardrail.register_run(run_id, context)
    # At run termination (Succeeded / Failed / Cancelled):
    guardrail.unregister_run(run_id)

The instance is passed to BOTH:
* ``LLMGateway`` (tracks actual tokens via ``consume``)
* ``AgentWorker`` → ``AgentInstance`` (tracks cycles)

so run-level totals are accumulated in one place.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from agentkit.common.logging import get_logger
from agentkit.guardrail.errors import QuotaExceeded
from agentkit.guardrail.models import GuardrailContext
from agentkit.llm.guardrail_iface import Reservation

log = get_logger(__name__)


@dataclass
class _RunState:
    ctx: GuardrailContext
    used_tokens: int = 0
    used_cycles: int = 0
    used_cost_usd: float = 0.0
    #: template_key → cycles used by THAT agent (for per-agent caps).
    used_cycles_by_agent: dict[str, int] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class InProcessGuardrail:
    """Thread-safe (asyncio) in-process guardrail.

    Satisfies the :class:`~agentkit.llm.guardrail_iface.GuardrailHandle`
    Protocol, so it can be passed to ``LLMGateway`` and
    ``AgentWorker`` directly.
    """

    def __init__(self) -> None:
        # run_id → _RunState. register_run / unregister_run are called
        # serially by the Orchestrator (one caller per run), and each
        # run's mutations are guarded by its own ``_RunState.lock``, so
        # no separate global lock is needed.
        self._runs: dict[str, _RunState] = {}

    # ------------------------------------------------------------------
    # Lifecycle — called by AppState / Orchestrator
    # ------------------------------------------------------------------

    def register_run(self, run_id: str, ctx: GuardrailContext) -> None:
        """Register quota context for a new run.  Idempotent."""
        if run_id not in self._runs:
            self._runs[run_id] = _RunState(ctx=ctx)
            log.debug(
                "guardrail.run.registered",
                run_id=run_id,
                max_tokens=ctx.run.max_total_tokens,
                max_cycles=ctx.run.max_cycles_per_run,
                max_tokens_per_call=ctx.agent.max_tokens_per_call,
            )

    def unregister_run(self, run_id: str) -> None:
        """Drop tracking state for a terminated run (optional — GC)."""
        self._runs.pop(run_id, None)

    def get_usage(self, run_id: str) -> dict:
        """Return current usage snapshot for UI / API."""
        rs = self._runs.get(run_id)
        if rs is None:
            return {}
        return {
            "run_id": run_id,
            "used_tokens": rs.used_tokens,
            "used_cycles": rs.used_cycles,
            "used_cost_usd": rs.used_cost_usd,
            "limit_tokens": rs.ctx.run.max_total_tokens,
            "limit_cycles": rs.ctx.run.max_cycles_per_run,
            "limit_tokens_per_call": rs.ctx.agent.max_tokens_per_call,
        }

    # ------------------------------------------------------------------
    # GuardrailHandle Protocol
    # ------------------------------------------------------------------

    async def precheck(
        self,
        *,
        run_id: str,
        agent_id: str,
        est_tokens: int,
        cycle_inc: int = 1,
        dry_charge: bool = False,
        template_key: str = "",
    ) -> Reservation:
        rs = self._runs.get(run_id)
        if rs is None:
            # Unknown run (e.g. started before guardrail was active) —
            # fail-open: issue a reservation but enforce nothing.
            return Reservation(
                run_id=run_id,
                agent_id=agent_id,
                template_key=template_key,
                est_tokens=est_tokens,
                cycle_inc=cycle_inc,
                dry_charge=dry_charge,
            )

        # Per-agent ("this agent only") override wins over the run-wide
        # agent default when set for this template_key.
        agent_q = rs.ctx.agent_overrides.get(template_key, rs.ctx.agent)

        async with rs.lock:
            # ── per-call token cap (per-agent) ──────────────────────
            call_cap = agent_q.max_tokens_per_call
            if est_tokens > call_cap:
                raise QuotaExceeded(
                    "agent", "tokens",
                    used=0,
                    limit=call_cap,
                    increment=est_tokens,
                )

            # ── per-agent cycle cap (this agent's invocations) ──────
            agent_cyc_cap = agent_q.max_cycles
            used_agent_cyc = rs.used_cycles_by_agent.get(template_key, 0)
            if (not dry_charge and template_key
                    and used_agent_cyc + cycle_inc > agent_cyc_cap):
                raise QuotaExceeded(
                    "agent", "cycles",
                    used=used_agent_cyc,
                    limit=agent_cyc_cap,
                    increment=cycle_inc,
                )

            # ── run-level token cap ─────────────────────────────────
            tok_cap = rs.ctx.run.max_total_tokens
            if not dry_charge and rs.used_tokens + est_tokens > tok_cap:
                raise QuotaExceeded(
                    "run", "tokens",
                    used=rs.used_tokens,
                    limit=tok_cap,
                    increment=est_tokens,
                )

            # ── run-level cycle cap ─────────────────────────────────
            cyc_cap = rs.ctx.run.max_cycles_per_run
            if not dry_charge and rs.used_cycles + cycle_inc > cyc_cap:
                raise QuotaExceeded(
                    "run", "cycles",
                    used=rs.used_cycles,
                    limit=cyc_cap,
                    increment=cycle_inc,
                )

        return Reservation(
            run_id=run_id,
            agent_id=agent_id,
            template_key=template_key,
            est_tokens=est_tokens,
            cycle_inc=cycle_inc,
            dry_charge=dry_charge,
        )

    async def consume(
        self,
        reservation: Reservation,
        *,
        actual_tokens: int,
        actual_cost: float = 0.0,
    ) -> None:
        if reservation.dry_charge:
            return
        rs = self._runs.get(reservation.run_id)
        if rs is None:
            return
        async with rs.lock:
            rs.used_tokens += actual_tokens
            rs.used_cycles += reservation.cycle_inc
            if reservation.template_key:
                rs.used_cycles_by_agent[reservation.template_key] = (
                    rs.used_cycles_by_agent.get(reservation.template_key, 0)
                    + reservation.cycle_inc
                )
            rs.used_cost_usd += actual_cost
            log.debug(
                "guardrail.run.consumed",
                run_id=reservation.run_id,
                delta_tokens=actual_tokens,
                delta_cycles=reservation.cycle_inc,
                used_tokens=rs.used_tokens,
                used_cycles=rs.used_cycles,
            )

    async def release(self, reservation: Reservation, *, reason: str) -> None:
        # In-process: nothing to free (no optimistic reservation deduction).
        log.debug(
            "guardrail.reservation.released",
            reservation_id=reservation.id,
            run_id=reservation.run_id,
            reason=reason,
        )
