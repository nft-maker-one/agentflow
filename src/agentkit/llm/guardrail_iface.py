"""Guardrail handle Protocol — keeps LLM Gateway decoupled from Doc07.

The Gateway pre-checks token / cycle quotas before each call and
reports actual consumption afterwards. To avoid an actual import
dependency on :mod:`agentkit.guardrail` (which arrives in a later
module), we define the *minimal* shape the Gateway needs here.

When :mod:`agentkit.guardrail` lands, its real ``Guardrail`` class
will satisfy this Protocol structurally (Python's duck typing) —
no edits to LLM gateway code needed.

The :class:`NoOpGuardrail` is a real production-ready default for
local development / unit tests; it never blocks a call but tracks
totals so the Gateway's telemetry remains accurate.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

from agentkit.common.ids import new_reservation_id
from agentkit.common.time import utcnow


class Reservation(BaseModel):
    """A pre-reservation of token / cycle budget for a single call.

    The real Guardrail (Doc07) returns this from :meth:`precheck`;
    the Gateway hands the same object back to :meth:`consume`.
    """

    model_config = ConfigDict(extra="allow")

    id: str = Field(default_factory=new_reservation_id)
    run_id: str = ""
    agent_id: str = ""
    workflow_id: str = ""
    #: Agent template key — used to apply per-agent guardrail overrides
    #: and to track per-agent cycle counts.
    template_key: str = ""
    est_tokens: int = 0
    cycle_inc: int = 1
    dry_charge: bool = False
    issued_at: datetime = Field(default_factory=utcnow)
    expires_at: datetime | None = None


@runtime_checkable
class GuardrailHandle(Protocol):
    """The minimal Guardrail surface the Gateway needs."""

    async def precheck(
        self,
        *,
        run_id: str,
        agent_id: str,
        est_tokens: int,
        cycle_inc: int = 1,
        dry_charge: bool = False,
        template_key: str = "",
    ) -> Reservation: ...

    async def consume(
        self,
        reservation: Reservation,
        *,
        actual_tokens: int,
        actual_cost: float = 0.0,
    ) -> None: ...

    async def release(self, reservation: Reservation, *, reason: str) -> None: ...


class NoOpGuardrail:
    """No-op Guardrail used when no quota enforcement is configured.

    Maintains an in-process running total so :func:`get_run_usage`-style
    inspection in tests and dev REPLs stays accurate.
    """

    def __init__(self) -> None:
        self._used_tokens_by_run: dict[str, int] = {}
        self._used_cost_by_run: dict[str, float] = {}
        self._used_cycles_by_run: dict[str, int] = {}

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
        run = reservation.run_id
        self._used_tokens_by_run[run] = (
            self._used_tokens_by_run.get(run, 0) + actual_tokens
        )
        self._used_cost_by_run[run] = (
            self._used_cost_by_run.get(run, 0.0) + actual_cost
        )
        self._used_cycles_by_run[run] = (
            self._used_cycles_by_run.get(run, 0) + reservation.cycle_inc
        )

    async def release(self, reservation: Reservation, *, reason: str) -> None:
        # No persistent state to free.
        del reservation, reason

    # ---- inspection helpers (not part of the Protocol) ----

    def used_tokens(self, run_id: str) -> int:
        return self._used_tokens_by_run.get(run_id, 0)

    def used_cost(self, run_id: str) -> float:
        return self._used_cost_by_run.get(run_id, 0.0)

    def used_cycles(self, run_id: str) -> int:
        return self._used_cycles_by_run.get(run_id, 0)
