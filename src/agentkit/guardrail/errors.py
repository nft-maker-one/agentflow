"""Guardrail-specific exceptions.

Note ``QuotaExceeded`` is a *permanent* failure — Runtime FSM routes
straight to ``Failure`` (not ``Retry``); a quota touch is not
self-healing within the same Run.
"""

from __future__ import annotations

from typing import Literal

from agentkit.common.errors import (
    AgentKitError,
    PermanentError,
    TransientError,
)


class GuardrailError(AgentKitError):
    """Base for guardrail-layer errors."""


# A guardrail-touched event class users may catch / log without
# importing ``QuotaExceeded`` itself. Permanent because retrying
# the same call after a quota touch will fail the same way.
class QuotaExceeded(PermanentError, GuardrailError):
    """Raised by ``precheck`` when a quota would be breached.

    ``layer`` ∈ {"agent", "run"} — which scope ran out.
    ``dim``   ∈ {"tokens", "cycles"} — which dimension.
    Caller code can show the user a precise diagnosis.
    """

    def __init__(
        self,
        layer: Literal["agent", "run"],
        dim: Literal["tokens", "cycles"],
        *,
        used: int,
        limit: int,
        increment: int,
    ) -> None:
        msg = (
            f"guardrail.{layer}.{dim} would exceed limit: "
            f"used={used} + delta={increment} > limit={limit}"
        )
        super().__init__(msg)
        self.layer = layer
        self.dim = dim
        self.used = used
        self.limit = limit
        self.increment = increment


class GuardrailUnavailable(TransientError, GuardrailError):
    """Backend (Redis) is unreachable.

    Whether this *blocks* or *fail-opens* depends on the configured
    ``fail_mode`` — see ``RedisGuardrail`` and Doc07 §5.3.
    """
