"""4-gate ingress pipeline.

Per ``Doc03 §6.1`` every Envelope passes through (in order):

1. **TagFilter** — agent tags must satisfy ``event.to_filter.tags``.
2. **SchemaCheck** — payload must validate against ``schema_in``.
3. **Guardrail.precheck** — Run-level token / cycle quota.
4. **Dedup** — drop replays seen within the window.

Each gate returns a :class:`GatingVerdict`; a non-pass verdict
short-circuits with a structured reason. The dispatch loop turns
those into different actions:

* ``DROP`` (Gate 1, Gate 4)            → ack + metric.
* ``DLQ`` (Gate 2)                      → send to DLQ + metric.
* ``GUARDRAIL_BLOCK`` (Gate 3)          → FSM ``Failure`` (no Retry).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import jsonschema

from agentkit.common.jsonschema_cache import validate_cached
from agentkit.common.logging import get_logger
from agentkit.llm.guardrail_iface import GuardrailHandle, Reservation
from agentkit.models.envelope import Envelope
from agentkit.runtime.dedup import DedupStore

log = get_logger(__name__)


# ============================================================
# Gate verdict + result
# ============================================================


class GatingVerdict(StrEnum):
    PASS = "pass"
    DROP_TAG = "drop_tag"
    DROP_DEDUP = "drop_dedup"
    DLQ_SCHEMA = "dlq_schema"
    GUARDRAIL_BLOCK = "guardrail_block"


@dataclass(frozen=True)
class GatingResult:
    """The outcome of running an envelope through all 4 gates."""

    verdict: GatingVerdict
    reason: str = ""
    reservation: Reservation | None = None  # populated only when Gate 3 ran successfully

    @property
    def passed(self) -> bool:
        return self.verdict is GatingVerdict.PASS

    @property
    def should_dlq(self) -> bool:
        return self.verdict is GatingVerdict.DLQ_SCHEMA

    @property
    def is_guardrail_block(self) -> bool:
        return self.verdict is GatingVerdict.GUARDRAIL_BLOCK


# ============================================================
# Gate 1 — Tag filter
# ============================================================


def gate_tag(envelope: Envelope, *, agent_tags: dict[str, str]) -> GatingResult:
    """Drop events whose ``to_filter.tags`` aren't a subset of agent's tags.

    Semantics (per Doc03 §5.2): event ``to_filter.tags`` express "what
    tags the recipient must have". The agent's own tags are the
    *candidate* — we accept the event iff every required (k,v) is
    present in the agent's tags.
    """
    required = envelope.to_filter.tags
    if not required:
        return GatingResult(verdict=GatingVerdict.PASS)

    for k, v in required.items():
        if agent_tags.get(k) != v:
            return GatingResult(
                verdict=GatingVerdict.DROP_TAG,
                reason=(
                    f"tag mismatch: required {k}={v!r}, "
                    f"agent has {k}={agent_tags.get(k)!r}"
                ),
            )
    return GatingResult(verdict=GatingVerdict.PASS)


# ============================================================
# Gate 2 — Schema check
# ============================================================


def gate_schema(
    envelope: Envelope, *, schema_in: dict[str, Any] | None,
) -> GatingResult:
    """Validate ``envelope.payload`` against the agent's input schema.

    No schema configured → pass (declaring is opt-in). Failures
    route to DLQ rather than Retry — schema violations are
    permanent for that exact event.
    """
    if schema_in is None:
        return GatingResult(verdict=GatingVerdict.PASS)
    try:
        # Cached validator — avoids recompiling schema_in per event (B5).
        validate_cached(envelope.payload, schema_in)
    except jsonschema.ValidationError as e:
        return GatingResult(
            verdict=GatingVerdict.DLQ_SCHEMA,
            reason=f"schema violation: {e.message}",
        )
    return GatingResult(verdict=GatingVerdict.PASS)


# ============================================================
# Gate 3 — Guardrail precheck
# ============================================================


async def gate_guardrail(
    envelope: Envelope,
    *,
    guardrail: GuardrailHandle,
    agent_id: str,
    est_tokens: int,
    template_key: str = "",
) -> GatingResult:
    """Reserve quota *before* invoking the handler.

    Quota touched at this gate cannot be self-recovered — the
    Runtime takes the FSM straight to ``Failure``.
    """
    try:
        reservation = await guardrail.precheck(
            run_id=envelope.run_id,
            agent_id=agent_id,
            est_tokens=est_tokens,
            cycle_inc=1,
            dry_charge=envelope.headers.replayed_from is not None,
            template_key=template_key,
        )
    except Exception as e:  # Guardrail raises QuotaExceeded subclass
        return GatingResult(
            verdict=GatingVerdict.GUARDRAIL_BLOCK,
            reason=str(e),
        )
    return GatingResult(
        verdict=GatingVerdict.PASS, reservation=reservation,
    )


# ============================================================
# Gate 4 — Dedup
# ============================================================


def gate_dedup(envelope: Envelope, *, store: DedupStore) -> GatingResult:
    """Drop replays already seen within the dedup window."""
    if store.seen(envelope.event_id):
        return GatingResult(
            verdict=GatingVerdict.DROP_DEDUP,
            reason=f"event_id {envelope.event_id!r} seen within dedup window",
        )
    return GatingResult(verdict=GatingVerdict.PASS)


# ============================================================
# Top-level pipeline
# ============================================================


async def run_gating(
    envelope: Envelope,
    *,
    agent_tags: dict[str, str],
    schema_in: dict[str, Any] | None,
    guardrail: GuardrailHandle,
    agent_id: str,
    est_tokens: int,
    dedup_store: DedupStore,
    template_key: str = "",
) -> GatingResult:
    """Run all four gates in order; short-circuit on first non-pass."""
    r1 = gate_tag(envelope, agent_tags=agent_tags)
    if not r1.passed:
        log.debug(
            "gating.tag.drop",
            agent_id=agent_id,
            event_id=envelope.event_id,
            reason=r1.reason,
        )
        return r1

    r2 = gate_schema(envelope, schema_in=schema_in)
    if not r2.passed:
        log.warning(
            "gating.schema.dlq",
            agent_id=agent_id,
            event_id=envelope.event_id,
            reason=r2.reason,
        )
        return r2

    r3 = await gate_guardrail(
        envelope,
        guardrail=guardrail,
        agent_id=agent_id,
        est_tokens=est_tokens,
        template_key=template_key,
    )
    if not r3.passed:
        log.warning(
            "gating.guardrail.block",
            agent_id=agent_id,
            event_id=envelope.event_id,
            reason=r3.reason,
        )
        return r3

    r4 = gate_dedup(envelope, store=dedup_store)
    if not r4.passed:
        log.debug(
            "gating.dedup.drop",
            agent_id=agent_id,
            event_id=envelope.event_id,
            reason=r4.reason,
        )
        return r4

    # Carry the reservation through so downstream code can consume it.
    return GatingResult(verdict=GatingVerdict.PASS, reservation=r3.reservation)
