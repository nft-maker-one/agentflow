"""Sanctioned helper for constructing :class:`Envelope` objects.

Even though :class:`agentkit.models.envelope.Envelope` can be
constructed directly, ALL framework code paths that publish events
should go through :func:`build_envelope`. This guarantees:

* ``event_id`` / ``trace_id`` / ``ts`` are populated consistently;
* future enrichments (Trace context propagation, runtime token
  injection) live in *one* place.
"""

from __future__ import annotations

from typing import Any

from agentkit.common.ids import new_event_id, new_trace_id
from agentkit.common.time import utcnow
from agentkit.models.envelope import (
    AgentRef,
    Envelope,
    EnvelopeHeaders,
    ToFilter,
)


def build_envelope(
    *,
    topic: str,
    payload: dict[str, Any],
    workflow_id: str = "",
    run_id: str = "",
    trace_id: str | None = None,
    from_: AgentRef | None = None,
    to_filter: ToFilter | None = None,
    headers: EnvelopeHeaders | None = None,
    causation_id: str | None = None,
    schema_ref: str | None = None,
) -> Envelope:
    """Construct a fully-populated :class:`Envelope`.

    Parameters
    ----------
    topic, payload
        Required. ``topic`` must be non-empty.
    workflow_id, run_id
        Defaulted to empty string (intentionally — system / debug
        events may not be tied to a Run).
    trace_id
        If ``None`` a fresh trace_id is generated. Pass an existing
        trace_id to continue an in-flight trace (the common case).
    from_, to_filter, headers
        Defaulted to empty value objects when not provided.
    causation_id, schema_ref
        Optional metadata.
    """
    return Envelope(
        event_id=new_event_id(),
        topic=topic,
        payload=payload,
        workflow_id=workflow_id,
        run_id=run_id,
        trace_id=trace_id or new_trace_id(),
        from_=from_ or AgentRef(),
        to_filter=to_filter or ToFilter(),
        headers=headers or EnvelopeHeaders(),
        causation_id=causation_id,
        schema_ref=schema_ref,
        ts=utcnow(),
    )
