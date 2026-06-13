"""Event envelope — the unit of communication on the EventBus.

The envelope is the *only* shape that flows through the bus. See
``Doc02 §2.3``. Producers MUST go through
:func:`agentkit.bus.builder.build_envelope` rather than constructing
this directly, so trace_id / event_id / from / ts are populated
consistently.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from agentkit.common.ids import new_event_id, new_trace_id
from agentkit.common.time import utcnow
from agentkit.models.enums import Role


class AgentRef(BaseModel):
    """Identifies the producer of an event."""

    model_config = ConfigDict(extra="forbid")

    role: Role | None = None
    agent_id: str | None = None
    runtime_node: str | None = None


class ToFilter(BaseModel):
    """Tag-based filter consumed by Runtime's first Gating step.

    Note this is a *hint* the producer attaches; the actual filtering
    happens inside :mod:`agentkit.runtime` (see ``Doc03 §5``).
    """

    model_config = ConfigDict(extra="forbid")

    role: Role | None = None
    tags: dict[str, str] = Field(default_factory=dict)


class EnvelopeHeaders(BaseModel):
    """Control headers attached to each event."""

    model_config = ConfigDict(extra="forbid")

    priority: int = Field(default=5, ge=0, le=9)
    deadline_ms: int | None = Field(default=None, ge=0)
    retry_attempt: int = Field(default=0, ge=0)
    runtime_token: str | None = None
    replayed_from: str | None = None
    user_headers: dict[str, str] = Field(default_factory=dict)


class Envelope(BaseModel):
    """The unit of communication on the EventBus."""

    # ``populate_by_name`` lets us alias ``from_`` <-> ``from`` so the
    # JSON form on the wire matches Doc02's spec while Python code
    # uses the non-keyword attribute name.
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    # ---- identity ----
    event_id: str = Field(default_factory=new_event_id)
    schema_ver: str = "1.0"

    # ---- routing ----
    topic: str = Field(min_length=1)
    to_filter: ToFilter = Field(default_factory=ToFilter)

    # ---- correlation ----
    trace_id: str = Field(default_factory=new_trace_id)
    workflow_id: str = ""
    run_id: str = ""
    causation_id: str | None = None

    # ---- source ----
    # Aliased to ``from`` for the wire format. We keep ``from_`` in
    # Python to avoid clashing with the ``from`` keyword.
    from_: AgentRef = Field(default_factory=AgentRef, alias="from")

    # ---- content ----
    payload: dict[str, Any] = Field(default_factory=dict)
    schema_ref: str | None = None

    # ---- control ----
    headers: EnvelopeHeaders = Field(default_factory=EnvelopeHeaders)

    # ---- time ----
    ts: datetime = Field(default_factory=utcnow)
