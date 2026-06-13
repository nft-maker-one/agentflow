"""Agent finite-state machine.

The FSM is implemented as a *pure data + pure function* — no
threading, no I/O, no hidden state. Higher-level instance code
calls :func:`apply_transition` to compute the next snapshot;
persistence (``agent_state_log``) is the caller's responsibility.

State chart (per ``Doc03 §4.1``)::

    ┌────────────┐  env_check_fail   ┌──────┐
    │  Init      │──────────────────▶│ Down │
    └────┬───────┘                   └──┬───┘
         │ env_check_pass               │ unfreeze
         ▼                              ▼
    ┌────────────┐  event_arrived ┌────────────┐
    │  Active    │───────────────▶│ Processing │
    └────────────┘◀────handler_ok─└────┬───────┘
         ▲                             │
         │                             │ recoverable_error & retry_count < max
         │                             ▼
         │                       ┌──────────┐
         │                       │  Retry   │
         │                       └────┬─────┘
         │                            │ retry_due
         └────────────────────────────┘
                                      │ retry_count == max
                                      ▼
                                ┌──────────┐
                                │ Failure  │
                                └──────────┘
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from agentkit.common.time import utcnow
from agentkit.models.enums import AgentState
from agentkit.runtime.fallback import FailureClass


# ============================================================
# Events that drive transitions
# ============================================================


class FSMTransition(StrEnum):
    """Logical events the dispatcher feeds into :func:`apply_transition`.

    Maps to columns in the Doc03 §4.1 transition table.
    """

    ENV_CHECK_PASS = "env_check_pass"
    ENV_CHECK_FAIL = "env_check_fail"

    EVENT_ARRIVED = "event_arrived"
    HANDLER_OK = "handler_ok"
    NEED_DEP = "need_dep"
    DEP_RESOLVED = "dep_resolved"
    WAITING_TIMEOUT = "waiting_timeout"

    RECOVERABLE_ERROR = "recoverable_error"
    FATAL_ERROR = "fatal_error"
    GUARDRAIL_EXCEEDED = "guardrail_exceeded"

    RETRY_DUE = "retry_due"

    FREEZE = "freeze"
    UNFREEZE = "unfreeze"

    WORKFLOW_DONE = "workflow_done"


# ============================================================
# Snapshot + metadata
# ============================================================


class AgentStateMeta(BaseModel):
    """Per-state structured metadata. See ``Doc03 §2.4``."""

    model_config = ConfigDict(extra="forbid")

    description: str = ""
    reason: str | None = None
    last_error_at: datetime | None = None
    last_active_at: datetime | None = None
    retry_count: int = 0
    max_retries: int = 0
    next_retry_at: datetime | None = None
    waiting_for: list[str] = Field(default_factory=list)
    timeout_at: datetime | None = None
    current_event_id: str | None = None
    llm_calls: int = 0
    tokens_used_so_far: int = 0
    result_topic: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class AgentSnapshot(BaseModel):
    """Immutable view of an Agent instance's FSM state at one moment."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    agent_id: str
    template_key: str
    workflow_id: str
    state: AgentState = AgentState.INIT
    state_meta: AgentStateMeta = Field(default_factory=AgentStateMeta)
    last_change_at: datetime = Field(default_factory=utcnow)


def initial_snapshot(
    *,
    agent_id: str,
    template_key: str,
    workflow_id: str,
    description: str = "",
    max_retries: int = 3,
) -> AgentSnapshot:
    """Build the starting snapshot — Init state with description filled in."""
    return AgentSnapshot(
        agent_id=agent_id,
        template_key=template_key,
        workflow_id=workflow_id,
        state=AgentState.INIT,
        state_meta=AgentStateMeta(
            description=description,
            max_retries=max_retries,
        ),
    )


# ============================================================
# Transition function
# ============================================================


# Mapping (current_state, transition) → next_state. Side conditions
# (e.g. retry_count vs max_retries) are evaluated in
# :func:`apply_transition`. Anything not in this table is rejected
# as an invalid transition.
_BASE_TABLE: dict[tuple[AgentState, FSMTransition], AgentState] = {
    # Init
    (AgentState.INIT, FSMTransition.ENV_CHECK_PASS): AgentState.ACTIVE,
    (AgentState.INIT, FSMTransition.ENV_CHECK_FAIL): AgentState.DOWN,
    # Active
    (AgentState.ACTIVE, FSMTransition.EVENT_ARRIVED): AgentState.PROCESSING,
    (AgentState.ACTIVE, FSMTransition.FREEZE): AgentState.DOWN,
    # Processing
    (AgentState.PROCESSING, FSMTransition.HANDLER_OK): AgentState.ACTIVE,
    (AgentState.PROCESSING, FSMTransition.NEED_DEP): AgentState.WAITING,
    (AgentState.PROCESSING, FSMTransition.FATAL_ERROR): AgentState.DOWN,
    (AgentState.PROCESSING, FSMTransition.GUARDRAIL_EXCEEDED): AgentState.FAILURE,
    (AgentState.PROCESSING, FSMTransition.WORKFLOW_DONE): AgentState.COMPLETE,
    # Retry
    (AgentState.RETRY, FSMTransition.RETRY_DUE): AgentState.PROCESSING,
    (AgentState.RETRY, FSMTransition.FREEZE): AgentState.DOWN,
    # Waiting
    (AgentState.WAITING, FSMTransition.DEP_RESOLVED): AgentState.PROCESSING,
    (AgentState.WAITING, FSMTransition.WAITING_TIMEOUT): AgentState.FAILURE,
    # Down
    (AgentState.DOWN, FSMTransition.UNFREEZE): AgentState.INIT,
    (AgentState.DOWN, FSMTransition.ENV_CHECK_PASS): AgentState.ACTIVE,
}


class InvalidTransition(ValueError):
    """Raised when ``transition`` is not legal in the current state."""


def apply_transition(
    snap: AgentSnapshot,
    transition: FSMTransition,
    *,
    reason: str | None = None,
    event_id: str | None = None,
    next_retry_at: datetime | None = None,
    waiting_for: list[str] | None = None,
    timeout_at: datetime | None = None,
    result_topic: str | None = None,
    now: datetime | None = None,
) -> AgentSnapshot:
    """Compute the next snapshot.

    The trickiest case is :class:`FSMTransition.RECOVERABLE_ERROR`:
    its target state depends on ``retry_count`` versus ``max_retries``
    (Retry vs Failure). All other transitions consult ``_BASE_TABLE``.

    Returns a new :class:`AgentSnapshot` (frozen). Caller is
    responsible for persisting state-log entries.
    """
    cur = snap.state
    now = now or utcnow()

    next_state = _resolve_next_state(snap, transition)
    if next_state is None:
        raise InvalidTransition(
            f"transition {transition.value!r} not legal from state {cur.value!r}",
        )

    new_meta = _build_meta(
        snap=snap,
        transition=transition,
        next_state=next_state,
        reason=reason,
        event_id=event_id,
        next_retry_at=next_retry_at,
        waiting_for=waiting_for,
        timeout_at=timeout_at,
        result_topic=result_topic,
        now=now,
    )
    return snap.model_copy(
        update={"state": next_state, "state_meta": new_meta, "last_change_at": now},
    )


# ----------------------------------------------------------------
# Internals
# ----------------------------------------------------------------


def _resolve_next_state(
    snap: AgentSnapshot, transition: FSMTransition,
) -> AgentState | None:
    """Apply the table + side conditions for retry budget."""
    if transition is FSMTransition.RECOVERABLE_ERROR and snap.state is AgentState.PROCESSING:
        meta = snap.state_meta
        # ``retry_count`` is the count of retries *already done*.
        # On entering Retry we increment it. So the cap check is
        # ``retry_count < max_retries`` BEFORE incrementing.
        if meta.retry_count < meta.max_retries:
            return AgentState.RETRY
        return AgentState.FAILURE
    return _BASE_TABLE.get((snap.state, transition))


def _build_meta(
    *,
    snap: AgentSnapshot,
    transition: FSMTransition,
    next_state: AgentState,
    reason: str | None,
    event_id: str | None,
    next_retry_at: datetime | None,
    waiting_for: list[str] | None,
    timeout_at: datetime | None,
    result_topic: str | None,
    now: datetime,
) -> AgentStateMeta:
    """Compute metadata for the next state.

    Each state has different mandatory fields per ``Doc03 §2.4``;
    we honor those and forward only the relevant pieces.
    """
    cur = snap.state_meta

    # Default: carry meta forward, then patch per-state.
    base = cur.model_copy()

    # Always update last_active_at when leaving Active.
    if snap.state is AgentState.ACTIVE and next_state is not AgentState.ACTIVE:
        base.last_active_at = now

    # Reset reason on success-ish transitions.
    if next_state in (AgentState.ACTIVE, AgentState.COMPLETE):
        base.reason = None
        base.last_error_at = None

    # Per-target-state population.
    if next_state is AgentState.PROCESSING:
        base.current_event_id = event_id or base.current_event_id
        # Note: do NOT clear retry_count here. RETRY → PROCESSING
        # increments retry_count; HANDLER_OK is what resets it.
    elif next_state is AgentState.RETRY:
        base.reason = reason
        base.last_error_at = now
        base.retry_count = base.retry_count + 1
        base.next_retry_at = next_retry_at
        # current_event_id stays the same (same session reused)
    elif next_state is AgentState.FAILURE:
        base.reason = reason
        base.last_error_at = now
    elif next_state is AgentState.DOWN:
        base.reason = reason
        base.last_error_at = now
    elif next_state is AgentState.WAITING:
        base.waiting_for = list(waiting_for or [])
        base.timeout_at = timeout_at
    elif next_state is AgentState.COMPLETE:
        base.result_topic = result_topic
    elif (
        next_state is AgentState.ACTIVE
        and transition is FSMTransition.HANDLER_OK
    ):
        # Successful handler — reset retry counter.
        base.retry_count = 0
        base.next_retry_at = None
        base.current_event_id = None

    return base


# ----------------------------------------------------------------
# Helper: classify a handler exception → FSM transition
# ----------------------------------------------------------------


def transition_for_failure(klass: FailureClass) -> FSMTransition:
    """Map :class:`FailureClass` to the right FSM transition.

    Used by the dispatch loop after :func:`classify_handler_exception`.
    """
    if klass is FailureClass.RECOVERABLE:
        return FSMTransition.RECOVERABLE_ERROR
    if klass is FailureClass.FATAL:
        return FSMTransition.FATAL_ERROR
    if klass is FailureClass.GUARDRAIL_EXCEEDED:
        return FSMTransition.GUARDRAIL_EXCEEDED
    raise ValueError(f"no FSM transition for {klass!r}")
