"""Run + cursor + branch-event data models.

See ``Doc05 §2``.

Phase 1 scope: minimal Run lifecycle (Pending / Running /
Succeeded / Failed / Cancelled). HumanNode-aware ``AwaitingHuman``
and ``Degraded`` are tracked in ``RunStatus`` already (defined in
``agentkit.models.enums``); the orchestrator uses only the four
above for now and will gain the others when Doc04 §4.3 / §8 land.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from agentkit.common.ids import new_run_id, new_trace_id
from agentkit.common.time import utcnow
from agentkit.models.enums import RunStatus

# Trigger kinds we recognize at the API level. ``api`` covers the
# direct ``Orchestrator.create_run(...)`` path; ``cron`` / ``webhook``
# / ``event`` arrive in Doc05 §10 territory.
TriggerKind = Literal["api", "cron", "webhook", "event"]


# Statuses that mean "this Run will never produce more events" —
# used to short-circuit completion waits and reject
# subsequent control commands.
TERMINAL_STATUSES: Final[frozenset[RunStatus]] = frozenset(
    {
        RunStatus.SUCCEEDED,
        RunStatus.FAILED,
        RunStatus.CANCELLED,
        RunStatus.ARCHIVED,
    },
)


class BranchEvent(BaseModel):
    """One step in a Run's history (switch decision / overlay / human / fanout).

    Each entry is append-only; the run's ``cursor.branch_log`` is a
    chronological list. UI / audit reads the list directly.
    """

    model_config = ConfigDict(extra="forbid", frozen=False)

    at: datetime = Field(default_factory=utcnow)
    edge_id: str = ""
    chosen: str = ""
    by: Literal["auto", "overlay", "human", "system"] = "auto"
    reason: str | None = None


class RunCursor(BaseModel):
    """The current "where is this Run" snapshot.

    Phase 1 keeps this lightweight: just the branch log. Active-node
    tracking (Doc05 §2.3 ``active_nodes``) is wired in once Runtime
    forwards state changes via heartbeat.
    """

    model_config = ConfigDict(extra="forbid")

    branch_log: list[BranchEvent] = Field(default_factory=list)
    updated_at: datetime = Field(default_factory=utcnow)


class Run(BaseModel):
    """One execution instance of a Workflow.

    Frozen=False so the orchestrator can mutate ``status`` /
    ``cursor`` directly. Mutations go through helper methods on
    :class:`Orchestrator` — never user code.
    """

    model_config = ConfigDict(extra="forbid")

    # ── identity ──
    run_id: str
    workflow_id: str
    workflow_version: int = 1
    trace_id: str

    # ── lifecycle ──
    trigger: TriggerKind = "api"
    status: RunStatus = RunStatus.PENDING
    started_at: datetime = Field(default_factory=utcnow)
    ended_at: datetime | None = None

    # ── inputs / outputs ──
    input: dict[str, Any] = Field(default_factory=dict)
    output: dict[str, Any] | None = None

    # ── path & overlays ──
    cursor: RunCursor = Field(default_factory=RunCursor)

    # ── failure context ──
    failure_reason: str | None = None

    # ── workflow snapshot ──
    #: The compiled WorkflowIR (as a dict) at the moment this run was
    #: created — so ``runs/<id>`` always shows the topology/agents AS
    #: THEY WERE when it ran, even after the workflow is later edited.
    workflow_snapshot: dict[str, Any] | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def add_branch_event(self, ev: BranchEvent) -> None:
        """Append an immutable BranchEvent to the cursor."""
        self.cursor.branch_log.append(ev)
        self.cursor.updated_at = utcnow()


def new_run(
    *,
    workflow_id: str,
    workflow_version: int = 1,
    input: dict[str, Any] | None = None,
    trigger: TriggerKind = "api",
    trace_id: str | None = None,
) -> Run:
    """Construct a fresh ``Run`` in ``Pending`` state."""
    return Run(
        run_id=new_run_id(),
        workflow_id=workflow_id,
        workflow_version=workflow_version,
        trace_id=trace_id or new_trace_id(),
        trigger=trigger,
        input=input or {},
    )
