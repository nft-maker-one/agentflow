"""Pydantic response models for the control-plane API.

These are the on-wire contract with the React UI — separate from the
internal :class:`WorkflowIR` / :class:`Run` to keep the UI from
coupling to internal field renames.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# ============================================================
# Workflow
# ============================================================


class AgentSummary(BaseModel):
    """Per-agent summary in a Workflow detail response."""

    model_config = ConfigDict(extra="forbid")

    template_key: str
    role: str
    description: str = ""
    subscribe: list[str] = Field(default_factory=list)
    publish: list[str] = Field(default_factory=list)
    llm: dict[str, Any] | None = None
    # Handler-level: the field this agent adds to the payload. Used by
    # the UI's beginner-mode prompt builder to suggest variables to
    # downstream agents. None when the agent isn't a class-based Agent
    # instance (e.g. raw @agent decorated functions).
    output_field: str | None = None
    # Per-agent guardrail caps. None means inherit framework defaults.
    agent_guardrail: dict[str, int] | None = None
    # Fan-in aggregator gating. None when subscribe < 2 topics
    # (aggregator inactive). Shape: {threshold, required}.
    aggregate: dict[str, Any] | None = None


class GraphNode(BaseModel):
    """One node in the workflow DAG (React Flow shape)."""

    id: str            # template_key OR __start__ / __end__ / __error__
    label: str
    kind: Literal["start", "end", "error", "agent"]
    role: str | None = None


class GraphEdge(BaseModel):
    """One edge in the workflow DAG."""

    id: str            # IR edge_id, or auto for switch cases
    source: str        # node id
    target: str        # node id
    via: str | None = None
    is_switch: bool = False
    case: str | None = None        # for switch case branches


class WorkflowSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    version: int
    description: str = ""
    ir_hash: str
    n_agents: int
    n_edges: int
    project_id: str = "default"


class WorkflowDetail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    version: int
    description: str = ""
    ir_hash: str
    project_id: str = "default"
    agents: list[AgentSummary]
    nodes: list[GraphNode]
    edges: list[GraphEdge] = Field(default_factory=list)
    # The conventional payload field names that __start__ injects when
    # a Run is triggered. Used by the UI's prompt-builder to suggest
    # the right ``payload.<field>`` variables to downstream agents.
    # Defaults to ``["q"]`` for backward compat.
    start_input_fields: list[str] = Field(default_factory=lambda: ["q"])
    # Static validation: agent prompts that reference a `payload.<field>`
    # no source can produce. Non-empty ⇒ the workflow is NOT runnable; the
    # UI disables Run and shows these, and POST /runs is rejected.
    prompt_field_errors: list[dict[str, str]] = Field(default_factory=list)
    # Number of topology snapshots in the undo stack. Frontend uses
    # this to enable/disable the "Undo" button.
    undo_depth: int = 0
    # Workflow-level run-quota caps (max_total_tokens, max_cycles_per_run).
    # None means no override — falls back to project / framework defaults.
    workflow_guardrail: dict[str, int] | None = None
    # Execution mode: "normal" = one Run per POST, "event_driven"
    # = continuous, external sources auto-spawn Runs.
    mode: str = "normal"
    # Active event-driven session run_id, when in event_driven
    # mode. Empty string when in normal mode. Used by the UI to
    # auto-lock the Live Event Timeline onto this run so users
    # see ext source/sink traces stream by even without POSTing
    # /api/runs.
    session_run_id: str = ""


# ============================================================
# Runs
# ============================================================


class RunSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    workflow_id: str
    status: str
    started_at: datetime | None = None
    ended_at: datetime | None = None
    failure_reason: str | None = None
    trace_id: str | None = None


class BranchEventOut(BaseModel):
    edge_id: str
    chosen: str
    by: str
    reason: str | None = None


class RunDetail(RunSummary):
    input: dict[str, Any] = Field(default_factory=dict)
    output: dict[str, Any] | None = None
    current_node: str | None = None
    branch_log: list[BranchEventOut] = Field(default_factory=list)
    # Topology/agents AS THEY WERE when this run executed (from the run's
    # stored ``workflow_snapshot``). Empty if the run predates snapshots.
    snapshot_nodes: list[GraphNode] = Field(default_factory=list)
    snapshot_edges: list[GraphEdge] = Field(default_factory=list)
    snapshot_agents: list[AgentSummary] = Field(default_factory=list)


class CreateRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_id: str
    input: dict[str, Any] = Field(default_factory=dict)


# ============================================================
# Events (SSE payload)
# ============================================================


class EventOut(BaseModel):
    """One envelope rendered for SSE consumption."""

    event_id: str
    topic: str
    payload: dict[str, Any]
    from_template: str | None = None
    from_agent: str | None = None
    causation_id: str | None = None
    created_at: datetime


# ============================================================
# Health
# ============================================================


class HealthResponse(BaseModel):
    ok: bool
    version: str
    deployed_workflows: list[str]
