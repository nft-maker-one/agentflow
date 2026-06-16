"""Quota resolver — collapse 4 sources into one effective ``GuardrailContext``.

Priority (Doc07 §2.3, narrowest wins)::

    run_overlay      ←  only ALLOWED to shrink
    workflow         ←  Workflow IR's `guardrails:` block
    project_default  ←  `.agentkit/project.yaml`
    framework_default ←  built-in safety net

We deliberately model "narrowing only" with a clean ``min()``
operation — overlay/workflow can never raise project/framework
caps. This matches the design intent: a debugging override
shouldn't *expand* runaway loops.

The resolver is **pure** — no I/O, no Redis. Orchestrator calls
it at ``create_run`` time after parsing the IR.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from agentkit.guardrail.models import (
    AgentGuardrail,
    GuardrailContext,
    OverrideRecord,
    QuotaSource,
    RunGuardrail,
)

# ============================================================
# Built-in defaults — Doc07 §2.5
# ============================================================

FRAMEWORK_DEFAULT_AGENT: AgentGuardrail = AgentGuardrail(
    max_tokens_per_call=8_000,
    max_cycles=50,
)

FRAMEWORK_DEFAULT_RUN: RunGuardrail = RunGuardrail(
    max_total_tokens=200_000,
    max_cycles_per_run=200,
)


@dataclass(frozen=True)
class FrameworkDefaults:
    """Wraps the framework defaults so callers can swap them in tests
    (e.g. very tight limits to exercise overflow paths)."""

    agent: AgentGuardrail = field(default_factory=lambda: FRAMEWORK_DEFAULT_AGENT)
    run: RunGuardrail = field(default_factory=lambda: FRAMEWORK_DEFAULT_RUN)


# ============================================================
# Resolver
# ============================================================


def resolve_guardrail_context(
    *,
    run_id: str,
    workflow_id: str,
    workflow_version: int = 1,
    owner: str | None = None,
    project: str | None = None,
    workflow_agent: AgentGuardrail | None = None,
    workflow_run: RunGuardrail | None = None,
    project_agent: AgentGuardrail | None = None,
    project_run: RunGuardrail | None = None,
    overlay_agent: AgentGuardrail | None = None,
    overlay_run: RunGuardrail | None = None,
    agent_overrides: dict[str, AgentGuardrail] | None = None,
    framework: FrameworkDefaults | None = None,
) -> GuardrailContext:
    """Resolve effective Agent + Run quotas with full provenance.

    Returns a :class:`GuardrailContext` whose ``overrides`` list
    documents every chosen field's origin (``framework_default`` ↔
    ``run_overlay``).
    """
    fw = framework or FrameworkDefaults()
    overrides: list[OverrideRecord] = []

    # ---- Agent layer ----
    eff_agent_tokens, src = _pick_min_int(
        field="max_tokens_per_call",
        framework=fw.agent.max_tokens_per_call,
        project=getattr(project_agent, "max_tokens_per_call", None),
        workflow=getattr(workflow_agent, "max_tokens_per_call", None),
        overlay=getattr(overlay_agent, "max_tokens_per_call", None),
    )
    overrides.append(
        OverrideRecord(
            layer="agent", field="max_tokens_per_call",
            source=src, value=eff_agent_tokens,
        ),
    )

    eff_agent_cycles, src = _pick_min_int(
        field="max_cycles",
        framework=fw.agent.max_cycles,
        project=getattr(project_agent, "max_cycles", None),
        workflow=getattr(workflow_agent, "max_cycles", None),
        overlay=getattr(overlay_agent, "max_cycles", None),
    )
    overrides.append(
        OverrideRecord(
            layer="agent", field="max_cycles",
            source=src, value=eff_agent_cycles,
        ),
    )

    # ---- Run layer ----
    eff_run_tokens, src = _pick_min_int(
        field="max_total_tokens",
        framework=fw.run.max_total_tokens,
        project=getattr(project_run, "max_total_tokens", None),
        workflow=getattr(workflow_run, "max_total_tokens", None),
        overlay=getattr(overlay_run, "max_total_tokens", None),
    )
    overrides.append(
        OverrideRecord(
            layer="run", field="max_total_tokens",
            source=src, value=eff_run_tokens,
        ),
    )

    eff_run_cycles, src = _pick_min_int(
        field="max_cycles_per_run",
        framework=fw.run.max_cycles_per_run,
        project=getattr(project_run, "max_cycles_per_run", None),
        workflow=getattr(workflow_run, "max_cycles_per_run", None),
        overlay=getattr(overlay_run, "max_cycles_per_run", None),
    )
    overrides.append(
        OverrideRecord(
            layer="run", field="max_cycles_per_run",
            source=src, value=eff_run_cycles,
        ),
    )

    return GuardrailContext(
        run_id=run_id,
        workflow_id=workflow_id,
        workflow_version=workflow_version,
        owner=owner,
        project=project,
        agent=AgentGuardrail(
            max_tokens_per_call=eff_agent_tokens,
            max_cycles=eff_agent_cycles,
        ),
        run=RunGuardrail(
            max_total_tokens=eff_run_tokens,
            max_cycles_per_run=eff_run_cycles,
        ),
        agent_overrides=dict(agent_overrides or {}),
        overrides=overrides,
    )


# ----------------------------------------------------------------
# Internals
# ----------------------------------------------------------------


def _pick_min_int(
    *,
    field: str,
    framework: int,
    project: int | None,
    workflow: int | None,
    overlay: int | None,
) -> tuple[int, QuotaSource]:
    """Pick the smallest declared value with provenance.

    Iteration order matters only for tie-breaking — when two
    sources have the same value, we prefer the *narrower scope* so
    audit lines reflect the most-specific override.
    """
    del field  # only used at call sites for documentation
    candidates: list[tuple[int, QuotaSource]] = [
        (framework, "framework_default"),
    ]
    if project is not None:
        candidates.append((project, "project_default"))
    if workflow is not None:
        candidates.append((workflow, "workflow"))
    if overlay is not None:
        candidates.append((overlay, "run_overlay"))

    # Pick the minimum value; on ties prefer the narrower scope
    # (overlay > workflow > project > framework).
    scope_priority: dict[QuotaSource, int] = {
        "framework_default": 0,
        "project_default": 1,
        "workflow": 2,
        "run_overlay": 3,
    }
    best_value = min(c[0] for c in candidates)
    candidates_at_min = [c for c in candidates if c[0] == best_value]
    candidates_at_min.sort(key=lambda c: scope_priority[c[1]], reverse=True)
    return candidates_at_min[0]
