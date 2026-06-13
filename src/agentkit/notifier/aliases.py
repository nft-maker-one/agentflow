"""Topic alias resolution + built-in default rules.

The alias table mirrors ``Doc08 §2.3`` — short names users can
write in ``on:`` declarations get expanded to actual Bus topic
patterns the Notifier subscribes to.

Forward-compat note: when an alias maps to a ``workflow.*.X``
pattern AND the rule pins a ``workflow_id``, we substitute the
literal id in for tighter subscription. (Right now MockBus and
the Kafka adapter both support trailing ``*``.)
"""

from __future__ import annotations

from agentkit.notifier.models import (
    ChannelSpec,
    NotificationRule,
)

# ---- Alias → Bus topic pattern ----------------------------------

_ALIAS_TABLE: dict[str, str] = {
    # Run lifecycle
    "run.started":       "workflow.*.start",
    "run.failed":        "workflow.*.failed",
    "run.succeeded":     "workflow.*.end",
    "run.intermediate":  "agent.*",         # any agent.<role>.out.*
    # Runtime / Orchestrator
    "agent.state.changed": "system.agent.state.*",
    "role.down":           "system.runtime.alert.role_down",
    # Guardrail
    "guard.exceeded":      "system.guard.alert.#",
    # DLQ
    "dlq.received":        "*.dlq",
    # HumanNode
    "human.pending":       "human.*.pending",
    "human.timeout":       "human.*.timeout",
    # Collab @mention (Doc11)
    "collab.mention":      "collab.mention.created",
}


def resolve_alias(on: str, *, workflow_id: str | None = None) -> str:
    """Return the Bus topic pattern a rule's ``on`` field maps to.

    * Aliases (e.g. ``run.failed``) map to canonical patterns.
    * Already-qualified topics (containing a ``.``) pass through.
    * If ``workflow_id`` is set and the alias targets a workflow
      pattern, we substitute the literal id for a tighter
      subscription (``workflow.wf_x.failed`` instead of
      ``workflow.*.failed``).
    """
    pattern = _ALIAS_TABLE.get(on, on)
    if workflow_id and pattern.startswith("workflow.*."):
        suffix = pattern[len("workflow.*."):]
        return f"workflow.{workflow_id}.{suffix}"
    return pattern


# ---- Reverse map: alias → suggested template name -----------------

_DEFAULT_TEMPLATES: dict[str, str] = {
    "run.started":         "run_started_default",
    "run.failed":          "run_failed_default",
    "run.succeeded":       "run_succeeded_default",
    "run.intermediate":    "run_intermediate_default",
    "agent.state.changed": "agent_state_changed_default",
    "role.down":           "role_down_default",
    "guard.exceeded":      "guard_exceeded_default",
    "dlq.received":        "dlq_received_default",
    "human.pending":       "human_pending_default",
    "human.timeout":       "human_timeout_default",
    "collab.mention":      "collab_mention_default",
}


def default_template_for(on: str) -> str:
    """Pick a reasonable default template name for an alias."""
    return _DEFAULT_TEMPLATES.get(on, "generic_default")


# ============================================================
# Built-in default rules (Doc08 §2.1)
#
# These ship in every Notifier — DLQ + Guardrail alerts get
# delivered to the Log channel out-of-the-box so a fresh user
# at least sees them in their console. Production deployments
# replace / extend these with real channel rules.
# ============================================================


def _builtin_log_rule(*, on: str, severity: str = "warning") -> NotificationRule:
    return NotificationRule(
        on=on,
        channel=ChannelSpec(kind="log"),
        to="stderr",  # the LogChannel reads this as a sink hint
        severity=severity,  # type: ignore[arg-type]
    )


BUILTIN_DEFAULT_RULES: list[NotificationRule] = [
    _builtin_log_rule(on="guard.exceeded", severity="critical"),
    _builtin_log_rule(on="role.down", severity="critical"),
    _builtin_log_rule(on="dlq.received", severity="error"),
]


# ---- Subscription set helper -------------------------------------


def DEFAULT_SUBSCRIPTIONS() -> list[str]:
    """The Bus topic patterns covered by built-in rules."""
    return [resolve_alias(r.on) for r in BUILTIN_DEFAULT_RULES]
