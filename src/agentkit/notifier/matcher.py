"""Rule matching engine.

Splits the work into two steps:

* **Topic matching** — does this rule's resolved topic pattern
  match the envelope's topic? (Cheap pre-filter; runs first.)
* **Guard expression** — evaluate ``rule.when`` against the
  envelope's payload + headers; failure modes degrade gracefully
  to "no match" with an audit trail.
"""

from __future__ import annotations

from dataclasses import dataclass

from agentkit.common.logging import get_logger
from agentkit.common.time import utcnow
from agentkit.models.envelope import Envelope
from agentkit.notifier.aliases import resolve_alias
from agentkit.notifier.errors import RuleEvalError
from agentkit.notifier.expressions import evaluate_when
from agentkit.notifier.models import NotificationRule

log = get_logger(__name__)


@dataclass(frozen=True)
class MatchOutcome:
    """Why (or why not) a rule fired against an envelope."""

    matched: bool
    reason: str = ""


def topic_matches(pattern: str, topic: str) -> bool:
    """Apply the same wildcard semantics as MockBus / Kafka adapter.

    * Exact match.
    * ``foo.bar.*``  — any 1-or-more dot-suffix.
    * ``foo.bar.#``  — equal to ``foo.bar`` *or* any dot-suffix.
    * ``*.dlq``      — any prefix that ends in ``.dlq``.
    * ``#``          — match anything.
    """
    if pattern == topic or pattern == "#":
        return True
    if pattern.endswith(".*"):
        prefix = pattern[: -len(".*")]
        return topic.startswith(prefix + ".")
    if pattern.endswith(".#"):
        prefix = pattern[: -len(".#")]
        return topic == prefix or topic.startswith(prefix + ".")
    if pattern.startswith("*."):
        suffix = pattern[len("*."):]
        return topic.endswith("." + suffix) or topic == suffix
    return False


def build_when_context(envelope: Envelope) -> dict[str, object]:
    """Construct the variable bag used by ``evaluate_when``."""
    return {
        "topic": envelope.topic,
        "payload": envelope.payload or {},
        "headers": envelope.headers.model_dump() if envelope.headers else {},
        "event": envelope.model_dump(mode="json"),
        "run_id": envelope.run_id,
        "trace_id": envelope.trace_id,
        "workflow_id": envelope.workflow_id,
        "event_id": envelope.event_id,
        "now": utcnow().isoformat(),
    }


def rule_matches(
    rule: NotificationRule, envelope: Envelope,
) -> MatchOutcome:
    """End-to-end "does this rule fire on this envelope?".

    Performs:

    1. ``rule.enabled`` check
    2. ``rule.workflow_id`` filter (if set)
    3. Topic pattern match
    4. ``rule.when`` evaluation

    A bad ``when`` expression is treated as **no match** + log
    line — never a hard failure.
    """
    if not rule.enabled:
        return MatchOutcome(False, "rule disabled")

    if rule.workflow_id and rule.workflow_id != envelope.workflow_id:
        return MatchOutcome(False, "workflow_id filter mismatch")

    pattern = resolve_alias(rule.on, workflow_id=rule.workflow_id)
    if not topic_matches(pattern, envelope.topic):
        return MatchOutcome(False, f"topic {envelope.topic!r} not in {pattern!r}")

    if rule.when:
        ctx = build_when_context(envelope)
        try:
            ok = evaluate_when(rule.when, ctx)
        except RuleEvalError as e:
            log.warning(
                "notifier.rule.eval_failed",
                rule_id=rule.id, when=rule.when, error=str(e),
            )
            return MatchOutcome(False, f"when eval failed: {e}")
        if not ok:
            return MatchOutcome(False, "when evaluated False")

    return MatchOutcome(True, "match")
