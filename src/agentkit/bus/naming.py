"""Topic / consumer-group naming conventions.

Centralizing these constants here means callers (Runtime, Notifier,
Orchestrator, Hub) all derive consistent names. Changing the
convention is a one-line edit.

See ``Doc02 §3.1`` (topic naming), ``Doc02 §4.1`` (consumer group
derivation), ``Doc02 §6.1`` (DLQ derivation).
"""

from __future__ import annotations

DLQ_SUFFIX = ".dlq"
GROUP_PREFIX = "grp."
BROADCAST_GROUP_PREFIX = "grp.broadcast."


def dlq_topic_for(topic: str) -> str:
    """Return the DLQ topic name for ``topic``.

    Examples
    --------
    >>> dlq_topic_for("agent.research.in.q1")
    'agent.research.in.q1.dlq'
    """
    if not topic:
        raise ValueError("topic must be a non-empty string")
    if topic.endswith(DLQ_SUFFIX):
        return topic  # already a DLQ topic — idempotent
    return f"{topic}{DLQ_SUFFIX}"


def is_dlq_topic(topic: str) -> bool:
    """True iff ``topic`` is a DLQ topic by naming convention."""
    return topic.endswith(DLQ_SUFFIX)


def derive_consumer_group(workflow_id: str, template_key: str) -> str:
    """Derive the standard consumer group for a Workflow / Template pair.

    Per ``Doc02 §4.1``, all instances of the same Template share one
    group so partitions naturally load-balance across instances.

    >>> derive_consumer_group("wf_001", "researcher")
    'grp.wf_001.researcher'
    """
    if not workflow_id or not template_key:
        raise ValueError("workflow_id and template_key must be non-empty")
    return f"{GROUP_PREFIX}{workflow_id}.{template_key}"


def derive_broadcast_group(node_id: str) -> str:
    """Derive a per-instance broadcast group.

    Used for control-plane topics (``system.*.ctrl.*``) where every
    node needs every message — see ``Doc02 §4.3``.

    >>> derive_broadcast_group("node_a")
    'grp.broadcast.node_a'
    """
    if not node_id:
        raise ValueError("node_id must be non-empty")
    return f"{BROADCAST_GROUP_PREFIX}{node_id}"
