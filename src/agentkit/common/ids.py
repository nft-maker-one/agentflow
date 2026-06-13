"""ID generation helpers — all framework IDs are time-sortable ULIDs.

Why ULID instead of UUIDv4:

* 26 chars vs 36 chars — friendlier in URLs / logs.
* Lexicographically sortable by time — useful for index-friendly
  primary keys and time-bucketed queries.
* Crockford base32 — case-insensitive, no easily-confused chars.

We expose ``prefixed_id(prefix)`` and a handful of named convenience
helpers so the rest of the code can call ``new_event_id()`` rather
than sprinkling string prefixes everywhere.
"""

from __future__ import annotations

from ulid import ULID


def new_ulid() -> str:
    """Return a freshly generated ULID as a 26-character string."""
    return str(ULID())


def prefixed_id(prefix: str) -> str:
    """Return ``<prefix>_<ulid>`` — the canonical typed-id format."""
    if not prefix:
        raise ValueError("prefix must be a non-empty string")
    return f"{prefix}_{new_ulid()}"


# ---------- named helpers (so call sites read better) ----------


def new_event_id() -> str:
    """ID for an :class:`Envelope`."""
    return prefixed_id("evt")


def new_trace_id() -> str:
    """ID for an OpenTelemetry-style trace root."""
    return prefixed_id("trc")


def new_run_id() -> str:
    """ID for a :class:`Run` (Workflow execution instance)."""
    return prefixed_id("run")


def new_agent_id() -> str:
    """ID for an Agent runtime instance."""
    return prefixed_id("agt")


def new_reservation_id() -> str:
    """ID for a Guardrail token / cycle reservation."""
    return prefixed_id("rsv")


def new_notification_id() -> str:
    """ID for a single :class:`Notification` record."""
    return prefixed_id("ntf")


def new_rule_id() -> str:
    """ID for a Notifier :class:`NotificationRule` (used in audit / dedup)."""
    return prefixed_id("rule")
