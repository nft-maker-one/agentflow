"""UTC-only datetime helpers.

The framework treats *all* timestamps as timezone-aware UTC. Naive
datetimes are rejected at the boundary — this avoids the entire
class of "off by N hours" bugs that come from mixing local time and UTC.

Serialization format: ISO 8601 with millisecond precision and ``Z``
suffix (``2026-05-26T10:10:00.123Z``).
"""

from __future__ import annotations

from datetime import UTC, datetime


def utcnow() -> datetime:
    """Return the current time as a timezone-aware UTC ``datetime``."""
    return datetime.now(UTC)


def to_iso(dt: datetime) -> str:
    """Serialize a UTC datetime to ISO-8601 with ``Z`` suffix.

    Raises :class:`ValueError` when ``dt`` is naive — callers should
    have already converted to UTC before calling this.
    """
    if dt.tzinfo is None:
        raise ValueError("datetime must be timezone-aware (UTC)")
    # ``isoformat`` yields ``+00:00`` for UTC; we normalize to ``Z``
    # which is conventional for log lines and external APIs.
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def from_iso(s: str) -> datetime:
    """Parse an ISO-8601 string back to a timezone-aware datetime.

    Accepts both ``...Z`` and ``...+00:00`` forms.
    """
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)
