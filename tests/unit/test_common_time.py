"""Unit tests for ``agentkit.common.time``."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agentkit.common.time import from_iso, to_iso, utcnow


def test_utcnow_is_timezone_aware_utc() -> None:
    now = utcnow()
    assert now.tzinfo is not None
    assert now.tzinfo.utcoffset(now).total_seconds() == 0


def test_to_iso_uses_z_suffix() -> None:
    dt = datetime(2026, 5, 26, 10, 10, 0, 123_000, tzinfo=UTC)
    s = to_iso(dt)
    assert s == "2026-05-26T10:10:00.123Z"


def test_to_iso_rejects_naive_datetime() -> None:
    naive = datetime(2026, 5, 26, 10, 10, 0)
    with pytest.raises(ValueError):
        to_iso(naive)


@pytest.mark.parametrize(
    "iso",
    [
        "2026-05-26T10:10:00.123Z",
        "2026-05-26T10:10:00.123+00:00",
    ],
)
def test_from_iso_accepts_both_z_and_offset_form(iso: str) -> None:
    dt = from_iso(iso)
    assert dt.tzinfo is not None
    assert dt.year == 2026
    assert dt.minute == 10


def test_iso_round_trip_preserves_value() -> None:
    dt = datetime(2026, 5, 26, 10, 10, 0, 123_000, tzinfo=UTC)
    assert from_iso(to_iso(dt)) == dt
