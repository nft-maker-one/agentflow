"""Unit tests for ``agentkit.common.ids``."""

from __future__ import annotations

import re

import pytest

from agentkit.common.ids import (
    new_agent_id,
    new_event_id,
    new_run_id,
    new_trace_id,
    new_ulid,
    prefixed_id,
)

ULID_RE = re.compile(r"^[0-7][0-9A-HJKMNP-TV-Z]{25}$")


def test_new_ulid_is_26_chars_and_crockford_base32() -> None:
    s = new_ulid()
    assert len(s) == 26
    assert ULID_RE.match(s), f"unexpected ULID format: {s}"


def test_new_ulid_uniqueness_under_burst() -> None:
    ids = {new_ulid() for _ in range(10_000)}
    assert len(ids) == 10_000


def test_prefixed_id_format() -> None:
    s = prefixed_id("xyz")
    assert s.startswith("xyz_")
    assert ULID_RE.match(s.removeprefix("xyz_"))


def test_prefixed_id_rejects_empty_prefix() -> None:
    with pytest.raises(ValueError):
        prefixed_id("")


@pytest.mark.parametrize(
    ("factory", "expected_prefix"),
    [
        (new_event_id, "evt_"),
        (new_trace_id, "trc_"),
        (new_run_id, "run_"),
        (new_agent_id, "agt_"),
    ],
)
def test_named_factories_use_correct_prefix(factory, expected_prefix: str) -> None:
    s = factory()
    assert s.startswith(expected_prefix)


def test_ulids_are_time_sortable() -> None:
    """ULIDs sort lexicographically by their *millisecond* timestamp prefix.

    ULID guarantees ms-level monotonicity. Strict in-millisecond
    ordering requires an opt-in *monotonic* factory which
    ``python-ulid`` does not enable by default — within the same
    ms the 80-bit random tail can produce any order.

    The framework relies on the ms-level guarantee for time-bucketed
    indexing (audit logs, run history); we verify that here by
    spacing generations across a 2 ms gap so each ULID lands in a
    distinct ms bucket.
    """
    import time

    ids: list[str] = []
    for _ in range(20):
        ids.append(new_ulid())
        time.sleep(0.002)  # > 1 ms — guarantees distinct ms buckets

    # The 10-character base32 timestamp prefix encodes the ms.
    timestamps = [u[:10] for u in ids]
    assert timestamps == sorted(timestamps), (
        "millisecond-prefix ordering is the core ULID contract"
    )
    # Full IDs also sort, because the prefixes differ.
    assert ids == sorted(ids)
