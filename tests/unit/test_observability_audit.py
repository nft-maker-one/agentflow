"""Tests for the audit Protocol + InMemoryAuditWriter."""

from __future__ import annotations

import pytest

from agentkit.observability import (
    AuditEntry,
    AuditWriter,
    InMemoryAuditWriter,
)


class TestAuditEntry:
    def test_minimal_construction(self) -> None:
        e = AuditEntry(kind="run.terminate")
        assert e.audit_id.startswith("aud_")
        assert e.at is not None
        assert e.kind == "run.terminate"
        assert e.data == {}

    def test_extra_field_rejected(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            AuditEntry(kind="x", weird=1)  # type: ignore[call-arg]


class TestInMemoryAuditWriter:
    async def test_write_and_query(self) -> None:
        w = InMemoryAuditWriter()
        await w.write(AuditEntry(
            kind="run.terminate", run_id="run_1", workflow_id="wf_a",
        ))
        await w.write(AuditEntry(
            kind="notifier.delivery", run_id="run_1", workflow_id="wf_a",
        ))
        await w.write(AuditEntry(
            kind="run.terminate", run_id="run_2", workflow_id="wf_b",
        ))

        assert len(w) == 3
        assert len(w.filter(kind="run.terminate")) == 2
        assert len(w.filter(run_id="run_1")) == 2
        assert len(w.filter(kind="run.terminate", run_id="run_2")) == 1

    async def test_max_entries_bounded(self) -> None:
        w = InMemoryAuditWriter(max_entries=3)
        for i in range(10):
            await w.write(AuditEntry(kind="test", data={"i": i}))
        # Deque is capped — only the last 3 survive.
        assert len(w) == 3
        kept = [e.data["i"] for e in w.all()]
        assert kept == [7, 8, 9]

    async def test_clear(self) -> None:
        w = InMemoryAuditWriter()
        await w.write(AuditEntry(kind="x"))
        w.clear()
        assert len(w) == 0


class TestProtocolConformance:
    def test_in_memory_satisfies_protocol(self) -> None:
        w = InMemoryAuditWriter()
        # ``runtime_checkable`` Protocol — structural duck-type check.
        assert isinstance(w, AuditWriter)
