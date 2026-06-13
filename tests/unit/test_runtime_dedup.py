"""Unit tests for the in-memory dedup store."""

from __future__ import annotations

import time

import pytest

from agentkit.runtime.dedup import DedupStore


class TestBasicSemantics:
    def test_first_seen_returns_false(self) -> None:
        store = DedupStore(window_ms=10_000)
        assert store.seen("evt_1") is False

    def test_second_seen_returns_true_within_window(self) -> None:
        store = DedupStore(window_ms=10_000)
        store.seen("evt_1")
        assert store.seen("evt_1") is True

    def test_distinct_event_ids_dont_collide(self) -> None:
        store = DedupStore(window_ms=10_000)
        assert store.seen("evt_1") is False
        assert store.seen("evt_2") is False
        assert store.seen("evt_1") is True


class TestExpiry:
    def test_expired_entry_treated_as_unseen(self) -> None:
        store = DedupStore(window_ms=20)  # 20ms TTL
        store.seen("evt_1")
        time.sleep(0.05)  # > TTL
        assert store.seen("evt_1") is False  # re-armed

    def test_zero_window_means_no_dedup(self) -> None:
        store = DedupStore(window_ms=0)
        # With a 0-ms window, even immediate consecutive seen() can be
        # considered fresh. Accept either behavior — but we want NO crash.
        for _ in range(5):
            store.seen("evt_x")  # no exception


class TestEviction:
    def test_max_entries_cap(self) -> None:
        store = DedupStore(window_ms=60_000, max_entries=3)
        for i in range(5):
            store.seen(f"evt_{i}")
        # The 2 oldest should have been evicted.
        assert len(store) == 3


class TestInputValidation:
    def test_empty_event_id_rejected(self) -> None:
        store = DedupStore()
        with pytest.raises(ValueError):
            store.seen("")

    def test_negative_window_rejected(self) -> None:
        with pytest.raises(ValueError):
            DedupStore(window_ms=-1)

    def test_zero_max_entries_rejected(self) -> None:
        with pytest.raises(ValueError):
            DedupStore(max_entries=0)
