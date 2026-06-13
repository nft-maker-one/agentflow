"""Shared enums used across modules.

Using ``StrEnum`` so values serialize to plain strings in JSON
(they survive Pydantic ``model_dump_json`` round-trip cleanly).
"""

from __future__ import annotations

from enum import StrEnum


class Role(StrEnum):
    """Agent Role taxonomy. See ``Architecture.md §4`` and ``Doc04 §4``."""

    FETCH = "fetch"
    THINKING = "thinking"
    JUDGE = "judge"
    TOOL = "tool"
    MEMORY = "memory"
    GUARD = "guard"
    HUMAN = "human"
    AGGREGATOR = "aggregator"


class AgentState(StrEnum):
    """Agent FSM states. See ``Doc03 §2.3``.

    Note ``RETRY`` exists as a distinct state from ``FAILURE``: it
    represents "failed but retry budget not exhausted". Autoscaler
    and Notifier treat the two differently.
    """

    INIT = "Init"
    ACTIVE = "Active"
    PROCESSING = "Processing"
    RETRY = "Retry"
    WAITING = "Waiting"
    FAILURE = "Failure"
    DOWN = "Down"
    COMPLETE = "Complete"


class RunStatus(StrEnum):
    """Run lifecycle. See ``Doc05 §2.2``."""

    PENDING = "Pending"
    RUNNING = "Running"
    SUSPENDED = "Suspended"
    AWAITING_HUMAN = "AwaitingHuman"
    DEGRADED = "Degraded"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    ARCHIVED = "Archived"
