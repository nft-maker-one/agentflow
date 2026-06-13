"""External I/O Protocols — pluggable contract for sources / sinks.

Design notes
------------

* **Source** = "outside world → Bus".  Owns one background async
  task that polls / streams the outside system; on each new item
  it ``await bus.publish(envelope)`` to a configured topic.
* **Sink**   = "Bus → outside world".  Owns a Subscriber on a
  configured topic; ``messages()`` yields envelopes which the sink
  then renders + delivers (HTTP POST, SMTP send, …).
* Both share a tiny lifecycle: ``start()`` / ``stop()`` and a small
  ``health()`` snapshot used by the UI / API.

The Protocols below are intentionally minimal — extra adapter-specific
behaviour (e.g. Telegram's reply queue) lives inside each adapter.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class KindMetadata:
    """Static descriptor for a source / sink kind, surfaced to the UI."""

    kind: str                       # e.g. "telegram", "email_imap"
    direction: str                  # "source" | "sink"
    label: str                      # human label shown in the dropdown
    description: str = ""
    # Field name → (type, label, default, required, secret).
    # Drives the dynamic config form on the frontend.
    fields: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class IOHealth:
    """Lightweight snapshot returned by ``health()``."""

    running: bool
    started_at: datetime | None = None
    last_event_at: datetime | None = None
    events_total: int = 0
    last_error: str | None = None


class ExternalSource(abc.ABC):
    """Outside → Bus."""

    kind: str = "abstract"

    def __init__(
        self,
        *,
        name: str,
        publish_topic: str,
        config: dict[str, Any],
    ) -> None:
        self.name = name
        self.publish_topic = publish_topic
        self.config = config
        self.started_at: datetime | None = None
        self.last_event_at: datetime | None = None
        self.events_total: int = 0
        self.last_error: str | None = None
        # Wired by ExternalIOManager.add — controls whether each
        # publish auto-creates a Run (event-driven mode) or just
        # ships the envelope as-is (normal/standalone mode).
        self._emit = None  # type: ignore[assignment]
        # Late-bound by start(bus=...): used by trace_received().
        self._bus: Any = None
        # Late-bound by manager.add: workflow this source belongs to.
        # Empty string when used standalone (no manager wiring).
        self.workflow_id: str = ""

    async def emit(self, payload: dict[str, Any]) -> None:
        """Adapter publish helper. Routes through the manager-supplied
        callback so Run-creation policy is honoured. Falls back to a
        no-emit warning if nobody wired us up."""
        if self._emit is None:
            return
        await self._emit(
            payload, source_name=self.name, source_topic=self.publish_topic,
        )
        self._mark_event()
        await self._trace_received(payload)

    async def _trace_received(self, payload: dict[str, Any]) -> None:
        """Publish ``system.ext.source.<name>.received`` so the Live
        Event Timeline records each inbound message. Best-effort."""
        if self._bus is None:
            return
        try:
            from agentkit.bus.builder import build_envelope  # noqa: PLC0415
            # Build a tiny preview of the payload (≤120 chars) for the
            # Timeline to display without dumping secrets / huge blobs.
            preview = ""
            for k, v in payload.items():
                if k.startswith("_"):
                    continue
                if isinstance(v, (str, int, float, bool)):
                    preview = f"{k}={v!s}"[:120]
                    break
            topic = f"system.ext.source.{self.name}.received"
            trace_env = build_envelope(
                topic=topic,
                payload={
                    "source_name": self.name,
                    "publish_topic": self.publish_topic,
                    "kind": self.kind,
                    "preview": preview,
                    "fields": [k for k in payload if not k.startswith("_")],
                },
                workflow_id=self.workflow_id or "external",
                run_id="",
            )
            # Patch run_id from the session if present (best-effort —
            # the manager.emit just published with the session run_id,
            # but we don't have direct access to it from here).
            await self._bus.publish(trace_env)
        except Exception:  # noqa: BLE001
            pass

    @abc.abstractmethod
    async def start(self, *, bus: Any) -> None:
        """Begin polling/streaming.  Idempotent."""

    @abc.abstractmethod
    async def stop(self) -> None:
        """Stop polling and release resources.  Idempotent."""

    def health(self) -> IOHealth:
        return IOHealth(
            running=self.started_at is not None,
            started_at=self.started_at,
            last_event_at=self.last_event_at,
            events_total=self.events_total,
            last_error=self.last_error,
        )

    # ----- helpers ---------------------------------------------------

    def _mark_started(self) -> None:
        self.started_at = datetime.now(timezone.utc)
        self.last_error = None

    def _mark_event(self) -> None:
        self.last_event_at = datetime.now(timezone.utc)
        self.events_total += 1


class ExternalSink(abc.ABC):
    """Bus → outside."""

    kind: str = "abstract"

    def __init__(
        self,
        *,
        name: str,
        subscribe_topic: str,
        config: dict[str, Any],
    ) -> None:
        self.name = name
        self.subscribe_topic = subscribe_topic
        self.config = config
        self.started_at: datetime | None = None
        self.last_event_at: datetime | None = None
        self.events_total: int = 0
        self.last_error: str | None = None
        # Late-bound by manager (kept for backward compat).
        self._mark_terminal = None  # type: ignore[assignment]
        # Late-bound by start(bus=...): used by trace_delivered() to
        # publish a synthetic ``system.ext.sink.<name>.delivered``
        # envelope so the Live Event Timeline records each outbound.
        self._bus: Any = None
        # Late-bound by manager.add: workflow this sink belongs to.
        self.workflow_id: str = ""

    async def mark_terminal(self, run_id: str) -> None:
        """Notify the orchestrator that this envelope's run is done.
        No-op if no callback is wired (e.g. when used standalone)."""
        if self._mark_terminal is None or not run_id:
            return
        try:
            await self._mark_terminal(run_id)
        except Exception:  # noqa: BLE001
            pass  # best-effort; error visible via orchestrator logs

    async def trace_delivered(
        self,
        envelope: Any,
        *,
        target: str = "",
        preview: str = "",
        ok: bool = True,
        error: str | None = None,
    ) -> None:
        """Publish a synthetic ``system.ext.sink.<name>.delivered``
        envelope on the bus so the Live Event Timeline records each
        outbound delivery. Best-effort — failure is logged but never
        raised back into the sink loop."""
        if self._bus is None:
            return
        try:
            from agentkit.bus.builder import build_envelope  # noqa: PLC0415
            topic = f"system.ext.sink.{self.name}.delivered"
            payload = {
                "sink_name": self.name,
                "subscribe_topic": self.subscribe_topic,
                "target": target,
                "preview": preview[:120] if preview else "",
                "ok": ok,
                "error": error,
                "src_event_id": getattr(envelope, "event_id", None),
            }
            trace_env = build_envelope(
                topic=topic,
                payload=payload,
                workflow_id=(
                    getattr(envelope, "workflow_id", "")
                    or self.workflow_id or "external"
                ),
                run_id=getattr(envelope, "run_id", "") or "",
                trace_id=getattr(envelope, "trace_id", "") or "",
                causation_id=getattr(envelope, "event_id", None),
            )
            await self._bus.publish(trace_env)
        except Exception:  # noqa: BLE001
            pass  # best-effort; never break sink delivery

    @abc.abstractmethod
    async def start(self, *, bus: Any) -> None:
        """Subscribe to the Bus topic and begin delivering."""

    @abc.abstractmethod
    async def stop(self) -> None:
        """Tear down subscription + adapter-specific resources."""

    def health(self) -> IOHealth:
        return IOHealth(
            running=self.started_at is not None,
            started_at=self.started_at,
            last_event_at=self.last_event_at,
            events_total=self.events_total,
            last_error=self.last_error,
        )

    def _mark_started(self) -> None:
        self.started_at = datetime.now(timezone.utc)
        self.last_error = None

    def _mark_event(self) -> None:
        self.last_event_at = datetime.now(timezone.utc)
        self.events_total += 1
