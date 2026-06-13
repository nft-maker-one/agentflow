"""Per-AppState lifecycle manager for external sources / sinks.

Sources & sinks are scoped to a workflow_id (so the UI can list them
alongside the workflow's topology) but they live OUTSIDE the IR — adding
or removing them does NOT recompile the workflow, and the bus topic
they speak on is the only contract with the agents.

State shape::

    sources_by_workflow[wf_id] = {name: ExternalSource, ...}
    sinks_by_workflow[wf_id]   = {name: ExternalSink, ...}
"""

from __future__ import annotations

import asyncio
from typing import Any

from agentkit.common.logging import get_logger
from agentkit.external_io.env import apply_env_defaults
from agentkit.external_io.interface import ExternalSink, ExternalSource
from agentkit.external_io.registry import lookup

log = get_logger(__name__)


class ExternalIOManager:
    """Owns the live source / sink instances + their config snapshots."""

    def __init__(self, *, bus: Any) -> None:
        self._bus = bus
        # Late-bound by AppState.start() so we can call into the
        # orchestrator if we ever need it.
        self.orchestrator: Any = None
        # Late-bound: ``(workflow_id) -> str`` returning the workflow's
        # current mode ("normal" | "event_driven").
        self.mode_getter: Any = None
        # Late-bound: ``(workflow_id) -> str`` returning the active
        # event-driven SESSION run_id (empty string if not in
        # event_driven mode). All external events of one session share
        # this run_id so the UI shows one persistent "listening" run
        # instead of one Run per inbound message.
        self.session_run_getter: Any = None
        # workflow_id → {name: source}
        self._sources: dict[str, dict[str, ExternalSource]] = {}
        # workflow_id → {name: sink}
        self._sinks: dict[str, dict[str, ExternalSink]] = {}
        # Persisted configs so we can rebind on workflow redeploy.
        # workflow_id → list[{kind, direction, name, topic, config}]
        self._configs: dict[str, list[dict]] = {}

    # ---------------- emit hook ---------------------------------------

    def make_emit(self, workflow_id: str):  # type: ignore[no-untyped-def]
        """Build the ``emit(payload)`` callback the source uses to publish.

        In event-driven mode the envelope is stamped with the workflow's
        SESSION run_id (allocated once when the user flips to
        event-driven, kept until they flip back). All inbound events
        in that session share the same run_id — semantically the
        workflow is one long-lived listener, not a discrete Run per
        message. We just emit a structured log line for each event.

        In normal mode the envelope ships with run_id="" (sources
        shouldn't be active in normal mode anyway — the manager is
        paused — but this keeps emit safe).
        """
        async def _emit(
            payload: dict, *, source_name: str, source_topic: str,
        ) -> None:
            from agentkit.bus.builder import build_envelope  # local import — keep deps tidy
            run_id = (
                self.session_run_getter(workflow_id)
                if self.session_run_getter else ""
            ) or ""
            env = build_envelope(
                topic=source_topic,
                payload=payload,
                workflow_id=workflow_id if run_id else "external",
                run_id=run_id,
            )
            log.info(
                "ext.event",
                workflow_id=workflow_id,
                source=source_name,
                topic=source_topic,
                run_id=run_id or None,
                payload_keys=list(payload),
            )
            await self._bus.publish(env)
        return _emit

    def _find_raw_config(
        self, wf_id: str, *, direction: str, name: str,
    ) -> dict | None:
        """Return the prior raw config dict for a (workflow, direction, name)
        triple, or ``None``. Used to merge redacted secrets on edit."""
        for c in self._configs.get(wf_id, []):
            if c["direction"] == direction and c["name"] == name:
                return c
        return None

    # ---------------- query ------------------------------------------

    def list_for_workflow(self, wf_id: str) -> dict[str, Any]:
        out_sources = []
        for name, src in self._sources.get(wf_id, {}).items():
            out_sources.append({
                "name": name,
                "kind": src.kind,
                "direction": "source",
                "topic": src.publish_topic,
                "config": _redact(src.config),
                "health": _health_dict(src.health()),
            })
        out_sinks = []
        for name, snk in self._sinks.get(wf_id, {}).items():
            out_sinks.append({
                "name": name,
                "kind": snk.kind,
                "direction": "sink",
                "topic": snk.subscribe_topic,
                "config": _redact(snk.config),
                "health": _health_dict(snk.health()),
            })
        return {"sources": out_sources, "sinks": out_sinks}

    def topics_for_workflow(self, wf_id: str) -> dict[str, list[str]]:
        """Return ``{"source_topics": [...], "sink_topics": [...]}``.

        Used by the UI to render virtual external nodes in the graph
        without having to fetch the per-IO list separately."""
        return {
            "source_topics": sorted({
                s.publish_topic for s in self._sources.get(wf_id, {}).values()
            }),
            "sink_topics": sorted({
                s.subscribe_topic for s in self._sinks.get(wf_id, {}).values()
            }),
        }

    # ---------------- mutation ---------------------------------------

    async def add(
        self, wf_id: str, *,
        direction: str, kind: str, name: str, topic: str,
        config: dict,
    ) -> None:
        if direction not in ("source", "sink"):
            raise ValueError(f"unknown direction {direction!r}")

        cls, _meta = lookup(kind, direction)

        # When editing an existing IO, redacted secret placeholders
        # ("***xxxx") submitted by the UI mean "keep the old value".
        # Look up the prior raw config (BEFORE remove drops it) and
        # merge.
        prior = self._find_raw_config(wf_id, direction=direction, name=name)
        if prior is not None:
            config = _restore_redacted(config, prior.get("config", {}))

        # Stop+remove any existing instance with the same name so we
        # behave like an upsert (re-edit replaces).
        await self.remove(wf_id, direction=direction, name=name)

        # Fill any omitted secret (token / SMTP creds / recipient …) from
        # the environment — explicit config always wins. We build the
        # adapter from ``resolved`` but persist the original (env-free)
        # ``config`` below, so env secrets are never snapshotted and get
        # re-resolved on every (re)deploy. See ``external_io/env.py``.
        resolved = apply_env_defaults(kind, direction, config)

        if direction == "source":
            inst = cls(name=name, publish_topic=topic, config=resolved)  # type: ignore[call-arg]
            inst._emit = self.make_emit(wf_id)  # type: ignore[attr-defined]
            inst._bus = self._bus  # type: ignore[attr-defined]
            inst.workflow_id = wf_id
            await inst.start(bus=self._bus)
            self._sources.setdefault(wf_id, {})[name] = inst
        else:
            inst = cls(name=name, subscribe_topic=topic, config=resolved)  # type: ignore[call-arg]
            inst._bus = self._bus  # type: ignore[attr-defined]
            inst.workflow_id = wf_id
            await inst.start(bus=self._bus)
            self._sinks.setdefault(wf_id, {})[name] = inst

        # Snapshot config (raw — used for redeploy & GET-with-secrets).
        self._configs.setdefault(wf_id, [])
        # Drop any prior config matching (direction,name).
        self._configs[wf_id] = [
            c for c in self._configs[wf_id]
            if not (c["direction"] == direction and c["name"] == name)
        ]
        self._configs[wf_id].append({
            "direction": direction, "kind": kind, "name": name,
            "topic": topic, "config": config,
        })
        log.info(
            "ext.io.added", workflow_id=wf_id, direction=direction,
            kind=kind, name=name, topic=topic,
        )

    async def remove(
        self, wf_id: str, *, direction: str, name: str,
    ) -> bool:
        registry = (
            self._sources if direction == "source" else self._sinks
        ).get(wf_id, {})
        inst = registry.pop(name, None)
        if inst is None:
            return False
        try:
            await inst.stop()
        except Exception:  # noqa: BLE001
            log.exception(
                "ext.io.stop_failed", workflow_id=wf_id, direction=direction, name=name,
            )
        # Drop config snapshot
        if wf_id in self._configs:
            self._configs[wf_id] = [
                c for c in self._configs[wf_id]
                if not (c["direction"] == direction and c["name"] == name)
            ]
        log.info(
            "ext.io.removed", workflow_id=wf_id, direction=direction, name=name,
        )
        return True

    # ---------------- pause / resume (mode flip) ---------------------

    async def pause_for_workflow(self, wf_id: str) -> None:
        """Stop every running source/sink for ``wf_id`` but keep the
        instances + configs around. Used when the user flips back to
        normal mode — the configuration is preserved (greyed out in
        the UI) and resumed on next event-driven flip."""
        for inst in self._sources.get(wf_id, {}).values():
            try:
                if inst.started_at is not None:
                    await inst.stop()
            except Exception:  # noqa: BLE001
                log.exception("ext.io.pause_source_failed",
                              workflow_id=wf_id, name=inst.name)
        for inst in self._sinks.get(wf_id, {}).values():
            try:
                if inst.started_at is not None:
                    await inst.stop()
            except Exception:  # noqa: BLE001
                log.exception("ext.io.pause_sink_failed",
                              workflow_id=wf_id, name=inst.name)
        log.info("ext.io.paused", workflow_id=wf_id)

    async def resume_for_workflow(self, wf_id: str) -> None:
        """Re-start every paused source/sink for ``wf_id``."""
        for inst in self._sources.get(wf_id, {}).values():
            try:
                if inst.started_at is None:
                    inst._emit = self.make_emit(wf_id)  # type: ignore[attr-defined]
                    inst._bus = self._bus  # type: ignore[attr-defined]
                    inst.workflow_id = wf_id
                    await inst.start(bus=self._bus)
            except Exception:  # noqa: BLE001
                log.exception("ext.io.resume_source_failed",
                              workflow_id=wf_id, name=inst.name)
        for inst in self._sinks.get(wf_id, {}).values():
            try:
                if inst.started_at is None:
                    inst._bus = self._bus  # type: ignore[attr-defined]
                    inst.workflow_id = wf_id
                    await inst.start(bus=self._bus)
            except Exception:  # noqa: BLE001
                log.exception("ext.io.resume_sink_failed",
                              workflow_id=wf_id, name=inst.name)
        log.info("ext.io.resumed", workflow_id=wf_id)

    async def remove_all_for_workflow(self, wf_id: str) -> None:
        await asyncio.gather(
            *[s.stop() for s in self._sources.pop(wf_id, {}).values()],
            return_exceptions=True,
        )
        await asyncio.gather(
            *[s.stop() for s in self._sinks.pop(wf_id, {}).values()],
            return_exceptions=True,
        )
        self._configs.pop(wf_id, None)

    async def stop_all(self) -> None:
        for wf_id in list(self._sources) + list(self._sinks):
            await self.remove_all_for_workflow(wf_id)


def _health_dict(h) -> dict:  # type: ignore[no-untyped-def]
    return {
        "running": h.running,
        "started_at": h.started_at.isoformat() if h.started_at else None,
        "last_event_at": h.last_event_at.isoformat() if h.last_event_at else None,
        "events_total": h.events_total,
        "last_error": h.last_error,
    }


def _redact(config: dict) -> dict:
    """Hide ``token``/``password``/``api_key`` fields when sent to the UI."""
    SECRET_KEYS = {"token", "password", "api_key", "secret"}
    out = {}
    for k, v in config.items():
        if k.lower() in SECRET_KEYS and isinstance(v, str) and v:
            out[k] = "***" + v[-4:] if len(v) > 4 else "***"
        else:
            out[k] = v
    return out


def _restore_redacted(submitted: dict, prior: dict) -> dict:
    """If a value in ``submitted`` looks like the redacted placeholder
    we send to the UI (``***`` prefix), pull the original value from
    ``prior`` so the user can edit non-secret fields without re-entering
    every token / password."""
    SECRET_KEYS = {"token", "password", "api_key", "secret"}
    out = dict(submitted)
    for k, v in submitted.items():
        if k.lower() not in SECRET_KEYS:
            continue
        if isinstance(v, str) and v.startswith("***") and k in prior:
            out[k] = prior[k]
    return out
