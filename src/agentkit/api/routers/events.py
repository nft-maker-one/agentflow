"""Run event Server-Sent Events stream.

GET /api/runs/{run_id}/events
    Streams every Bus envelope tagged with this ``run_id`` as it
    happens, plus replays any backlog already buffered in memory.

Format: ``text/event-stream``, one envelope per ``data:`` line as JSON.

CORRECTNESS NOTE — this loop guards against a classic condition-variable
missed-wakeup. The tap loop appends events to the buffer + ``notify_all``;
if the SSE generator is busy ``yield``-ing earlier events when the
notification fires, there's no waiter at that moment so the notification
is dropped. The next ``wait()`` would then block until the 5 s ping
timeout — and even then we'd never re-check the buffer in the timeout
branch. The fix is the standard one: check the predicate
(``len(events) > replayed``) BEFORE waiting on the condition, and again
after the timeout, so any events appended during a yield gap are
delivered on the next pass.
"""

from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

from agentkit.api.models import EventOut

router = APIRouter(tags=["events"])


@router.get("/runs/{run_id}/events")
async def run_events(run_id: str, req: Request):  # type: ignore[no-untyped-def]
    state = req.app.state.app_state

    async def event_gen():
        buf = state.buffer(run_id)
        replayed = 0

        # ① Replay backlog so a late subscriber catches up. For an
        #    archived run whose in-memory buffer is gone, fall back to the
        #    durably-persisted timeline.
        backlog = state.snapshot_events(run_id)
        if not backlog:
            backlog = await state.load_persisted_events(run_id)
        for env in backlog:
            yield {"event": "envelope", "data": _serialize(env)}
            replayed += 1

        # ② Live loop. The classic correct pattern is "predicate-then-wait":
        #    check whether new events are already available BEFORE
        #    blocking on cond, and again on timeout. This makes the
        #    loop robust to notifications that fire while we're busy
        #    yielding earlier events (no waiter ⇒ lost notify).
        while True:
            if await req.is_disconnected():
                break

            async with buf.cond:
                if len(buf.events) <= replayed:
                    # No new events visible → wait for one (or time out
                    # so we can heartbeat + check disconnect).
                    try:
                        await asyncio.wait_for(buf.cond.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        # Fall through; we re-check len(buf.events) below.
                        pass
                events = list(buf.events)

            # Predicate check OUTSIDE the lock so we don't hold it
            # while yielding (and blocking the tap loop).
            if len(events) > replayed:
                for env in events[replayed:]:
                    yield {"event": "envelope", "data": _serialize(env)}
                replayed = len(events)
            else:
                # Idle — keep the connection alive. Some proxies
                # close after 30 s of silence.
                yield {"event": "ping", "data": ""}

    return EventSourceResponse(event_gen())


@router.get("/runs/{run_id}/events/snapshot", response_model=list[EventOut])
async def run_events_snapshot(run_id: str, req: Request):  # type: ignore[no-untyped-def]
    """Plain JSON dump of every buffered envelope for this run.

    Acts as a backstop for the SSE stream — if a dev server proxy
    buffers SSE chunks, the UI can periodically poll this endpoint to
    keep the topology view in sync. Returns an empty list when the run
    has no buffered events (yet)."""
    state = req.app.state.app_state
    out: list[EventOut] = []
    backlog = state.snapshot_events(run_id)
    if not backlog:
        backlog = await state.load_persisted_events(run_id)
    for env in backlog:
        out.append(
            EventOut(
                event_id=env.event_id,
                topic=env.topic,
                payload=dict(env.payload or {}),
                from_template=(
                    env.from_.role if env.from_ and env.from_.role else None
                ),
                from_agent=env.from_.agent_id if env.from_ else None,
                causation_id=env.causation_id,
                created_at=env.ts,
            ),
        )
    return out


def _serialize(env) -> str:  # type: ignore[no-untyped-def]
    out = EventOut(
        event_id=env.event_id,
        topic=env.topic,
        payload=dict(env.payload or {}),
        from_template=(
            env.from_.role if env.from_ and env.from_.role else None
        ),
        from_agent=env.from_.agent_id if env.from_ else None,
        causation_id=env.causation_id,
        created_at=env.ts,
    )
    return json.dumps(out.model_dump(mode="json"), ensure_ascii=False)
