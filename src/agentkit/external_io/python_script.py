"""User-script source/sink — let users wire arbitrary Python.

A *Python source* runs a user-supplied async generator that yields
``dict`` payloads; each is published to the configured Bus topic.

A *Python sink* runs a user-supplied async function once per envelope.

Both are sandboxed only by Python's normal import / exception machinery —
treat them as trusted code in a developer-tool context (same posture as
the per-agent ``python_script`` field).
"""

from __future__ import annotations

import asyncio
import inspect
import textwrap
from typing import Any

from agentkit.bus.interface import SubscribeSpec
from agentkit.common.logging import get_logger
from agentkit.external_io.interface import (
    ExternalSink,
    ExternalSource,
    KindMetadata,
)
from agentkit.external_io.registry import register_kind

log = get_logger(__name__)


def _compile_user_script(script: str, *, must_define: str) -> Any:
    src = textwrap.dedent(script)
    namespace: dict[str, Any] = {}
    code = compile(src, f"<user_external:{must_define}>", "exec")
    exec(code, namespace)  # noqa: S102
    fn = namespace.get(must_define)
    if fn is None:
        raise ValueError(f"user script must define ``{must_define}``")
    return fn


# ---------- source -------------------------------------------------------

class PythonScriptSource(ExternalSource):
    """Run a user async-generator that yields payload dicts.

    Required script::

        async def stream(ctx):
            while True:
                await asyncio.sleep(60)
                yield {"hello": "world"}

    ``ctx`` is a small object with ``ctx.config`` (the user-supplied
    config dict) and ``ctx.log`` (a structlog-bound logger).
    """

    kind = "python_script"

    def __init__(self, *, name: str, publish_topic: str, config: dict) -> None:
        super().__init__(name=name, publish_topic=publish_topic, config=config)
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    async def start(self, *, bus: Any) -> None:
        if self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._loop(bus))
        self._mark_started()
        log.info("ext.py.source.start", name=self.name)

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
        self.started_at = None

    async def _loop(self, bus: Any) -> None:
        script = self.config.get("script", "")
        try:
            fn = _compile_user_script(script, must_define="stream")
        except Exception as e:  # noqa: BLE001
            self.last_error = f"compile: {e}"
            log.error("ext.py.source.compile_failed", name=self.name, error=self.last_error)
            return

        ctx = type("Ctx", (), {"config": self.config, "log": log})()
        if not inspect.isasyncgenfunction(fn):
            self.last_error = "stream() must be an async generator"
            return
        out_field = self.config.get("output_field", "")
        try:
            async for raw in fn(ctx):
                if self._stop_event.is_set():
                    break
                # If user configured output_field, wrap whatever they
                # yield into ``{output_field: <yielded>}`` so the agent
                # contract is uniform with Telegram / Email sources.
                # If not configured, keep current dict-pass-through.
                if out_field:
                    if isinstance(raw, dict) and out_field not in raw:
                        # Promote raw to that field while preserving any
                        # other keys (e.g. ``_reply_to``).
                        payload = {out_field: raw}
                    elif isinstance(raw, dict):
                        payload = raw
                    else:
                        payload = {out_field: raw}
                else:
                    payload = dict(raw) if isinstance(raw, dict) else {"value": raw}
                await self.emit(payload)
        except asyncio.CancelledError:
            return
        except Exception as e:  # noqa: BLE001
            self.last_error = f"runtime: {e}"
            log.exception("ext.py.source.runtime_error", name=self.name)


# ---------- sink ---------------------------------------------------------

class PythonScriptSink(ExternalSink):
    """Run a user async function once per envelope.

    Required script::

        async def handle(ctx, payload):
            print(payload)
    """

    kind = "python_script"

    def __init__(self, *, name: str, subscribe_topic: str, config: dict) -> None:
        super().__init__(name=name, subscribe_topic=subscribe_topic, config=config)
        self._sub: Any = None
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    async def start(self, *, bus: Any) -> None:
        if self._task is not None:
            return
        self._sub = await bus.subscribe(
            SubscribeSpec(
                topic_pattern=self.subscribe_topic,
                group=f"grp.ext.py.sink.{self.name}",
                starting_position="latest",
            ),
        )
        self._stop_event.clear()
        self._task = asyncio.create_task(self._loop())
        self._mark_started()

    async def stop(self) -> None:
        self._stop_event.set()
        if self._sub:
            await self._sub.close()
            self._sub = None
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
        self.started_at = None

    async def _loop(self) -> None:
        script = self.config.get("script", "")
        try:
            fn = _compile_user_script(script, must_define="handle")
        except Exception as e:  # noqa: BLE001
            self.last_error = f"compile: {e}"
            return
        if not inspect.iscoroutinefunction(fn):
            self.last_error = "handle() must be ``async def``"
            return
        ctx = type("Ctx", (), {"config": self.config, "log": log})()
        try:
            async for delivered in self._sub.messages():
                envelope = delivered.envelope
                try:
                    await fn(ctx, dict(envelope.payload or {}))
                    self._mark_event()
                    await self.trace_delivered(
                        envelope, target=f"python:{self.name}", ok=True,
                    )
                except Exception as e:  # noqa: BLE001
                    self.last_error = f"runtime: {e}"
                    log.exception("ext.py.sink.runtime_error", name=self.name)
                    await self.trace_delivered(
                        envelope, target=f"python:{self.name}",
                        ok=False, error=self.last_error,
                    )
        except asyncio.CancelledError:
            return


# ---------- registration -------------------------------------------------

_PY_SCRIPT_FIELDS = {
    "script": {
        "type": "code",
        "label": "Python script",
        "required": True,
        "help": "Source must define ``async def stream(ctx)`` (yields dicts). "
                "Sink must define ``async def handle(ctx, payload)``.",
    },
}

register_kind(
    PythonScriptSource,
    KindMetadata(
        kind="python_script",
        direction="source",
        label="Custom Python script (source)",
        description="Run a user async-generator and publish each yielded payload.",
        fields={
            **_PY_SCRIPT_FIELDS,
            "output_field": {
                "type": "string",
                "label": "Output field name (optional)",
                "required": False,
                "default": "",
                "help": (
                    "If set, each yielded value is wrapped into "
                    "``{output_field: <yielded>}``. Leave blank to "
                    "publish the dict you yielded as-is."
                ),
            },
        },
    ),
)
register_kind(
    PythonScriptSink,
    KindMetadata(
        kind="python_script",
        direction="sink",
        label="Custom Python script (sink)",
        description="Run a user async function for each envelope.",
        fields=_PY_SCRIPT_FIELDS,
    ),
)
