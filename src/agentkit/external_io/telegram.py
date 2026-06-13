"""Telegram BotFather adapter — long-polling source + sendMessage sink.

The source uses the Bot API's ``getUpdates`` long-polling loop (no
webhook server required), so it works cleanly behind NAT / on a
developer laptop.  The sink uses ``sendMessage``.

Adapter shape mirrors the pluggable Protocol — no Telegram dependency
leaks outside this module; if the user wants to support Slack / Discord
they'd add a new file with the same structure.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from agentkit.bus.interface import SubscribeSpec
from agentkit.common.logging import get_logger
from agentkit.external_io.interface import (
    ExternalSink,
    ExternalSource,
    KindMetadata,
)
from agentkit.external_io.registry import register_kind

log = get_logger(__name__)

API_BASE = "https://api.telegram.org/bot"


class TelegramSource(ExternalSource):
    """Long-poll Telegram updates and republish each ``message`` on the
    Bus.  Payload shape::

        {
            "kind": "telegram_message",
            "chat_id": int,
            "from": {user_id, username, first_name},
            "text": str,
            "raw": <full Telegram update>,
        }

    The ``chat_id`` is **also** copied to ``payload._reply_to.chat_id``
    so a downstream Telegram sink can reply without the user wiring it
    explicitly — making request/response loops trivial.
    """

    kind = "telegram"

    def __init__(self, *, name: str, publish_topic: str, config: dict) -> None:
        super().__init__(name=name, publish_topic=publish_topic, config=config)
        self._task: asyncio.Task | None = None
        self._client: httpx.AsyncClient | None = None
        self._stop_event = asyncio.Event()
        self._offset = 0

    async def start(self, *, bus: Any) -> None:
        if self._task is not None:
            return
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(35.0))
        self._stop_event.clear()
        self._task = asyncio.create_task(self._poll_loop(bus))
        self._mark_started()
        log.info("ext.telegram.source.start", name=self.name, topic=self.publish_topic)

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
        if self._client:
            await self._client.aclose()
            self._client = None
        self.started_at = None
        log.info("ext.telegram.source.stop", name=self.name)

    async def _poll_loop(self, bus: Any) -> None:
        token = self.config["token"]
        url = f"{API_BASE}{token}/getUpdates"
        # Drop any pre-existing backlog on first call so we don't replay
        # ancient messages every restart.
        first = True
        while not self._stop_event.is_set():
            try:
                params: dict[str, Any] = {"timeout": 25, "offset": self._offset}
                if first:
                    params["offset"] = -1
                    params["timeout"] = 0
                rsp = await self._client.get(url, params=params)  # type: ignore[union-attr]
                rsp.raise_for_status()
                data = rsp.json()
                if not data.get("ok"):
                    self.last_error = str(data)
                    await asyncio.sleep(2.0)
                    continue
                for upd in data.get("result", []):
                    self._offset = upd["update_id"] + 1
                    if first:
                        # On startup we pull -1 just to advance offset; skip publish.
                        continue
                    await self._publish_update(bus, upd)
                first = False
            except asyncio.CancelledError:
                break
            except Exception as e:  # noqa: BLE001
                self.last_error = f"{type(e).__name__}: {e}"
                log.warning("ext.telegram.source.error", name=self.name, error=self.last_error)
                await asyncio.sleep(3.0)

    async def _publish_update(self, bus: Any, upd: dict) -> None:
        msg = upd.get("message") or upd.get("edited_message") or upd.get("channel_post")
        if not msg:
            return
        text = msg.get("text", "")
        # User-configurable field — same idea as Agent ``output_field``.
        # Defaults to ``text`` so the contract is obvious; setting it to
        # e.g. ``q`` lets a Telegram source plug straight into a
        # workflow whose first agent expects ``payload.q``.
        out_field = self.config.get("output_field", "text") or "text"
        payload: dict[str, Any] = {
            out_field: text,
            "_meta": {
                "kind": "telegram_message",
                "chat_id": msg["chat"]["id"],
                "from": {
                    "user_id": msg.get("from", {}).get("id"),
                    "username": msg.get("from", {}).get("username"),
                    "first_name": msg.get("from", {}).get("first_name"),
                },
                "raw": upd,
            },
            # Convenience: a downstream Telegram sink can read this to
            # reply to the same chat without the user wiring it.
            "_reply_to": {"channel": "telegram", "chat_id": msg["chat"]["id"]},
        }
        await self.emit(payload)


class TelegramSink(ExternalSink):
    """Send each envelope's text to a Telegram chat.

    Two ways to pick the recipient (priority order):
    1. ``payload._reply_to.chat_id`` (auto-reply when paired with TelegramSource).
    2. ``config["chat_id"]`` static fallback.

    Two ways to pick the text:
    1. ``config["text_field"]`` — JSON-pointer-ish dotted path into payload.
       Default ``"text"``.
    2. If empty, fall back to JSON-dump of the entire payload.
    """

    kind = "telegram"

    def __init__(self, *, name: str, subscribe_topic: str, config: dict) -> None:
        super().__init__(name=name, subscribe_topic=subscribe_topic, config=config)
        self._task: asyncio.Task | None = None
        self._sub: Any = None
        self._client: httpx.AsyncClient | None = None
        self._stop_event = asyncio.Event()

    async def start(self, *, bus: Any) -> None:
        if self._task is not None:
            return
        self._sub = await bus.subscribe(
            SubscribeSpec(
                topic_pattern=self.subscribe_topic,
                group=f"grp.ext.tg.sink.{self.name}",
                starting_position="latest",
            ),
        )
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(15.0))
        self._stop_event.clear()
        self._task = asyncio.create_task(self._consume_loop())
        self._mark_started()
        log.info("ext.telegram.sink.start", name=self.name, topic=self.subscribe_topic)
        # Send a deterministic startup ping to the configured chat so
        # the user sees the sink is wired up. This replaces the
        # previous behaviour where the channel might surface a stale
        # cached message after re-deploy.
        await self._send_hello()

    async def _send_hello(self) -> None:
        token = self.config.get("token")
        chat_id = self.config.get("chat_id")
        if not token or not chat_id or not self._client:
            return
        try:
            url = f"{API_BASE}{token}/sendMessage"
            await self._client.post(
                url,
                json={
                    "chat_id": chat_id,
                    "text": f"{self.name} is working now",
                },
            )
        except Exception as e:  # noqa: BLE001
            log.warning(
                "ext.telegram.sink.hello_failed",
                name=self.name, error=str(e),
            )

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
        if self._client:
            await self._client.aclose()
            self._client = None
        self.started_at = None
        log.info("ext.telegram.sink.stop", name=self.name)

    async def _consume_loop(self) -> None:
        token = self.config["token"]
        send_url = f"{API_BASE}{token}/sendMessage"
        # Default to "result" so a Telegram sink listening on an
        # agent's output topic forwards the agent's reasoning result
        # by default — not the raw incoming text. Users can still
        # set this explicitly to any payload field.
        text_field: str = self.config.get("text_field", "result") or "result"
        static_chat_id = self.config.get("chat_id")

        try:
            async for delivered in self._sub.messages():
                envelope = delivered.envelope
                payload = dict(envelope.payload or {})
                chat_id = static_chat_id or (payload.get("_reply_to") or {}).get("chat_id")
                if not chat_id:
                    log.warning(
                        "ext.telegram.sink.no_chat_id",
                        name=self.name, payload_keys=list(payload),
                    )
                    await self.trace_delivered(
                        envelope, target="(missing chat_id)",
                        ok=False, error="no chat_id",
                    )
                    continue
                text = _read_path(payload, text_field) if text_field else None
                if text is None or text == "":
                    import json
                    text = json.dumps(payload, ensure_ascii=False, default=str)
                send_text = str(text)[:4000]
                try:
                    rsp = await self._client.post(  # type: ignore[union-attr]
                        send_url,
                        json={"chat_id": chat_id, "text": send_text},
                    )
                    rsp.raise_for_status()
                    body = rsp.json()
                    if not body.get("ok", False):
                        self.last_error = (
                            f"telegram returned ok=false: "
                            f"{body.get('description', '<no description>')}"
                        )
                        log.warning(
                            "ext.telegram.sink.api_rejected",
                            name=self.name, chat_id=chat_id,
                            description=body.get("description"),
                            error_code=body.get("error_code"),
                        )
                        await self.trace_delivered(
                            envelope, target=f"telegram:{chat_id}",
                            preview=send_text, ok=False,
                            error=str(body.get("description")),
                        )
                    else:
                        self._mark_event()
                        log.info(
                            "ext.telegram.sink.sent",
                            name=self.name, chat_id=chat_id,
                            preview=send_text[:80],
                        )
                        await self.trace_delivered(
                            envelope, target=f"telegram:{chat_id}",
                            preview=send_text, ok=True,
                        )
                except Exception as e:  # noqa: BLE001
                    self.last_error = f"{type(e).__name__}: {e}"
                    log.warning(
                        "ext.telegram.sink.send_failed",
                        name=self.name, chat_id=chat_id,
                        error=self.last_error,
                    )
                    await self.trace_delivered(
                        envelope, target=f"telegram:{chat_id}",
                        preview=send_text, ok=False, error=self.last_error,
                    )
        except asyncio.CancelledError:
            return


def _read_path(payload: dict, path: str) -> Any:
    cur: Any = payload
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


# ----- registration --------------------------------------------------

register_kind(
    TelegramSource,
    KindMetadata(
        kind="telegram",
        direction="source",
        label="Telegram (BotFather)",
        description=(
            "Long-poll updates from a BotFather token. Each user message "
            "becomes an envelope: ``{<output_field>: text, _meta: {...}, "
            "_reply_to: {chat_id}}``."
        ),
        fields={
            "token": {"type": "secret", "label": "Bot token", "required": True},
            "output_field": {
                "type": "string",
                "label": "Output field name",
                "required": False,
                "default": "text",
                "help": (
                    "Name of the payload field that carries the user's "
                    "message text. Match this to whatever payload field "
                    "the downstream agent expects — e.g. ``q`` for a "
                    "workflow whose first agent reads ``payload.q``."
                ),
            },
        },
    ),
)
register_kind(
    TelegramSink,
    KindMetadata(
        kind="telegram",
        direction="sink",
        label="Telegram (BotFather)",
        description="Send each envelope as a Telegram message via sendMessage.",
        fields={
            "token": {"type": "secret", "label": "Bot token", "required": True},
            "chat_id": {
                "type": "string", "label": "Target chat_id (optional)",
                "required": False,
                "help": (
                    "Negative IDs are groups / channels. When set, this "
                    "wins over any ``payload._reply_to.chat_id`` so you "
                    "can forward agent output to a different chat than "
                    "the original sender. Leave blank to auto-reply to "
                    "the user who triggered the workflow."
                ),
            },
            "text_field": {
                "type": "string", "label": "Body field (payload.<name>)",
                "default": "result",
                "required": False,
                "help": (
                    "Dotted path inside payload. Default ``result`` "
                    "matches the Agent's default ``output_field``. Set "
                    "to your agent's actual ``output_field`` if you "
                    "customized it."
                ),
            },
        },
    ),
)
