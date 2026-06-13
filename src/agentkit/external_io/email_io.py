"""Email adapters — IMAP poll source + SMTP sink.

Uses the stdlib ``imaplib`` / ``smtplib`` wrapped in
``loop.run_in_executor`` so we don't pull in another async-email dep.
For workflow-level integration this is plenty — large mailboxes can
later swap to ``aioimaplib``.
"""

from __future__ import annotations

import asyncio
import email
import smtplib
import ssl
from email.header import decode_header
from email.mime.text import MIMEText
from imaplib import IMAP4_SSL
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


# ---------- shared helpers ----------------------------------------------

def _decode_header(raw: str | None) -> str:
    if not raw:
        return ""
    parts = decode_header(raw)
    out: list[str] = []
    for txt, enc in parts:
        if isinstance(txt, bytes):
            try:
                out.append(txt.decode(enc or "utf-8", errors="replace"))
            except Exception:  # noqa: BLE001
                out.append(txt.decode("utf-8", errors="replace"))
        else:
            out.append(txt)
    return "".join(out)


def _extract_text(msg) -> str:  # type: ignore[no-untyped-def]
    """Best-effort extract a text/plain body."""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == "text/plain":
                charset = part.get_content_charset() or "utf-8"
                payload = part.get_payload(decode=True)
                if payload:
                    try:
                        return payload.decode(charset, errors="replace")
                    except Exception:  # noqa: BLE001
                        return payload.decode("utf-8", errors="replace")
        # Fallback to first part
        for part in msg.walk():
            charset = part.get_content_charset() or "utf-8"
            payload = part.get_payload(decode=True)
            if payload:
                try:
                    return payload.decode(charset, errors="replace")
                except Exception:  # noqa: BLE001
                    return payload.decode("utf-8", errors="replace")
        return ""
    payload = msg.get_payload(decode=True)
    if payload:
        charset = msg.get_content_charset() or "utf-8"
        try:
            return payload.decode(charset, errors="replace")
        except Exception:  # noqa: BLE001
            return payload.decode("utf-8", errors="replace")
    return msg.get_payload() or ""


# ---------- IMAP source -------------------------------------------------

class ImapSource(ExternalSource):
    """Poll an IMAP INBOX and publish each new (UNSEEN) message.

    Default poll interval = 30 s; QQ / Gmail tolerate this fine.
    Marks the message as ``\\Seen`` after publishing so we don't re-emit.
    """

    kind = "email_imap"

    def __init__(self, *, name: str, publish_topic: str, config: dict) -> None:
        super().__init__(name=name, publish_topic=publish_topic, config=config)
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    async def start(self, *, bus: Any) -> None:
        if self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._poll_loop(bus))
        self._mark_started()
        log.info("ext.imap.source.start", name=self.name, topic=self.publish_topic)

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
        log.info("ext.imap.source.stop", name=self.name)

    async def _poll_loop(self, bus: Any) -> None:
        host = self.config["host"]
        port = int(self.config.get("port", 993))
        user = self.config["user"]
        password = self.config["password"]
        mailbox = self.config.get("mailbox", "INBOX")
        interval = float(self.config.get("poll_interval_s", 30.0))

        while not self._stop_event.is_set():
            try:
                msgs = await asyncio.get_running_loop().run_in_executor(
                    None, _imap_fetch_unseen, host, port, user, password, mailbox,
                )
                out_field = self.config.get("output_field", "text") or "text"
                for parsed in msgs:
                    payload = {
                        out_field: parsed["text"],
                        "_meta": {
                            "kind": "email",
                            "from": parsed["from"],
                            "to": parsed["to"],
                            "subject": parsed["subject"],
                            "uid": parsed["uid"],
                        },
                        # Sink reply hint — to address copies the From.
                        "_reply_to": {"channel": "email", "to": parsed["from"]},
                    }
                    await self.emit(payload)
            except asyncio.CancelledError:
                break
            except Exception as e:  # noqa: BLE001
                self.last_error = f"{type(e).__name__}: {e}"
                log.warning("ext.imap.source.error", name=self.name, error=self.last_error)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass


def _imap_fetch_unseen(
    host: str, port: int, user: str, password: str, mailbox: str,
) -> list[dict]:
    out: list[dict] = []
    with IMAP4_SSL(host, port) as M:
        M.login(user, password)
        M.select(mailbox)
        typ, data = M.search(None, "UNSEEN")
        if typ != "OK":
            return out
        for num in data[0].split():
            typ, raw = M.fetch(num, "(RFC822)")
            if typ != "OK" or not raw or not raw[0]:
                continue
            msg = email.message_from_bytes(raw[0][1])
            out.append({
                "from": _decode_header(msg.get("From")),
                "to":   _decode_header(msg.get("To")),
                "subject": _decode_header(msg.get("Subject")),
                "text": _extract_text(msg),
                "uid":  num.decode("ascii", errors="replace"),
            })
            # Mark seen so we don't re-emit on next poll.
            M.store(num, "+FLAGS", "\\Seen")
    return out


# ---------- SMTP sink ---------------------------------------------------

class SmtpSink(ExternalSink):
    """Send each envelope as an email."""

    kind = "email_smtp"

    def __init__(self, *, name: str, subscribe_topic: str, config: dict) -> None:
        super().__init__(name=name, subscribe_topic=subscribe_topic, config=config)
        self._task: asyncio.Task | None = None
        self._sub: Any = None
        self._stop_event = asyncio.Event()

    async def start(self, *, bus: Any) -> None:
        if self._task is not None:
            return
        self._sub = await bus.subscribe(
            SubscribeSpec(
                topic_pattern=self.subscribe_topic,
                group=f"grp.ext.smtp.sink.{self.name}",
                starting_position="latest",
            ),
        )
        self._stop_event.clear()
        self._task = asyncio.create_task(self._consume_loop())
        self._mark_started()
        log.info("ext.smtp.sink.start", name=self.name, topic=self.subscribe_topic)
        # Send a deterministic startup ping so the configured recipient
        # immediately knows the sink is wired up.
        await self._send_hello()

    async def _send_hello(self) -> None:
        to_addr = self.config.get("to")
        if not to_addr:
            return
        try:
            await asyncio.get_running_loop().run_in_executor(
                None,
                _smtp_send,
                self.config,
                to_addr,
                f"AgentKit sink ready: {self.name}",
                f"{self.name} is working now",
            )
        except Exception as e:  # noqa: BLE001
            log.warning(
                "ext.smtp.sink.hello_failed",
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
        self.started_at = None
        log.info("ext.smtp.sink.stop", name=self.name)

    async def _consume_loop(self) -> None:
        try:
            async for delivered in self._sub.messages():
                envelope = delivered.envelope
                payload = dict(envelope.payload or {})
                to_addr = (
                    self.config.get("to")
                    or (payload.get("_reply_to") or {}).get("to")
                )
                if not to_addr:
                    log.warning("ext.smtp.sink.no_to", name=self.name)
                    await self.trace_delivered(
                        envelope, target="(missing to-address)",
                        ok=False, error="no to address",
                    )
                    continue
                subject = (
                    payload.get("subject")
                    or self.config.get("subject", "AgentKit notification")
                )
                text_field = self.config.get("text_field", "result") or "result"
                body = _read_path(payload, text_field)
                if body is None:
                    import json
                    body = json.dumps(payload, ensure_ascii=False, default=str)
                try:
                    await asyncio.get_running_loop().run_in_executor(
                        None,
                        _smtp_send,
                        self.config,
                        to_addr,
                        subject,
                        str(body),
                    )
                    self._mark_event()
                    log.info(
                        "ext.smtp.sink.sent", name=self.name,
                        to=to_addr, subject=subject,
                    )
                    await self.trace_delivered(
                        envelope, target=f"email:{to_addr}",
                        preview=str(body), ok=True,
                    )
                except Exception as e:  # noqa: BLE001
                    self.last_error = f"{type(e).__name__}: {e}"
                    log.warning(
                        "ext.smtp.sink.send_failed",
                        name=self.name, error=self.last_error,
                    )
                    await self.trace_delivered(
                        envelope, target=f"email:{to_addr}",
                        preview=str(body), ok=False, error=self.last_error,
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


def _smtp_send(cfg: dict, to_addr: str, subject: str, body: str) -> None:
    host = cfg["host"]
    port = int(cfg.get("port", 465))
    user = cfg["user"]
    password = cfg["password"]
    use_tls = bool(cfg.get("use_tls", True))
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to_addr
    if use_tls and port == 465:
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(host, port, context=ctx, timeout=15) as s:
            s.login(user, password)
            s.send_message(msg)
    else:
        with smtplib.SMTP(host, port, timeout=15) as s:
            s.ehlo()
            if use_tls:
                s.starttls()
                s.ehlo()
            s.login(user, password)
            s.send_message(msg)


# ---------- registration ------------------------------------------------

register_kind(
    ImapSource,
    KindMetadata(
        kind="email_imap",
        direction="source",
        label="Email IMAP poll",
        description=(
            "Periodically check an IMAP mailbox for unseen messages. "
            "Each message becomes ``{<output_field>: body, _meta: {from, "
            "to, subject, uid}, _reply_to: {to}}``."
        ),
        fields={
            "host":     {"type": "string", "label": "IMAP host", "required": True, "default": "imap.qq.com"},
            "port":     {"type": "number", "label": "Port",      "required": False, "default": 993},
            "user":     {"type": "string", "label": "Username (full email)", "required": True},
            "password": {"type": "secret", "label": "Password / app key", "required": True},
            "mailbox":  {"type": "string", "label": "Mailbox", "required": False, "default": "INBOX"},
            "poll_interval_s": {"type": "number", "label": "Poll interval (s)", "required": False, "default": 30},
            "output_field": {
                "type": "string",
                "label": "Output field name",
                "required": False,
                "default": "text",
                "help": (
                    "Name of the payload field that carries the email "
                    "body. Match this to the field expected by the "
                    "downstream agent."
                ),
            },
        },
    ),
)
register_kind(
    SmtpSink,
    KindMetadata(
        kind="email_smtp",
        direction="sink",
        label="Email SMTP send",
        description="Send each envelope as an email.",
        fields={
            "host":     {"type": "string", "label": "SMTP host", "required": True, "default": "smtp.qq.com"},
            "port":     {"type": "number", "label": "Port",      "required": False, "default": 465},
            "user":     {"type": "string", "label": "Username (full email)", "required": True},
            "password": {"type": "secret", "label": "Password / app key", "required": True},
            "to":       {"type": "string", "label": "Target to-address (optional)", "required": False,
                         "help": (
                             "When set, wins over ``payload._reply_to.to`` "
                             "so you can forward agent output to a fixed "
                             "recipient instead of replying to the sender."
                         )},
            "subject":  {"type": "string", "label": "Default subject", "required": False,
                         "default": "AgentKit notification"},
            "text_field": {"type": "string", "label": "Body field (payload.<name>)",
                           "required": False, "default": "result",
                           "help": (
                               "Dotted path inside payload. Default ``result`` "
                               "matches the Agent's default ``output_field``."
                           )},
            "use_tls":  {"type": "bool",   "label": "Use TLS", "required": False, "default": True},
        },
    ),
)
