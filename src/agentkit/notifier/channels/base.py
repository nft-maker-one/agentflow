"""Channel abstraction shared by all delivery adapters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from agentkit.notifier.models import Notification


@dataclass(frozen=True)
class DeliveryResult:
    """The outcome of one ``Channel.deliver`` call.

    The Notifier dispatcher uses ``ok`` to decide whether to mark
    the notification as ``sent`` vs ``failed``; ``detail`` lands in
    the audit table for diagnostics; ``status_code`` is informative
    only (HTTP-style adapters fill it in).
    """

    ok: bool
    detail: str = ""
    status_code: int | None = None


@runtime_checkable
class Channel(Protocol):
    """Contract every channel adapter must satisfy."""

    kind: str

    async def deliver(self, *, notification: Notification) -> DeliveryResult:
        """Ship the rendered notification. MUST NOT raise on remote
        errors — return ``DeliveryResult(ok=False, detail=...)``
        instead. Raise only for *programming* errors (config bug,
        bad data).
        """
        ...

    async def close(self) -> None:
        """Release any resources (HTTP clients, SMTP connections)."""
        ...
