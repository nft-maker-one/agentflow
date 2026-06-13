"""External I/O — connect a workflow to the outside world.

External *sources* listen to an outside system (Telegram updates, IMAP
mailbox, custom Python script…) and publish each event onto a Bus topic
that one or more agents subscribe to.

External *sinks* subscribe to a Bus topic that some agent publishes to,
then push that envelope's payload to the outside (Telegram reply, SMTP,
custom Python script…).

The two are deliberately symmetric so a single registry can manage both.
Adapters are pluggable — new ones implement :class:`ExternalSource` /
:class:`ExternalSink` and register via :func:`register_kind`.
"""

from __future__ import annotations

from agentkit.external_io.interface import (
    ExternalSink,
    ExternalSource,
    KindMetadata,
)
from agentkit.external_io.manager import ExternalIOManager
from agentkit.external_io.registry import KIND_REGISTRY, register_kind

# Built-in adapters self-register via import side-effect.
from agentkit.external_io import telegram as _telegram  # noqa: F401
from agentkit.external_io import email_io as _email      # noqa: F401
from agentkit.external_io import python_script as _py    # noqa: F401

__all__ = [
    "ExternalSource",
    "ExternalSink",
    "ExternalIOManager",
    "KIND_REGISTRY",
    "KindMetadata",
    "register_kind",
]
