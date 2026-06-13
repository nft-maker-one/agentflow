"""Settings for the Redis Streams bus.

Env vars use the ``AGENTKIT_BUS_`` prefix (shared with the Kafka adapter;
field names don't collide). The connection URL falls back across a few
conventional names so a single Redis can serve bus + guardrail.
"""

from __future__ import annotations

import os

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_redis_url() -> str:
    """Resolve the Redis URL from the conventional env names, in order."""
    return (
        os.environ.get("AGENTKIT_BUS_REDIS_URL")
        or os.environ.get("AGENTKIT_REDIS_URL")
        or os.environ.get("AGENTKIT_GUARDRAIL_REDIS_URL")
        or "redis://localhost:6379/0"
    )


class RedisStreamSettings(BaseSettings):
    """Tunables for :class:`~agentkit.bus.redis_stream.RedisStreamBus`."""

    model_config = SettingsConfigDict(
        env_prefix="AGENTKIT_BUS_",
        extra="ignore",
    )

    #: Connection URL. Defaults via :func:`_default_redis_url`.
    redis_url: str = Field(default_factory=_default_redis_url)
    #: Key namespace. Streams live at ``{key_prefix}:s:{topic}``; the
    #: topic registry (for wildcard discovery) at ``{key_prefix}:topics``.
    key_prefix: str = "ak"
    #: ``XREADGROUP`` block window (ms). Lower = snappier wildcard pickup
    #: of brand-new streams; higher = fewer Redis round-trips when idle.
    block_ms: int = 400
    #: Per-stream ``XADD`` MAXLEN (approximate) — bounds memory.
    maxlen: int = 10_000
    #: How often a wildcard subscriber re-scans the topic registry for
    #: streams created by OTHER processes (ms). Same-process publishes are
    #: picked up immediately via the in-process registry notification.
    scan_interval_ms: int = 500
    #: Batch size per ``XREADGROUP``.
    batch: int = 32
