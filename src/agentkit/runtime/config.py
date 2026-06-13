"""Runtime process-level configuration.

See ``Doc03 §9.1``.
"""

from __future__ import annotations

import socket

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RuntimeSettings(BaseSettings):
    """Per-process Runtime config.

    Read from ``AGENTKIT_RUNTIME_*`` env vars (and an optional
    ``.env`` file). Sensible defaults so unit tests don't need any
    explicit setup.
    """

    model_config = SettingsConfigDict(
        env_prefix="AGENTKIT_RUNTIME_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ---- identity ----
    runtime_id: str = Field(default_factory=socket.gethostname)
    roles: str = Field(
        default="*",
        description="Comma-separated Role names this Runtime serves; '*' = all",
    )

    # ---- pacing ----
    heartbeat_ms: int = Field(default=5_000, ge=1_000)
    drain_timeout_ms: int = Field(default=30_000, ge=0)
    handler_timeout_ms: int = Field(default=60_000, ge=1_000)
    dedup_window_ms: int = Field(default=300_000, ge=0)
    max_inflight: int = Field(default=16, ge=1)
    #: Default per-instance handler concurrency (B3). When > 1 the
    #: dispatch loop runs ``_process_message`` for multiple in-flight
    #: messages concurrently (gated by a Semaphore), letting LLM/I-O
    #: overlap on the same instance. Safe only because retry state is
    #: per-message-local (B4) and ack/nack targets are threaded
    #: explicitly (B17). Set to 1 to restore strict-serial semantics.
    #: See ``docs/CONCURRENCY.md`` (B3/B4).
    default_max_concurrent: int = Field(default=4, ge=1)
    #: Per-instance fan-in queue capacity (back-pressure bound). When
    #: full, subscriber drain tasks block on put(), slowing broker
    #: consumption. Raise for bursty multi-topic agents. See
    #: ``docs/CONCURRENCY.md`` (B-fanin).
    fanin_queue_size: int = Field(default=512, ge=1)
    #: Aggregator buffer eviction (B19) — drop a run's buffered inputs
    #: if it never completes within this window (a required topic that
    #: never arrives). 0 disables TTL eviction.
    aggregate_buffer_ttl_ms: int = Field(default=3_600_000, ge=0)
    #: Hard cap on concurrently-buffered aggregator run_ids per instance;
    #: oldest are evicted beyond this to bound memory.
    aggregate_max_buckets: int = Field(default=10_000, ge=1)

    # ---- retry within a single instance ----
    max_handler_retries: int = Field(default=3, ge=0)
    retry_base_ms: int = Field(default=500, ge=0)
    retry_factor: float = Field(default=2.0, ge=1.0)
    retry_max_ms: int = Field(default=8_000, ge=0)
    retry_jitter: bool = Field(default=True)

    @property
    def role_filter(self) -> set[str] | None:
        """Return set of accepted roles, or ``None`` for "any role"."""
        if self.roles.strip() == "*":
            return None
        return {r.strip() for r in self.roles.split(",") if r.strip()}
