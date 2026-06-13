"""Kafka adapter configuration.

Module-local settings — does NOT live in :mod:`agentkit.common.config`
to keep config surface area localized.

Env vars use the ``AGENTKIT_BUS_`` prefix per ``Doc02 §9.1``.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    """Connection + behavior settings for the Kafka adapter."""

    model_config = SettingsConfigDict(
        env_prefix="AGENTKIT_BUS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ---- connection ----
    brokers: str = Field(
        default="localhost:9092",
        description="Comma-separated bootstrap.servers list.",
    )
    client_id: str = Field(default="agentkit", description="Producer/consumer client_id")

    # ---- producer ----
    producer_acks: str = Field(
        default="all",
        description="0 / 1 / all. Always 'all' in prod for durability.",
    )
    producer_max_batch_size: int = Field(default=16_384, ge=1)
    producer_linger_ms: int = Field(default=5, ge=0)
    producer_compression: str | None = Field(
        default="gzip",
        description="None / 'gzip' / 'snappy' / 'lz4' / 'zstd'",
    )
    producer_request_timeout_ms: int = Field(default=30_000, ge=1_000)
    producer_max_message_bytes: int = Field(default=1_000_000, ge=1)

    # ---- consumer ----
    consumer_max_poll_records: int = Field(default=500, ge=1)
    consumer_session_timeout_ms: int = Field(default=10_000, ge=1_000)

    # ---- topic management ----
    default_partitions: int = Field(default=6, ge=1)
    default_retention_hours: int = Field(default=24, ge=1)
    dlq_retention_days: int = Field(default=7, ge=1)

    # ---- visibility / redelivery ----
    visibility_timeout_ms: int = Field(default=60_000, ge=1_000)
    max_redelivery: int = Field(default=6, ge=0)

    @property
    def bootstrap_servers(self) -> list[str]:
        """Return broker list as a list of ``host:port`` strings."""
        return [s.strip() for s in self.brokers.split(",") if s.strip()]
