"""Guardrail backend settings — Doc07 §11."""

from __future__ import annotations

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


FailMode = Literal["strict", "permissive"]


class GuardrailSettings(BaseSettings):
    """Per-process Guardrail config.

    Read from ``AGENTKIT_GUARDRAIL_*`` env vars (and an optional
    ``.env`` file). Defaults are biased for *local development*:
    permissive when Redis is unreachable so demos don't hard-fail.
    Production deployments flip ``fail_mode`` to ``strict`` so a
    Redis outage actually trips the guard.
    """

    model_config = SettingsConfigDict(
        env_prefix="AGENTKIT_GUARDRAIL_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL used as the quota ledger.",
    )
    fail_mode: FailMode = Field(
        default="permissive",
        description=(
            "Behavior when Redis is unreachable. ``strict`` rejects "
            "every precheck (returning QuotaUnavailable); ``permissive`` "
            "allows the call through and tags the audit as bypassed."
        ),
    )
    reservation_ttl_factor: float = Field(
        default=1.5, ge=1.0,
        description=(
            "Multiplied with the request's effective timeout to set the "
            "reservation's Redis TTL. Reservations that expire without "
            "consume() being called release their pre-charge automatically."
        ),
    )
    default_reservation_ttl_ms: int = Field(
        default=120_000, ge=1_000,
        description=(
            "Fallback TTL for reservations when the caller does not "
            "supply a request timeout. Two minutes is enough for any "
            "non-streaming chat call we ship with."
        ),
    )
    run_hash_ttl_seconds: int = Field(
        default=24 * 3600, ge=60,
        description=(
            "Soft TTL on the per-run quota hash. After finalize_run the "
            "ledger lingers for this long so UIs / CLIs can still query "
            "the final usage breakdown."
        ),
    )
    audit_batch_ms: int = Field(
        default=2_000, ge=0,
        description="Batching interval before flushing audit records to PG.",
    )
    namespace_prefix: str = Field(
        default="guardrail",
        description=(
            "Top-level Redis key prefix. Override when multiple "
            "AgentKit deployments share a Redis."
        ),
    )

    @property
    def is_strict(self) -> bool:
        return self.fail_mode == "strict"
