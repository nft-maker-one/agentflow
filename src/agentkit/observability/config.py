"""Observability configuration — Doc10 §12."""

from __future__ import annotations

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ObservabilitySettings(BaseSettings):
    """Per-process observability config."""

    model_config = SettingsConfigDict(
        env_prefix="AGENTKIT_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Tracing
    otel_endpoint: str | None = Field(
        default=None,
        description=(
            "OTLP collector endpoint (e.g. ``http://localhost:4317``). "
            "Empty / unset → tracing emits to a no-op exporter."
        ),
    )
    trace_sample_rate: float = Field(
        default=1.0, ge=0.0, le=1.0,
        description="Span sample probability (1.0=all, 0.1=10%).",
    )
    service_name: str = Field(
        default="agentkit",
        description="Resource service.name reported on every span.",
    )

    # Logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
    )
    log_format: Literal["json", "text"] = Field(default="json")
    audit_full_payload: bool = Field(
        default=False,
        description=(
            "Whether to log + audit full request/response payloads. "
            "Default off — tracking only hash + length for PII safety."
        ),
    )

    # Metrics / probe HTTP server
    prom_port: int = Field(
        default=9100, ge=0, le=65535,
        description="Port on which the probe server exposes /metrics and /health.",
    )
    enable_probe_server: bool = Field(
        default=False,
        description=(
            "Auto-start the probe HTTP server on import? Defaults off so "
            "tests / Notebook contexts don't surprise-bind ports."
        ),
    )
