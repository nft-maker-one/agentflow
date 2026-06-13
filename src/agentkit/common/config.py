"""Centralized configuration via pydantic-settings.

Settings are populated from environment variables with the
``AGENTKIT_`` prefix and (optionally) a ``.env`` file in the project root.

Each module that needs configuration should define its OWN settings
class in its OWN module (e.g. ``agentkit.bus.kafka.config.KafkaSettings``)
and call ``get_settings(KafkaSettings)`` — this keeps the config
surface localized rather than dumping every knob into a god-object.

Usage::

    from agentkit.common.config import get_settings
    from agentkit.bus.kafka.config import KafkaSettings

    settings = get_settings(KafkaSettings)
"""

from __future__ import annotations

from typing import TypeVar

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CommonSettings(BaseSettings):
    """Cross-module settings (logging, profile, etc).

    Subclasses for module-specific config live alongside the modules
    that own them; they should also subclass ``BaseSettings`` directly
    rather than ``CommonSettings`` to keep concerns separate.
    """

    model_config = SettingsConfigDict(
        env_prefix="AGENTKIT_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ---- logging ----
    log_level: str = Field(default="INFO", description="DEBUG / INFO / WARNING / ERROR")
    log_format: str = Field(default="json", description="json | console")

    # ---- runtime metadata ----
    profile: str = Field(default="local", description="local / staging / prod")
    project_dir: str = Field(default=".", description="Project root directory")

    # ---- observability ----
    trace_sample_rate: float = Field(default=1.0, ge=0.0, le=1.0)


T = TypeVar("T", bound=BaseSettings)

_settings_cache: dict[type[BaseSettings], BaseSettings] = {}


def get_settings(cls: type[T]) -> T:
    """Return a cached singleton instance of the given settings class.

    Caching avoids re-parsing env vars on every call. Use
    :func:`reset_settings_for_testing` between tests when needed.
    """
    if cls not in _settings_cache:
        _settings_cache[cls] = cls()
    # mypy: cache is heterogeneous; runtime guarantees the right class.
    return _settings_cache[cls]  # type: ignore[return-value]


def reset_settings_for_testing() -> None:
    """Drop the settings cache.

    Tests that monkey-patch environment variables should call this
    in their ``setup`` / ``teardown`` to ensure fresh values.
    """
    _settings_cache.clear()
