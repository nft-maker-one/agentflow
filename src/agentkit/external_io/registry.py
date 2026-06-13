"""Registry mapping ``kind`` strings → adapter class + metadata."""

from __future__ import annotations

from typing import Type

from agentkit.external_io.interface import (
    ExternalSink,
    ExternalSource,
    KindMetadata,
)

# kind → (cls, metadata)
KIND_REGISTRY: dict[
    str, tuple[Type[ExternalSource] | Type[ExternalSink], KindMetadata],
] = {}


def register_kind(
    cls: Type[ExternalSource] | Type[ExternalSink], meta: KindMetadata,
) -> None:
    """Register an adapter class.  Last write wins (allows overrides)."""
    KIND_REGISTRY[meta.kind + ":" + meta.direction] = (cls, meta)


def lookup(kind: str, direction: str):  # type: ignore[no-untyped-def]
    """Return ``(cls, metadata)`` or raise ``KeyError``."""
    return KIND_REGISTRY[kind + ":" + direction]


def list_kinds(direction: str | None = None) -> list[KindMetadata]:
    """Return all registered metadata, optionally filtered by direction."""
    out = [m for (_, m) in KIND_REGISTRY.values()]
    if direction:
        out = [m for m in out if m.direction == direction]
    out.sort(key=lambda m: (m.direction, m.kind))
    return out
