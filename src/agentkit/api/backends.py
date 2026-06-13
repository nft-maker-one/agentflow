"""Backend selection for the control plane.

Maps the ``--mq`` / ``--store`` CLI selectors to concrete
:class:`~agentkit.bus.interface.EventBus` / RunStore implementations.

Design rules:

* **Default is fully in-process** — zero external dependencies, so
  ``agentkit serve`` works out-of-the-box with no Docker / Kafka /
  Postgres running.
* **Heavy adapters are lazy-imported** — ``aiokafka`` / ``asyncpg`` are
  only imported when the user actually opts into ``kafka`` / ``pg``.
  This keeps the default install slim and the default path import-safe
  even when those extras aren't installed.
* Connection details come from the same ``AGENTKIT_*`` env vars the
  adapters already read (``AGENTKIT_BUS_BROKERS`` / ``AGENTKIT_PG_DSN``
  …), so behaviour matches ``docker-compose.yml`` defaults.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from agentkit.bus.inprocess import InProcessEventBus
from agentkit.common.logging import get_logger
from agentkit.guardrail.inprocess import InProcessGuardrail
from agentkit.orchestrator import InMemoryRunStore

if TYPE_CHECKING:
    from agentkit.bus.interface import EventBus
    from agentkit.llm.guardrail_iface import GuardrailHandle
    from agentkit.orchestrator import RunStore

log = get_logger(__name__)


# Buildable selector values accepted by ``build_*`` (``redpanda`` is an
# alias for ``kafka`` — Redpanda is Kafka-API-compatible). The ``auto``
# selector is NOT here: it must be resolved (see
# :func:`resolve_backend_selection`) into one of these before building.
MQ_CHOICES: tuple[str, ...] = ("memory", "kafka", "redpanda", "redis", "redis_stream")
STORE_CHOICES: tuple[str, ...] = ("memory", "pg", "postgres")
GUARDRAIL_CHOICES: tuple[str, ...] = ("memory", "redis")

#: Selects the durable bus when ``--mq auto``. ``redis`` (Redis Streams)
#: is the default — it fits AgentKit's many-low-fan-out-topics pattern far
#: better than Kafka. Set to ``kafka`` to opt into Kafka/Redpanda. Either
#: way the chosen middleware is TCP-probed before use and SILENTLY
#: DEGRADES to the in-memory bus if unreachable (so the server still boots).
BUS_BACKEND_ENV = "AGENTKIT_BUS_BACKEND"
DEFAULT_BUS_BACKEND = "redis"


def _first_env(*names: str) -> str | None:
    """Return the first non-empty environment variable among ``names``."""
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return None


def _tcp_open(host: str, port: int, timeout: float = 0.4) -> bool:
    """True if a TCP connection to ``host:port`` succeeds within timeout."""
    import socket  # noqa: PLC0415

    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _kafka_addr() -> tuple[str, int]:
    """Resolve the Kafka broker host:port to probe (first broker)."""
    brokers = _first_env("AGENTKIT_BUS_BROKERS") or "localhost:9092"
    first = brokers.split(",")[0].strip()
    host, _, port = first.partition(":")
    return host or "localhost", int(port or "9092")


def _redis_addr() -> tuple[str, int]:
    """Resolve the Redis host:port to probe from the bus/guardrail URL."""
    url = _first_env(
        "AGENTKIT_BUS_REDIS_URL", "AGENTKIT_REDIS_URL",
        "AGENTKIT_GUARDRAIL_REDIS_URL",
    ) or "redis://localhost:6379/0"
    # redis[s]://[user:pass@]host:port[/db]
    rest = url.split("://", 1)[-1]
    hostport = rest.split("@")[-1].split("/")[0]
    host, _, port = hostport.partition(":")
    return host or "localhost", int(port or "6379")


def _resolve_bus(mq: str) -> str:
    """Resolve the bus selector to a concrete, reachable backend.

    ``auto`` reads ``AGENTKIT_BUS_BACKEND`` (default ``redis``). The chosen
    middleware is TCP-probed; if it's offline we degrade to ``memory`` so
    the server still starts. An explicit non-auto selector is probed too.
    """
    requested = (mq or "auto").strip().lower()
    if requested == "auto":
        requested = (os.environ.get(BUS_BACKEND_ENV) or DEFAULT_BUS_BACKEND).strip().lower()
    if requested == "memory":
        return "memory"
    if requested in ("kafka", "redpanda"):
        host, port = _kafka_addr()
        if _tcp_open(host, port):
            return "kafka"
        log.warning("backend.bus.kafka_unreachable_degrade_memory", addr=f"{host}:{port}")
        return "memory"
    if requested in ("redis", "redis_stream"):
        host, port = _redis_addr()
        if _tcp_open(host, port):
            return "redis"
        log.warning("backend.bus.redis_unreachable_degrade_memory", addr=f"{host}:{port}")
        return "memory"
    log.warning("backend.bus.unknown_selector_degrade_memory", requested=requested)
    return "memory"


def resolve_backend_selection(
    *, mq: str, store: str, guardrail: str,
) -> tuple[str, str, str]:
    """Resolve ``auto`` selectors against the environment.

    MUST be called AFTER ``.env`` is loaded so values declared there are
    visible. An explicit selector (``memory`` / ``pg`` / ``kafka`` /
    ``redis`` …) is always honoured as-is; only ``auto`` is resolved:

    * store     → ``pg``    when ``AGENTKIT_PG_DSN`` (or the
      ``AGENTKIT_PG_HOST`` parts) is set, else ``memory``.
    * mq        → ``AGENTKIT_BUS_BACKEND`` (default ``redis``), TCP-probed;
      degrades to ``memory`` if the middleware is offline (see
      :func:`_resolve_bus`).
    * guardrail → ``redis`` when ``AGENTKIT_GUARDRAIL_REDIS_URL`` is set, else ``memory``.
    """
    def _resolve(value: str, *, detected: bool, persistent: str) -> str:
        v = (value or "auto").strip().lower()
        if v != "auto":
            return v               # explicit choice wins
        return persistent if detected else "memory"

    pg_detected = _first_env("AGENTKIT_PG_DSN", "AGENTKIT_PG_HOST") is not None
    redis_detected = _first_env("AGENTKIT_GUARDRAIL_REDIS_URL") is not None

    r_store = _resolve(store, detected=pg_detected, persistent="pg")
    # Bus: AGENTKIT_BUS_BACKEND-driven (default redis), TCP-probed with
    # degrade-to-memory. See :func:`_resolve_bus`.
    r_mq = _resolve_bus(mq)
    r_guardrail = _resolve(guardrail, detected=redis_detected, persistent="redis")

    log.info(
        "backend.selection.resolved",
        mq=r_mq, store=r_store, guardrail=r_guardrail,
        auto_pg=pg_detected, auto_redis=redis_detected,
    )
    return r_mq, r_store, r_guardrail


def build_bus(kind: str) -> EventBus:
    """Return an :class:`EventBus` for the given ``--mq`` selector.

    * ``memory`` (default) → :class:`InProcessEventBus`
    * ``kafka`` / ``redpanda`` → :class:`KafkaEventBus` (reads
      ``AGENTKIT_BUS_*`` env, e.g. ``AGENTKIT_BUS_BROKERS``)
    """
    k = (kind or "memory").strip().lower()
    if k == "memory":
        return InProcessEventBus()
    if k in ("redis", "redis_stream"):
        # Lazy import (redis is a core dep, but keep the pattern uniform).
        from agentkit.bus.redis_stream import (  # noqa: PLC0415
            RedisStreamBus, RedisStreamSettings,
        )

        settings = RedisStreamSettings()
        log.info("backend.bus.redis", url=settings.redis_url, prefix=settings.key_prefix)
        return RedisStreamBus(settings)
    if k in ("kafka", "redpanda"):
        # Lazy import — keeps aiokafka out of the default path.
        from agentkit.bus.kafka import KafkaEventBus, KafkaSettings  # noqa: PLC0415

        settings = KafkaSettings()
        log.info("backend.bus.kafka", brokers=settings.brokers, client_id=settings.client_id)
        return KafkaEventBus(settings)
    raise ValueError(
        f"unknown --mq backend {kind!r}; choose one of {', '.join(MQ_CHOICES)}",
    )


def build_store(kind: str) -> RunStore:
    """Return a RunStore for the given ``--store`` selector.

    * ``memory`` (default) → :class:`InMemoryRunStore` (lost on restart)
    * ``pg`` / ``postgres`` → :class:`PostgresRunStore` (reads
      ``AGENTKIT_PG_DSN`` or the ``AGENTKIT_PG_*`` parts)

    The caller is responsible for awaiting ``store.start()`` — see
    :meth:`agentkit.api.state.AppState.start`, which starts any store
    exposing a ``start`` coroutine.
    """
    k = (kind or "memory").strip().lower()
    if k == "memory":
        return InMemoryRunStore()
    if k in ("pg", "postgres"):
        # Lazy import — keeps asyncpg out of the default path.
        import os  # noqa: PLC0415

        from agentkit.orchestrator import PostgresRunStore  # noqa: PLC0415
        from agentkit.orchestrator.store_postgres import connect_from_env  # noqa: PLC0415

        dsn = connect_from_env()
        # Pool size bounds concurrent run-store operations; tune to the
        # expected concurrency (~ concurrent runs + 2×router replicas).
        min_size = int(os.environ.get("AGENTKIT_PG_POOL_MIN", "2"))
        max_size = int(os.environ.get("AGENTKIT_PG_POOL_MAX", "10"))
        log.info(
            "backend.store.postgres",
            dsn=_redact_dsn(dsn), pool_min=min_size, pool_max=max_size,
        )
        return PostgresRunStore(dsn=dsn, min_size=min_size, max_size=max_size)
    raise ValueError(
        f"unknown --store backend {kind!r}; choose one of {', '.join(STORE_CHOICES)}",
    )


def build_guardrail(kind: str) -> GuardrailHandle:
    """Return a guardrail backend for the given ``--guardrail`` selector.

    * ``memory`` (default) → :class:`InProcessGuardrail` (per-process,
      lost on restart)
    * ``redis`` → :class:`RedisGuardrail` (shared quota ledger across
      processes; reads ``AGENTKIT_GUARDRAIL_REDIS_URL``)

    Both satisfy the :class:`GuardrailHandle` Protocol. The Orchestrator
    registers run quotas through whichever lifecycle the backend exposes
    (sync ``register_run`` for in-process, async ``init_run_quota`` for
    Redis) — see ``Orchestrator._register_run_quota``.
    """
    k = (kind or "memory").strip().lower()
    if k == "memory":
        return InProcessGuardrail()
    if k == "redis":
        # Lazy import — keeps redis.asyncio out of the default path.
        from agentkit.guardrail.redis_backend.config import GuardrailSettings  # noqa: PLC0415
        from agentkit.guardrail.redis_backend.guardrail import RedisGuardrail  # noqa: PLC0415

        settings = GuardrailSettings()
        log.info(
            "backend.guardrail.redis",
            redis_url=_redact_url(settings.redis_url),
            fail_mode=settings.fail_mode,
        )
        return RedisGuardrail(settings=settings)
    raise ValueError(
        f"unknown --guardrail backend {kind!r}; choose one of {', '.join(GUARDRAIL_CHOICES)}",
    )


def describe(*, mq: str, store: str, guardrail: str = "memory") -> str:
    """One-line human summary for the serve banner / logs."""
    return (
        f"mq={(mq or 'memory').lower()}  "
        f"store={(store or 'memory').lower()}  "
        f"guardrail={(guardrail or 'memory').lower()}"
    )


# ------------------------------------------------------------
# Internals
# ------------------------------------------------------------


def _redact_dsn(dsn: str) -> str:
    """Hide the password component of a Postgres DSN before logging."""
    # postgresql://user:password@host:port/db  →  postgresql://user:***@host:port/db
    return _redact_url(dsn)


def _redact_url(url: str) -> str:
    """Hide the password in any ``scheme://user:pwd@host`` URL.

    Used for both Postgres DSNs and Redis URLs. URLs without
    credentials (``redis://localhost:6379/0``) are returned unchanged.
    """
    if "@" not in url or "://" not in url:
        return url
    scheme, _, rest = url.partition("://")
    creds, _, location = rest.partition("@")
    if ":" in creds:
        user, _, _pwd = creds.partition(":")
        creds = f"{user}:***"
    return f"{scheme}://{creds}@{location}"
