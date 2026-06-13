"""Shared pytest fixtures.

Convention: anything that talks to a real broker / DB lives behind
the ``integration`` marker; anything that times publish/consume
loops lives behind the ``perf`` marker. ``make test-unit`` skips
both.
"""

from __future__ import annotations

import os
import socket
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from agentkit.bus.kafka import KafkaEventBus, KafkaSettings
from agentkit.common.config import reset_settings_for_testing
from agentkit.common.logging import setup_logging


@pytest.fixture(autouse=True)
def _reset_singletons() -> None:
    """Drop config singletons between tests so env mutation works."""
    reset_settings_for_testing()


@pytest.fixture(scope="session", autouse=True)
def _setup_logging_once() -> None:
    """Configure logging once for the whole test session."""
    setup_logging(force=True)


# ---------------------------------------------------------------
# Helpers used by integration / perf tests
# ---------------------------------------------------------------


def _broker_reachable(host: str, port: int, timeout: float = 0.5) -> bool:
    """Return True iff TCP ``host:port`` is currently accepting."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def kafka_brokers() -> str:
    """Resolve broker bootstrap address; skip session if unreachable."""
    brokers = os.getenv("AGENTKIT_BUS_BROKERS", "localhost:9092")
    host, _, port = brokers.split(",")[0].partition(":")
    if not _broker_reachable(host, int(port or "9092")):
        pytest.skip(
            f"Kafka broker {brokers} unreachable — run `make up` to start dev infra",
        )
    return brokers


@pytest_asyncio.fixture
async def kafka_bus(kafka_brokers: str) -> AsyncIterator[KafkaEventBus]:
    """A started :class:`KafkaEventBus`. Stops on teardown."""
    settings = KafkaSettings(brokers=kafka_brokers)
    bus = KafkaEventBus(settings)
    await bus.start()
    try:
        yield bus
    finally:
        await bus.stop()


# ---------------------------------------------------------------
# pytest config
# ---------------------------------------------------------------


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item],
) -> None:
    """Auto-mark items in tests/integration and tests/perf dirs."""
    for item in items:
        rel = str(item.fspath)
        if "/tests/integration/" in rel or "\\tests\\integration\\" in rel:
            item.add_marker(pytest.mark.integration)
        if "/tests/perf/" in rel or "\\tests\\perf\\" in rel:
            item.add_marker(pytest.mark.perf)
        if "/tests/e2e/" in rel or "\\tests\\e2e\\" in rel:
            item.add_marker(pytest.mark.e2e)
