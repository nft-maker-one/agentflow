"""Unit tests for ``agentkit.common.logging``.

Note on capture strategy: we deliberately avoid pytest's ``capsys``
here. Our ``setup_logging`` attaches a ``StreamHandler(sys.stdout)``
at fixture-time, which captures whatever ``sys.stdout`` is *at that
moment*. By the time the test body runs, pytest may have rotated
its capture buffer — the result is that the JSON line shows up in
"Captured stdout call" but ``capsys.readouterr()`` returns empty.

Two robust alternatives we use below:

1. ``structlog.testing.capture_logs`` — official test helper,
   intercepts structlog event dicts before any renderer runs.
   Use this for asserting on bound key/values.
2. A direct ``io.StringIO`` wired into ``WriteLoggerFactory`` —
   exercises the JSON renderer end-to-end without any stdout magic.
"""

from __future__ import annotations

import io
import json
import logging

import pytest
import structlog
from structlog.testing import capture_logs

from agentkit.common.logging import _build_processors, get_logger, setup_logging


# ----------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------


@pytest.fixture
def _json_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure structlog with JSON output for the duration of a test."""
    monkeypatch.setenv("AGENTKIT_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("AGENTKIT_LOG_FORMAT", "json")
    setup_logging(force=True)


# ----------------------------------------------------------------
# Tests
# ----------------------------------------------------------------


def test_get_logger_returns_a_bound_logger(_json_logging: None) -> None:
    """Smoke test — the public API works without raising."""
    log = get_logger("test")
    log.info("hello", x=1)
    bound = log.bind(y=2)
    bound.info("again")


def test_log_event_carries_user_supplied_fields(_json_logging: None) -> None:
    """Use ``structlog.testing.capture_logs`` to inspect bound fields.

    This bypasses the renderer/stream entirely — the most reliable way
    to verify that user-supplied kwargs reach the event dict.
    """
    log = get_logger("test_capture")
    with capture_logs() as captured:
        log.info("event_x", run_id="run_test", a=1)

    assert len(captured) == 1
    entry = captured[0]
    assert entry["event"] == "event_x"
    assert entry["run_id"] == "run_test"
    assert entry["a"] == 1
    # ``capture_logs`` uses ``log_level`` instead of ``level``.
    assert entry["log_level"] == "info"


def test_json_renderer_produces_valid_json_with_required_fields() -> None:
    """End-to-end: configure a writeable buffer + JSON processors,
    emit one line, parse it as JSON, assert the Doc10 strict fields.
    """
    buf = io.StringIO()

    structlog.configure(
        processors=_build_processors(json_output=True),
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        context_class=dict,
        logger_factory=structlog.WriteLoggerFactory(file=buf),
        cache_logger_on_first_use=False,
    )
    log = structlog.get_logger("test_json")
    log.info("event_x", run_id="run_test", a=1)

    line = buf.getvalue().strip()
    assert line, "renderer produced empty output"

    obj = json.loads(line)
    assert obj["event"] == "event_x"
    assert obj["run_id"] == "run_test"
    assert obj["a"] == 1
    # Doc10 §3.3: strict required fields
    assert "ts" in obj
    assert "level" in obj


def test_setup_logging_idempotent() -> None:
    """Calling ``setup_logging`` multiple times must not stack handlers."""
    setup_logging(force=True)
    initial_handlers = len(logging.getLogger().handlers)
    setup_logging()  # not forced — should be a no-op for handlers
    assert len(logging.getLogger().handlers) == initial_handlers
