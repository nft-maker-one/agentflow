"""Tests for pluggable control-plane backends (``--mq`` / ``--store`` /
``--guardrail``).

Covers four layers:

1. The :mod:`agentkit.api.backends` factory — selector → concrete
   adapter, alias handling, case-insensitivity, invalid-value errors,
   and DSN/URL password redaction.
2. :class:`agentkit.api.AppState` accepting injected backends and
   driving their async ``start`` / ``stop`` lifecycle.
3. The Orchestrator's backend-agnostic quota registration (sync
   ``register_run`` *or* async ``init_run_quota``).
4. CLI validation — ``agentkit serve`` rejects unknown selectors with
   a clean exit code.

All adapters are *constructed* (not connected) here, so the suite needs
no Kafka / Postgres / Redis running — construction is cheap and pure;
network I/O only happens in ``start()``.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from agentkit.api import AppState
from agentkit.api import backends
from agentkit.api.backends import (
    GUARDRAIL_CHOICES,
    MQ_CHOICES,
    STORE_CHOICES,
    build_bus,
    build_guardrail,
    build_store,
    describe,
)
from agentkit.bus.inprocess import InProcessEventBus
from agentkit.cli.main import app
from agentkit.guardrail.inprocess import InProcessGuardrail
from agentkit.orchestrator import InMemoryRunStore, Orchestrator
from agentkit.workflow import compile_from_dict
from tests.helpers.workflow_fixtures import minimal_workflow


# ============================================================
# Factory — build_bus
# ============================================================


class TestBuildBus:
    def test_memory_is_default_inprocess(self) -> None:
        bus = build_bus("memory")
        assert isinstance(bus, InProcessEventBus)

    def test_empty_string_falls_back_to_memory(self) -> None:
        assert isinstance(build_bus(""), InProcessEventBus)

    def test_kafka_builds_kafka_bus(self) -> None:
        bus = build_bus("kafka")
        # Construction must not connect — only ``start()`` opens sockets.
        assert type(bus).__name__ == "KafkaEventBus"

    def test_redpanda_is_kafka_alias(self) -> None:
        bus = build_bus("redpanda")
        assert type(bus).__name__ == "KafkaEventBus"

    def test_case_insensitive(self) -> None:
        assert type(build_bus("KaFkA")).__name__ == "KafkaEventBus"

    def test_invalid_raises_with_choices(self) -> None:
        with pytest.raises(ValueError, match="unknown --mq backend 'nope'"):
            build_bus("nope")


# ============================================================
# Factory — build_store
# ============================================================


class TestBuildStore:
    def test_memory_is_default_inmemory(self) -> None:
        assert isinstance(build_store("memory"), InMemoryRunStore)

    def test_pg_builds_postgres_store(self) -> None:
        store = build_store("pg")
        # No pool is opened until ``start()`` — safe without a DB.
        assert type(store).__name__ == "PostgresRunStore"

    def test_postgres_alias(self) -> None:
        assert type(build_store("postgres")).__name__ == "PostgresRunStore"

    def test_case_insensitive(self) -> None:
        assert type(build_store("PG")).__name__ == "PostgresRunStore"

    def test_invalid_raises_with_choices(self) -> None:
        with pytest.raises(ValueError, match="unknown --store backend 'nope'"):
            build_store("nope")


# ============================================================
# Factory — build_guardrail
# ============================================================


class TestBuildGuardrail:
    def test_memory_is_default_inprocess(self) -> None:
        assert isinstance(build_guardrail("memory"), InProcessGuardrail)

    def test_redis_builds_redis_guardrail(self) -> None:
        g = build_guardrail("redis")
        assert type(g).__name__ == "RedisGuardrail"

    def test_invalid_raises_with_choices(self) -> None:
        with pytest.raises(ValueError, match="unknown --guardrail backend 'nope'"):
            build_guardrail("nope")


# ============================================================
# Choice tuples stay in sync with what the factories accept
# ============================================================


class TestChoiceTuples:
    @pytest.mark.parametrize("kind", MQ_CHOICES)
    def test_every_mq_choice_builds(self, kind: str) -> None:
        assert build_bus(kind) is not None

    @pytest.mark.parametrize("kind", STORE_CHOICES)
    def test_every_store_choice_builds(self, kind: str) -> None:
        assert build_store(kind) is not None

    @pytest.mark.parametrize("kind", GUARDRAIL_CHOICES)
    def test_every_guardrail_choice_builds(self, kind: str) -> None:
        assert build_guardrail(kind) is not None


# ============================================================
# Redaction + describe
# ============================================================


class TestRedaction:
    def test_dsn_password_hidden(self) -> None:
        out = backends._redact_dsn("postgresql://agentkit:secret@localhost:5432/agentkit")
        assert "secret" not in out
        assert "agentkit:***@localhost:5432/agentkit" in out

    def test_redis_url_password_hidden(self) -> None:
        out = backends._redact_url("redis://:hunter2@redis.example.com:6379/0")
        assert "hunter2" not in out
        assert ":***@redis.example.com" in out

    def test_url_without_credentials_unchanged(self) -> None:
        url = "redis://localhost:6379/0"
        assert backends._redact_url(url) == url

    def test_describe_includes_all_three(self) -> None:
        s = describe(mq="kafka", store="pg", guardrail="redis")
        assert "mq=kafka" in s and "store=pg" in s and "guardrail=redis" in s


# ============================================================
# AppState — injection + lifecycle
# ============================================================


class _StubStore:
    """Minimal RunStore-shaped stub recording lifecycle calls."""

    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class _StubGuardrail:
    """Guardrail stub recording lifecycle calls (no quota logic)."""

    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class TestAppStateBackends:
    def test_defaults_are_in_process(self) -> None:
        st = AppState()
        assert isinstance(st.bus, InProcessEventBus)
        assert isinstance(st.store, InMemoryRunStore)
        assert isinstance(st.guardrail, InProcessGuardrail)

    def test_accepts_injected_backends(self) -> None:
        bus = InProcessEventBus()
        store = InMemoryRunStore()
        guardrail = InProcessGuardrail()
        st = AppState(bus=bus, store=store, guardrail=guardrail)
        assert st.bus is bus
        assert st.store is store
        assert st.guardrail is guardrail

    async def test_no_arg_backward_compat_start_stop(self) -> None:
        # The historical ``AppState()`` call site must keep working.
        st = AppState()
        await st.start()
        await st.stop()

    async def test_drives_store_and_guardrail_lifecycle(self) -> None:
        store = _StubStore()
        guardrail = _StubGuardrail()
        st = AppState(store=store, guardrail=guardrail)

        await st.start()
        assert store.started is True
        assert guardrail.started is True

        await st.stop()
        assert store.stopped is True
        assert guardrail.stopped is True

    async def test_in_memory_backends_have_no_lifecycle_methods(self) -> None:
        # InMemoryRunStore / InProcessGuardrail must NOT grow async
        # start/stop — the probe in AppState relies on their absence.
        st = AppState()
        assert not hasattr(st.store, "start")
        assert not hasattr(st.guardrail, "start")
        # And start/stop must still succeed (probe is a no-op).
        await st.start()
        await st.stop()


# ============================================================
# Orchestrator — backend-agnostic quota registration
# ============================================================


class _SyncRegisterGuardrail:
    """Mimics InProcessGuardrail's synchronous ``register_run``."""

    def __init__(self) -> None:
        self.registered: list[str] = []

    def register_run(self, run_id: str, ctx: object) -> None:
        self.registered.append(run_id)


class _AsyncInitGuardrail:
    """Mimics RedisGuardrail's async ``init_run_quota`` (no register_run)."""

    def __init__(self) -> None:
        self.inited: list[str] = []

    async def init_run_quota(self, ctx: object) -> None:
        self.inited.append(ctx.run_id)


class _NoApiGuardrail:
    """A guardrail exposing neither registration API — must not crash."""


@pytest.fixture
async def deployed_orch_factory():
    """Returns a coroutine: (guardrail) -> (orch, ir) ready for create_run."""
    created: list[Orchestrator] = []

    async def _make(guardrail):
        ir, _plan = compile_from_dict(minimal_workflow())
        orch = Orchestrator(bus=InProcessEventBus(), store=InMemoryRunStore(),
                            guardrail=guardrail)
        await orch.start()
        await orch.deploy(ir)
        created.append(orch)
        return orch, ir

    yield _make

    for orch in created:
        await orch.stop()


class TestOrchestratorGuardrailAgnostic:
    async def test_sync_register_run_called(self, deployed_orch_factory) -> None:
        guardrail = _SyncRegisterGuardrail()
        orch, ir = await deployed_orch_factory(guardrail)
        run = await orch.create_run(workflow_id=ir.id, input={"x": 1})
        assert run.run_id in guardrail.registered

    async def test_async_init_run_quota_awaited(self, deployed_orch_factory) -> None:
        guardrail = _AsyncInitGuardrail()
        orch, ir = await deployed_orch_factory(guardrail)
        run = await orch.create_run(workflow_id=ir.id, input={"x": 1})
        # Proves the async path was selected AND awaited (list mutated).
        assert run.run_id in guardrail.inited

    async def test_no_registration_api_does_not_crash(
        self, deployed_orch_factory,
    ) -> None:
        guardrail = _NoApiGuardrail()
        orch, ir = await deployed_orch_factory(guardrail)
        # Should log a warning but still create the run successfully.
        run = await orch.create_run(workflow_id=ir.id, input={"x": 1})
        assert run.run_id

    async def test_real_inprocess_guardrail_registers_usage(
        self, deployed_orch_factory,
    ) -> None:
        guardrail = InProcessGuardrail()
        orch, ir = await deployed_orch_factory(guardrail)
        run = await orch.create_run(workflow_id=ir.id, input={"x": 1})
        usage = guardrail.get_usage(run.run_id)
        assert usage.get("run_id") == run.run_id


# ============================================================
# CLI — selector validation
# ============================================================


class TestServeCliValidation:
    """``agentkit serve`` rejects bad selectors *before* booting."""

    runner = CliRunner()

    def test_rejects_invalid_mq(self) -> None:
        result = self.runner.invoke(app, ["serve", "--mq", "rabbitmq"])
        assert result.exit_code == 2
        assert "invalid --mq" in result.output

    def test_rejects_invalid_store(self) -> None:
        result = self.runner.invoke(app, ["serve", "--store", "mongo"])
        assert result.exit_code == 2
        assert "invalid --store" in result.output

    def test_rejects_invalid_guardrail(self) -> None:
        result = self.runner.invoke(app, ["serve", "--guardrail", "memcached"])
        assert result.exit_code == 2
        assert "invalid --guardrail" in result.output

    def test_flags_appear_in_help(self) -> None:
        result = self.runner.invoke(app, ["serve", "--help"])
        assert result.exit_code == 0
        for flag in ("--mq", "--store", "--guardrail"):
            assert flag in result.output


class TestAutoResolveBackends:
    """`auto` selectors resolve against env; explicit choices win.

    The bus is driven by ``AGENTKIT_BUS_BACKEND`` (default ``redis``) and
    TCP-probed; ``_tcp_open`` is patched per-test so resolution is
    deterministic regardless of which ports happen to be open.
    """

    def _clear(self, monkeypatch):
        for k in ("AGENTKIT_PG_DSN", "AGENTKIT_PG_HOST", "AGENTKIT_BUS_BROKERS",
                  "AGENTKIT_BUS_BACKEND", "AGENTKIT_BUS_REDIS_URL",
                  "AGENTKIT_REDIS_URL", "AGENTKIT_GUARDRAIL_REDIS_URL"):
            monkeypatch.delenv(k, raising=False)

    def _probe(self, monkeypatch, reachable: bool):
        import agentkit.api.backends as backends
        monkeypatch.setattr(backends, "_tcp_open", lambda *a, **k: reachable)

    def test_default_bus_is_redis_when_reachable(self, monkeypatch):
        from agentkit.api.backends import resolve_backend_selection
        self._clear(monkeypatch)
        self._probe(monkeypatch, True)   # redis up
        assert resolve_backend_selection(mq="auto", store="auto", guardrail="auto") \
            == ("redis", "memory", "memory")

    def test_bus_degrades_to_memory_when_redis_down(self, monkeypatch):
        from agentkit.api.backends import resolve_backend_selection
        self._clear(monkeypatch)
        self._probe(monkeypatch, False)   # redis unreachable → degrade
        assert resolve_backend_selection(mq="auto", store="auto", guardrail="auto")[0] == "memory"

    def test_bus_backend_kafka_when_reachable(self, monkeypatch):
        from agentkit.api.backends import resolve_backend_selection
        self._clear(monkeypatch)
        monkeypatch.setenv("AGENTKIT_BUS_BACKEND", "kafka")
        self._probe(monkeypatch, True)
        assert resolve_backend_selection(mq="auto", store="auto", guardrail="auto")[0] == "kafka"

    def test_bus_backend_kafka_degrades_when_down(self, monkeypatch):
        from agentkit.api.backends import resolve_backend_selection
        self._clear(monkeypatch)
        monkeypatch.setenv("AGENTKIT_BUS_BACKEND", "kafka")
        self._probe(monkeypatch, False)
        assert resolve_backend_selection(mq="auto", store="auto", guardrail="auto")[0] == "memory"

    def test_bus_backend_memory_skips_probe(self, monkeypatch):
        from agentkit.api.backends import resolve_backend_selection
        self._clear(monkeypatch)
        monkeypatch.setenv("AGENTKIT_BUS_BACKEND", "memory")
        self._probe(monkeypatch, False)   # would degrade anyway; memory is explicit
        assert resolve_backend_selection(mq="auto", store="auto", guardrail="auto")[0] == "memory"

    def test_pg_dsn_enables_pg(self, monkeypatch):
        from agentkit.api.backends import resolve_backend_selection
        self._clear(monkeypatch)
        self._probe(monkeypatch, False)   # keep bus = memory
        monkeypatch.setenv("AGENTKIT_PG_DSN", "postgresql://a:b@localhost:5433/agentkit")
        assert resolve_backend_selection(mq="auto", store="auto", guardrail="auto") \
            == ("memory", "pg", "memory")

    def test_pg_host_parts_enable_pg(self, monkeypatch):
        from agentkit.api.backends import resolve_backend_selection
        self._clear(monkeypatch)
        self._probe(monkeypatch, False)
        monkeypatch.setenv("AGENTKIT_PG_HOST", "db.internal")
        assert resolve_backend_selection(mq="auto", store="auto", guardrail="auto")[1] == "pg"

    def test_all_three_enable_all(self, monkeypatch):
        from agentkit.api.backends import resolve_backend_selection
        self._clear(monkeypatch)
        monkeypatch.setenv("AGENTKIT_BUS_BACKEND", "kafka")
        self._probe(monkeypatch, True)
        monkeypatch.setenv("AGENTKIT_PG_DSN", "postgresql://a:b@h:5433/d")
        monkeypatch.setenv("AGENTKIT_GUARDRAIL_REDIS_URL", "redis://localhost:6379/0")
        assert resolve_backend_selection(mq="auto", store="auto", guardrail="auto") \
            == ("kafka", "pg", "redis")

    def test_explicit_store_choice_overrides_env(self, monkeypatch):
        from agentkit.api.backends import resolve_backend_selection
        self._clear(monkeypatch)
        self._probe(monkeypatch, False)   # bus → memory
        monkeypatch.setenv("AGENTKIT_PG_DSN", "postgresql://a:b@h:5433/d")
        # explicit --store memory wins despite PG env (SDK override)
        assert resolve_backend_selection(mq="auto", store="memory", guardrail="auto") \
            == ("memory", "memory", "memory")
