"""External-I/O env-var fallback resolution (``external_io/env.py``).

Covers the centralized secret resolution that mirrors the LLM provider
auto-detection in ``api/server.py``: omitted source/sink config fields
fall back to documented env vars, while explicit config always wins.
"""

from __future__ import annotations

import pytest

from agentkit.bus.inprocess import InProcessEventBus
from agentkit.external_io import env as env_mod
from agentkit.external_io.env import apply_env_defaults, external_env_var_names
from agentkit.external_io.interface import ExternalSource, KindMetadata
from agentkit.external_io.manager import ExternalIOManager
from agentkit.external_io.registry import register_kind


# ----------------------------------------------------------------
# Pure resolver
# ----------------------------------------------------------------


class TestApplyEnvDefaults:
    def test_fills_missing_token_from_env(self, monkeypatch) -> None:
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok-from-env")
        out = apply_env_defaults("telegram", "source", {"output_field": "theme"})
        assert out["token"] == "tok-from-env"
        assert out["output_field"] == "theme"

    def test_explicit_config_overrides_env(self, monkeypatch) -> None:
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok-from-env")
        out = apply_env_defaults("telegram", "source", {"token": "explicit"})
        assert out["token"] == "explicit"  # config wins

    def test_empty_string_is_treated_as_missing(self, monkeypatch) -> None:
        monkeypatch.setenv("SMTP_HOST", "smtp.env.com")
        out = apply_env_defaults("email_smtp", "sink", {"host": ""})
        assert out["host"] == "smtp.env.com"

    def test_alias_precedence_first_non_empty_wins(self, monkeypatch) -> None:
        monkeypatch.delenv("SMTP_TO", raising=False)
        monkeypatch.setenv("EMAIL_TO", "alias@example.com")
        out = apply_env_defaults("email_smtp", "sink", {})
        assert out["to"] == "alias@example.com"

    def test_smtp_full_set_from_env(self, monkeypatch) -> None:
        monkeypatch.setenv("SMTP_HOST", "smtp.qq.com")
        monkeypatch.setenv("SMTP_USER", "me@qq.com")
        monkeypatch.setenv("SMTP_PASSWORD", "authcode")
        monkeypatch.setenv("SMTP_TO", "to@x.com")
        out = apply_env_defaults("email_smtp", "sink", {"text_field": "result"})
        assert out["host"] == "smtp.qq.com"
        assert out["user"] == "me@qq.com"
        assert out["password"] == "authcode"
        assert out["to"] == "to@x.com"
        assert out["text_field"] == "result"

    def test_unknown_kind_passthrough(self) -> None:
        cfg = {"script": "..."}
        out = apply_env_defaults("python_script", "source", cfg)
        assert out == cfg
        assert out is not cfg  # copy, not the same object

    def test_input_not_mutated(self, monkeypatch) -> None:
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "x")
        original = {"output_field": "theme"}
        apply_env_defaults("telegram", "source", original)
        assert "token" not in original  # input left untouched

    def test_env_var_names_table_is_flat(self) -> None:
        names = external_env_var_names()
        assert "telegram.source.token" in names
        assert "TELEGRAM_BOT_TOKEN" in names["telegram.source.token"]
        assert "email_smtp.sink.to" in names


# ----------------------------------------------------------------
# Manager integration (network-free via a dummy kind)
# ----------------------------------------------------------------


class _DummySource(ExternalSource):
    kind = "dummy_env"

    async def start(self, *, bus) -> None:  # no network
        self.started_at = None

    async def stop(self) -> None:
        pass


@pytest.fixture
def _register_dummy(monkeypatch):
    register_kind(
        _DummySource,
        KindMetadata(
            kind="dummy_env", direction="source",
            label="dummy", description="test", fields={},
        ),
    )
    # Add an env fallback rule for the dummy kind.
    patched = dict(env_mod._EXTERNAL_ENV_FALLBACKS)
    patched[("dummy_env", "source")] = {"secret": ("DUMMY_ENV_SECRET",)}
    monkeypatch.setattr(env_mod, "_EXTERNAL_ENV_FALLBACKS", patched)
    yield


class TestManagerAppliesEnvDefaults:
    async def test_adapter_gets_env_secret_but_snapshot_stays_clean(
        self, monkeypatch, _register_dummy,
    ) -> None:
        monkeypatch.setenv("DUMMY_ENV_SECRET", "s3cr3t")
        bus = InProcessEventBus()
        await bus.start()
        mgr = ExternalIOManager(bus=bus)

        await mgr.add(
            "wf1", direction="source", kind="dummy_env",
            name="d1", topic="ext.d", config={"keep": "me"},
        )

        # The live adapter received the env-resolved secret …
        inst = mgr._sources["wf1"]["d1"]  # noqa: SLF001
        assert inst.config["secret"] == "s3cr3t"
        assert inst.config["keep"] == "me"

        # … but the persisted snapshot is env-free (secret never stored).
        snap = mgr._configs["wf1"][0]["config"]  # noqa: SLF001
        assert "secret" not in snap
        assert snap == {"keep": "me"}

        await mgr.stop_all()

    async def test_explicit_config_overrides_env_through_manager(
        self, monkeypatch, _register_dummy,
    ) -> None:
        monkeypatch.setenv("DUMMY_ENV_SECRET", "from-env")
        bus = InProcessEventBus()
        await bus.start()
        mgr = ExternalIOManager(bus=bus)

        await mgr.add(
            "wf2", direction="source", kind="dummy_env",
            name="d2", topic="ext.d", config={"secret": "explicit"},
        )
        inst = mgr._sources["wf2"]["d2"]  # noqa: SLF001
        assert inst.config["secret"] == "explicit"
        await mgr.stop_all()
