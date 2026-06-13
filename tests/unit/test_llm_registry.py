"""Unit tests for ``agentkit.llm.registry``."""

from __future__ import annotations

import pytest

from agentkit.llm import (
    LLMBinding,
    LLMError,
    LLMErrorClass,
    LLMInstanceConfig,
    LLMRequest,
    build_llm_gateway,
    list_adapters,
    register_adapter,
)
from agentkit.llm.models import ChatMessage
from agentkit.llm.providers.openai_provider import OPENAI_COMPAT_PROVIDERS, OpenAIProvider
from tests.helpers.mock_provider import MockProvider


# ----------------------------------------------------------------
# Adapter registry basics
# ----------------------------------------------------------------


class TestAdapterRegistry:
    def test_openai_adapter_preregistered(self) -> None:
        assert "openai" in list_adapters()

    def test_register_custom_adapter(self) -> None:
        register_adapter("mock", lambda cfg: MockProvider(cfg.name))
        try:
            assert "mock" in list_adapters()
            gw = build_llm_gateway(
                instances=[LLMInstanceConfig(name="m", adapter="mock")],
                default_provider="m",
                default_model="mock-model",
            )
            assert "m" in gw.providers
            assert isinstance(gw.providers["m"], MockProvider)
        finally:
            # Cleanup so we don't leak state into other tests.
            from agentkit.llm.registry import _ADAPTER_REGISTRY

            _ADAPTER_REGISTRY.pop("mock", None)

    def test_unknown_adapter_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown adapter"):
            build_llm_gateway(
                instances=[LLMInstanceConfig(name="x", adapter="nope")],
                default_provider="x",
                default_model="m",
            )

    def test_register_adapter_rejects_empty_name(self) -> None:
        with pytest.raises(ValueError):
            register_adapter("", lambda cfg: MockProvider("x"))


# ----------------------------------------------------------------
# OpenAI compat presets
# ----------------------------------------------------------------


class TestOpenAICompat:
    @pytest.mark.parametrize(
        "compat", list(OPENAI_COMPAT_PROVIDERS),
    )
    def test_each_preset_builds_without_credentials(self, compat: str) -> None:
        # Construction must succeed even without API keys — health()
        # reports unhealthy at runtime, but the registry should never
        # fail at build time.
        gw = build_llm_gateway(
            instances=[LLMInstanceConfig(name=f"{compat}-test", compat=compat)],
            default_provider=f"{compat}-test",
            default_model="any-model",
        )
        prov = gw.providers[f"{compat}-test"]
        assert isinstance(prov, OpenAIProvider)
        assert prov.instance_name == f"{compat}-test"
        assert prov.compat == compat


# ----------------------------------------------------------------
# Multi-instance scenarios
# ----------------------------------------------------------------


class TestMultiInstance:
    def test_two_instances_of_same_compat_with_different_keys(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("KEY_TEAM_A", "team_a_key")
        monkeypatch.setenv("KEY_TEAM_B", "team_b_key")

        gw = build_llm_gateway(
            instances=[
                LLMInstanceConfig(
                    name="openai-team-a",
                    compat="openai",
                    api_key_env="KEY_TEAM_A",
                ),
                LLMInstanceConfig(
                    name="openai-team-b",
                    compat="openai",
                    api_key_env="KEY_TEAM_B",
                ),
            ],
            default_provider="openai-team-a",
            default_model="gpt-4o-mini",
        )
        assert sorted(gw.providers) == ["openai-team-a", "openai-team-b"]

    def test_duplicate_names_rejected(self) -> None:
        with pytest.raises(ValueError, match="duplicate instance name"):
            build_llm_gateway(
                instances=[
                    LLMInstanceConfig(name="dup"),
                    LLMInstanceConfig(name="dup"),
                ],
                default_provider="dup",
                default_model="m",
            )

    def test_default_must_match_one_instance(self) -> None:
        with pytest.raises(ValueError, match="default_provider"):
            build_llm_gateway(
                instances=[LLMInstanceConfig(name="a")],
                default_provider="ghost",
                default_model="m",
            )

    def test_no_instances_no_default_rejected(self) -> None:
        with pytest.raises(ValueError, match="instances|default_provider"):
            build_llm_gateway()


# ----------------------------------------------------------------
# Auto-derive single instance from defaults
# ----------------------------------------------------------------


class TestAutoDerive:
    def test_default_only_creates_implicit_instance(self) -> None:
        gw = build_llm_gateway(
            default_provider="openai",
            default_model="gpt-4o-mini",
        )
        assert "openai" in gw.providers

    def test_default_picks_compat_when_name_matches_preset(self) -> None:
        gw = build_llm_gateway(
            default_provider="deepseek",
            default_model="deepseek-chat",
        )
        prov = gw.providers["deepseek"]
        assert isinstance(prov, OpenAIProvider)
        assert prov.compat == "deepseek"

    def test_default_falls_back_to_openai_compat_for_custom_name(self) -> None:
        # A made-up name that's NOT in OPENAI_COMPAT_PROVIDERS still
        # builds — falls back to "openai" compat preset.
        gw = build_llm_gateway(
            default_provider="my-custom",
            default_model="any",
        )
        prov = gw.providers["my-custom"]
        assert isinstance(prov, OpenAIProvider)


# ----------------------------------------------------------------
# Gateway default behavior end-to-end with a custom adapter
# ----------------------------------------------------------------


class TestGatewayDefaults:
    @pytest.fixture
    def mock_gateway(self):
        register_adapter("mock", lambda cfg: MockProvider(cfg.name))
        gw = build_llm_gateway(
            instances=[
                LLMInstanceConfig(name="primary", adapter="mock"),
                LLMInstanceConfig(name="secondary", adapter="mock"),
            ],
            default_provider="primary",
            default_model="mock-model",
        )
        yield gw
        from agentkit.llm.registry import _ADAPTER_REGISTRY

        _ADAPTER_REGISTRY.pop("mock", None)

    async def test_chat_works_without_provider_or_model(self, mock_gateway) -> None:
        prov: MockProvider = mock_gateway.providers["primary"]
        prov.queue_text("default hello")

        text = await mock_gateway.chat("hi")
        assert text == "default hello"
        assert prov.calls[0].provider == "primary"
        assert prov.calls[0].model == "mock-model"

    async def test_request_can_override_default_provider(self, mock_gateway) -> None:
        secondary: MockProvider = mock_gateway.providers["secondary"]
        secondary.queue_text("from secondary")

        rsp = await mock_gateway.complete(
            LLMRequest(
                provider="secondary",
                model="mock-model",
                messages=[ChatMessage(role="user", content="hi")],
            ),
        )
        assert rsp.text == "from secondary"
        assert rsp.provider == "secondary"

    async def test_request_with_no_provider_uses_default(
        self, mock_gateway,
    ) -> None:
        prov: MockProvider = mock_gateway.providers["primary"]
        prov.queue_text("default path")

        rsp = await mock_gateway.complete(
            LLMRequest(messages=[ChatMessage(role="user", content="hi")]),
        )
        assert rsp.text == "default path"

    async def test_no_default_and_no_provider_raises(self) -> None:
        register_adapter("mock", lambda cfg: MockProvider(cfg.name))
        try:
            gw = build_llm_gateway(
                instances=[LLMInstanceConfig(name="m", adapter="mock")],
                # No default_provider / default_model.
            )
            with pytest.raises(LLMError) as ei:
                await gw.complete(
                    LLMRequest(messages=[ChatMessage(role="user", content="hi")]),
                )
            assert ei.value.klass is LLMErrorClass.INVALID_REQUEST
        finally:
            from agentkit.llm.registry import _ADAPTER_REGISTRY

            _ADAPTER_REGISTRY.pop("mock", None)


# ----------------------------------------------------------------
# Bindings registered with the Gateway
# ----------------------------------------------------------------


class TestBindingsThroughBuilder:
    def test_bindings_are_passed_through(self) -> None:
        register_adapter("mock", lambda cfg: MockProvider(cfg.name))
        try:
            gw = build_llm_gateway(
                instances=[
                    LLMInstanceConfig(name="primary", adapter="mock"),
                    LLMInstanceConfig(name="backup", adapter="mock"),
                ],
                bindings={
                    "researcher": LLMBinding(
                        provider="primary",
                        model="m1",
                        fallback=[LLMBinding(provider="backup", model="m2")],
                    ),
                },
                default_provider="primary",
                default_model="m1",
            )
            # Internal check — easier than running an end-to-end here.
            assert "researcher" in gw._bindings  # type: ignore[attr-defined]
        finally:
            from agentkit.llm.registry import _ADAPTER_REGISTRY

            _ADAPTER_REGISTRY.pop("mock", None)
