"""Configuration-driven Gateway construction.

This module is the high-level entry point for *building* an
:class:`LLMGatewayClient`. It supports two ergonomics:

1. **Simple** — one provider, one default model::

       gateway = build_llm_gateway(
           default_provider="openai",
           default_model="gpt-4o-mini",
           # AGENTKIT_LLM_OPENAI_API_KEY in env
       )
       text = await gateway.chat("hi")

2. **Multi-instance** — declare each named instance separately,
   each with its own credentials and (optionally) endpoint::

       gateway = build_llm_gateway(
           instances=[
               LLMInstanceConfig(name="openai", compat="openai"),
               LLMInstanceConfig(name="deepseek", compat="deepseek"),
               LLMInstanceConfig(
                   name="openai-research",
                   compat="openai",
                   api_key=os.environ["OPENAI_KEY_RESEARCH"],
                   organization="org-research",
               ),
               LLMInstanceConfig(
                   name="azure-prod",
                   adapter="openai",
                   base_url="https://my-az.openai.azure.com/v1",
                   api_key=os.environ["AZURE_OPENAI_API_KEY"],
               ),
           ],
           default_provider="openai",
           default_model="gpt-4o-mini",
       )

Custom adapters (e.g. Anthropic, Gemini native) can register here::

    from agentkit.llm.registry import register_adapter
    register_adapter("anthropic", lambda cfg: MyAnthropicProvider(cfg))
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field

from agentkit.llm.gateway import LLMGatewayClient
from agentkit.llm.guardrail_iface import GuardrailHandle
from agentkit.llm.models import LLMBinding
from agentkit.llm.provider import LLMProvider
from agentkit.llm.providers.openai_provider import (
    OPENAI_COMPAT_PROVIDERS,
    OpenAIProvider,
    OpenAIProviderConfig,
)
from agentkit.llm.ratelimit import RateLimiter
from agentkit.llm.retry import RetryPolicy


# ----------------------------------------------------------------
# Public configuration object
# ----------------------------------------------------------------


@dataclass(frozen=True)
class LLMInstanceConfig:
    """Spec for a single named LLM provider instance.

    See module docstring for usage patterns.

    Fields
    ------
    name
        Registry key. This is what users put in ``LLMRequest.provider``
        or ``LLMBinding.provider`` to route to this instance. Must be
        unique within a Gateway.
    adapter
        Which adapter class to use. Currently only ``"openai"`` is
        built in (covers all OpenAI-compatible endpoints — DeepSeek,
        Qwen DashScope-compat, vLLM, Together, ...). Plug in custom
        adapters via :func:`register_adapter`.
    compat
        For the OpenAI adapter: which preset in
        :data:`OPENAI_COMPAT_PROVIDERS` supplies default ``base_url``
        and env-var lookup. Also used by :meth:`OpenAIProvider.price`
        for pricing tables. Defaults to ``"openai"``.
    base_url, api_key, api_key_env, organization, timeout_s
        Connection details. Resolution order for ``api_key``:

        1. ``api_key`` literal
        2. ``api_key_env`` env var
        3. The ``compat`` preset's default env var
    """

    name: str
    adapter: str = "openai"
    compat: str = "openai"

    base_url: str | None = None
    api_key: str | None = None
    api_key_env: str | None = None
    organization: str | None = None
    timeout_s: float = 60.0

    extra: dict[str, str] = field(default_factory=dict)


# ----------------------------------------------------------------
# Adapter registry — pluggable
# ----------------------------------------------------------------


AdapterFactory = Callable[[LLMInstanceConfig], LLMProvider]

_ADAPTER_REGISTRY: dict[str, AdapterFactory] = {}


def register_adapter(name: str, factory: AdapterFactory) -> None:
    """Register a custom adapter under ``name``.

    Subsequent calls with the same name overwrite the previous
    factory — useful for tests.
    """
    if not name:
        raise ValueError("adapter name must be non-empty")
    _ADAPTER_REGISTRY[name] = factory


def list_adapters() -> list[str]:
    """Return registered adapter names (for diagnostics / CLI)."""
    return sorted(_ADAPTER_REGISTRY)


def build_provider(cfg: LLMInstanceConfig) -> LLMProvider:
    """Construct one provider instance from its config."""
    factory = _ADAPTER_REGISTRY.get(cfg.adapter)
    if factory is None:
        raise ValueError(
            f"unknown adapter {cfg.adapter!r}; registered: {list_adapters()}",
        )
    return factory(cfg)


# ----------------------------------------------------------------
# Built-in adapter: openai (and all its OpenAI-compat siblings)
# ----------------------------------------------------------------


def _resolve_openai_api_key(cfg: LLMInstanceConfig) -> str | None:
    """Resolution order: explicit > api_key_env > preset's env var."""
    if cfg.api_key:
        return cfg.api_key
    if cfg.api_key_env:
        return os.environ.get(cfg.api_key_env)
    preset = OPENAI_COMPAT_PROVIDERS.get(cfg.compat) or OPENAI_COMPAT_PROVIDERS["openai"]
    return os.environ.get(preset["api_key_env"])


def _resolve_openai_base_url(cfg: LLMInstanceConfig) -> str | None:
    if cfg.base_url:
        return cfg.base_url
    preset = OPENAI_COMPAT_PROVIDERS.get(cfg.compat) or OPENAI_COMPAT_PROVIDERS["openai"]
    return preset["base_url"]


def _build_openai(cfg: LLMInstanceConfig) -> OpenAIProvider:
    return OpenAIProvider(
        OpenAIProviderConfig(
            instance_name=cfg.name,
            compat=cfg.compat,
            api_key=_resolve_openai_api_key(cfg),
            base_url=_resolve_openai_base_url(cfg),
            organization=cfg.organization,
            timeout_s=cfg.timeout_s,
        ),
    )


# Auto-register the OpenAI adapter at import time so users don't
# have to remember to call register_adapter() in the simple case.
register_adapter("openai", _build_openai)


def _build_anthropic(cfg: LLMInstanceConfig) -> LLMProvider:
    """Factory for the native Anthropic adapter. Imported lazily so
    a missing ``anthropic`` SDK doesn't break the OpenAI-only path."""
    from agentkit.llm.providers.anthropic_provider import (
        AnthropicProvider,
        AnthropicProviderConfig,
    )

    api_key = cfg.api_key
    if not api_key and cfg.api_key_env:
        api_key = os.environ.get(cfg.api_key_env)
    if not api_key:
        api_key = (
            os.environ.get("AGENTKIT_LLM_ANTHROPIC_API_KEY")
            or os.environ.get("ANTHROPIC_API_KEY")
        )
    return AnthropicProvider(
        AnthropicProviderConfig(
            instance_name=cfg.name,
            api_key=api_key,
            base_url=cfg.base_url,
            timeout_s=cfg.timeout_s,
        ),
    )


register_adapter("anthropic", _build_anthropic)


# ----------------------------------------------------------------
# High-level factory
# ----------------------------------------------------------------


def build_llm_gateway(
    *,
    instances: list[LLMInstanceConfig] | None = None,
    bindings: dict[str, LLMBinding] | None = None,
    default_provider: str | None = None,
    default_model: str | None = None,
    guardrail: GuardrailHandle | None = None,
    rate_limiter: RateLimiter | None = None,
    retry_policy: RetryPolicy | None = None,
) -> LLMGatewayClient:
    """Build a fully-wired :class:`LLMGatewayClient` from config.

    Two common shapes:

    * **Simple** (one provider): pass only ``default_provider`` /
      ``default_model``. We auto-create a single instance named
      after ``default_provider`` (so the user doesn't have to write
      an ``LLMInstanceConfig`` themselves).
    * **Multi-instance**: pass ``instances=[...]`` and (recommended)
      a ``default_provider``/``default_model`` so unspecified
      requests can use the simple ``chat()`` shortcut.
    """
    instances_to_build = list(instances or [])

    if not instances_to_build and default_provider:
        # Auto-derive a single instance from the default name.
        # ``compat`` defaults to the same value, which gives nice
        # behavior for built-in compat presets (openai/deepseek/qwen).
        compat = default_provider if default_provider in OPENAI_COMPAT_PROVIDERS else "openai"
        instances_to_build.append(
            LLMInstanceConfig(name=default_provider, compat=compat),
        )

    if not instances_to_build:
        raise ValueError(
            "build_llm_gateway requires either `instances=...` or "
            "`default_provider=...`",
        )

    # Reject duplicate names early — silent overwrites would be a
    # debugging nightmare.
    seen: set[str] = set()
    providers: dict[str, LLMProvider] = {}
    for cfg in instances_to_build:
        if cfg.name in seen:
            raise ValueError(f"duplicate instance name: {cfg.name!r}")
        seen.add(cfg.name)
        providers[cfg.name] = build_provider(cfg)

    if default_provider and default_provider not in providers:
        raise ValueError(
            f"default_provider {default_provider!r} not found in instances "
            f"({sorted(providers)})",
        )

    return LLMGatewayClient(
        providers=providers,
        bindings=bindings,
        default_provider=default_provider,
        default_model=default_model,
        guardrail=guardrail,
        rate_limiter=rate_limiter,
        retry_policy=retry_policy,
    )
