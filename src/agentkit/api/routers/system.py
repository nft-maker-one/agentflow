"""System info endpoints — version, providers, available models."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field

from agentkit import __version__

router = APIRouter(prefix="/system", tags=["system"])


class ProviderInfo(BaseModel):
    """One LLM provider visible to the gateway."""

    model_config = ConfigDict(extra="forbid")

    name: str
    adapter: str = "openai"
    compat: str | None = None
    base_url: str | None = None
    has_api_key: bool = False
    available_models: list[str] = Field(default_factory=list)


class SystemInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    n_projects: int
    n_workflows: int
    n_agents: int
    n_active_runs: int
    providers: list[ProviderInfo]


# Curated chat-completion model list per provider — for the UI's
# "select a model" dropdown.
#
# NOTE: The moderation API (``omni-moderation-latest`` /
# ``omni-moderation-2024-09-26`` — ``text-moderation-latest`` was
# deprecated in 2025) is a SEPARATE endpoint and not listed here.
# Use ``client.moderations.create(model="omni-moderation-latest", ...)``
# directly via the SDK if you need it; agent prompts go through
# chat completions.
#
# Phase 2.x: hard-coded; Phase 3 should query providers' /models endpoints.
_PROVIDER_MODELS: dict[str, list[str]] = {
    "openai": [
        "gpt-5.5",
        "gpt-5.5-pro",
        "gpt-5.4",
        "gpt-5.4-pro",
        "gpt-5.4-mini",
        "gpt-5.4-nano",
        "gpt-5-mini",
        "gpt-5-nano",
        "gpt-5",
        "gpt-4.1",
    ],
    "deepseek": [
        # Newest first.
        "deepseek-v4-flash",
        "deepseek-v4-pro",
        # Older / cheap models — kept for cost-sensitive smoke tests.
        "deepseek-chat",
        "deepseek-reasoner",
    ],
    "qwen": [
        "qwen3.7-max",
        "qwen3.6-plus",
    ],
    "gemini": [
        "gemini-3.5-flash",
        "gemini-3.1-flash-lite",
        "gemini-3.1-pro-preview",
        "gemini-2.5-pro",
    ],
    "anthropic": [
        "claude-opus-4-8",
        "claude-sonnet-4-6",
        "claude-haiku-4-5",
    ],
    "mock":     ["mock", "echo"],
}


@router.get("", response_model=SystemInfo)
async def get_system_info(req: Request) -> SystemInfo:
    state = req.app.state.app_state
    providers = _gateway_providers(state._llm_gateway)
    n_agents = sum(len(ir.agents) for ir in state.ir_by_id.values())
    # Backend-agnostic: count active runs via the public RunStore API
    # (``_runs`` only exists on the in-memory store, not Postgres).
    active_runs = await state.store.list_active()
    return SystemInfo(
        version=__version__,
        n_projects=len(state.projects),
        n_workflows=len(state.ir_by_id),
        n_agents=n_agents,
        n_active_runs=len(active_runs),
        providers=providers,
    )


@router.get("/llm-models", response_model=dict[str, list[str]])
async def list_llm_models() -> dict[str, list[str]]:
    """Return the curated provider→models map for UI dropdowns."""
    return _PROVIDER_MODELS


# ----------------------------------------------------------------
# Gateway introspection — best-effort, tolerates Mock vs real gateway
# ----------------------------------------------------------------


def _gateway_providers(gateway: Any) -> list[ProviderInfo]:
    if gateway is None:
        return []
    # Real LLMGatewayClient stores providers under .providers
    providers_dict = getattr(gateway, "providers", None)
    if not isinstance(providers_dict, dict):
        # MockLLMGateway exposes providers under .providers too.
        return []
    out: list[ProviderInfo] = []
    for name, prov in providers_dict.items():
        adapter_name = type(prov).__name__.lower()
        if "openai" in adapter_name:
            adapter = "openai"
        elif "mock" in adapter_name:
            adapter = "mock"
        else:
            adapter = adapter_name
        compat = getattr(prov, "compat", None) or getattr(prov, "_compat", None)
        base_url = getattr(prov, "base_url", None) or getattr(prov, "_base_url", None)
        has_key = bool(getattr(prov, "api_key", None) or getattr(prov, "_api_key", None))
        models = _PROVIDER_MODELS.get(name) or _PROVIDER_MODELS.get(compat or "", [])
        out.append(ProviderInfo(
            name=name,
            adapter=adapter,
            compat=compat,
            base_url=base_url,
            has_api_key=has_key,
            available_models=models,
        ))
    return out
