"""Static price tables for cost estimation (informational only).

Per Doc07 v0.2: cost is *not* a hard quota dimension; it's shown
to users for awareness. We keep the table small and let it grow
as new models become relevant. Unknown models price as $0.00 — the
caller can still see they used N tokens, just not the dollar amount.

Prices are USD per 1k tokens. Update via PRs as providers change them.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModelPrice:
    """USD per 1k tokens for prompt / completion / cached prompt."""

    prompt_per_1k: float
    completion_per_1k: float
    cached_prompt_per_1k: float = 0.0

    @classmethod
    def zero(cls) -> ModelPrice:
        return cls(0.0, 0.0, 0.0)


# OpenAI public pricing snapshots — accurate as of Phase 1 cut.
# Numbers are illustrative; treat them as informational, not a SLA.
_OPENAI_PRICES: dict[str, ModelPrice] = {
    "gpt-4o": ModelPrice(2.50 / 1000, 10.00 / 1000, 1.25 / 1000),
    "gpt-4o-mini": ModelPrice(0.15 / 1000, 0.60 / 1000, 0.075 / 1000),
    "o1": ModelPrice(15.00 / 1000, 60.00 / 1000, 7.50 / 1000),
    "o1-mini": ModelPrice(3.00 / 1000, 12.00 / 1000, 1.50 / 1000),
    "gpt-4-turbo": ModelPrice(10.00 / 1000, 30.00 / 1000),
    "gpt-3.5-turbo": ModelPrice(0.50 / 1000, 1.50 / 1000),
}


def lookup_openai_price(model: str) -> ModelPrice:
    """Return price entry for an OpenAI model, or zero for unknown."""
    return _OPENAI_PRICES.get(model, ModelPrice.zero())
