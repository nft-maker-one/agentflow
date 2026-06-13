"""Tool / function calling data models.

A single :class:`ToolSchema` written by the user is translated by
each provider adapter to the provider's native shape (OpenAI's
``tools[].function`` / Gemini's ``function_declarations`` / etc.).
Users only ever see the framework's normalized form.

See ``Doc06 §6``.
"""

from __future__ import annotations

from typing import Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, RootModel


class ToolSchema(BaseModel):
    """Specification of a callable tool the model can invoke."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=64)
    description: str = ""
    # ``parameters`` is a JSON Schema object describing arguments.
    parameters: dict[str, Any] = Field(default_factory=dict)


class ToolCall(BaseModel):
    """A completed tool call, returned by the model."""

    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    raw_arguments: str = ""


class ToolCallDelta(BaseModel):
    """An incremental tool-call fragment seen during streaming."""

    model_config = ConfigDict(extra="forbid")

    index: int
    id_delta: str | None = None
    name_delta: str | None = None
    arguments_delta: str | None = None


class _ToolChoiceNamed(BaseModel):
    """Force the model to call a specific tool by name."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)


# ``tool_choice`` may be one of: simple string keyword OR an object
# specifying a particular tool. We expose both via a RootModel so
# users can write either form interchangeably::
#
#     ToolChoice("auto")
#     ToolChoice({"name": "search_web"})
class ToolChoice(RootModel[Union[Literal["auto", "none", "required"], _ToolChoiceNamed]]):
    """User-facing union: keyword OR ``{name: ...}`` object."""

    @property
    def is_named(self) -> bool:
        return isinstance(self.root, _ToolChoiceNamed)

    @property
    def name(self) -> str | None:
        return self.root.name if isinstance(self.root, _ToolChoiceNamed) else None
