"""Notifier-specific exceptions."""

from __future__ import annotations

from agentkit.common.errors import (
    AgentKitError,
    PermanentError,
    TransientError,
    ValidationError,
)


class NotifierError(AgentKitError):
    """Base for notifier-layer errors."""


class RuleEvalError(ValidationError, NotifierError):
    """Raised when a ``when`` expression cannot be parsed / evaluated.

    The matcher catches this and treats the rule as *no match* —
    raising bubbles only at compile / unit-test time. We deliberately
    do NOT crash the dispatcher loop on a bad expression.
    """


class DispatchFailed(TransientError, NotifierError):
    """A channel reported delivery failure (e.g. webhook 5xx)."""


class TemplateRenderError(PermanentError, NotifierError):
    """Jinja2 rendering blew up (missing variable, syntax error, etc.)."""
