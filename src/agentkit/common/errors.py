"""Exception hierarchy used across AgentKit modules.

The hierarchy is intentionally shallow. Modules should subclass these
when they need a domain-specific error class, e.g.::

    class BusUnavailable(TransientError):
        '''Raised when the underlying broker is unreachable.'''

Avoid raw ``Exception`` in library code — always pick the right base
so callers can implement uniform retry / abort policies.
"""

from __future__ import annotations


class AgentKitError(Exception):
    """Root exception for all framework errors."""


class ConfigError(AgentKitError):
    """Raised when configuration loading or validation fails."""


class ValidationError(AgentKitError):
    """User-facing validation failure (IR / event schema / API input)."""


class TransientError(AgentKitError):
    """Transient failure: caller may retry (typically with backoff)."""


class PermanentError(AgentKitError):
    """Permanent failure: caller MUST NOT retry."""
