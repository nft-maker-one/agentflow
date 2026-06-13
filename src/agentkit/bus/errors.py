"""Bus-specific exceptions.

Each maps to a class of failure documented in ``Doc02 §12``. Higher
layers (Runtime, Orchestrator) catch these and decide retry / DLQ
policy based on the *class*, not the underlying broker error.
"""

from __future__ import annotations

from agentkit.common.errors import PermanentError, TransientError


class BusError(TransientError):
    """Base class for transient bus errors. Caller may retry."""


class BusUnavailable(BusError):
    """Underlying broker is unreachable (network / metadata failure)."""


class PublishFailed(BusError):
    """Single publish attempt failed; retry per :mod:`tenacity` policy."""


class OversizedPayload(PermanentError):
    """Envelope exceeds broker max message size — caller must shrink.

    Typically the producer should switch to claim-check (object store
    + URI in payload) rather than retry.
    """
