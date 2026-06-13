"""Backwards-compatible alias for ``InProcessEventBus``.

The actual implementation now lives in
:mod:`agentkit.bus.inprocess` (a real, ship-able adapter — not a mock).
This module remains so existing tests that import
``MockEventBus`` keep working.
"""

from __future__ import annotations

from agentkit.bus.inprocess import (
    InProcessEventBus as _InProcessEventBus,
    InProcessSubscriber as _InProcessSubscriber,
)

# Aliases for legacy tests that still import these names.
MockEventBus = _InProcessEventBus
MockSubscriber = _InProcessSubscriber

__all__ = ["MockEventBus", "MockSubscriber"]
