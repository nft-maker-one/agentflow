"""Testing helpers — for users writing pytest suites against AgentKit.

Public surface::

    from agentkit.testing import (
        LocalRuntime,
        MockLLMGateway,
        MockLLMProvider,
        run_agent_locally,
    )

These helpers are part of the SDK (shipped in src/), not test-only
fixtures. Every public AgentKit feature should be testable
**without** spinning up Kafka / Redis / a real LLM — that's the
whole point of this module.
"""

from agentkit.testing.local_runtime import LocalRuntime, run_agent_locally
from agentkit.testing.mock_llm import MockLLMGateway, MockLLMProvider

__all__ = [
    "LocalRuntime",
    "MockLLMGateway",
    "MockLLMProvider",
    "run_agent_locally",
]
