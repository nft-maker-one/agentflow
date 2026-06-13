"""SDK ergonomics — decorators, Builder, workflow factory.

This package is what most user code interacts with::

    from agentkit import workflow, agent, judge, IRBuilder

The actual classes / decorators live in submodules below; this
``__init__`` just curates the friendly surface.
"""

from agentkit.sdk.agent_class import Agent
from agentkit.sdk.builder import IRBuilder
from agentkit.sdk.decorators import agent, agent_meta_attr, get_agent_meta, judge
from agentkit.sdk.workflow import END, ERROR, START, WorkflowDef, workflow

__all__ = [
    "END",
    "ERROR",
    "START",
    "Agent",
    "IRBuilder",
    "WorkflowDef",
    "agent",
    "agent_meta_attr",
    "get_agent_meta",
    "judge",
    "workflow",
]
