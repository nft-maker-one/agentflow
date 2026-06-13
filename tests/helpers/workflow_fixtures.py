"""Reusable Workflow IR dict fixtures for tests.

These return *dicts* (not WorkflowIR instances) so individual tests
can mutate them — e.g. drop a field to assert validate() rejects it.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any


def minimal_workflow() -> dict[str, Any]:
    """Smallest valid Workflow: one Agent, one edge in, one edge out."""
    return {
        "id": "wf_minimal",
        "version": 1,
        "agents": {
            "echo": {
                "role": "thinking",
                "description": "echoes input",
                "subscribe": [{"topic": "agent.echo.in.q1"}],
                "publish": [{"topic": "agent.echo.out.summary"}],
            },
        },
        "edges": {
            "e_in": {
                "from": "__start__",
                "to": "echo",
                "via": "agent.echo.in.q1",
            },
            "e_out": {
                "from": "echo",
                "to": "__end__",
                "via": "agent.echo.out.summary",
            },
        },
    }


def linear_three_node_workflow() -> dict[str, Any]:
    """research → judge → writer with a switch deciding writer vs end.

    Topology semantics (Doc04 §2.1 / §3.3):

    * Each agent publishes to its OWN output topic (e.g. ``researcher``
      → ``agent.research.out.summary``).
    * The next agent subscribes to that topic to consume the result —
      that's the ``via`` on the direct edge.
    * For switch edges, the case-branch ``via`` is what the
      Orchestrator publishes after evaluating the switch expression
      against the from-Agent's output payload. The target Agent
      subscribes to it.
    """
    return {
        "id": "wf_research_to_report",
        "version": 1,
        "description": "research → judge → writer",
        "agents": {
            "researcher": {
                "role": "thinking",
                "subscribe": [{"topic": "agent.research.in.q1"}],
                "publish": [{"topic": "agent.research.out.summary"}],
                "llm": {"provider": "openai", "model": "gpt-4o-mini"},
            },
            "judge": {
                "role": "judge",
                "subscribe": [{"topic": "agent.research.out.summary"}],
                "publish": [{"topic": "agent.judge.out.choice"}],
                "llm": {"provider": "openai", "model": "gpt-4o-mini"},
            },
            "writer": {
                "role": "thinking",
                "subscribe": [{"topic": "agent.writer.in.draft"}],
                "publish": [{"topic": "agent.writer.out.report"}],
                "llm": {"provider": "openai", "model": "gpt-4o-mini"},
            },
        },
        "edges": {
            "e_in": {
                "from": "__start__",
                "to": "researcher",
                "via": "agent.research.in.q1",
            },
            "e_to_judge": {
                "from": "researcher",
                "to": "judge",
                "via": "agent.research.out.summary",
            },
            "e_judge_routing": {
                "from": "judge",
                "to": {"switch": "$.choice"},
                "cases": {
                    "writer": {
                        "to": "writer",
                        "via": "agent.writer.in.draft",
                    },
                    "end": {"to": "__end__"},
                },
            },
            "e_out": {
                "from": "writer",
                "to": "__end__",
                "via": "agent.writer.out.report",
            },
        },
        "guardrails": {
            "per_agent": {"max_tokens_per_call": 8000, "max_cycles": 5},
            "per_run": {"max_total_tokens": 200_000, "max_cycles_per_run": 200},
        },
    }


def fanout_workflow() -> dict[str, Any]:
    """A fanout edge: dispatcher → [worker_a, worker_b] via shared topic."""
    return {
        "id": "wf_fanout",
        "version": 1,
        "agents": {
            "dispatcher": {
                "role": "judge",
                "subscribe": [{"topic": "agent.dispatch.in.q1"}],
                "publish": [{"topic": "agent.work.in.task"}],
            },
            "worker_a": {
                "role": "thinking",
                "subscribe": [{"topic": "agent.work.in.task"}],
                "publish": [{"topic": "agent.work.out.result"}],
            },
            "worker_b": {
                "role": "thinking",
                "subscribe": [{"topic": "agent.work.in.task"}],
                "publish": [{"topic": "agent.work.out.result"}],
            },
        },
        "edges": {
            "e_in": {
                "from": "__start__",
                "to": "dispatcher",
                "via": "agent.dispatch.in.q1",
            },
            "e_fanout": {
                "from": "dispatcher",
                "to": ["worker_a", "worker_b"],
                "via": "agent.work.in.task",
            },
            "e_out_a": {
                "from": "worker_a",
                "to": "__end__",
                "via": "agent.work.out.result",
            },
            "e_out_b": {
                "from": "worker_b",
                "to": "__end__",
                "via": "agent.work.out.result",
            },
        },
    }


def deep_copy(spec: dict[str, Any]) -> dict[str, Any]:
    """Convenience deepcopy so tests can mutate freely."""
    return deepcopy(spec)
