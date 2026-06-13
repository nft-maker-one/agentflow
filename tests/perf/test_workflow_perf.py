"""Performance benchmarks for the Workflow Compiler.

The compiler is on the project's hot path: every ``deploy`` runs it.
We track parse / validate / compile end-to-end as separate
``pytest-benchmark`` cases so regressions in any step are visible.
"""

from __future__ import annotations

import pytest

from agentkit.workflow.compiler.parse import parse_dict
from agentkit.workflow.compiler.validate import validate_ir
from agentkit.workflow import compile_from_dict
from tests.helpers.workflow_fixtures import (
    deep_copy,
    linear_three_node_workflow,
)


def _large_spec(n_agents: int = 50) -> dict:
    """Build a chain workflow with ``n_agents`` thinking nodes.

    Used to time the compiler on a non-trivial input.
    """
    spec = {
        "id": "wf_large",
        "version": 1,
        "agents": {},
        "edges": {},
    }
    for i in range(n_agents):
        spec["agents"][f"a{i}"] = {
            "role": "thinking",
            "subscribe": [{"topic": f"agent.a{i}.in.t"}],
            "publish": [{"topic": f"agent.a{i}.out.t"}],
        }
    spec["edges"]["e_in"] = {
        "from": "__start__",
        "to": "a0",
        "via": "agent.a0.in.t",
    }
    for i in range(n_agents - 1):
        # Connect a{i} → a{i+1} via agent.a{i+1}.in.t. Need to add
        # publish on the source so the validator is happy.
        spec["agents"][f"a{i}"]["publish"].append(
            {"topic": f"agent.a{i + 1}.in.t"},
        )
        spec["edges"][f"e_{i}_{i + 1}"] = {
            "from": f"a{i}",
            "to": f"a{i + 1}",
            "via": f"agent.a{i + 1}.in.t",
        }
    spec["edges"]["e_out"] = {
        "from": f"a{n_agents - 1}",
        "to": "__end__",
        "via": f"agent.a{n_agents - 1}.out.t",
    }
    return spec


@pytest.mark.perf
def test_perf_parse_small(benchmark) -> None:
    spec = linear_three_node_workflow()
    benchmark(parse_dict, deep_copy(spec))


@pytest.mark.perf
def test_perf_validate_small(benchmark) -> None:
    ir = parse_dict(linear_three_node_workflow())
    benchmark(validate_ir, ir)


@pytest.mark.perf
def test_perf_compile_small(benchmark) -> None:
    spec = linear_three_node_workflow()
    benchmark(compile_from_dict, deep_copy(spec))


@pytest.mark.perf
def test_perf_compile_50_agents(benchmark) -> None:
    spec = _large_spec(50)
    benchmark(compile_from_dict, deep_copy(spec))
