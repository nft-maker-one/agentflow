"""Compiler 6-step pipeline.

The pipeline is a *pure function*::

    raw_dict → parse → resolve → inject → validate → lower → plan → (IR, RuntimePlan)

Each step lives in its own module and can be invoked / tested in
isolation. The high-level entry point is :func:`compile_from_dict`
(and :func:`compile_workflow` for YAML files).

See ``Doc04 §3``.
"""

from agentkit.workflow.compiler.pipeline import (
    compile_from_dict,
    compile_workflow,
)

__all__ = ["compile_from_dict", "compile_workflow"]
