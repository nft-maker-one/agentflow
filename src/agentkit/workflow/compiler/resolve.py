"""Step ② — Resolve: dereference external refs.

Phase 1 scope: this is a near-no-op. We:

* Validate that ``prompt_ref`` strings look like valid relative
  paths (no absolute paths / parent-dir traversal).
* Leave ``schema_ref`` strings alone (full JSON Schema loading is
  Phase 2 work).

The ``!include`` directive support documented in Doc04 §4.4 is
on the Phase 2 roadmap.

We return a *new* :class:`WorkflowIR` so the pipeline stays
side-effect free.
"""

from __future__ import annotations

from agentkit.workflow.errors import CompileError
from agentkit.workflow.ir import WorkflowIR


def resolve_refs(ir: WorkflowIR) -> WorkflowIR:
    """Validate ``prompt_ref`` paths; return ``ir`` unchanged on success."""
    violations: list[str] = []
    for key, agent in ir.agents.items():
        if agent.prompt_ref is not None:
            ref = agent.prompt_ref
            # Disallow absolute paths and parent-dir traversal — we
            # don't want a Workflow to point outside the project root.
            if ref.startswith("/") or ".." in ref.split("/"):
                violations.append(
                    f"agents.{key}.prompt_ref: must be a project-relative path "
                    f"(no '/' prefix, no '..'); got {ref!r}",
                )
    if violations:
        raise CompileError("resolve: invalid refs", violations=violations)
    return ir
