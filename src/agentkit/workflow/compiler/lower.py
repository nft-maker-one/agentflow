"""Step ⑤ — Lower: finalize IR with computed metadata.

The Lower step produces the *immutable* IR that downstream consumers
(Plan, Runtime, Orchestrator) see. It computes the deterministic
content-addressable hash and stamps the compile timestamp.
"""

from __future__ import annotations

from agentkit.common.time import to_iso, utcnow
from agentkit.workflow.ir import IRMeta, WorkflowIR


def lower_ir(ir: WorkflowIR) -> WorkflowIR:
    """Return a copy of ``ir`` with ``_meta`` populated."""
    meta = IRMeta(
        schema_ver="1.0",
        ir_hash=ir.compute_hash(),
        compiled_at=to_iso(utcnow()),
    )
    return ir.with_meta(meta)
