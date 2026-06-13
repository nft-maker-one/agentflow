"""Step ③ — Inject: add system-managed edges / nodes.

Phase 1 scope:

* For every ``ThinkingNode``-class Agent that has *no* edge leading
  to ``__error__``, **insert a default error-recovery edge**. This
  gives every node a way to reach a terminal state even when the
  user forgot to wire one.

Future scope (Phase 2 — Doc04 §4.2):

* GuardNode auto-attach for nodes declaring a ``GuardNode`` peer.
* Default DLQ subscriptions for the Notifier.

We return a fresh :class:`WorkflowIR` (Pydantic ``model_copy``).
"""

from __future__ import annotations

from agentkit.common.logging import get_logger
from agentkit.workflow.ir import (
    ERROR_NODE,
    EdgeSpec,
    WorkflowIR,
)

log = get_logger(__name__)

_INJECTED_EDGE_PREFIX = "_auto_error_"


def inject_defaults(ir: WorkflowIR) -> WorkflowIR:
    """Inject default ``__error__`` edges for nodes missing one.

    Idempotent — already-injected edges (recognized by the
    ``_auto_error_`` prefix) are not re-added.
    """
    new_edges = dict(ir.edges)

    # Collect set of nodes that already have an edge to __error__.
    has_error_edge = {
        e.from_
        for e in ir.edges.values()
        if ERROR_NODE in e.all_targets()
    }

    injected = 0
    for key in ir.agents:
        if key in has_error_edge:
            continue
        edge_id = f"{_INJECTED_EDGE_PREFIX}{key}"
        if edge_id in new_edges:
            continue  # idempotent
        new_edges[edge_id] = EdgeSpec(
            **{
                "from": key,
                "to": ERROR_NODE,
                # via=None — error edges don't carry a topic; the
                # Orchestrator routes node-failure events to ERROR_NODE
                # without bus traffic.
            },
        )
        injected += 1

    if injected:
        log.debug(
            "compile.inject.error_edges", workflow_id=ir.id, injected=injected,
        )

    return ir.model_copy(update={"edges": new_edges})
