"""Top-level :class:`WorkflowIR` model.

A :class:`WorkflowIR` instance is the *single source of truth* for
a Workflow. SDK / YAML / UI all compile down to this shape.
"""

from __future__ import annotations

import hashlib
from typing import Any, ClassVar, Final

import orjson
from pydantic import BaseModel, ConfigDict, Field

from agentkit.workflow.ir.agent import AgentTemplate
from agentkit.workflow.ir.edge import EdgeSpec
from agentkit.workflow.ir.runtime_directives import (
    BusOverride,
    NotificationRule,
    TriggerSpec,
    WorkflowGuardrail,
)

# Reserved virtual node identifiers — these are NOT keys in
# ``agents`` but legal targets in :class:`EdgeSpec`.
START_NODE: Final[str] = "__start__"
END_NODE: Final[str] = "__end__"
ERROR_NODE: Final[str] = "__error__"

VIRTUAL_NODES: Final[frozenset[str]] = frozenset({START_NODE, END_NODE, ERROR_NODE})


class IRMeta(BaseModel):
    """Compiler-injected metadata about a compiled IR.

    Users do not write this directly — the Compiler fills it in
    during the *Lower* step.
    """

    model_config = ConfigDict(extra="forbid")

    schema_ver: str = "1.0"
    ir_hash: str = ""  # 12-char shortened SHA-256 over canonical JSON
    compiled_at: str = ""  # ISO-8601 UTC


class WorkflowIR(BaseModel):
    """Top-level Workflow IR.

    Field shape mirrors ``Doc04 §2.1``. ``agents`` and ``edges`` are
    map-form (key is the unique identifier). ``_meta`` is filled by
    the Compiler.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    # ── identity ──
    id: str = Field(min_length=1)
    version: int = Field(default=1, ge=1)
    description: str = ""
    owner: str | None = None
    project: str | None = None

    # ── triggers ──
    triggers: list[TriggerSpec] = Field(default_factory=list)

    # ── core graph ──
    agents: dict[str, AgentTemplate] = Field(default_factory=dict)
    edges: dict[str, EdgeSpec] = Field(default_factory=dict)

    # ── workflow-level configuration ──
    guardrails: WorkflowGuardrail | None = None
    bus: BusOverride | None = None
    notifications: list[NotificationRule] = Field(default_factory=list)

    #: End-node fan-in (FlowControl). When ``True`` AND more than one
    #: *direct* edge targets ``__end__``, the run is only declared
    #: terminal once **every** end-edge's ``via`` topic has fired for
    #: that run — instead of ending on the first signal (the default).
    #: Mirrors the per-agent ``aggregate`` (fan-in) join, applied to the
    #: terminal node. See ``TerminalDetector``.
    end_join: bool = False

    # Compiler-injected (populate via alias since underscore prefixes
    # are filtered by Pydantic by default).
    meta: IRMeta = Field(default_factory=IRMeta, alias="_meta")

    # ----- helpers used by Compiler / consumers -----

    # Pydantic-set virtual-nodes constant for convenience.
    VIRTUAL: ClassVar[frozenset[str]] = VIRTUAL_NODES

    def all_node_keys(self) -> set[str]:
        """Return all *real* template keys (excludes virtual nodes)."""
        return set(self.agents.keys())

    def is_virtual(self, name: str) -> bool:
        return name in VIRTUAL_NODES

    def canonical_json(self) -> bytes:
        """Stable serialization for hashing.

        Excludes ``_meta`` so re-compiling the same IR produces the
        same hash regardless of compile timestamp.
        """
        d = self.model_dump(mode="json", by_alias=True, exclude_none=True)
        d.pop("_meta", None)
        # ``OPT_SORT_KEYS`` makes ordering deterministic across runs.
        return orjson.dumps(d, option=orjson.OPT_SORT_KEYS)

    def compute_hash(self) -> str:
        """12-char shortened SHA-256 over the canonical JSON."""
        h = hashlib.sha256(self.canonical_json()).hexdigest()
        return h[:12]

    def with_meta(self, meta: IRMeta) -> WorkflowIR:
        """Return a copy with ``_meta`` replaced (Compiler convenience)."""
        return self.model_copy(update={"meta": meta})

    # ----- introspection helpers used by validate / plan -----

    def edges_from(self, node: str) -> list[tuple[str, "EdgeSpec"]]:
        return [(eid, e) for eid, e in self.edges.items() if e.from_ == node]

    def edges_to(self, node: str) -> list[tuple[str, "EdgeSpec"]]:
        return [(eid, e) for eid, e in self.edges.items() if node in e.all_targets()]

    def graph_neighbors(self) -> dict[str, set[str]]:
        """Adjacency map (template_key | virtual → set of next nodes)."""
        adj: dict[str, set[str]] = {}
        for e in self.edges.values():
            adj.setdefault(e.from_, set()).update(e.all_targets())
        return adj

    @property
    def all_topics(self) -> list[str]:
        """Every ``via`` topic mentioned in any edge.

        Used by the Compiler's plan step to derive the Bus topic
        list.
        """
        out: set[str] = set()
        for e in self.edges.values():
            out.update(e.all_vias())
        return sorted(out)

    # The compiler-driven helpers also provide a typed accessor that
    # returns the underlying dict for friendly ``items()`` etc.

    def model_dump_canonical(self) -> dict[str, Any]:
        return self.model_dump(mode="json", by_alias=True, exclude_none=True)
