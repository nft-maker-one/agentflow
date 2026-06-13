"""Edge IR data models.

Edges are stored as a *map* in the IR (key = ``edge_id``, value =
:class:`EdgeSpec`). This lets UI / Run-Overlay refer to specific
edges by name rather than by computed-position.

See ``Doc04_WorkflowGraph.md §2``.
"""

from __future__ import annotations

from typing import Any, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Switch(BaseModel):
    """Dynamic-routing target: ``{ switch: "<expr>" }``.

    The expression is *just stored* by the Compiler — actual
    evaluation lives in the Orchestrator (Doc05 §5). For Phase 1
    we only validate that the string is non-empty.
    """

    model_config = ConfigDict(extra="forbid")

    switch: str = Field(min_length=1)


class EdgeBranch(BaseModel):
    """One branch in an edge's ``cases``/``default`` map.

    Used inside a Switch edge: ``cases.<value>: { to, via }``.
    """

    model_config = ConfigDict(extra="forbid")

    to: str = Field(min_length=1)  # template_key | __end__ | __error__
    via: str | None = None         # topic; may be None when target is __end__/__error__


class EdgeSpec(BaseModel):
    """A single edge in the Workflow IR.

    Three shapes (only one is active per edge):

    * **direct**: ``from`` → ``to`` (string template_key) via ``via`` topic.
    * **fanout**: ``to`` is a list[str] of template_keys → publish to
      each via either ``via`` (single topic) or per-target via the
      Compiler default. Fanout edges all share one ``via`` topic.
    * **switch**: ``to`` is a :class:`Switch` object; ``cases`` maps
      extracted value → :class:`EdgeBranch`; ``default`` is the
      catch-all branch.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    from_: str = Field(alias="from", min_length=1)
    to: Union[str, list[str], Switch]
    via: str | None = None

    cases: dict[str, EdgeBranch] | None = None
    default: EdgeBranch | None = None

    when: str | None = None  # Phase 2 guard expression; v1 stores only

    @property
    def is_switch(self) -> bool:
        return isinstance(self.to, Switch)

    @property
    def is_fanout(self) -> bool:
        return isinstance(self.to, list)

    @property
    def is_direct(self) -> bool:
        return isinstance(self.to, str)

    @model_validator(mode="after")
    def _check_shape_consistency(self) -> EdgeSpec:
        """Reject malformed combinations early.

        * Switch edges must have non-empty ``cases``.
        * Direct/fanout edges must NOT have ``cases``/``default``.
        * Switch edges MUST NOT have a top-level ``via`` (each
          branch carries its own).
        """
        if self.is_switch:
            if not self.cases:
                raise ValueError("switch edges require non-empty `cases`")
            if self.via is not None:
                raise ValueError(
                    "switch edges must not have top-level `via`; "
                    "specify per-branch `via` inside cases/default",
                )
        else:
            if self.cases is not None or self.default is not None:
                raise ValueError(
                    "`cases`/`default` are only valid on switch edges",
                )
            if self.is_fanout and not self.to:
                raise ValueError("fanout `to` must be a non-empty list")
        return self

    def all_targets(self) -> list[str]:
        """Return every target template_key (or virtual node) this edge could go to.

        Useful for topology / reachability checks.
        """
        if self.is_direct:
            assert isinstance(self.to, str)
            return [self.to]
        if self.is_fanout:
            assert isinstance(self.to, list)
            return list(self.to)
        # switch
        targets: list[str] = []
        if self.cases:
            targets.extend(b.to for b in self.cases.values())
        if self.default is not None:
            targets.append(self.default.to)
        return targets

    def all_vias(self) -> list[str]:
        """Return every ``via`` topic this edge could publish to."""
        if self.is_switch:
            vias: list[str] = []
            if self.cases:
                vias.extend(b.via for b in self.cases.values() if b.via)
            if self.default and self.default.via:
                vias.append(self.default.via)
            return vias
        if self.via:
            return [self.via]
        return []

    def model_dump_canonical(self) -> dict[str, Any]:
        """Return a Compiler-friendly dict (for hashing).

        Stable key order, no trivia.
        """
        return self.model_dump(mode="json", by_alias=True, exclude_none=True)
