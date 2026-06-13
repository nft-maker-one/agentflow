"""Thin YAML loader for Workflow IR documents.

Phase 1: ``yaml.safe_load`` based — simple, safe, no comment
preservation. UI ↔ YAML round-trip with comment preservation
(via ``ruamel.yaml``) is on the Phase 2 roadmap (Doc04 §7.2).

Top-level shape::

    workflow:
      id: ...
      agents: { ... }
      edges:  { ... }
      ...
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from agentkit.workflow.errors import CompileError


def load_workflow_yaml(path: str | Path) -> dict[str, Any]:
    """Load a YAML file and return the inner ``workflow:`` dict.

    Wrappers expecting a top-level ``workflow:`` key are unwrapped
    automatically; flat documents are accepted as-is.
    """
    p = Path(path)
    if not p.is_file():
        raise CompileError(f"YAML file not found: {p}")
    try:
        raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise CompileError(f"YAML parse error in {p}: {e}") from e

    if raw is None:
        raise CompileError(f"YAML file {p} is empty")
    if not isinstance(raw, dict):
        raise CompileError(
            f"YAML root must be a mapping; got {type(raw).__name__}",
        )

    # Accept either a wrapper ``{workflow: {...}}`` or a flat IR dict.
    if "workflow" in raw and isinstance(raw["workflow"], dict):
        return raw["workflow"]
    return raw
