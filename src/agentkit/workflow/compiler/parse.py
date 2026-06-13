"""Step ① — Parse: dict → :class:`WorkflowIR`.

We accept either a flat dict (already in canonical IR shape) or a
wrapper ``{"workflow": {...}}`` from the YAML loader. Pydantic
performs the heavy-lifting validation here; the next steps only
concern semantic / cross-field issues.
"""

from __future__ import annotations

from typing import Any

from pydantic import ValidationError

from agentkit.workflow.errors import CompileError
from agentkit.workflow.ir import WorkflowIR


def parse_dict(raw: dict[str, Any]) -> WorkflowIR:
    """Construct a :class:`WorkflowIR` from a raw dict.

    Raises :class:`CompileError` on Pydantic validation errors,
    with the inner errors flattened into a readable list.
    """
    if "workflow" in raw and isinstance(raw["workflow"], dict):
        raw = raw["workflow"]

    try:
        return WorkflowIR.model_validate(raw)
    except ValidationError as e:
        violations = [
            f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}"
            for err in e.errors()
        ]
        raise CompileError(
            "parse: schema validation failed", violations=violations,
        ) from e
