"""High-level pipeline orchestration: ``compile_from_dict`` / ``compile_workflow``.

Wraps the 6 steps in :mod:`agentkit.workflow.compiler.{parse,resolve,
inject,validate,lower,plan}`. The entry points return a tuple of
``(WorkflowIR, RuntimePlan)`` — the IR is the canonical truth, the
Plan is the deployment artifact.

Phase 2 optimization (#3): Compile cache.
A bounded LRU keyed by a stable hash of the canonicalized input dict
(plus the ``validate`` flag) is checked before running the 6-step
pipeline. Cache hits return the *same* (IR, Plan) tuple — both objects
are immutable Pydantic models, so sharing references across callers
is safe. The cache tracks hits/misses for observability.
"""

from __future__ import annotations

import hashlib
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Any

import orjson

from agentkit.common.logging import get_logger
from agentkit.observability import metrics
from agentkit.workflow.compiler.expand import expand_workflow
from agentkit.workflow.compiler.inject import inject_defaults
from agentkit.workflow.compiler.lower import lower_ir
from agentkit.workflow.compiler.parse import parse_dict
from agentkit.workflow.compiler.plan import make_plan
from agentkit.workflow.compiler.resolve import resolve_refs
from agentkit.workflow.compiler.validate import validate_ir
from agentkit.workflow.errors import CompileError, IRValidationError
from agentkit.workflow.ir import WorkflowIR
from agentkit.workflow.plan import RuntimePlan
from agentkit.workflow.yaml_loader import load_workflow_yaml

log = get_logger(__name__)


# ============================================================
# Compile cache (Phase 2.1 optimization)
# ============================================================

_CACHE_MAX_ENTRIES: int = 256
_cache: "OrderedDict[str, tuple[WorkflowIR, RuntimePlan]]" = OrderedDict()
_cache_lock = threading.Lock()
_cache_hits: int = 0
_cache_misses: int = 0


def _stable_hash(raw: dict[str, Any], *, validate: bool) -> str:
    """Deterministic content hash of the raw input + validate flag.

    orjson with OPT_SORT_KEYS gives canonical byte-level ordering so
    semantically-equivalent dicts produce the same hash.
    """
    canonical = orjson.dumps(
        {"raw": raw, "validate": validate},
        option=orjson.OPT_SORT_KEYS,
    )
    return hashlib.sha256(canonical).hexdigest()[:16]


def get_compile_cache_stats() -> dict[str, int]:
    """Return current cache hit/miss/size counters (read-only snapshot)."""
    with _cache_lock:
        return {
            "hits": _cache_hits,
            "misses": _cache_misses,
            "size": len(_cache),
            "max_entries": _CACHE_MAX_ENTRIES,
        }


def clear_compile_cache() -> None:
    """Drop all cached compile results — primarily for tests."""
    global _cache_hits, _cache_misses
    with _cache_lock:
        _cache.clear()
        _cache_hits = 0
        _cache_misses = 0


def compile_from_dict(
    raw: dict[str, Any], *, validate: bool = True, use_cache: bool = True,
) -> tuple[WorkflowIR, RuntimePlan]:
    """Run the 6-step compiler pipeline on a dict.

    Returns a tuple of the lowered IR and the derived RuntimePlan.

    Set ``validate=False`` to skip the validate step — useful for
    UI live-edit modes that want to keep editing while transient
    errors exist.

    Set ``use_cache=False`` to bypass the LRU compile cache (useful in
    tests that mutate the result in-place — the cache assumes IR/Plan
    are treated as immutable).
    """
    global _cache_hits, _cache_misses

    # ── Cache fast path ──────────────────────────────────────
    cache_key: str | None = None
    if use_cache:
        try:
            cache_key = _stable_hash(raw, validate=validate)
        except Exception:  # noqa: BLE001
            # Non-serializable input falls back to non-cached path.
            cache_key = None
        if cache_key is not None:
            with _cache_lock:
                cached = _cache.get(cache_key)
                if cached is not None:
                    _cache.move_to_end(cache_key)  # mark as MRU
                    _cache_hits += 1
                    return cached
                _cache_misses += 1

    counted_error = False
    try:
        # ① Parse
        ir = parse_dict(raw)

        # ②a Expand topic shorthand
        ir = expand_workflow(ir)

        # ②b Resolve refs
        ir = resolve_refs(ir)

        # ③ Inject
        ir = inject_defaults(ir)

        # ④ Validate
        if validate:
            violations = validate_ir(ir)
            if violations:
                # Doc10 §4.3 — count each violation by rule name.
                for v in violations:
                    rule_name = v.split(":", 1)[0].strip() if ":" in v else v
                    metrics.validate_violation_total.labels(rule=rule_name[:64]).inc()
                metrics.compile_total.labels(result="error").inc()
                counted_error = True
                raise IRValidationError(violations)

        # ⑤ Lower
        ir = lower_ir(ir)

        # ⑥ Plan
        plan = make_plan(ir)
    except Exception:
        if not counted_error:
            metrics.compile_total.labels(result="error").inc()
        raise

    metrics.compile_total.labels(result="ok").inc()
    log.info(
        "compile.ok",
        workflow_id=ir.id,
        version=ir.version,
        ir_hash=ir.meta.ir_hash,
        agents=len(ir.agents),
        edges=len(ir.edges),
        topics=len(plan.bus_topics.topics),
    )

    # ── Insert into cache (LRU eviction) ─────────────────────
    if use_cache and cache_key is not None:
        with _cache_lock:
            _cache[cache_key] = (ir, plan)
            _cache.move_to_end(cache_key)
            while len(_cache) > _CACHE_MAX_ENTRIES:
                _cache.popitem(last=False)

    return ir, plan


def compile_workflow(
    path: str | Path, *, validate: bool = True,
) -> tuple[WorkflowIR, RuntimePlan]:
    """Compile a Workflow from a YAML file on disk."""
    p = Path(path)
    raw = load_workflow_yaml(p)
    try:
        return compile_from_dict(raw, validate=validate)
    except CompileError as e:
        # Re-raise so callers see the file context.
        e.add_note(f"source: {p}")  # type: ignore[attr-defined]
        raise
