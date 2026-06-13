"""Sandboxed evaluator for Notifier ``when`` expressions.

Doc08 §3.2 sketches a small DSL — comparisons + boolean ops +
JSONPath-like access. We implement it as a *whitelist AST walker*
with **no use of Python's ``eval()``** anywhere; that mirrors how
:mod:`agentkit.orchestrator.switch_eval` keeps its own evaluator
sandboxed.

Allowed grammar (informally)::

    expr      := orexpr
    orexpr    := andexpr ('or' andexpr)*
    andexpr   := notexpr ('and' notexpr)*
    notexpr   := 'not' notexpr | compare | atom
    compare   := atom (('==' | '!=' | '<' | '<=' | '>' | '>=' | 'in' | 'not in') atom)+
    atom      := NAME ('.' NAME | '[' expr ']')* | LITERAL | '(' expr ')' | listexpr
    listexpr  := '[' (expr (',' expr)*)? ']'
    LITERAL   := str | int | float | bool | None

Variables exposed in the evaluation context (Doc08 §3.2):

* ``topic``    — event topic (str)
* ``payload``  — event payload (dict, attribute-accessible)
* ``headers``  — flattened envelope headers (dict)
* ``event``    — full envelope as a dict
* ``run``      — Run snapshot (dict | None)
* ``agent``    — Agent snapshot (dict | None)
* ``severity`` — rule's declared severity (str)
* ``now``      — UTC ISO timestamp (str)

Anything not on the whitelist (function calls, lambda, attribute
walking on non-dicts, list/dict comprehensions, etc.) raises
:class:`RuleEvalError` *before* any code is run.
"""

from __future__ import annotations

import ast
from typing import Any, Final

from agentkit.notifier.errors import RuleEvalError

# AST node types we accept (everything else is rejected at parse time).
_ALLOWED_NODES: Final[tuple[type[ast.AST], ...]] = (
    ast.Expression,
    # comparisons
    ast.Compare,
    ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE,
    ast.In, ast.NotIn,
    # boolean / unary
    ast.BoolOp, ast.And, ast.Or,
    ast.UnaryOp, ast.Not, ast.UAdd, ast.USub,
    # data
    ast.Constant,
    ast.Name, ast.Load,
    ast.Subscript,
    ast.Attribute,
    ast.List, ast.Tuple,
    # slice / index normalization for Python ≥ 3.9
    ast.Index,  # noqa: kept for compat — ast.Index is deprecated since 3.9
)


# ============================================================
# Public API
# ============================================================


def evaluate_when(expr: str | None, context: dict[str, Any]) -> bool:
    """Evaluate a guard expression against ``context`` and return bool.

    * ``expr=None`` or empty string → always ``True`` (no guard).
    * Any forbidden AST node → :class:`RuleEvalError`.
    * Runtime issues (missing variable, type mismatch in comparison)
      → ``False`` (rule does not match — Doc08 §3.2 design).

    The caller (matcher) catches ``RuleEvalError`` and surfaces it
    as a metric / log line, but does NOT crash the dispatcher.
    """
    if expr is None or not expr.strip():
        return True

    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise RuleEvalError(f"syntax error: {e.msg}") from e

    _validate_nodes(tree)

    try:
        return bool(_eval(tree.body, context))
    except RuleEvalError:
        raise
    except Exception:  # pragma: no cover — defensive: comparison weirdness
        return False


# ============================================================
# Internals
# ============================================================


def _validate_nodes(tree: ast.AST) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise RuleEvalError(
                f"forbidden expression node: {type(node).__name__}",
            )


def _eval(node: ast.AST, ctx: dict[str, Any]) -> Any:  # noqa: ANN401
    if isinstance(node, ast.Constant):
        return node.value

    if isinstance(node, ast.Name):
        # Unknown variable → None (so ``payload.score >= 0.5`` simply
        # evaluates falsy when payload doesn't have that key).
        return ctx.get(node.id)

    if isinstance(node, ast.Attribute):
        v = _eval(node.value, ctx)
        if v is None:
            return None
        # Strict refusal: anything that's not a dict can't be walked.
        # We do NOT use ``getattr`` because that would expose dict
        # methods (`.items`, `.keys`, …) and worse, arbitrary class
        # internals — a security hole. Force users to use plain
        # dict-style payloads.
        if isinstance(v, dict):
            return v.get(node.attr)
        raise RuleEvalError(
            f"attribute access disallowed on type {type(v).__name__!r}",
        )

    if isinstance(node, ast.Subscript):
        v = _eval(node.value, ctx)
        if v is None:
            return None
        # Python ≤3.8 wraps under ast.Index; ≥3.9 stores expression directly.
        slc = node.slice.value if isinstance(node.slice, ast.Index) else node.slice
        i = _eval(slc, ctx)
        try:
            return v[i]
        except (KeyError, IndexError, TypeError):
            return None

    if isinstance(node, ast.Compare):
        left = _eval(node.left, ctx)
        for op, comp in zip(node.ops, node.comparators, strict=True):
            right = _eval(comp, ctx)
            if not _compare(op, left, right):
                return False
            left = right
        return True

    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            for v in node.values:
                if not _eval(v, ctx):
                    return False
            return True
        # ast.Or
        for v in node.values:
            if _eval(v, ctx):
                return True
        return False

    if isinstance(node, ast.UnaryOp):
        v = _eval(node.operand, ctx)
        if isinstance(node.op, ast.Not):
            return not v
        if isinstance(node.op, ast.USub):
            return -v if v is not None else None
        if isinstance(node.op, ast.UAdd):
            return +v if v is not None else None

    if isinstance(node, (ast.List, ast.Tuple)):
        return [_eval(e, ctx) for e in node.elts]

    raise RuleEvalError(f"unsupported AST node: {type(node).__name__}")


def _compare(op: ast.cmpop, left: Any, right: Any) -> bool:  # noqa: ANN401
    # ``None`` is a no-op comparator most of the time — return False
    # rather than throwing TypeError so missing payload keys don't
    # crash ``payload.score >= 0.9``.
    if isinstance(op, ast.Eq):
        return left == right
    if isinstance(op, ast.NotEq):
        return left != right

    # Order comparisons against None are always False.
    if left is None or right is None:
        return False

    if isinstance(op, ast.Lt):
        return left < right
    if isinstance(op, ast.LtE):
        return left <= right
    if isinstance(op, ast.Gt):
        return left > right
    if isinstance(op, ast.GtE):
        return left >= right

    if isinstance(op, ast.In):
        return _contains(right, left)
    if isinstance(op, ast.NotIn):
        return not _contains(right, left)

    raise RuleEvalError(f"unsupported comparator: {type(op).__name__}")


def _contains(container: Any, needle: Any) -> bool:  # noqa: ANN401
    """Type-tolerant membership test."""
    if container is None:
        return False
    try:
        return needle in container
    except TypeError:
        return False
