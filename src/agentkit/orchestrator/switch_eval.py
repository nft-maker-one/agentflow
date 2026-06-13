"""Sandboxed switch-expression evaluator (Phase 1).

Per ``Doc04 §3.3`` and ``Doc05 §5.2``, switch expressions are
deliberately *limited* — JSONPath-style field extraction, optional
simple comparisons. We do NOT evaluate Python expressions at all;
``eval()`` would be a security disaster for natural-language-driven
workflows.

Phase 1 grammar — what's accepted::

    $.field
    $.parent.child
    $.list[0]
    $.list[0].key

Phase 2 will add ``==``, ``<``, ``in`` boolean operators (Doc04 §3.3).
For v1 the dispatcher just looks up ``str(extracted_value)`` against
the case map — that covers ~95% of real Workflow uses.

Result type: a stringified extracted value (or ``None`` if path
doesn't exist). The caller (``pick_case``) does case-map lookup
with sensible fallback semantics.
"""

from __future__ import annotations

import re
from typing import Any

from agentkit.orchestrator.errors import SwitchEvalError

# A path segment is either a bare identifier or a ``[index]`` access.
# We allow JSON-friendly identifiers: letters, digits, underscores,
# hyphens — but NOT spaces or pipes / function calls / etc.
_SEGMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*$")
_INDEX_RE = re.compile(r"^\[(-?\d+)\]$")


def evaluate_switch_expression(expr: str, payload: dict[str, Any]) -> Any:
    """Evaluate ``expr`` against ``payload``; return the extracted value.

    Returns ``None`` when the path doesn't exist (the caller routes
    to the ``default`` branch). Raises :class:`SwitchEvalError` only
    for malformed expressions — those are caught outside and routed
    to ``__error__``.

    Examples
    --------
    >>> evaluate_switch_expression("$.choice", {"choice": "writer"})
    'writer'
    >>> evaluate_switch_expression("$.nope", {"choice": "x"}) is None
    True
    >>> evaluate_switch_expression("$.list[1]", {"list": [10, 20]})
    20
    """
    if not isinstance(expr, str):
        raise SwitchEvalError(f"switch expression must be a string; got {type(expr).__name__}")
    expr = expr.strip()
    if not expr:
        raise SwitchEvalError("switch expression must be non-empty")
    if not expr.startswith("$."):
        raise SwitchEvalError(
            f"switch expression must start with '$.': got {expr!r}",
        )

    # Tokenize the path into segments / index ops. We don't allow
    # parentheses, function calls, slicing, filters, or wildcards
    # in v1 — keeping the surface tiny is the security strategy.
    tokens = _tokenize(expr[2:])
    cur: Any = payload
    for tok in tokens:
        if tok.kind == "key":
            if not isinstance(cur, dict):
                return None
            cur = cur.get(tok.value)
            if cur is None:
                return None
        elif tok.kind == "index":
            if not isinstance(cur, list):
                return None
            idx = int(tok.value)
            if idx < -len(cur) or idx >= len(cur):
                return None
            cur = cur[idx]
        else:  # pragma: no cover — defensive; tokenizer is exhaustive
            raise SwitchEvalError(f"unexpected token kind: {tok.kind!r}")
    return cur


# ----------------------------------------------------------------
# Internal tokenizer
# ----------------------------------------------------------------


class _Token:
    __slots__ = ("kind", "value")

    def __init__(self, kind: str, value: str) -> None:
        self.kind = kind
        self.value = value

    def __repr__(self) -> str:  # pragma: no cover — debug only
        return f"_Token({self.kind!r}, {self.value!r})"


def _tokenize(path: str) -> list[_Token]:
    """Split ``foo.bar[0].baz`` into ``[key foo, key bar, index 0, key baz]``.

    Strict: anything that doesn't match the grammar is a SwitchEvalError.
    """
    if not path:
        raise SwitchEvalError("switch path must be non-empty after '$.'")

    out: list[_Token] = []
    # We split on '.' but indices ([N]) attach to the preceding key.
    # Easier: walk char-by-char with a tiny state machine.
    i = 0
    n = len(path)
    while i < n:
        # Read a key segment.
        start = i
        while i < n and path[i] not in ".[":
            i += 1
        seg = path[start:i]
        if not _SEGMENT_RE.match(seg):
            raise SwitchEvalError(
                f"invalid path segment {seg!r} in switch expression",
            )
        out.append(_Token("key", seg))

        # Read any number of [N] indices.
        while i < n and path[i] == "[":
            j = path.find("]", i)
            if j == -1:
                raise SwitchEvalError("unbalanced '[' in switch expression")
            inner = path[i : j + 1]
            m = _INDEX_RE.match(inner)
            if not m:
                raise SwitchEvalError(
                    f"invalid index {inner!r} in switch expression",
                )
            out.append(_Token("index", m.group(1)))
            i = j + 1

        # Skip the dot before the next segment.
        if i < n:
            if path[i] != ".":
                raise SwitchEvalError(
                    f"expected '.' between segments at position {i}",
                )
            i += 1
            if i >= n:
                raise SwitchEvalError("path ends with trailing '.'")

    return out


# ----------------------------------------------------------------
# Case selection helper
# ----------------------------------------------------------------


def pick_case(
    cases: dict[str, Any],
    extracted: Any,
    *,
    default_key: str | None = None,
) -> str | None:
    """Look up the case whose key matches ``str(extracted)``.

    Returns the key (e.g. ``"approve"``) or ``default_key`` if no
    case matches. ``None`` if neither match nor default applies.
    """
    if extracted is None:
        return default_key
    str_val = str(extracted) if not isinstance(extracted, bool) else (
        "true" if extracted else "false"
    )
    if str_val in cases:
        return str_val
    return default_key
