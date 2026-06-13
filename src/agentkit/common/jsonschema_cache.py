"""Cached JSON Schema validators.

``jsonschema.validate(instance, schema)`` re-runs ``check_schema`` and
rebuilds a validator object **on every call** — pure CPU work repeated
per event/publish (see ``docs/CONCURRENCY.md`` §5, B5). Schemas in
AgentKit are long-lived config objects (``schema_in`` / ``schema_out``
on an Agent, ``json_schema`` on a handler), so we compile the validator
once and reuse it.

The cache is keyed by ``id(schema)``: schemas are constructed once and
never mutated in place, so identity is a stable, allocation-free key.
If a caller *does* mutate a schema dict in place (don't), drop the cache
with :func:`clear_cache`.
"""

from __future__ import annotations

from typing import Any

import jsonschema
from jsonschema.protocols import Validator

# id(schema) -> compiled validator. Bounded by the number of distinct
# schema objects in the process (one per agent field), so it does not
# grow with traffic.
_CACHE: dict[int, Validator] = {}


def get_validator(schema: dict[str, Any]) -> Validator:
    """Return a cached validator for ``schema`` (compiling on first use)."""
    key = id(schema)
    validator = _CACHE.get(key)
    if validator is None:
        cls = jsonschema.validators.validator_for(schema)
        cls.check_schema(schema)  # raises jsonschema.SchemaError on a bad schema
        validator = cls(schema)
        _CACHE[key] = validator
    return validator


def validate_cached(instance: Any, schema: dict[str, Any]) -> None:
    """Validate ``instance`` against ``schema`` using a cached validator.

    Drop-in for ``jsonschema.validate(instance, schema)`` — raises the
    same :class:`jsonschema.ValidationError` on failure, so existing
    ``except jsonschema.ValidationError`` handlers keep working.
    """
    get_validator(schema).validate(instance)


def clear_cache() -> None:
    """Drop all cached validators (tests / hot-reload)."""
    _CACHE.clear()
