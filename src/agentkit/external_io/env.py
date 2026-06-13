"""Centralized environment-variable fallbacks for external I/O secrets.

This is the external-I/O analogue of the LLM-provider auto-detection in
:func:`agentkit.api.server._build_gateway`. Where that block resolves
*LLM* API keys (``OPENAI_API_KEY`` / ``DASHSCOPE_API_KEY`` / …), this
module resolves *external-I/O* secrets (Telegram bot token, SMTP/IMAP
credentials, default recipient).

Precedence — **explicit config always wins**:

1. Value passed in ``add_source(..., config={...})`` / ``add_sink(...)``.
2. First non-empty environment variable from the alias list below.
3. The adapter's own hard default (``port`` etc.).

So a workflow may declare a source/sink with an *empty* secret and have
it filled from the process environment at deploy time — while a caller
that passes the value explicitly overrides the environment entirely.

All env-var names live HERE in one table so there's a single place to
audit which variables the framework reads.
"""

from __future__ import annotations

import os
from typing import Any

# (kind, direction) → { config_field: (env_var, alias, …) }.
# First non-empty env var wins (mirrors server.py's ``_key()`` helper).
_EXTERNAL_ENV_FALLBACKS: dict[tuple[str, str], dict[str, tuple[str, ...]]] = {
    ("telegram", "source"): {
        "token": ("TELEGRAM_BOT_TOKEN", "AGENTKIT_TELEGRAM_BOT_TOKEN"),
    },
    ("telegram", "sink"): {
        "token":   ("TELEGRAM_BOT_TOKEN", "AGENTKIT_TELEGRAM_BOT_TOKEN"),
        "chat_id": ("TELEGRAM_CHAT_ID", "AGENTKIT_TELEGRAM_CHAT_ID"),
    },
    ("email_imap", "source"): {
        "host":     ("IMAP_HOST", "EMAIL_IMAP_HOST"),
        "port":     ("IMAP_PORT",),
        "user":     ("IMAP_USER", "EMAIL_USER"),
        "password": ("IMAP_PASSWORD", "EMAIL_PASSWORD"),
    },
    ("email_smtp", "sink"): {
        "host":     ("SMTP_HOST", "EMAIL_SMTP_HOST"),
        "port":     ("SMTP_PORT",),
        "user":     ("SMTP_USER", "EMAIL_USER"),
        "password": ("SMTP_PASSWORD", "EMAIL_PASSWORD"),
        "to":       ("SMTP_TO", "EMAIL_TO"),
        "subject":  ("SMTP_SUBJECT",),
    },
}


def _first_env(*names: str) -> str | None:
    """Return the first non-empty environment variable among ``names``."""
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return None


def apply_env_defaults(
    kind: str, direction: str, config: dict[str, Any],
) -> dict[str, Any]:
    """Return a NEW config dict with env-var fallbacks filled in.

    A field is only filled from the environment when ``config`` has no
    truthy value for it — explicit config is never overwritten. The
    input dict is left untouched (a shallow copy is returned), so callers
    can keep the original (env-free) config for persistence/redeploy.
    """
    fallbacks = _EXTERNAL_ENV_FALLBACKS.get((kind, direction))
    resolved = dict(config)
    if not fallbacks:
        return resolved
    for field, env_names in fallbacks.items():
        # Explicit (truthy) config wins — never override it.
        if resolved.get(field) not in (None, ""):
            continue
        val = _first_env(*env_names)
        if val is not None:
            resolved[field] = val
    return resolved


def external_env_var_names() -> dict[str, list[str]]:
    """Flat ``"<kind>.<direction>.<field>" → [env vars]`` map, for docs/UI."""
    return {
        f"{kind}.{direction}.{field}": list(envs)
        for (kind, direction), fields in _EXTERNAL_ENV_FALLBACKS.items()
        for field, envs in fields.items()
    }
