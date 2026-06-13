"""Jinja2-based template rendering — Doc08 §5.

Phase 1:

* Built-in defaults for every alias the engine supports.
* Optional ``project_template_dir`` for user overrides — files
  named ``<template>.<channel>.j2`` get loaded automatically.
* Strict undefined: missing variables surface as a render error
  rather than blank strings (caller falls back to a tiny
  emergency template).
"""

from __future__ import annotations

from pathlib import Path

from jinja2 import (
    BaseLoader,
    ChoiceLoader,
    DictLoader,
    Environment,
    FileSystemLoader,
    StrictUndefined,
    TemplateError,
)

from agentkit.notifier.errors import TemplateRenderError

# ---- Built-in templates ----------------------------------------
# Each entry is keyed by ``<name>.<channel>.j2``.
#
# For the log channel we emit a single rendered "subject" line —
# multi-line bodies for terminal output are awkward. Webhook
# templates produce JSON-friendly bodies (one-shot strings); the
# WebhookChannel chooses how to wrap them.
#
# Note: log channel templates use only ``.txt`` content because
# the LogChannel doesn't separate subject / body. For richer
# channels (email / IM later) we keep the suffix pattern open.

_BUILTIN_TEMPLATES: dict[str, str] = {
    # ---- Generic fallback ----
    "generic_default.txt.j2": (
        "[{{ severity | upper }}] {{ topic }}"
        "{%- if run_id %} (run {{ run_id }}){% endif %}"
    ),
    "generic_default.body.j2": (
        "Topic: {{ topic }}\n"
        "Severity: {{ severity }}\n"
        "{% if run_id %}Run: {{ run_id }}\n{% endif %}"
        "{% if trace_id %}Trace: {{ trace_id }}\n{% endif %}"
        "Payload: {{ payload | tojson(indent=2) }}\n"
    ),

    # ---- Run lifecycle ----
    "run_failed_default.txt.j2": (
        "[CRITICAL] Run {{ run_id }} failed: {{ payload.reason | default('unknown') }}"
    ),
    "run_failed_default.body.j2": (
        "A workflow Run terminated with status=Failed.\n\n"
        "  workflow_id : {{ workflow_id }}\n"
        "  run_id      : {{ run_id }}\n"
        "  trace_id    : {{ trace_id }}\n"
        "  reason      : {{ payload.reason | default('unknown') }}\n"
        "  occurred at : {{ now }}\n"
    ),
    "run_succeeded_default.txt.j2": (
        "Run {{ run_id }} succeeded"
    ),
    "run_succeeded_default.body.j2": (
        "Workflow Run finished successfully.\n\n"
        "  workflow_id : {{ workflow_id }}\n"
        "  run_id      : {{ run_id }}\n"
        "  trace_id    : {{ trace_id }}\n"
    ),
    "run_started_default.txt.j2": (
        "Run {{ run_id }} started"
    ),
    "run_started_default.body.j2": (
        "Workflow Run started.\n\n"
        "  workflow_id : {{ workflow_id }}\n"
        "  run_id      : {{ run_id }}\n"
    ),
    "run_intermediate_default.txt.j2": (
        "[{{ severity | upper }}] {{ topic }} on run {{ run_id }}"
    ),
    "run_intermediate_default.body.j2": (
        "Intermediate event on Workflow Run.\n\n"
        "  topic       : {{ topic }}\n"
        "  workflow_id : {{ workflow_id }}\n"
        "  run_id      : {{ run_id }}\n"
        "  payload     : {{ payload | tojson(indent=2) }}\n"
    ),

    # ---- Guardrail ----
    "guard_exceeded_default.txt.j2": (
        "[CRITICAL] Guardrail tripped: "
        "{{ payload.layer }}.{{ payload.dim }} "
        "= {{ payload.used }}/{{ payload.limit }} "
        "(run {{ run_id }})"
    ),
    "guard_exceeded_default.body.j2": (
        "Guardrail rejected a call.\n\n"
        "  layer    : {{ payload.layer }}\n"
        "  dim      : {{ payload.dim }}\n"
        "  used     : {{ payload.used }}\n"
        "  limit    : {{ payload.limit }}\n"
        "  run_id   : {{ run_id }}\n"
        "  agent_id : {{ payload.agent_id | default('—') }}\n"
        "  workflow : {{ workflow_id }}\n\n"
        "Re-running the same call will continue to fail until quota resets.\n"
        "If this is unexpected, audit the workflow YAML guardrails: block.\n"
    ),

    # ---- Role-down ----
    "role_down_default.txt.j2": (
        "[CRITICAL] Role degraded: {{ payload.template_key }} "
        "({{ payload.down_ratio }} down)"
    ),
    "role_down_default.body.j2": (
        "An Agent role has degraded — too many instances reached Down state.\n\n"
        "  template_key : {{ payload.template_key }}\n"
        "  down_ratio   : {{ payload.down_ratio }}\n"
        "  time_window  : {{ payload.time_window | default('5m') }}\n"
        "  workflow_id  : {{ workflow_id }}\n"
    ),

    # ---- DLQ ----
    "dlq_received_default.txt.j2": (
        "[ERROR] DLQ received: {{ topic }}"
    ),
    "dlq_received_default.body.j2": (
        "An event landed in a Dead-Letter Queue.\n\n"
        "  topic    : {{ topic }}\n"
        "  event_id : {{ event_id }}\n"
        "  trace_id : {{ trace_id }}\n"
        "  reason   : {{ payload.reason | default('—') }}\n"
        "  payload  : {{ payload | tojson(indent=2) }}\n"
    ),

    # ---- Human node ----
    "human_pending_default.txt.j2": (
        "Human task awaiting input: {{ payload.task_id }}"
    ),
    "human_pending_default.body.j2": (
        "A HumanNode task is waiting for input.\n\n"
        "  task_id  : {{ payload.task_id }}\n"
        "  run_id   : {{ run_id }}\n"
        "  form_url : {{ payload.form_url | default('—') }}\n"
        "  deadline : {{ payload.deadline | default('—') }}\n"
    ),

    # ---- @mention (Doc11 forward-compat) ----
    "collab_mention_default.txt.j2": (
        "{{ payload.mentioner }} mentioned you on {{ payload.doc_id }}"
    ),
    "collab_mention_default.body.j2": (
        "{{ payload.mentioner }} mentioned you in a comment.\n\n"
        "  document : {{ payload.doc_id }}\n"
        "  snippet  : {{ payload.snippet | default('—') }}\n"
        "  link     : {{ payload.link | default('—') }}\n"
    ),
}


# ============================================================
# Renderer
# ============================================================


class TemplateRenderer:
    """Loads built-in templates + optional project overrides."""

    def __init__(
        self,
        *,
        project_template_dir: str | Path | None = None,
        extra_templates: dict[str, str] | None = None,
    ) -> None:
        loaders: list[BaseLoader] = [DictLoader(_BUILTIN_TEMPLATES)]
        if extra_templates:
            loaders.insert(0, DictLoader(extra_templates))
        if project_template_dir is not None:
            p = Path(project_template_dir)
            if p.exists():
                loaders.insert(0, FileSystemLoader(str(p)))
        # User overrides take precedence — order matters: user dirs
        # are tried first, the built-in dict is the fallback.
        self._env = Environment(
            loader=ChoiceLoader(loaders),
            undefined=StrictUndefined,
            autoescape=False,        # plain text/JSON output, not HTML
            keep_trailing_newline=True,
            trim_blocks=False,
            lstrip_blocks=False,
        )

    # ------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------

    def render(
        self,
        template: str,
        *,
        kind: str = "txt",
        context: dict | None = None,
    ) -> str:
        """Render ``<template>.<kind>.j2`` against ``context``.

        Falls back to ``generic_default.<kind>.j2`` if the named
        template is missing.
        """
        ctx = dict(context or {})
        candidates = [
            f"{template}.{kind}.j2",
            f"generic_default.{kind}.j2",
        ]
        last_err: Exception | None = None
        for name in candidates:
            try:
                t = self._env.get_template(name)
                return t.render(ctx)
            except TemplateError as e:
                last_err = e
        # Both failed → wrap as TemplateRenderError so dispatch can DLQ.
        raise TemplateRenderError(
            f"failed to render any of {candidates}: {last_err}",
        ) from last_err
