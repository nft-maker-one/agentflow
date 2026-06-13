"""Tests for Notifier data models + alias resolution."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from agentkit.notifier.aliases import (
    BUILTIN_DEFAULT_RULES,
    DEFAULT_SUBSCRIPTIONS,
    default_template_for,
    resolve_alias,
)
from agentkit.notifier.models import (
    ChannelSpec,
    DedupSpec,
    NotificationRule,
)


# ----------------------------------------------------------------
# Alias resolution
# ----------------------------------------------------------------


class TestResolveAlias:
    @pytest.mark.parametrize(
        ("alias", "expected"),
        [
            ("run.failed", "workflow.*.failed"),
            ("run.succeeded", "workflow.*.end"),
            ("guard.exceeded", "system.guard.alert.#"),
            ("dlq.received", "*.dlq"),
            ("role.down", "system.runtime.alert.role_down"),
        ],
    )
    def test_known_aliases(self, alias, expected) -> None:
        assert resolve_alias(alias) == expected

    def test_unknown_alias_passthrough(self) -> None:
        # Already-qualified topics pass straight through.
        assert resolve_alias("agent.researcher.out.summary") == "agent.researcher.out.summary"

    def test_workflow_id_substitution(self) -> None:
        # When a rule is pinned to a workflow, the alias gets tightened.
        assert (
            resolve_alias("run.failed", workflow_id="wf_x")
            == "workflow.wf_x.failed"
        )

    def test_default_template_lookup(self) -> None:
        assert default_template_for("guard.exceeded") == "guard_exceeded_default"
        assert default_template_for("dlq.received") == "dlq_received_default"
        # Unknown alias → generic.
        assert default_template_for("brand.new.alias") == "generic_default"


# ----------------------------------------------------------------
# Built-in defaults
# ----------------------------------------------------------------


class TestBuiltinDefaults:
    def test_builtin_rules_are_loadable(self) -> None:
        assert len(BUILTIN_DEFAULT_RULES) >= 3
        # All builtins use the log channel by design.
        assert all(r.channel.kind == "log" for r in BUILTIN_DEFAULT_RULES)

    def test_default_subscriptions_distinct(self) -> None:
        subs = DEFAULT_SUBSCRIPTIONS()
        assert len(subs) == len(set(subs))


# ----------------------------------------------------------------
# NotificationRule validation
# ----------------------------------------------------------------


class TestNotificationRule:
    def test_minimum_construction(self) -> None:
        rule = NotificationRule(
            on="run.failed",
            channel=ChannelSpec(kind="log"),
            to="stderr",
        )
        assert rule.id.startswith("rule_")
        assert rule.severity == "warning"
        assert rule.enabled is True

    def test_to_list_normalization(self) -> None:
        single = NotificationRule(
            on="run.failed",
            channel=ChannelSpec(kind="webhook"),
            to="https://x/y",
        )
        assert single.to_list() == ["https://x/y"]

        many = NotificationRule(
            on="run.failed",
            channel=ChannelSpec(kind="email"),
            to=["a@x.com", "b@x.com"],
        )
        assert many.to_list() == ["a@x.com", "b@x.com"]

    def test_empty_to_rejected(self) -> None:
        with pytest.raises(ValidationError):
            NotificationRule(
                on="run.failed",
                channel=ChannelSpec(kind="log"),
                to="",
            )
        with pytest.raises(ValidationError):
            NotificationRule(
                on="run.failed",
                channel=ChannelSpec(kind="log"),
                to=[],
            )

    def test_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            NotificationRule(
                on="run.failed",
                channel=ChannelSpec(kind="log"),
                to="x",
                bogus_field=1,  # type: ignore[call-arg]
            )

    def test_with_dedup_spec(self) -> None:
        rule = NotificationRule(
            on="dlq.received",
            channel=ChannelSpec(kind="log"),
            to="stderr",
            dedup=DedupSpec(window_seconds=60, by=["rule_id", "topic"]),
        )
        assert rule.dedup is not None
        assert rule.dedup.window_seconds == 60
