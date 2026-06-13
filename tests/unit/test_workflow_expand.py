"""Unit tests for the topic-shorthand expansion compiler step."""

from __future__ import annotations

import pytest

from agentkit.workflow import compile_from_dict
from agentkit.workflow.compiler.expand import expand_workflow
from agentkit.workflow.compiler.parse import parse_dict


# ----------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------


def _two_agent_yaml(
    *,
    out_subscribe: str = "topic",
    out_publish: str = "outline",
    pol_subscribe: str = "outliner.out.outline",
    pol_publish: str = "prose",
    via_in: str = "topic",
    via_to_polisher: str = "outline",
    via_out: str = "prose",
    outliner_prefix: str | None = None,
) -> dict:
    outliner: dict = {
        "role": "thinking",
        "subscribe": [{"topic": out_subscribe}],
        "publish": [{"topic": out_publish}],
    }
    if outliner_prefix is not None:
        outliner["topic_prefix"] = outliner_prefix
    return {
        "id": "wf_x",
        "version": 1,
        "agents": {
            "outliner": outliner,
            "polisher": {
                "role": "thinking",
                "subscribe": [{"topic": pol_subscribe}],
                "publish": [{"topic": pol_publish}],
            },
        },
        "edges": {
            "e_in": {
                "from": "__start__",
                "to": "outliner",
                "via": via_in,
            },
            "e_to_polisher": {
                "from": "outliner",
                "to": "polisher",
                "via": via_to_polisher,
            },
            "e_out": {
                "from": "polisher",
                "to": "__end__",
                "via": via_out,
            },
        },
    }


# ----------------------------------------------------------------
# Bare-suffix expansion
# ----------------------------------------------------------------


class TestBareSuffix:
    def test_subscribe_bare_gets_in_prefix(self) -> None:
        ir = parse_dict(_two_agent_yaml(out_subscribe="topic"))
        ir = expand_workflow(ir)
        assert ir.agents["outliner"].subscribe[0].topic == "agent.outliner.in.topic"

    def test_publish_bare_gets_out_prefix(self) -> None:
        ir = parse_dict(_two_agent_yaml(out_publish="outline"))
        ir = expand_workflow(ir)
        assert ir.agents["outliner"].publish[0].topic == "agent.outliner.out.outline"

    def test_via_bare_from_start_uses_target_in(self) -> None:
        ir = parse_dict(_two_agent_yaml(via_in="topic"))
        ir = expand_workflow(ir)
        assert ir.edges["e_in"].via == "agent.outliner.in.topic"

    def test_via_bare_from_agent_uses_producer_out(self) -> None:
        ir = parse_dict(_two_agent_yaml(via_to_polisher="outline"))
        ir = expand_workflow(ir)
        assert ir.edges["e_to_polisher"].via == "agent.outliner.out.outline"

    def test_via_bare_to_end_uses_producer_out(self) -> None:
        ir = parse_dict(_two_agent_yaml(via_out="prose"))
        ir = expand_workflow(ir)
        assert ir.edges["e_out"].via == "agent.polisher.out.prose"


# ----------------------------------------------------------------
# Cross-agent shorthand
# ----------------------------------------------------------------


class TestCrossAgentShorthand:
    def test_subscribe_cross_agent_shorthand(self) -> None:
        ir = parse_dict(
            _two_agent_yaml(pol_subscribe="outliner.out.outline"),
        )
        ir = expand_workflow(ir)
        assert (
            ir.agents["polisher"].subscribe[0].topic
            == "agent.outliner.out.outline"
        )

    def test_publish_cross_agent_shorthand_keeps_format(self) -> None:
        # Edge-case: a publish that uses cross-agent shorthand pointing
        # at *itself* should still resolve consistently.
        ir = parse_dict(_two_agent_yaml(out_publish="outliner.out.out.outline"))
        ir = expand_workflow(ir)
        assert (
            ir.agents["outliner"].publish[0].topic
            == "agent.outliner.out.out.outline"
        )

    def test_cross_agent_with_in_direction(self) -> None:
        ir = parse_dict(_two_agent_yaml(pol_subscribe="outliner.in.topic"))
        ir = expand_workflow(ir)
        assert (
            ir.agents["polisher"].subscribe[0].topic == "agent.outliner.in.topic"
        )


# ----------------------------------------------------------------
# Fully-qualified pass-through
# ----------------------------------------------------------------


class TestFullyQualified:
    def test_full_form_unchanged(self) -> None:
        ir = parse_dict(
            _two_agent_yaml(out_subscribe="agent.outliner.in.topic"),
        )
        ir = expand_workflow(ir)
        assert (
            ir.agents["outliner"].subscribe[0].topic == "agent.outliner.in.topic"
        )

    def test_user_namespace_unchanged(self) -> None:
        ir = parse_dict(
            _two_agent_yaml(out_subscribe="events.audit.user_login"),
        )
        ir = expand_workflow(ir)
        assert (
            ir.agents["outliner"].subscribe[0].topic == "events.audit.user_login"
        )

    def test_idempotent_on_second_pass(self) -> None:
        # Run expand twice — second pass must not re-prefix.
        ir1 = parse_dict(_two_agent_yaml())
        once = expand_workflow(ir1)
        twice = expand_workflow(once)
        assert (
            once.agents["outliner"].subscribe[0].topic
            == twice.agents["outliner"].subscribe[0].topic
        )
        assert (
            once.edges["e_in"].via == twice.edges["e_in"].via
        )


# ----------------------------------------------------------------
# Per-agent topic_prefix override
# ----------------------------------------------------------------


class TestTopicPrefixOverride:
    def test_custom_prefix_applies_to_local_shorthand(self) -> None:
        ir = parse_dict(
            _two_agent_yaml(
                out_subscribe="topic",
                out_publish="outline",
                outliner_prefix="events.outliner",
            ),
        )
        ir = expand_workflow(ir)
        outliner = ir.agents["outliner"]
        assert outliner.subscribe[0].topic == "events.outliner.in.topic"
        assert outliner.publish[0].topic == "events.outliner.out.outline"

    def test_custom_prefix_applies_to_via(self) -> None:
        ir = parse_dict(
            _two_agent_yaml(
                via_in="topic",
                via_to_polisher="outline",
                outliner_prefix="events.outliner",
            ),
        )
        ir = expand_workflow(ir)
        # __start__ → outliner uses outliner's in prefix
        assert ir.edges["e_in"].via == "events.outliner.in.topic"
        # outliner → polisher uses producer (outliner)'s out prefix
        assert (
            ir.edges["e_to_polisher"].via == "events.outliner.out.outline"
        )


# ----------------------------------------------------------------
# Switch edges
# ----------------------------------------------------------------


class TestSwitchEdgeBranches:
    def test_branch_via_uses_target_in_namespace(self) -> None:
        """Switch branches resolve via in the *target* Agent's IN
        namespace (the Orchestrator publishes there; the target
        subscribes to its own input).
        """
        raw = {
            "id": "wf_judge",
            "version": 1,
            "agents": {
                "judge": {
                    "role": "judge",
                    "subscribe": [{"topic": "input"}],
                    "publish": [{"topic": "choice"}],
                },
                "writer": {
                    "role": "thinking",
                    "subscribe": [{"topic": "draft"}],
                    "publish": [{"topic": "report"}],
                },
            },
            "edges": {
                "e_in": {"from": "__start__", "to": "judge", "via": "input"},
                "e_branch": {
                    "from": "judge",
                    "to": {"switch": "$.choice"},
                    "cases": {
                        "writer": {"to": "writer", "via": "draft"},
                        "end": {"to": "__end__"},
                    },
                },
                "e_out": {"from": "writer", "to": "__end__", "via": "report"},
            },
        }
        ir = expand_workflow(parse_dict(raw))

        # writer's subscribe and the switch branch's via must MATCH —
        # both should land on writer's input namespace.
        assert (
            ir.agents["writer"].subscribe[0].topic
            == "agent.writer.in.draft"
        )
        assert (
            ir.edges["e_branch"].cases["writer"].via
            == "agent.writer.in.draft"
        )

    def test_cross_agent_shorthand_in_branch_via(self) -> None:
        raw = {
            "id": "wf_judge",
            "version": 1,
            "agents": {
                "judge": {
                    "role": "judge",
                    "subscribe": [{"topic": "input"}],
                    "publish": [{"topic": "choice"}],
                },
                "writer": {
                    "role": "thinking",
                    "subscribe": [{"topic": "draft"}],
                    "publish": [{"topic": "report"}],
                },
            },
            "edges": {
                "e_in": {"from": "__start__", "to": "judge", "via": "input"},
                "e_branch": {
                    "from": "judge",
                    "to": {"switch": "$.choice"},
                    "cases": {
                        "writer": {
                            "to": "writer",
                            "via": "writer.in.draft",  # cross-agent shorthand
                        },
                        "end": {"to": "__end__"},
                    },
                },
                "e_out": {"from": "writer", "to": "__end__", "via": "report"},
            },
        }
        ir = expand_workflow(parse_dict(raw))
        assert (
            ir.edges["e_branch"].cases["writer"].via
            == "agent.writer.in.draft"
        )


# ----------------------------------------------------------------
# Full pipeline integration
# ----------------------------------------------------------------


class TestPipelineIntegration:
    def test_short_form_yaml_compiles(self) -> None:
        raw = _two_agent_yaml()
        ir, plan = compile_from_dict(raw)

        # All topics are now fully qualified after compile.
        biz_topics = {t.topic for t in plan.bus_topics.topics if not t.is_dlq}
        assert biz_topics == {
            "agent.outliner.in.topic",
            "agent.outliner.out.outline",
            "agent.polisher.out.prose",
        }

    def test_short_and_long_mixed_compiles(self) -> None:
        # outliner uses short forms, polisher uses full forms.
        raw = _two_agent_yaml(
            out_subscribe="topic",
            out_publish="outline",
            pol_subscribe="agent.outliner.out.outline",
            pol_publish="agent.polisher.out.prose",
            via_in="topic",
            via_to_polisher="agent.outliner.out.outline",
            via_out="agent.polisher.out.prose",
        )
        ir, plan = compile_from_dict(raw)
        biz_topics = {t.topic for t in plan.bus_topics.topics if not t.is_dlq}
        assert "agent.outliner.in.topic" in biz_topics
        assert "agent.outliner.out.outline" in biz_topics
        assert "agent.polisher.out.prose" in biz_topics
