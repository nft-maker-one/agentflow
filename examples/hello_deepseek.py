"""End-to-end AgentKit demo against DeepSeek's OpenAI-compatible API.

Run::

    # Reads DEEPSEEK_API_KEY from ../.env or your shell environment.
    uv run python examples/hello_deepseek.py "the future of TCP/IP"

What this exercises:

* Workflow YAML (Doc04) → Compiler → RuntimePlan
* Orchestrator (Doc05) — Run lifecycle + terminal detection
* AgentWorker / AgentInstance (Doc03) — main loop, FSM, gating
* LLMGatewayClient (Doc06) — DeepSeek via OpenAI-compat adapter
* PublishPipeline — sanctioned publish path with sanitized headers

The pipeline is two LLM calls (outliner → polisher), so it costs
a few cents at most against DeepSeek's ``deepseek-chat`` model.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

# ---- Make the in-repo src/ importable when running directly ------
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "src"))

from agentkit.bus.kafka.adapter import KafkaEventBus  # noqa: E402  # imported for side-effect ref check
from agentkit.llm import LLMInstanceConfig, build_llm_gateway  # noqa: E402
from agentkit.orchestrator import InMemoryRunStore, Orchestrator  # noqa: E402
from agentkit.runtime import (  # noqa: E402
    AgentWorker,
    Event,
    HandlerRegistry,
    agent_handler,
)
from agentkit.workflow import compile_from_dict, load_workflow_yaml  # noqa: E402
from tests.helpers.mock_bus import MockEventBus  # noqa: E402  # used as the in-process bus

# Suppress unused-import warning — KafkaEventBus is here as a type
# reference for users who'd swap MockEventBus for the real bus.
_ = KafkaEventBus


# =============================================================
# .env loader (zero-dep)
# =============================================================


def load_env_file(path: Path) -> None:
    """Naively parse a ``.env`` file and mutate ``os.environ``.

    Lines like ``KEY=VALUE`` get pushed into the env (without
    overriding pre-existing values). Comments (``#``) and blank
    lines are ignored. We don't try to support shell-style quoting
    or expansion — that's what python-dotenv is for.
    """
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


# =============================================================
# Handlers
# =============================================================

WORKFLOW_ID = "wf_hello_deepseek"


@agent_handler(workflow_id=WORKFLOW_ID, template_key="outliner")
async def outliner_handler(ctx, event):
    """Topic → 4–6 bullet outline."""
    topic = (event.payload or {}).get("topic", "").strip() or "an interesting subject"

    ctx.logger.info("outliner.start", topic=topic)
    outline = await ctx.llm.chat(
        system=(
            "You are a precise outliner. Given a topic, produce a 4–6 bullet "
            "outline that captures the most important points. Each bullet must "
            "be a single concise line starting with '- '."
        ),
        prompt=f"Topic: {topic}\n\nProduce the outline now.",
        temperature=0.4,
    )
    ctx.logger.info("outliner.done", chars=len(outline))

    return [
        Event(
            topic="agent.outliner.out.outline",
            payload={"topic": topic, "outline": outline},
        ),
    ]


@agent_handler(workflow_id=WORKFLOW_ID, template_key="polisher")
async def polisher_handler(ctx, event):
    """Outline → polished ~120-word paragraph."""
    payload = event.payload or {}
    topic = payload.get("topic", "").strip()
    outline = payload.get("outline", "").strip()

    ctx.logger.info("polisher.start", topic=topic, outline_chars=len(outline))
    prose = await ctx.llm.chat(
        system=(
            "You are a careful technical writer. Given a topic and a bullet "
            "outline, render a single coherent paragraph (about 120 words) "
            "suitable for a blog post intro. Do NOT use bullets or headings "
            "in your output."
        ),
        prompt=(
            f"Topic: {topic}\n\n"
            f"Outline:\n{outline}\n\n"
            "Produce the polished paragraph now."
        ),
        temperature=0.6,
    )
    ctx.logger.info("polisher.done", chars=len(prose))

    return [
        Event(
            topic="agent.polisher.out.prose",
            payload={"topic": topic, "outline": outline, "prose": prose},
        ),
    ]


# =============================================================
# Main demo flow
# =============================================================


async def main(topic: str) -> int:
    # ── 1) Resolve credentials ────────────────────────────────
    repo_root = Path(__file__).resolve().parents[2]
    load_env_file(repo_root / ".env")
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        sys.stderr.write(
            "ERROR: DEEPSEEK_API_KEY is not set. Either export it or add it to "
            f"{repo_root / '.env'}\n",
        )
        return 2

    # ── 2) Build the LLM Gateway ──────────────────────────────
    # One DeepSeek instance with the cheapest model as default,
    # so handlers can call ``ctx.llm.chat(prompt)`` without
    # restating provider+model every time.
    gateway = build_llm_gateway(
        instances=[
            LLMInstanceConfig(
                name="deepseek",
                compat="deepseek",         # base_url preset
                api_key=api_key,           # explicit; bypasses env-var
            ),
        ],
        default_provider="deepseek",
        default_model="deepseek-chat",     # cheapest DeepSeek model
    )

    # ── 3) Compile the YAML workflow ──────────────────────────
    yaml_path = Path(__file__).with_name("hello_deepseek.yaml")
    raw = load_workflow_yaml(yaml_path)
    ir, plan = compile_from_dict(raw)
    print(
        f"\n✓ compiled {ir.id}@v{ir.version} "
        f"hash={ir.meta.ir_hash}  "
        f"{len(plan.agents)} agent(s)  "
        f"{len(plan.bus_topics.topics)} topic(s)",
    )

    # ── 4) Set up the in-process EventBus ─────────────────────
    # MockEventBus is fine for a single-process demo. To run
    # against real Kafka/Redpanda, swap with KafkaEventBus and
    # call ``await bus.start()`` after configuring brokers.
    bus = MockEventBus()
    await bus.start()

    # ── 5) Wire handlers + worker ─────────────────────────────
    handlers = HandlerRegistry.global_default()  # the @agent_handler defaults
    worker = AgentWorker(
        plan=plan, bus=bus, llm=gateway, handlers=handlers,
    )

    # ── 6) Wire orchestrator ──────────────────────────────────
    orch = Orchestrator(bus=bus, store=InMemoryRunStore())
    await orch.start()
    await orch.deploy(ir)
    await worker.start()

    # Subscription-readiness fence: ``AgentWorker.start()`` schedules
    # ``AgentInstance.run()`` tasks but does NOT block until they've
    # called ``EventBus.subscribe()``. Without this brief yield the
    # initial event we're about to publish would arrive before any
    # subscriber exists and be dropped by the in-memory bus.
    # In a Kafka deployment you'd instead use ``starting_position=
    # "earliest"`` or wait on a readiness signal.
    await asyncio.sleep(0.2)

    # ── 7) Trigger the Run ────────────────────────────────────
    print(f"→ running: {topic!r}")
    t0 = time.perf_counter()
    run = await orch.create_run(
        workflow_id=WORKFLOW_ID, input={"topic": topic},
    )
    print(f"  run_id={run.run_id}  trace_id={run.trace_id}")

    # ── 8) Await completion ───────────────────────────────────
    try:
        finished = await orch.wait_for_completion(run.run_id, timeout=120.0)
    finally:
        await worker.stop()
        await orch.stop()
        await bus.stop()

    elapsed = time.perf_counter() - t0
    print(
        f"\n✓ run.{finished.status.value}  ({elapsed:.1f}s, "
        f"{len(finished.cursor.branch_log)} branch event(s))",
    )

    # ── 9) Render the final output ────────────────────────────
    final_envelopes = bus.published_for_topic("agent.polisher.out.prose")
    if not final_envelopes:
        print("\n✗ no prose was emitted — inspect logs above for errors.")
        return 1

    final = final_envelopes[-1].payload
    print("\n" + "=" * 64)
    print("OUTLINE")
    print("=" * 64)
    print(final.get("outline", "<missing>"))
    print("\n" + "=" * 64)
    print("POLISHED PARAGRAPH")
    print("=" * 64)
    print(final.get("prose", "<missing>"))
    print("=" * 64 + "\n")
    return 0


def cli() -> int:
    parser = argparse.ArgumentParser(
        description="AgentKit DeepSeek demo — outline + polish a topic.",
    )
    parser.add_argument(
        "topic",
        nargs="?",
        default="the future of TCP/IP",
        help="the subject to outline & polish (default: %(default)r)",
    )
    args = parser.parse_args()
    try:
        return asyncio.run(main(args.topic))
    except KeyboardInterrupt:
        print("\n^C — aborted")
        return 130


if __name__ == "__main__":
    raise SystemExit(cli())
