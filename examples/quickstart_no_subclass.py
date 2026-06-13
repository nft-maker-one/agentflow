"""Quickstart — multi-agent pipeline **without subclassing**.

Demonstrates the simplest possible API:

* :class:`Agent` is instantiated directly with config kwargs.
* The default handler does ``Jinja2 prompt → LLM → publish`` automatically.
* Per-agent ``max_retries``, ``fallback_response`` knobs.

Pipeline::

    __start__ → tagger → translator → __end__

* ``tagger``     — tags input text with a topic label
* ``translator`` — translates tagged text to English

Both agents are LLM-driven, no custom code.

Prereq::

    export DEEPSEEK_API_KEY=sk-...   # or in ../.env

Run::

    PYTHONPATH=src python examples/quickstart_no_subclass.py "今天北京下雪了。"
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

# Make in-repo src/ importable.
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "src"))


def _load_dotenv() -> None:
    for candidate in (_REPO_ROOT / ".env", _REPO_ROOT.parent / ".env"):
        if not candidate.exists():
            continue
        for line in candidate.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())
        break


_load_dotenv()


from agentkit import END, START, Agent, workflow                    # noqa: E402
from agentkit.bus.inprocess import InProcessEventBus                  # noqa: E402
from agentkit.llm import LLMInstanceConfig, build_llm_gateway         # noqa: E402
from agentkit.orchestrator import InMemoryRunStore, Orchestrator      # noqa: E402
from agentkit.runtime import AgentWorker, HandlerRegistry             # noqa: E402


async def main(text: str) -> int:
    if not (os.getenv("DEEPSEEK_API_KEY") or os.getenv("AGENTKIT_LLM_DEEPSEEK_API_KEY")):
        print("✗ Missing DEEPSEEK_API_KEY", file=sys.stderr)
        return 2

    api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("AGENTKIT_LLM_DEEPSEEK_API_KEY")
    gateway = build_llm_gateway(
        instances=[LLMInstanceConfig(
            name="deepseek", adapter="openai", compat="deepseek",
            api_key=api_key,
        )],
    )

    # ---- Two LLM agents, no subclass ---------------------------

    tagger = Agent(
        template_key="tagger",
        role="thinking",
        description="Tag input text with a topic label",
        subscribe=["agent.tagger.in.q"],
        publish=["agent.tagger.out.r"],
        llm="deepseek/deepseek-chat",
        system_prompt="你是一名语言学家。给输入文本打一个最相关的话题标签(单词,中文)。",
        prompt="文本:{{ payload.text }}\n\n只输出一个话题标签,不要其他内容。",
        output_field="tag",
        max_retries=1,
        fallback_response={"tag": "(打标签失败)"},
    )

    translator = Agent(
        template_key="translator",
        role="thinking",
        description="Translate Chinese text to English",
        subscribe=["agent.tagger.out.r"],
        publish=["agent.translator.out.r"],
        llm="deepseek/deepseek-chat",
        system_prompt="You are a precise translator. Output ONLY the translation, no quotes, no commentary.",
        prompt="Translate to English:\n\n{{ payload.text }}",
        output_field="translation",
        max_retries=1,
        fallback_response={"translation": "(translation failed)"},
    )

    # ---- Workflow + runtime ------------------------------------

    wf = workflow("wf_quickstart_no_subclass")
    wf.add(tagger).add(translator)
    wf.connect(START, tagger)
    wf.connect(tagger, translator)
    wf.connect(translator, END)
    ir, plan = wf.compile()

    bus = InProcessEventBus()
    await bus.start()
    orch = Orchestrator(bus=bus, store=InMemoryRunStore())
    await orch.start()
    await orch.deploy(ir)
    worker = AgentWorker(
        plan=plan, bus=bus, llm=gateway,
        handlers=HandlerRegistry.global_default(),
    )
    await worker.start()
    await asyncio.sleep(0.2)

    print(f"\n→ input: {text!r}\n")
    try:
        run = await orch.create_run(workflow_id=ir.id, input={"text": text})
        final = await orch.wait_for_completion(run.run_id, timeout=60)
        print(f"  status   = {final.status.value}")
        if final.status.value == "Succeeded":
            tagged = bus.published_for_topic("agent.tagger.out.r")[-1].payload
            translated = bus.published_for_topic("agent.translator.out.r")[-1].payload
            print(f"\n  tag         : {tagged.get('tag')}")
            print(f"  translation : {translated.get('translation')}")
        else:
            print(f"  reason   = {final.failure_reason}")
        return 0 if final.status.value == "Succeeded" else 1
    finally:
        await worker.stop()
        await orch.stop()
        await bus.stop()


def _cli() -> int:
    parser = argparse.ArgumentParser(
        description="Multi-agent quickstart — no Agent subclass needed",
    )
    parser.add_argument(
        "text",
        nargs="?",
        default="今天北京的雪下得真大,孩子们都在堆雪人。",
        help="The Chinese text to tag and translate.",
    )
    args = parser.parse_args()
    return asyncio.run(main(args.text))


if __name__ == "__main__":
    raise SystemExit(_cli())
