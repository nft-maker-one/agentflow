"""Demo of ``json_output=True`` against real DeepSeek.

Two agents wired in series:

* ``extractor`` — extract structured info (entities + sentiment) from
  free text. Uses ``json_output=True`` + a JSON schema so the
  downstream payload is a guaranteed dict, not a string.
* ``rewriter``  — read the extracted dict and produce a one-sentence
  summary. (Plain text mode.)

Run::

    PYTHONPATH=src python examples/json_output_demo.py "我对这家餐厅的服务很失望,但菜品味道还不错"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

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


from agentkit import END, START, Agent, workflow                # noqa: E402
from agentkit.bus.inprocess import InProcessEventBus              # noqa: E402
from agentkit.llm import LLMInstanceConfig, build_llm_gateway     # noqa: E402
from agentkit.orchestrator import InMemoryRunStore, Orchestrator  # noqa: E402
from agentkit.runtime import AgentWorker, HandlerRegistry         # noqa: E402


SENTIMENT_SCHEMA = {
    "type": "object",
    "required": ["sentiment", "entities", "score"],
    "properties": {
        "sentiment": {"type": "string", "enum": ["positive", "negative", "mixed", "neutral"]},
        "entities":  {"type": "array", "items": {"type": "string"}},
        "score":     {"type": "number", "minimum": -1, "maximum": 1},
    },
}


async def main(text: str) -> int:
    if not (os.getenv("DEEPSEEK_API_KEY") or os.getenv("AGENTKIT_LLM_DEEPSEEK_API_KEY")):
        print("✗ Missing DEEPSEEK_API_KEY", file=sys.stderr)
        return 2

    api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("AGENTKIT_LLM_DEEPSEEK_API_KEY")
    gateway = build_llm_gateway(
        instances=[LLMInstanceConfig(
            name="deepseek", adapter="openai", compat="deepseek", api_key=api_key,
        )],
    )

    extractor = Agent(
        template_key="extractor",
        role="thinking",
        description="Extract structured info from text",
        subscribe=["agent.extractor.in.q"],
        publish=["agent.extractor.out.r"],
        llm="deepseek/deepseek-chat",
        system_prompt=(
            "你是一个信息提取助手。从输入文本中提取实体、情感倾向、置信度。\n\n"
            "**严格按以下 JSON 结构返回** (所有字段都必须出现):\n"
            "{\n"
            '  "sentiment": "positive" 或 "negative" 或 "mixed" 或 "neutral",\n'
            '  "entities":  ["实体1", "实体2", ...],\n'
            '  "score":     -1 到 1 之间的小数\n'
            "}\n\n"
            "score = -1 表示非常负面,+1 表示非常正面,0 表示中性。"
        ),
        prompt="文本:{{ payload.text }}",
        json_output=True,
        json_schema=SENTIMENT_SCHEMA,    # provider-side enforcement + local validation
        output_field="analysis",          # downstream sees payload.analysis = {...}
        max_retries=2,
        retry_backoff_s=0.5,
        fallback_response={"analysis": {
            "sentiment": "neutral", "entities": [], "score": 0.0,
            "_failed": True,
        }},
    )

    rewriter = Agent(
        template_key="rewriter",
        role="thinking",
        description="Summarize the analysis in one Chinese sentence",
        subscribe=["agent.extractor.out.r"],
        publish=["agent.rewriter.out.r"],
        llm="deepseek/deepseek-chat",
        system_prompt="用一句话(不超过 30 字)总结分析结果,要包含情感倾向。",
        prompt=(
            "原文:{{ payload.text }}\n"
            "情感:{{ payload.analysis.sentiment }} (置信度 {{ payload.analysis.score }})\n"
            "提到的实体:{{ payload.analysis.entities | join('、') }}"
        ),
        output_field="summary",
    )

    wf = workflow("wf_json_output_demo")
    wf.add(extractor).add(rewriter)
    wf.connect(START, extractor)
    wf.connect(extractor, rewriter)
    wf.connect(rewriter, END)
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
            extr = bus.published_for_topic("agent.extractor.out.r")[-1].payload
            summ = bus.published_for_topic("agent.rewriter.out.r")[-1].payload
            print(f"\n  ── extractor.analysis (typed dict) ──")
            print(f"  {json.dumps(extr['analysis'], ensure_ascii=False, indent=2)}")
            print(f"\n  ── rewriter.summary ──")
            print(f"  {summ['summary']}")
        else:
            print(f"  reason   = {final.failure_reason}")
        return 0 if final.status.value == "Succeeded" else 1
    finally:
        await worker.stop()
        await orch.stop()
        await bus.stop()


def _cli() -> int:
    parser = argparse.ArgumentParser(
        description="json_output demo — extract structured info via DeepSeek",
    )
    parser.add_argument(
        "text", nargs="?",
        default="我对这家餐厅的服务很失望,但菜品味道还不错。",
    )
    args = parser.parse_args()
    return asyncio.run(main(args.text))


if __name__ == "__main__":
    raise SystemExit(_cli())
