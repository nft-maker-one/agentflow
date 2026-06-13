"""Research → Critic → Writer feedback loop, powered by real DeepSeek calls.

This is the **flagship Phase 1 demo** — exercises the full stack with
*actual* LLM reasoning, not a mock:

    __start__
       ↓
    [researcher]   draft + self-rated confidence
       ↓
    [critic]       judges the draft → emits {"choice": "publish" | "refine", ...}
       ↓
    switch on $.choice
       ├─ "publish" → [writer] → __end__
       └─ "refine"  → [researcher]   ← loop back, with critic's feedback

A hard cap (``MAX_ITERATIONS``) on the loop prevents runaway costs:
the critic forces ``"publish"`` once we've seen ``MAX_ITERATIONS``
iterations.

What this demonstrates
----------------------
* **Class-based SDK** — ``class XxxAgent(Agent): async def handle(...)``
* **Switch-edge routing** — ``wf.connect_switch(..., expr="$.choice", cases=...)``
* **Real LLM calls** — the OpenAI-compatible adapter wired to DeepSeek
* **Observability** — ``ProbeServer`` exposes ``/metrics`` so Grafana
  paints the LLM tokens / cost / latency in real time
* **Loop edges** — Compiler accepts cycles (Doc04) when guarded by a
  switch + an exit branch

Prereq
------

Set ``DEEPSEEK_API_KEY`` (the demo also reads ``../.env`` if present)::

    export DEEPSEEK_API_KEY=sk-...

Bring up the docker stack so Grafana can paint live numbers::

    make up

Run
---

::

    PYTHONPATH=src python examples/research_writer_loop.py "2026 年 LLM 推理成本会怎么变化"

Costs roughly $0.001 / run on ``deepseek-chat`` (well below 1 cent).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import signal
import sys
import time
from pathlib import Path

# Make in-repo src/ importable when running directly.
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "src"))


# Load DEEPSEEK_API_KEY from ../.env if present.
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


from agentkit import (  # noqa: E402
    END,
    START,
    Agent,
    Event,
    workflow,
)
from agentkit.bus.inprocess import InProcessEventBus  # noqa: E402
from agentkit.llm import LLMInstanceConfig, build_llm_gateway  # noqa: E402
from agentkit.llm.models import LLMBinding  # noqa: E402
from agentkit.observability import ProbeServer  # noqa: E402
from agentkit.orchestrator import InMemoryRunStore, Orchestrator  # noqa: E402
from agentkit.runtime import AgentWorker, HandlerRegistry  # noqa: E402
from agentkit.workflow.ir import START_NODE  # noqa: E402

PROBE_PORT = int(os.getenv("AGENTKIT_PROM_PORT", "9100"))
MAX_ITERATIONS = int(os.getenv("MAX_ITERATIONS", "3"))


# ============================================================
# Prompts (Chinese — produces native Chinese output)
# ============================================================


RESEARCHER_SYSTEM = """你是一名严谨的中文研究员。给定一个研究问题,产出一段 200-400 字的研究综述。

要求:
- 中文输出。
- 提供具体事实、数据、趋势,而不是泛泛而谈。
- 标注你对自己输出的 confidence(0.0~1.0):有把握就高,凭推测就低。
- 如果上一轮你的草稿被 critic 退回,要根据 critic 的反馈做出*实质性*改进,不要只换说法。

严格按以下 JSON 格式输出(不要包裹 markdown,不要解释):

{"summary": "...你的研究综述...", "confidence": 0.85}
"""


CRITIC_SYSTEM = """你是一名严苛的中文编辑。给定 researcher 的研究综述草稿,判断它是否可以发布。

判断标准:
- 是否包含具体事实/数据,而非空话?
- 论述是否完整、自洽?
- 中文是否通顺、专业?
- researcher 自评的 confidence 是否与实际质量相符?

如果 researcher 的草稿满足以上,选择 "publish";否则选择 "refine" 并给出**具体可执行**的修改建议。

严格按以下 JSON 格式输出(不要包裹 markdown,不要解释):

{"choice": "publish", "feedback": "可以发布。"}
或:
{"choice": "refine", "feedback": "缺少 2026 年具体推理成本数字;请补充 H100 / B200 算力对比的具体百分比。"}
"""


WRITER_SYSTEM = """你是一名中文出版编辑。给定一段研究综述,把它润色成一段可以直接发布的成稿。

要求:
- 中文,300-500 字。
- 保留原文的事实和数据,不要新增未在原文中的信息。
- 调整语序、用词,让语言更专业、流畅。
- 不要加任何免责声明或元话语(例如"以下是成稿"),直接输出成稿正文。
"""


# ============================================================
# Agents
# ============================================================


class Researcher(Agent):
    """Iteratively writes a research draft, incorporating critic feedback."""

    role = "thinking"
    description = "中文研究员 — 写综述并自评 confidence"
    subscribe = ["agent.researcher.in.q"]
    publish = ["agent.researcher.out.draft"]
    llm = LLMBinding(provider="deepseek", model="deepseek-chat")

    async def handle(self, ctx, event):
        question = event.payload.get("question", "")
        feedback = event.payload.get("feedback", "")
        iteration = int(event.payload.get("iteration", 0)) + 1

        user_msg = f"研究问题:{question}"
        if feedback:
            user_msg += f"\n\nCritic 上一轮的修改建议:{feedback}"

        ctx.logger.info(
            "researcher.start",
            iteration=iteration, question=question[:60],
        )

        raw = await ctx.llm.chat(
            prompt=user_msg,
            system=RESEARCHER_SYSTEM,
            temperature=0.4,
        )
        parsed = _safe_parse_json(raw, default={"summary": raw, "confidence": 0.5})

        return [Event(
            "agent.researcher.out.draft",
            {
                "question": question,
                "summary": parsed.get("summary", raw),
                "confidence": float(parsed.get("confidence", 0.5)),
                "iteration": iteration,
            },
        )]


class Critic(Agent):
    """Judges researcher's draft → switch decides loop or publish."""

    role = "judge"
    description = "中文编辑 — 判断是否可发布"
    subscribe = ["agent.researcher.out.draft"]
    publish = ["agent.critic.out.verdict"]
    llm = LLMBinding(provider="deepseek", model="deepseek-chat")

    async def handle(self, ctx, event):
        question = event.payload.get("question", "")
        summary = event.payload.get("summary", "")
        confidence = event.payload.get("confidence", 0.0)
        iteration = event.payload.get("iteration", 1)

        ctx.logger.info(
            "critic.start", iteration=iteration, draft_chars=len(summary),
        )

        # Force-publish at the iteration cap so we never spin forever.
        if iteration >= MAX_ITERATIONS:
            return [Event(
                "agent.critic.out.verdict",
                {
                    "choice": "publish",
                    "feedback": f"强制发布(已迭代 {iteration} 轮,达到上限)",
                    "summary": summary,
                    "question": question,
                    "confidence": confidence,
                    "iteration": iteration,
                },
            )]

        user_msg = (
            f"研究问题:{question}\n"
            f"researcher 自评 confidence:{confidence}\n"
            f"researcher 的草稿:\n{summary}"
        )
        raw = await ctx.llm.chat(
            prompt=user_msg,
            system=CRITIC_SYSTEM,
            temperature=0.2,
        )
        parsed = _safe_parse_json(
            raw, default={"choice": "publish", "feedback": raw},
        )
        choice = parsed.get("choice", "publish")
        if choice not in ("publish", "refine"):
            choice = "publish"

        return [Event(
            "agent.critic.out.verdict",
            {
                "choice": choice,
                "feedback": parsed.get("feedback", ""),
                "summary": summary,        # forwarded so writer can use it
                "question": question,      # forwarded for next iteration
                "confidence": confidence,
                "iteration": iteration,
            },
        )]


class Writer(Agent):
    """Polishes the approved draft into the final output."""

    role = "thinking"
    description = "中文出版编辑 — 润色成稿"
    subscribe = ["agent.writer.in.draft"]
    publish = ["agent.writer.out.final"]
    llm = LLMBinding(provider="deepseek", model="deepseek-chat")

    async def handle(self, ctx, event):
        summary = event.payload.get("summary", "")
        question = event.payload.get("question", "")
        iteration = event.payload.get("iteration", 1)

        ctx.logger.info(
            "writer.start", iteration=iteration, draft_chars=len(summary),
        )
        final = await ctx.llm.chat(
            prompt=summary,
            system=WRITER_SYSTEM,
            temperature=0.3,
        )

        return [Event(
            "agent.writer.out.final",
            {
                "question": question,
                "final": final,
                "iterations_used": iteration,
            },
        )]


# ============================================================
# Helpers
# ============================================================


def _safe_parse_json(text: str, *, default: dict) -> dict:
    """Best-effort JSON parse — handle markdown-wrapped payloads."""
    s = text.strip()
    # Strip ```json ... ``` fences if the model added them despite our prompt.
    s = re.sub(r"^```(?:json)?\s*|\s*```$", "", s, flags=re.MULTILINE)
    try:
        return json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return default


# ============================================================
# Main
# ============================================================


async def main(question: str) -> int:
    if not (os.getenv("DEEPSEEK_API_KEY") or os.getenv("AGENTKIT_LLM_DEEPSEEK_API_KEY")):
        print(
            "✗ Missing DEEPSEEK_API_KEY — set it in the shell or in ../.env",
            file=sys.stderr,
        )
        return 2

    # ---- ProbeServer for Grafana ----
    probe = ProbeServer(host="0.0.0.0", port=PROBE_PORT)
    try:
        probe.start()
        print(f"✓ Probe server at http://localhost:{PROBE_PORT}/metrics")
    except OSError:
        print(f"(probe port {PROBE_PORT} busy — continuing without it)")
        probe = None  # type: ignore[assignment]

    # ---- LLM Gateway wired to DeepSeek ----
    api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("AGENTKIT_LLM_DEEPSEEK_API_KEY")
    gateway = build_llm_gateway(
        instances=[
            LLMInstanceConfig(
                name="deepseek",
                adapter="openai",
                compat="deepseek",
                api_key=api_key,
            ),
        ],
    )
    print("✓ LLM gateway connected to DeepSeek (deepseek-chat)")

    # ---- Build the workflow ----
    researcher = Researcher()
    critic = Critic()
    writer = Writer()

    wf = workflow(
        "wf_research_writer_loop",
        description="Researcher → Critic → (loop|Writer) → __end__",
    )
    wf.add(researcher).add(critic).add(writer)

    # __start__ fires the question into researcher.
    wf.connect(START, researcher)         # via auto = researcher.subscribe[0]
    # researcher → critic (auto-derived)
    wf.connect(researcher, critic)
    # critic emits to its publish topic; switch on $.choice routes:
    wf.connect_switch(
        critic,
        expr="$.choice",
        cases={
            "publish": {"to": writer.key, "via": "agent.writer.in.draft"},
            "refine":  {"to": researcher.key, "via": "agent.researcher.in.q"},
        },
    )
    # writer → __end__
    wf.connect(writer, END)

    ir, plan = wf.compile()
    print(f"✓ Workflow compiled (ir_hash={ir.meta.ir_hash}, agents={len(ir.agents)})")

    # ---- Wire orchestrator + worker on InProcess bus (single-process demo) ----
    bus = InProcessEventBus()
    await bus.start()
    orch = Orchestrator(bus=bus, store=InMemoryRunStore())
    await orch.start()
    await orch.deploy(ir)
    worker = AgentWorker(
        plan=plan,
        bus=bus,
        llm=gateway,
        handlers=HandlerRegistry.global_default(),
    )
    await worker.start()
    await asyncio.sleep(0.2)

    # ---- Optional: install Ctrl+C handler so users can abort mid-run ----
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    # ---- Run ----
    print(f"\n→ question: {question!r}\n")
    t0 = time.monotonic()
    try:
        run = await orch.create_run(
            workflow_id=ir.id,
            input={"question": question},
        )
        print(f"  run_id   = {run.run_id}")
        print(f"  trace_id = {run.trace_id}")

        wait_task = asyncio.create_task(
            orch.wait_for_completion(run.run_id, timeout=180.0),
        )
        stop_task = asyncio.create_task(stop.wait())
        done, pending = await asyncio.wait(
            {wait_task, stop_task}, return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
        if stop_task in done:
            print("\n✗ aborted by user")
            return 130
        final = wait_task.result()

        elapsed = time.monotonic() - t0
        print(f"\n  status   = {final.status.value}")
        print(f"  branches = {len(final.cursor.branch_log)}")
        print(f"  elapsed  = {elapsed:.1f}s")

        if final.status.value == "Succeeded":
            outputs = bus.published_for_topic("agent.writer.out.final")
            if outputs:
                payload = outputs[-1].payload
                print(f"\n──── 最终成稿 (迭代 {payload.get('iterations_used')} 轮) ────")
                print(payload.get("final", "(empty)"))
                print(  "──────────────────────────────────────────────────")
        else:
            print(f"  reason   = {final.failure_reason}")

        # Show per-agent token totals from the bus log.
        print("\nLLM calls by topic:")
        for topic in (
            "agent.researcher.out.draft",
            "agent.critic.out.verdict",
            "agent.writer.out.final",
        ):
            n = len(bus.published_for_topic(topic))
            print(f"  {topic:<35} {n} event(s)")

        return 0 if final.status.value == "Succeeded" else 1
    finally:
        await worker.stop()
        await orch.stop()
        await bus.stop()
        if probe is not None:
            probe.stop()


def _cli() -> int:
    parser = argparse.ArgumentParser(
        description="Multi-agent research+critic+writer loop powered by DeepSeek",
    )
    parser.add_argument(
        "question",
        nargs="?",
        default="2026 年 LLM 推理成本将如何变化?给出趋势与具体数字。",
        help="The research question (Chinese).",
    )
    args = parser.parse_args()
    return asyncio.run(main(args.question))


if __name__ == "__main__":
    raise SystemExit(_cli())
