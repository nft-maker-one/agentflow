"""fanin_join_demo — end-node FlowControl (``end_join``) 演示。

三个 agent 各自处理后**直接汇入 ``__end__``**：

        ┌── collect_a ──┐
START ──┼── collect_b ──┼──▶ __end__
        └── collect_c ──┘

- **默认（``end_join=False``）**：任意一路 end 信号先到 → run 立即 Succeeded，
  其余两路被丢弃（"先到的那个就结束了整个 workflow"）。
- **``end_join=True``**：run 只有在 a/b/c **三路 end 信号都到齐**后，
  才判定一次 workflow execution 结束。

用 MockLLM 在进程内直接跑，无需任何凭证：

    uv run python examples/fanin_join_demo.py
"""

from __future__ import annotations

import asyncio
import time

from agentkit import Agent, END, Event, START, workflow
from agentkit.testing import LocalRuntime, MockLLMGateway

SLOW_S = 0.4  # branch "c" takes this long; a/b are instant


class SlowBranch(Agent):
    """最慢的一路：故意 sleep，模拟一个耗时较久的下游 agent。"""

    template_key = "collect_c"
    role = "thinking"
    subscribe = ["c.in"]
    publish = ["c.out"]

    async def handle(self, ctx, event):
        await asyncio.sleep(SLOW_S)
        return [Event("c.out", {**event.payload, "from_c": True})]


def build(*, end_join: bool):
    """三路 fan-in 到 __end__；a/b 瞬时、c 慢。"""
    wf = workflow("wf_fanin_join", end_join=end_join)
    for name in ("a", "b"):
        ag = Agent(                       # 无 llm/prompt → 默认 handler 原样转发
            template_key=f"collect_{name}",
            role="thinking",
            subscribe=[f"{name}.in"],
            publish=[f"{name}.out"],
        )
        wf.add(ag)
        wf.connect(START, ag, via=f"{name}.in")
        wf.connect(ag, END, via=f"{name}.out")
    slow = SlowBranch()
    wf.add(slow)
    wf.connect(START, slow, via="c.in")
    wf.connect(slow, END, via="c.out")
    return wf


async def run_once(*, end_join: bool) -> float:
    wf = build(end_join=end_join)
    async with LocalRuntime(wf, llm=MockLLMGateway()) as rt:
        # 只测 run 判定终态的耗时（不含 context 退出时的 worker 排空）。
        t0 = time.monotonic()
        run = await rt.run(input={"q": "go"}, timeout=5.0)
        elapsed = time.monotonic() - t0
    tag = "end_join=True " if end_join else "end_join=False"
    waited = "✅ 等到了慢分支" if elapsed >= SLOW_S else "⚠️ 没等慢分支就结束了"
    print(f"[{tag}] status={run.status.value}  完成耗时={elapsed*1000:.0f}ms  {waited}")
    return elapsed


async def main() -> None:
    print(f"== 三路 fan-in 到 __end__（慢分支 c sleep {SLOW_S*1000:.0f}ms）==")
    t_off = await run_once(end_join=False)   # 默认：a/b 先到就结束，不等 c
    t_on = await run_once(end_join=True)     # FlowControl：必须收齐 a/b/c 才结束
    print(
        f"\n对比：end_join=False 在 ~{t_off*1000:.0f}ms 就结束（丢掉慢分支 c 的产出）；"
        f"end_join=True 等到 ~{t_on*1000:.0f}ms、三路 end 信号全到齐才结束一次。"
    )


if __name__ == "__main__":
    asyncio.run(main())
