"""story_mailer — 监听消息 → Qwen 写短篇小说 → 发到指定邮箱.

数据流（全部走 EventBus topic，不进 IR，可热插拔）::

    Telegram 私聊/群消息            Novelist Agent (Qwen)              Email (SMTP)
    ───────────────────▶  ext.story.in ──▶ 写一篇短篇小说 ──▶ ext.story.out ──▶ 发邮件给收件人
       (用户发来的"主题")          output_field=result                     body=payload.result

- **Source** `tg_in`（kind=telegram）：长轮询 Bot，把每条消息文本写入 ``payload.theme``，
  发布到 ``ext.story.in``。
- **Agent** `novelist`：订阅 ``ext.story.in``，用 **Qwen** 按 ``payload.theme`` 写一篇短篇小说，
  把正文写入 ``payload.result``，发布到 ``ext.story.out``（由内置默认 handler 完成，无需自定义代码）。
- **Sink** `mail_out`（kind=email_smtp）：订阅 ``ext.story.out``，取 ``payload.result`` 作正文，
  通过 SMTP 发送到固定收件人 ``STORY_TO_EMAIL``。

所有密钥/地址都来自环境变量，**不在代码里硬编码任何凭证**。

运行方式见文件底部 ``__main__`` 与同目录说明。
"""

from __future__ import annotations

import os

from agentkit import Agent, workflow
from agentkit.sdk.workflow import WorkflowDef

# Bus topics that bridge source → agent → sink.
STORY_IN = "ext.story.in"
STORY_OUT = "ext.story.out"


# Novelist 的写作指令（Jinja2 模板，渲染时注入 payload.theme）。
NOVELIST_PROMPT = (
    "你是一位优秀的中文短篇小说作家。\n"
    "请根据下面的主题，写一篇 600–900 字、有完整起承转合、画面感强的短篇小说。\n"
    "只输出小说正文，不要任何解释或标题之外的内容。\n\n"
    "主题：{{ payload.theme }}"
)


def make_novelist(llm: str | None = None) -> Agent:
    """用 **Direct instantiation** 方式构造 Novelist agent（无需继承 Agent）。

    设置了 ``llm`` + ``prompt`` 后，框架的**默认 handler** 会自动：
    渲染 prompt → 调 ``ctx.llm.chat()`` → emit ``{result: <小说正文>, **payload}``，
    所以不需要自定义 ``handle()``。

    ``llm`` 省略时用 ``STORY_LLM`` 环境变量，默认 ``qwen/qwen3.6-35b-a3b``
    （DashScope OpenAI 兼容端点）。传入则覆盖（如测试用 ``"mock/mock"``）。
    """
    return Agent(
        template_key="novelist",                 # 直接构造必须显式给 key
        role="thinking",
        description="Write a short story from the incoming theme using Qwen.",
        subscribe=[STORY_IN],
        publish=[STORY_OUT],
        llm=llm or os.environ.get("STORY_LLM", "qwen/qwen3.6-35b-a3b"),
        prompt=NOVELIST_PROMPT,
        output_field="result",
        max_retries=2,
    )


def build_workflow(
    *,
    llm: str | None = None,
    recipient: str | None = None,
    subject: str | None = None,
) -> WorkflowDef:
    """组装可部署的 event-driven workflow。

    参数用于测试覆盖；不传则全部从环境变量读取。

    密钥/凭证由框架的 **external_io/env.py** 统一从环境变量解析（见下表），
    source/sink 的 config 里**不必**再写它们；显式传入则覆盖环境变量。

    实跑时设置以下环境变量::

        TELEGRAM_BOT_TOKEN   # @BotFather 的 Bot Token（telegram source 自动读取）
        DASHSCOPE_API_KEY    # 阿里云 DashScope / Qwen API Key（serve 的 _build_gateway 自动探测）
        SMTP_HOST SMTP_USER SMTP_PASSWORD   # 发件邮箱（email_smtp sink 自动读取）
        SMTP_TO              # 收件人（小说发到这里；亦可用 build_workflow(recipient=...) 覆盖）
    """
    wf = workflow("wf_story_mailer", event_driven=True)

    # Direct instantiation：make_novelist 内部用 Agent(...) 构造，llm 可覆盖。
    novelist = make_novelist(llm=llm)
    wf.add(novelist)
    # 线性可达：__start__ → novelist → __end__（满足 IR 可达性校验；
    # event-driven 模式下真正的触发来自 source 向 ext.story.in 发消息）。
    wf.connect("__start__", novelist, via=STORY_IN)
    wf.connect(novelist, "__end__", via=STORY_OUT)

    # ── Source：Telegram 监听 → ext.story.in（消息文本写入 payload.theme）──
    # token 省略 → 由 env(TELEGRAM_BOT_TOKEN) 填充；写在 config 里则覆盖 env。
    wf.add_source(
        name="tg_in",
        kind="telegram",
        topic=STORY_IN,
        config={"output_field": "theme"},
    )

    # ── Sink：ext.story.out → 指定邮箱（取 payload.result 作正文）──
    # host/user/password/to 省略 → 由 env(SMTP_HOST/SMTP_USER/SMTP_PASSWORD/SMTP_TO)
    # 填充。显式传入（如下面的 recipient/subject）则覆盖 env。
    sink_config: dict = {
        "text_field": "result",    # 与 Novelist.output_field 对齐
        "use_tls": True,
    }
    if recipient is not None:
        sink_config["to"] = recipient          # 覆盖 SMTP_TO
    if subject is not None:
        sink_config["subject"] = subject        # 覆盖 SMTP_SUBJECT
    wf.add_sink(
        name="mail_out",
        kind="email_smtp",
        topic=STORY_OUT,
        config=sink_config,
    )
    return wf


async def _main() -> None:
    """把 workflow 部署到正在运行的 ``agentkit serve`` 控制面。"""
    from agentkit import AgentKitClient

    base = os.environ.get("AGENTKIT_API", "http://localhost:8080")
    wf = build_workflow()

    missing = [
        k for k in ("TELEGRAM_BOT_TOKEN", "SMTP_HOST", "SMTP_USER", "SMTP_PASSWORD", "SMTP_TO")
        if not os.environ.get(k)
    ]
    if missing:
        print(f"⚠ 缺少环境变量：{', '.join(missing)} —— 仍会部署，但实跑前请补全。")

    async with AgentKitClient(base) as client:
        detail = await client.deploy(wf)
        print(f"✓ 已部署 {wf.id} → {base}")
        print(f"  agents = {list(detail.get('agents', {}) or detail)}")
        ext = await client.list_external(wf.id)
        print(f"  sources = {[s['name'] for s in ext['sources']]}")
        print(f"  sinks   = {[s['name'] for s in ext['sinks']]}")
        print("  现在向你的 Telegram Bot 发一条消息（主题），稍后小说会发到："
              f"{os.environ.get('STORY_TO_EMAIL', '(未设置 STORY_TO_EMAIL)')}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(_main())
