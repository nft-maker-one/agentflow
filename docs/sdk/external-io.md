# External I/O — 外部源与汇

External I/O 让 Agentflow 的 EventBus 与外部世界通信。Source 把外部消息注入 bus，Sink 把 bus 上的 envelope 转发到外面。

设计原则：**外部 I/O 不进入 IR**（不影响编译 / 不改拓扑结构），纯走 bus topic，热插拔零停机。

---

## 支持的 Kind

| Kind | 方向 | 用途 |
|------|------|------|
| `telegram` | source | Bot 长轮询 Telegram 群/私聊，发到 bus |
| `telegram` | sink | 收到 bus envelope，调 sendMessage 发到指定 chat |
| `email_imap` | source | 轮询邮箱新邮件，解析为 payload |
| `email_smtp` | sink | 收到 bus envelope，通过 SMTP 发邮件 |
| `python_script` | source | 自定义 `async def stream(ctx)` 生成器 |
| `python_script` | sink | 自定义 `async def handle(ctx, payload)` 消费函数 |

---

## SDK 声明式用法

```python
from agentkit import workflow

wf = workflow("chatbot", event_driven=True)

# 声明 source
wf.add_source(
    name="tg_in",
    kind="telegram",
    topic="ext.tg.in",
    config={
        "token": "YOUR_BOT_TOKEN",
        "output_field": "q",        # 消息文本写入 payload.q
    },
)

# 声明 sink
wf.add_sink(
    name="tg_out",
    kind="telegram",
    topic="agent.reply.out",
    config={
        "token": "YOUR_BOT_TOKEN",
        "chat_id": "-5066792506",   # 目标群 ID（可选；不填则用 _reply_to）
        "text_field": "result",     # 从 payload 取哪个字段发送
    },
)
```

部署时 `AgentKitClient.deploy()` 自动注册。

---

## Telegram Source

### 配置字段

| 字段 | 必填 | 默认 | 说明 |
|------|------|------|------|
| `token` | ✅ | — | Bot API Token（@BotFather 获取） |
| `output_field` | — | `"text"` | 消息文本写入的 payload 字段名 |

### 行为

- 使用 Telegram `getUpdates` 长轮询（25s timeout）
- 首次启动 offset=-1 跳过历史消息
- 每条消息 emit 到指定 topic：

```json
{
  "<output_field>": "用户消息内容",
  "_meta": {"chat_id": 12345, "from": "username", "message_id": 678},
  "_reply_to": {"chat_id": 12345}
}
```

---

## Telegram Sink

### 配置字段

| 字段 | 必填 | 默认 | 说明 |
|------|------|------|------|
| `token` | ✅ | — | Bot API Token |
| `chat_id` | — | — | 固定目标（优先于 `_reply_to.chat_id`） |
| `text_field` | — | `"result"` | 从 payload 取哪个字段做消息体 |

### 行为

- 订阅指定 topic，每条 envelope → `sendMessage`
- chat_id 优先级：`config.chat_id` > `payload._reply_to.chat_id`
- 启动时自动发送 `{name} is working now` 确认消息到 chat_id
- 发送结果 → Inbox 通知（成功 / 失败）

---

## Email IMAP Source

### 配置字段

| 字段 | 必填 | 默认 | 说明 |
|------|------|------|------|
| `host` | ✅ | — | IMAP 服务器（如 `imap.qq.com`） |
| `port` | — | `993` | SSL 端口 |
| `user` | ✅ | — | 邮箱地址 |
| `password` | ✅ | — | 授权码 / 密码 |
| `mailbox` | — | `"INBOX"` | 监控的邮箱文件夹 |
| `poll_interval_s` | — | `30` | 轮询间隔（秒） |
| `output_field` | — | `"text"` | 邮件正文写入的字段名 |

### Payload 格式

```json
{
  "<output_field>": "邮件正文",
  "_meta": {"from": "sender@example.com", "subject": "..."},
  "_reply_to": {"to": "sender@example.com"}
}
```

---

## Email SMTP Sink

### 配置字段

| 字段 | 必填 | 默认 | 说明 |
|------|------|------|------|
| `host` | ✅ | — | SMTP 服务器（如 `smtp.qq.com`） |
| `port` | — | `465` | SSL 端口 |
| `user` | ✅ | — | 登录邮箱 |
| `password` | ✅ | — | 授权码 |
| `to` | — | — | 固定收件人（优先于 `_reply_to.to`） |
| `subject` | — | `"Agentflow Notification"` | 邮件主题 |
| `text_field` | — | `"result"` | payload 中取做邮件正文的字段 |
| `use_tls` | — | `true` | 是否使用 SSL |

### 行为

- 启动时发送确认邮件 `{name} is working now` 给 `to`
- 收到 envelope → 渲染正文 → 发邮件

---

## Python Script Source

完全自定义的异步生成器：

```python
wf.add_source(
    name="timer",
    kind="python_script",
    topic="ext.timer.tick",
    config={
        "script": """
import asyncio
async def stream(ctx):
    while True:
        await asyncio.sleep(60)
        yield {"tick": True, "ts": __import__("datetime").datetime.now().isoformat()}
""",
    },
)
```

规则：

- 必须定义 `async def stream(ctx)` 异步生成器
- 每次 `yield` 一个 dict，自动发到 topic
- `ctx` 无其他方法（预留扩展）

---

## Python Script Sink

```python
wf.add_sink(
    name="webhook",
    kind="python_script",
    topic="agent.output.out",
    config={
        "script": """
import httpx
async def handle(ctx, payload):
    async with httpx.AsyncClient() as c:
        await c.post("https://hooks.example.com/notify", json=payload)
""",
    },
)
```

规则：

- 必须定义 `async def handle(ctx, payload)`
- `payload` 是 envelope.payload dict
- 无返回值要求

---

## 通过 AgentKitClient 管理

```python
from agentkit import AgentKitClient

async with AgentKitClient("http://localhost:8080") as c:
    # 列出可用 kind
    kinds = await c.list_external_kinds()

    # 查看某 workflow 的 sources/sinks
    ext = await c.list_external("wf_chatbot")
    print(ext["sources"], ext["sinks"])

    # 动态添加
    await c.add_external(
        "wf_chatbot",
        direction="source", name="new_src",
        kind="telegram", topic="ext.new.in",
        config={"token": "..."},
    )

    # 删除
    await c.remove_external("wf_chatbot", direction="source", name="new_src")
```

---

## Agent 如何接收 Source 消息

Source 发布到 topic（如 `ext.tg.in`）。Agent 只需 subscribe 同一 topic：

```python
class Responder(Agent):
    subscribe = ["ext.tg.in"]       # 直接订阅 source 的 topic
    publish = ["agent.responder.out"]
    prompt = "回复用户消息：{{ payload.q }}"
    llm = "deepseek/deepseek-chat"
```

---

## 安全：Secret Redaction

通过 API `GET /workflows/{id}/external` 查看时，config 中的敏感字段（`token` / `password` / `api_key` / `secret`）自动 redact 为 `***xxxx`（保留最后 4 字符）。

编辑时如果 config 值为 `***xxxx` 形式，server 自动保留旧值（不覆盖）。
