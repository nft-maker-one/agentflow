# External I/O — Sources and Sinks

External I/O allows Agentflow's EventBus to communicate with the outside world. A Source injects external messages into the bus, while a Sink forwards envelopes from the bus to the outside.

Design Principle: **External I/O does not enter the IR** (it neither affects compilation nor alters the topology). It operates entirely via bus topics and supports hot-plugging with zero downtime.

---

## Supported Kinds

| Kind | Direction | Purpose |
| --- | --- | --- |
| `telegram` | source | Long-polls Telegram groups/private chats and sends messages to the bus. |
| `telegram` | sink | Receives bus envelopes and calls `sendMessage` to a specified chat. |
| `email_imap` | source | Polls a mailbox for new emails and parses them into a payload. |
| `email_smtp` | sink | Receives bus envelopes and sends emails via SMTP. |
| `python_script` | source | Custom `async def stream(ctx)` generator. |
| `python_script` | sink | Custom `async def handle(ctx, payload)` consumer function. |

---

## SDK Declarative Usage

```python
from agentkit import workflow

wf = workflow("chatbot", event_driven=True)

# Declare a source
wf.add_source(
    name="tg_in",
    kind="telegram",
    topic="ext.tg.in",
    config={
        "token": "YOUR_BOT_TOKEN",
        "output_field": "q",        # Message text will be written to payload.q
    },
)

# Declare a sink
wf.add_sink(
    name="tg_out",
    kind="telegram",
    topic="agent.reply.out",
    config={
        "token": "YOUR_BOT_TOKEN",
        "chat_id": "-5066792506",   # Target chat ID (optional; uses _reply_to if omitted)
        "text_field": "result",     # Which field from the payload to send
    },
)

```

These are automatically registered during deployment via `AgentKitClient.deploy()`.

---

## Telegram Source

### Configuration Fields

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `token` | ✅ | — | Bot API Token (obtained from @BotFather) |
| `output_field` | — | `"text"` | Payload field name where the message text will be written |

### Behavior

* Uses Telegram `getUpdates` for long-polling (25s timeout).
* Skips historical messages on the first startup using `offset=-1`.
* Emits each message to the specified topic:

```json
{
  "<output_field>": "User's message content",
  "_meta": {"chat_id": 12345, "from": "username", "message_id": 678},
  "_reply_to": {"chat_id": 12345}
}

```

---

## Telegram Sink

### Configuration Fields

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `token` | ✅ | — | Bot API Token |
| `chat_id` | — | — | Fixed target (takes priority over `_reply_to.chat_id`) |
| `text_field` | — | `"result"` | Payload field to use as the message body |

### Behavior

* Subscribes to the specified topic, translating every envelope into a `sendMessage` API call.
* `chat_id` resolution priority: `config.chat_id` > `payload._reply_to.chat_id`.
* Automatically sends a `{name} is working now` confirmation message to the `chat_id` upon startup.
* Sending results trigger an Inbox notification (success/failure).

---

## Email IMAP Source

### Configuration Fields

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | ✅ | — | IMAP server (e.g., `imap.qq.com`) |
| `port` | — | `993` | SSL port |
| `user` | ✅ | — | Email address |
| `password` | ✅ | — | Authorization code / Password |
| `mailbox` | — | `"INBOX"` | Mailbox folder to monitor |
| `poll_interval_s` | — | `30` | Polling interval (seconds) |
| `output_field` | — | `"text"` | Payload field name where the email body is written |

### Payload Format

```json
{
  "<output_field>": "Email body text",
  "_meta": {"from": "sender@example.com", "subject": "..."},
  "_reply_to": {"to": "sender@example.com"}
}

```

---

## Email SMTP Sink

### Configuration Fields

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | ✅ | — | SMTP server (e.g., `smtp.qq.com`) |
| `port` | — | `465` | SSL port |
| `user` | ✅ | — | Login email address |
| `password` | ✅ | — | Authorization code / Password |
| `to` | — | — | Fixed recipient (takes priority over `_reply_to.to`) |
| `subject` | — | `"Agentflow Notification"` | Email subject |
| `text_field` | — | `"result"` | Payload field used as the email body |
| `use_tls` | — | `true` | Whether to use SSL/TLS |

### Behavior

* Sends a `{name} is working now` confirmation email to the `to` address upon startup.
* Upon receiving an envelope $\rightarrow$ renders the body $\rightarrow$ sends the email.

---

## Python Script Source

A fully customizable asynchronous generator:

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

Rules:

* Must define an `async def stream(ctx)` async generator.
* Each `yield` must produce a dictionary, which is automatically published to the topic.
* `ctx` currently has no other methods (reserved for future extensions).

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

Rules:

* Must define an `async def handle(ctx, payload)` function.
* `payload` is the `envelope.payload` dictionary.
* No return value is required.

---

## Managing via AgentKitClient

```python
from agentkit import AgentKitClient

async with AgentKitClient("http://localhost:8080") as c:
    # List available kinds
    kinds = await c.list_external_kinds()

    # View sources/sinks for a specific workflow
    ext = await c.list_external("wf_chatbot")
    print(ext["sources"], ext["sinks"])

    # Add dynamically
    await c.add_external(
        "wf_chatbot",
        direction="source", name="new_src",
        kind="telegram", topic="ext.new.in",
        config={"token": "..."},
    )

    # Remove
    await c.remove_external("wf_chatbot", direction="source", name="new_src")

```

---

## How Agents Receive Source Messages

A Source publishes to a topic (e.g., `ext.tg.in`). An Agent simply needs to subscribe to the same topic:

```python
class Responder(Agent):
    subscribe = ["ext.tg.in"]       # Directly subscribe to the source's topic
    publish = ["agent.responder.out"]
    prompt = "Reply to the user's message: {{ payload.q }}"
    llm = "deepseek/deepseek-chat"

```

---

## Security: Secret Redaction

When viewing configurations via the `GET /workflows/{id}/external` API, sensitive fields in the config (`token` / `password` / `api_key` / `secret`) are automatically redacted to `***xxxx` (retaining only the last 4 characters).

During edits, if a config value is passed in the `***xxxx` format, the server will automatically preserve the old secret value without overwriting it.