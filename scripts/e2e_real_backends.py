"""Real-backend end-to-end check for the pluggable control plane.

Boots an ``AppState`` wired through the *new* ``build_bus`` /
``build_store`` / ``build_guardrail`` factories against the live
docker-compose infra (Kafka/Redpanda + Postgres + Redis), runs a real
workflow, and asserts the run actually persisted to Postgres and the
quota ledger landed in Redis.

Run with the infra up (``make up-core`` equivalent) and env pointing at it::

    AGENTKIT_BUS_BROKERS=localhost:9092 \
    REDIS_URL=redis://localhost:6379/0 \
    AGENTKIT_GUARDRAIL_REDIS_URL=redis://localhost:6379/0 \
    AGENTKIT_PG_DSN=postgresql://agentkit:agentkit@localhost:5433/agentkit \
    uv run python scripts/e2e_real_backends.py
"""

from __future__ import annotations

import asyncio

from agentkit import END, START, Agent, Event, workflow
from agentkit.api import AppState
from agentkit.api.backends import build_bus, build_guardrail, build_store
from agentkit.runtime.executor import _FunctionExecutor
from agentkit.testing import MockLLMGateway


class _Echo(Agent):
    role = "thinking"
    template_key = "echo"
    subscribe = ["agent.echo.in.q"]
    publish = ["agent.echo.out.r"]

    async def handle(self, ctx, event):
        return [Event("agent.echo.out.r", {"text": event.payload.get("q", "")})]


def _ok(label: str, cond: bool, detail: str = "") -> bool:
    mark = "✅" if cond else "❌"
    print(f"  {mark} {label}{(' — ' + detail) if detail else ''}")
    return cond


async def main() -> int:
    print("→ Building backends from the factory (kafka / pg / redis)…")
    bus = build_bus("kafka")
    store = build_store("pg")
    guardrail = build_guardrail("redis")
    print(f"  bus={type(bus).__name__}  store={type(store).__name__}  "
          f"guardrail={type(guardrail).__name__}")

    state = AppState(bus=bus, store=store, guardrail=guardrail)
    await state.start()  # opens kafka producer, pg pool+migrate, redis ping
    print("→ AppState.start() OK (kafka producer + pg pool/migrate + redis up)")

    passed = True
    run_id = None
    try:
        # Deploy a one-agent echo workflow.
        echo = _Echo()
        wf = workflow("wf_real_e2e")
        wf.add(echo).connect(START, echo).connect(echo, END)
        ir, plan = wf.compile()
        state.handler_registry.register(
            workflow_id=ir.id, template_key=echo.key,
            executor=_FunctionExecutor(echo.handler), replace=True,
        )
        await state.deploy_workflow(ir, plan, llm_gateway=MockLLMGateway())
        # Let Kafka consumer-group join + topic auto-create settle.
        await asyncio.sleep(3.0)
        print("→ Workflow deployed; creating a run over Kafka…")

        run = await state.orchestrator.create_run(
            workflow_id=ir.id, input={"q": "real-backend-hello"},
        )
        run_id = run.run_id
        final = await state.orchestrator.wait_for_completion(run_id, timeout=40)
        print(f"→ Run finished: status={final.status.value}  run_id={run_id}")

        # ── Assertion 1: run reached a terminal state over Kafka ──
        passed &= _ok("Kafka: run reached terminal status",
                      final.status.value in ("Succeeded", "Failed"),
                      final.status.value)

        # ── Assertion 2: run persisted to Postgres (read back via store) ──
        fetched = await state.store.get(run_id)
        passed &= _ok("Postgres: run row persisted + readable",
                      fetched is not None and fetched.run_id == run_id,
                      f"workflow={getattr(fetched, 'workflow_id', '?')}")

        # ── Assertion 3: Redis guardrail ledger seeded for this run ──
        redis = guardrail._redis  # type: ignore[attr-defined]
        keys = await redis.keys("*")
        run_keyed = [k for k in keys if run_id in (k if isinstance(k, str) else k.decode())]
        passed &= _ok("Redis: guardrail quota key present for run",
                      len(run_keyed) > 0,
                      f"{len(run_keyed)} key(s): {run_keyed[:2]}")

    finally:
        await state.stop()
        print("→ AppState.stop() OK (drained workers, closed pg pool + redis + kafka)")

    print()
    print("RESULT:", "ALL REAL-BACKEND CHECKS PASSED ✅" if passed
          else "SOME CHECKS FAILED ❌")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
