import { useEffect, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  listRuns, getRun, type EnvelopeOut, type WorkflowDetail,
} from "../lib/api";

interface Props {
  workflow: WorkflowDetail;
  /** "start" | "end" — which terminal node was clicked. null = closed. */
  kind: "start" | "end" | null;
  /** Run-context events (live SSE feed) — used for the active run. */
  runEvents?: EnvelopeOut[];
  onClose: () => void;
}

export function StartEndDrawer({ workflow, kind, runEvents = [], onClose }: Props) {
  // Fetch the most recent run only when "end" is opened — used to
  // pull historical events when we don't have a live runEvents feed.
  const { data: latestRun } = useQuery({
    queryKey: ["latest-run", workflow.id],
    queryFn: async () => {
      const runs = await listRuns({ workflow_id: workflow.id, limit: 1 });
      if (runs.length === 0) return null;
      return await getRun(runs[0].run_id);
    },
    enabled: kind === "end",
  });

  // Prefer live runEvents (which match the user's current run) — fall
  // back to fetching the latest run's events from the SSE replay.
  const eventsForLatestRun = useEventsForRun(
    runEvents.length > 0 ? null : latestRun?.run_id ?? null,
  );

  const events = runEvents.length > 0 ? runEvents : eventsForLatestRun;

  // ---- Derive: agents that have an outbound edge to __end__ ----
  // (or to "__error__" when kind is wired that way — we only handle
  // the end side here.)
  const endAgents = useMemo(() => {
    if (kind !== "end") return [];
    // Find agents that have at least one edge ending in __end__.
    const agents: { key: string; topic: string }[] = [];
    for (const e of workflow.edges) {
      if (e.target !== "__end__") continue;
      if (e.source === "__start__") continue; // shouldn't happen
      // Find the agent's publish topic that triggers this edge.
      const a = workflow.agents.find((x) => x.template_key === e.source);
      if (!a) continue;
      // Use the edge's via if available, else first publish topic.
      const topic = e.via ?? a.publish[0] ?? "";
      agents.push({ key: a.template_key, topic });
    }
    return agents;
  }, [workflow, kind]);

  // The first agent (used for "Start" content).
  const firstAgent = workflow.agents[0];

  useEffect(() => {
    if (!kind) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [kind, onClose]);

  if (!kind) return null;

  return (
    <>
      <div className="fixed inset-0 bg-slate-900/20 z-40" onClick={onClose} />
      <div
        className="fixed top-0 right-0 h-full w-full sm:w-[520px] z-50
                   bg-white border-l border-slate-200 shadow-xl flex flex-col"
      >
        <div className="flex items-center justify-between px-5 py-4 border-b border-slate-200">
          <div>
            <div className="text-xs text-slate-500 uppercase tracking-wide">
              {kind === "start" ? "Workflow Input" : "Workflow Output"}
            </div>
            <div className="font-mono text-lg font-semibold">
              {kind === "start" ? "▶ Start" : "■ End"}
            </div>
          </div>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-700 text-2xl leading-none px-2"
          >×</button>
        </div>

        <div className="flex-1 overflow-y-auto thin-scroll px-5 py-4 space-y-5">
          {kind === "start" ? (
            <StartContent firstAgent={firstAgent} />
          ) : (
            <EndContent
              workflow={workflow}
              latestRun={latestRun}
              endAgents={endAgents}
              events={events}
            />
          )}
        </div>
      </div>
    </>
  );
}

// ----------------------------------------------------------------
// Hook to load events of an arbitrary run_id via the SSE backlog.
// ----------------------------------------------------------------

function useEventsForRun(runId: string | null) {
  const { data } = useQuery({
    queryKey: ["run-events-snapshot", runId],
    queryFn: async (): Promise<EnvelopeOut[]> => {
      if (!runId) return [];
      // The SSE endpoint replays the backlog before going live —
      // we can use `fetch` against it and parse what arrives in the
      // first ~500ms (everything currently buffered).
      const ctl = new AbortController();
      const tid = setTimeout(() => ctl.abort(), 800);
      const events: EnvelopeOut[] = [];
      try {
        const r = await fetch(
          `/api/runs/${encodeURIComponent(runId)}/events`,
          { signal: ctl.signal },
        );
        if (!r.body) return [];
        const reader = r.body.getReader();
        const decoder = new TextDecoder();
        let buf = "";
        // eslint-disable-next-line no-constant-condition
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buf += decoder.decode(value, { stream: true });
          const lines = buf.split("\n");
          buf = lines.pop() ?? "";
          for (const line of lines) {
            if (line.startsWith("data: ")) {
              const body = line.slice(6).trim();
              if (!body) continue;
              try {
                events.push(JSON.parse(body) as EnvelopeOut);
              } catch { /* ignore */ }
            }
          }
        }
      } catch {
        // expected on abort
      } finally {
        clearTimeout(tid);
      }
      return events;
    },
    enabled: !!runId,
  });
  return data ?? [];
}

// ----------------------------------------------------------------

function StartContent({
  firstAgent,
}: { firstAgent: WorkflowDetail["agents"][number] | undefined }) {
  if (!firstAgent) {
    return (
      <p className="text-slate-500 italic">This workflow has no agents yet.</p>
    );
  }
  return (
    <>
      <section>
        <h3 className="text-xs font-semibold text-slate-700 uppercase tracking-wide mb-2">
          Entry agent
        </h3>
        <div className="bg-slate-50 rounded p-3 text-sm">
          <div className="font-mono font-medium">{firstAgent.template_key}</div>
          <div className="text-xs text-slate-600 mt-1">
            {firstAgent.description || "(no description)"}
          </div>
          <div className="text-xs text-slate-500 mt-2">
            Subscribes to: <code>{firstAgent.subscribe.join(", ")}</code>
          </div>
        </div>
      </section>

      <section>
        <h3 className="text-xs font-semibold text-slate-700 uppercase tracking-wide mb-2">
          How input flows
        </h3>
        <p className="text-sm text-slate-600">
          When you click <code className="font-mono bg-slate-100 px-1 rounded">▶ Run</code>,
          the input you provide becomes the <code>payload</code> of the
          first envelope on{" "}
          <code className="font-mono">{firstAgent.subscribe[0] ?? "—"}</code>.
          Each agent reads that payload and adds its own field, so the
          dictionary grows as it flows downstream.
        </p>
      </section>
    </>
  );
}

// ----------------------------------------------------------------

function EndContent({
  workflow, latestRun, endAgents, events,
}: {
  workflow: WorkflowDetail;
  latestRun: Awaited<ReturnType<typeof getRun>> | null | undefined;
  endAgents: { key: string; topic: string }[];
  events: EnvelopeOut[];
}) {
  // Build per-agent latest output by filtering events by topic.
  const agentOutputs = useMemo(() => {
    const out = new Map<string, EnvelopeOut>();
    for (const env of events) {
      const match = endAgents.find((a) => a.topic === env.topic);
      if (match && !out.has(match.key)) {
        // First match wins — that's the EARLIEST event for this topic.
        // Walk backwards instead so we keep the LATEST (the most
        // recent emit, in case of multiple).
      }
    }
    // Walk events backwards — pick the LATEST event per agent topic.
    for (let i = events.length - 1; i >= 0; i--) {
      const env = events[i];
      const match = endAgents.find((a) => a.topic === env.topic);
      if (match && !out.has(match.key)) {
        out.set(match.key, env);
      }
    }
    return out;
  }, [events, endAgents]);

  if (endAgents.length === 0) {
    return (
      <p className="text-slate-500 italic">
        No agents are wired to <code>__end__</code> in this workflow.
      </p>
    );
  }

  return (
    <>
      {latestRun && (
        <section>
          <h3 className="text-xs font-semibold text-slate-700 uppercase tracking-wide mb-2">
            Last run
          </h3>
          <div className="text-sm space-y-0.5">
            <div className="font-mono text-xs text-slate-500">{latestRun.run_id}</div>
            <div>Status: <strong>{latestRun.status}</strong></div>
            {latestRun.started_at && (
              <div className="text-xs text-slate-500">
                {new Date(latestRun.started_at).toLocaleString()}
              </div>
            )}
          </div>
        </section>
      )}

      <section>
        <h3 className="text-xs font-semibold text-slate-700 uppercase tracking-wide mb-2">
          Agents wired to End ({endAgents.length})
        </h3>
        <p className="text-xs text-slate-500 mb-3">
          Each card shows the most recent envelope this agent emitted on
          its publish topic — i.e. what it contributed to the workflow's
          final output.
        </p>
        <div className="space-y-3">
          {endAgents.map((a) => {
            const env = agentOutputs.get(a.key);
            const meta = workflow.agents.find((x) => x.template_key === a.key);
            return (
              <div
                key={a.key}
                className="bg-white border border-slate-200 rounded-lg overflow-hidden"
              >
                <div className="px-3 py-2 bg-slate-50 border-b border-slate-200 flex items-baseline justify-between">
                  <span className="font-mono font-semibold text-sm">{a.key}</span>
                  <span className="text-xs text-slate-500">{meta?.role ?? ""}</span>
                </div>
                <div className="px-3 py-2.5">
                  <div className="text-[10px] text-slate-400 mb-1">
                    via <code>{a.topic}</code>
                  </div>
                  {env ? (
                    <pre className="text-xs bg-slate-50 rounded p-2 overflow-x-auto thin-scroll">
                      {JSON.stringify(env.payload, null, 2)}
                    </pre>
                  ) : (
                    <div className="text-xs text-slate-400 italic">
                      No emit recorded for the last run.
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </section>
    </>
  );
}
