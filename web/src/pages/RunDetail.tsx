import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  cancelRun,
  type EnvelopeOut,
  getRun,
  getWorkflow,
  streamRunEvents,
} from "../lib/api";
import { WorkflowGraph } from "../components/WorkflowGraph";
import { StatusBadge } from "../components/StatusBadge";
import { AgentDrawer } from "../components/AgentDrawer";

export default function RunDetailPage() {
  const { id = "" } = useParams();
  const runId = decodeURIComponent(id);
  const qc = useQueryClient();
  const [openAgentKey, setOpenAgentKey] = useState<string | null>(null);

  const { data: run } = useQuery({
    queryKey: ["run", runId],
    queryFn: () => getRun(runId),
    enabled: !!runId,
    refetchInterval: (q) => {
      const status = q.state.data?.status;
      if (status && ["Succeeded", "Failed", "Cancelled"].includes(status)) {
        return false;
      }
      return 1_000;
    },
  });

  const { data: wf } = useQuery({
    queryKey: ["workflow", run?.workflow_id],
    queryFn: () => getWorkflow(run!.workflow_id),
    enabled: !!run?.workflow_id,
  });

  // ---- Live event stream ------------------------------------
  const [events, setEvents] = useState<EnvelopeOut[]>([]);
  const seenIds = useRef<Set<string>>(new Set());

  useEffect(() => {
    if (!runId) return;
    seenIds.current.clear();
    setEvents([]);
    const cleanup = streamRunEvents(runId, (env) => {
      if (seenIds.current.has(env.event_id)) return;
      seenIds.current.add(env.event_id);
      setEvents((prev) => [...prev, env]);
    });
    return cleanup;
  }, [runId]);

  // Compute visited node ids from the event topics (for DAG colouring).
  const visited = useMemo(() => {
    const set = new Set<string>();
    if (!wf) return set;
    // Match each envelope's topic against agents' subscribe/publish.
    for (const env of events) {
      for (const a of wf.agents) {
        if (a.subscribe.includes(env.topic) || a.publish.includes(env.topic)) {
          set.add(a.template_key);
        }
      }
    }
    if (run?.status === "Succeeded") set.add("__end__");
    if (run?.status === "Failed") set.add("__error__");
    return set;
  }, [events, wf, run?.status]);

  // ---- Live-active set ----
  // Any agent that emitted or consumed an envelope in the last 1.5s
  // shows the pulsing halo. Multiple agents can be active at once
  // (fanout / parallel branches). A 200ms tick re-evaluates the
  // window so the halo fades gracefully instead of snapping off.
  const [tick, setTick] = useState(0);
  useEffect(() => {
    if (run?.status !== "Running") return;
    const id = setInterval(() => setTick((t) => t + 1), 200);
    return () => clearInterval(id);
  }, [run?.status]);

  const activeNodes = useMemo(() => {
    const set = new Set<string>();
    if (!wf || run?.status !== "Running") return set;
    const cutoff = Date.now() - 1500;
    // Walk recent events (last 30 is plenty); for each, mark every
    // agent whose subscribe / publish topic matches.
    for (const env of events.slice(-30)) {
      const ts = new Date(env.created_at).getTime();
      if (ts < cutoff) continue;
      for (const a of wf.agents) {
        if (a.subscribe.includes(env.topic) || a.publish.includes(env.topic)) {
          set.add(a.template_key);
        }
      }
    }
    return set;
    // tick is intentional — it forces re-evaluation against the moving cutoff.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [events, wf, run?.status, tick]);

  const cancelMutation = useMutation({
    mutationFn: () => cancelRun(runId),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["run", runId] }),
  });

  // ---- Stable graph props ----
  // These MUST keep stable references across the high-frequency run
  // re-renders (event stream + 200ms active-halo tick). WorkflowGraph's
  // layout memo depends on `descriptions`; a fresh object each render
  // rebuilds node POSITIONS, making React Flow (controlled) flicker to
  // blank — visible as the graph "white-screening" whenever an event
  // (e.g. an agent's terminal `.out`) arrives during a run.
  const graphNodes = useMemo(
    () => (run?.snapshot_nodes?.length ? run.snapshot_nodes : wf?.nodes ?? []),
    [run?.snapshot_nodes, wf?.nodes],
  );
  const graphEdges = useMemo(
    () => (run?.snapshot_edges?.length ? run.snapshot_edges : wf?.edges ?? []),
    [run?.snapshot_edges, wf?.edges],
  );
  const graphDescriptions = useMemo<Record<string, string>>(() => {
    const d: Record<string, string> = {
      __start__: "Workflow entry — Orchestrator publishes the user's input here when a Run starts.",
      __end__:   "Workflow output — any event on a topic wired to __end__ marks the Run as Succeeded.",
      __error__: "Failure sink — auto-injected edges. Only fire when an agent's handler fails after retries.",
    };
    for (const a of wf?.agents ?? [])
      d[a.template_key] = `${a.role}\n${a.description || "(no description)"}`;
    return d;
  }, [wf]);

  if (!run) return <div className="text-slate-500">Loading…</div>;

  const isTerminal = ["Succeeded", "Failed", "Cancelled"].includes(run.status);

  return (
    <div className="space-y-6">
      <div>
        <Link
          to={`/workflows/${encodeURIComponent(run.workflow_id)}`}
          className="text-sm text-slate-500 hover:text-slate-900"
        >
          ← {run.workflow_id}
        </Link>
        <div className="mt-1 flex items-center gap-3">
          <h1 className="text-xl font-mono font-medium">{run.run_id}</h1>
          <StatusBadge status={run.status} />
        </div>
        <div className="mt-1 text-xs text-slate-500 tabular-nums space-x-3">
          <span>workflow: {run.workflow_id}</span>
          <span>·</span>
          <span>trace: <code>{run.trace_id ?? "—"}</code></span>
        </div>
        {run.failure_reason && (
          <p className="mt-2 text-rose-600 text-sm">{run.failure_reason}</p>
        )}
        {!isTerminal && (
          <button
            onClick={() => cancelMutation.mutate()}
            disabled={cancelMutation.isPending}
            className="mt-3 px-3 py-1.5 bg-amber-100 hover:bg-amber-200
                       text-amber-900 text-xs font-medium rounded transition"
          >
            Cancel run
          </button>
        )}
      </div>

      {wf && (
        <section>
          <h2 className="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-2">
            Topology <span className="text-slate-400 font-normal normal-case">— click an agent to inspect</span>
          </h2>
          <WorkflowGraph
            nodes={graphNodes}
            edges={graphEdges}
            visitedNodes={visited}
            activeNodes={activeNodes}
            runStatus={run?.status ?? null}
            descriptions={graphDescriptions}
            onNodeClick={(id) => setOpenAgentKey(id)}
          />
        </section>
      )}

      <section className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Input + (eventual) output */}
        <div>
          <h2 className="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-2">
            Input / Output
          </h2>
          <div className="bg-white border border-slate-200 rounded-lg divide-y divide-slate-100">
            <PayloadBlock title="Input" data={run.input} />
            {run.output && <PayloadBlock title="Output" data={run.output} />}
          </div>
        </div>

        {/* Branch log */}
        <div>
          <h2 className="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-2">
            Branches
          </h2>
          <div className="bg-white border border-slate-200 rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <tbody className="divide-y divide-slate-100">
                {run.branch_log.length === 0 && (
                  <tr>
                    <td className="text-slate-500 italic px-4 py-3">
                      No branches yet.
                    </td>
                  </tr>
                )}
                {run.branch_log.map((b, i) => (
                  <tr key={i} className="hover:bg-slate-50">
                    <td className="px-4 py-2 font-mono text-xs text-slate-500">
                      {b.edge_id}
                    </td>
                    <td className="px-4 py-2 font-mono text-xs">
                      → {b.chosen}
                    </td>
                    <td className="px-4 py-2 text-xs text-slate-500">
                      by <code>{b.by}</code>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      {/* Live event timeline */}
      <section>
        <h2 className="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-2">
          Event Timeline ({events.length + 1 + (isTerminal ? 1 : 0)})
        </h2>
        <div className="bg-white border border-slate-200 rounded-lg divide-y divide-slate-100
                        max-h-[420px] overflow-y-auto thin-scroll">
          {/* Synthetic START row — the run's input. Always shown (and works
              for archived runs whose live event buffer is gone). */}
          {run && (
            <details className="px-4 py-2 group bg-slate-50/60" open>
              <summary className="cursor-pointer flex justify-between items-baseline gap-3 list-none">
                <span className="flex items-baseline gap-2 truncate">
                  <code className="text-xs text-slate-500 tabular-nums">
                    {run.started_at ? new Date(run.started_at).toLocaleTimeString() : ""}
                  </code>
                  <code className="text-xs text-slate-800 font-semibold truncate">
                    ▶ Start · input
                  </code>
                </span>
                <span className="text-xs text-slate-400 group-open:rotate-90 transition-transform">▶</span>
              </summary>
              <pre className="mt-1.5 text-xs bg-white border border-slate-100 rounded p-2 overflow-x-auto thin-scroll">
                {JSON.stringify(run.input ?? {}, null, 2)}
              </pre>
            </details>
          )}
          {events.map((e) => (
            <details key={e.event_id} className="px-4 py-2 group">
              <summary className="cursor-pointer flex justify-between items-baseline gap-3 list-none">
                <span className="flex items-baseline gap-2 truncate">
                  <code className="text-xs text-slate-500 tabular-nums">
                    {new Date(e.created_at).toLocaleTimeString()}
                  </code>
                  <code className="text-xs text-blue-700 font-medium truncate">
                    {e.topic}
                  </code>
                </span>
                <span className="text-xs text-slate-400 group-open:rotate-90 transition-transform">
                  ▶
                </span>
              </summary>
              <pre className="mt-1.5 text-xs bg-slate-50 rounded p-2 overflow-x-auto thin-scroll">
                {JSON.stringify(e.payload, null, 2)}
              </pre>
            </details>
          ))}
          {/* Synthetic END row — the run's output / terminal result. */}
          {run && isTerminal && (
            <details
              className={
                "px-4 py-2 group " +
                (run.status === "Failed" ? "bg-rose-50/60" : "bg-emerald-50/60")
              }
              open
            >
              <summary className="cursor-pointer flex justify-between items-baseline gap-3 list-none">
                <span className="flex items-baseline gap-2 truncate">
                  <code className="text-xs text-slate-500 tabular-nums">
                    {run.ended_at ? new Date(run.ended_at).toLocaleTimeString() : ""}
                  </code>
                  <code className="text-xs text-slate-800 font-semibold truncate">
                    {run.status === "Failed" ? "✗ End · failed" : "■ End · output"}
                  </code>
                </span>
                <span className="text-xs text-slate-400 group-open:rotate-90 transition-transform">▶</span>
              </summary>
              <pre className="mt-1.5 text-xs bg-white border border-slate-100 rounded p-2 overflow-x-auto thin-scroll">
                {JSON.stringify(
                  run.output ?? (run.failure_reason ? { error: run.failure_reason } : {}),
                  null, 2,
                )}
              </pre>
            </details>
          )}
        </div>
      </section>

      {wf && (
        <AgentDrawer
          workflowId={wf.id}
          workflow={wf}
          agentKey={openAgentKey}
          agentMeta={wf.agents.find((a) => a.template_key === openAgentKey)}
          runEvents={events}
          onClose={() => setOpenAgentKey(null)}
        />
      )}
    </div>
  );
}

function PayloadBlock({
  title, data,
}: { title: string; data: Record<string, unknown> }) {
  return (
    <div className="px-4 py-3">
      <div className="text-xs font-medium text-slate-500 mb-1">{title}</div>
      <pre className="text-xs bg-slate-50 rounded p-2 overflow-x-auto thin-scroll">
        {JSON.stringify(data, null, 2)}
      </pre>
    </div>
  );
}
