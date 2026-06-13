import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  cancelRun, createRun, deleteWorkflow, getAgentConfig, getRunEventsSnapshot, getWorkflow,
  listRuns, listWorkflowExternal,
  removeAgent, removeExternalSink, removeExternalSource,
  saveWorkflowGuardrail, setWorkflowMode, streamRunEvents, undoWorkflow,
  type AgentConfig, type AgentSummary, type EnvelopeOut, type RunSummary,
} from "../lib/api";
import { WorkflowGraph } from "../components/WorkflowGraph";
import { StatusBadge } from "../components/StatusBadge";
import { AgentDrawer } from "../components/AgentDrawer";
import { AddAgentModal } from "../components/AddAgentModal";
import { ExternalIOModal } from "../components/ExternalIOModal";
import { StartEndDrawer } from "../components/StartEndDrawer";
import { PromptInputForm } from "../components/PromptInputForm";

export default function WorkflowDetailPage() {
  const { id = "" } = useParams();
  const wfId = decodeURIComponent(id);
  const qc = useQueryClient();
  const navigate = useNavigate();

  const { data: wf, isLoading, error } = useQuery({
    queryKey: ["workflow", wfId],
    queryFn: () => getWorkflow(wfId),
    enabled: !!wfId,
    refetchInterval: 4_000,
  });

  // If the workflow no longer exists (deleted here or elsewhere, or a
  // stale/bookmarked URL), this page is invalid — bounce back to the
  // list instead of rendering a blank screen. Scoped to 404 so a
  // transient network error doesn't kick the user out.
  useEffect(() => {
    if (error && String((error as Error).message).startsWith("404")) {
      navigate("/", { replace: true });
    }
  }, [error, navigate]);

  const { data: runs } = useQuery({
    queryKey: ["runs", { workflow_id: wfId }],
    queryFn: () => listRuns({ workflow_id: wfId, limit: 20 }),
    enabled: !!wfId,
    refetchInterval: 2_000,
  });

  const { data: extIO } = useQuery({
    queryKey: ["external-io", wfId],
    queryFn: () => listWorkflowExternal(wfId),
    enabled: !!wfId,
    refetchInterval: 4_000,
  });

  // ---- Drawer / modal state ----
  const [openAgentKey, setOpenAgentKey] = useState<string | null>(null);
  const [openTerminal, setOpenTerminal] = useState<"start" | "end" | "error" | null>(null);
  const [showAddAgent, setShowAddAgent] = useState(false);
  const [extModal, setExtModal] = useState<
    { dir: "source" | "sink"; editName?: string | null } | null
  >(null);

  // ---- Selection + clipboard for in-graph edit shortcuts ----
  /** Single-click on an agent node selects it for keyboard ops. */
  const [selectedAgentKey, setSelectedAgentKey] = useState<string | null>(null);
  /** Stores a snapshot of the last copied agent (full config). */
  const [clipboard, setClipboard] = useState<{
    meta: AgentSummary;
    config: AgentConfig;
  } | null>(null);
  /** Flag to open the AddAgentModal in duplicate mode after a paste. */
  const [showDuplicateAgent, setShowDuplicateAgent] = useState(false);

  // ---- Live run state — embedded so we don't redirect to /runs/{id} ----
  const [activeRunId, setActiveRunId] = useState<string | null>(null);

  // Auto-lock the Timeline onto the event-driven session run
  // when there is no active manual run.  This is what surfaces
  // ext source/sink trace events to the user without them
  // having to POST /api/runs first.
  // (Declared as a useEffect below where wf is loaded.)
  const [events, setEvents] = useState<EnvelopeOut[]>([]);
  const seenIds = useRef<Set<string>>(new Set());

  // The run summary for the active embedded run (status / timing).
  const { data: activeRun } = useQuery({
    queryKey: ["run", activeRunId],
    queryFn: () => fetch(`/api/runs/${activeRunId}`).then((r) => r.json()),
    enabled: !!activeRunId,
    refetchInterval: (q) => {
      const status = q.state.data?.status;
      if (status && ["Succeeded", "Failed", "Cancelled"].includes(status)) {
        return false;
      }
      return 1_000;
    },
  });

  // Subscribe to SSE for the active run.
  useEffect(() => {
    if (!activeRunId) return;
    seenIds.current.clear();
    setEvents([]);
    const cleanup = streamRunEvents(activeRunId, (env) => {
      if (seenIds.current.has(env.event_id)) return;
      seenIds.current.add(env.event_id);
      setEvents((prev) => [...prev, env]);
    });
    return cleanup;
  }, [activeRunId]);

  // ---- REST fallback poll: belt-and-braces against an SSE proxy
  // (e.g. Vite dev server) buffering chunks. Every 1.5 s while the
  // run is Running, fetch the full event snapshot and merge any
  // missing event_ids into our local state. Stops polling once the
  // run terminates so we don't churn forever. ----
  useEffect(() => {
    if (!activeRunId) return;
    if (activeRun?.status && activeRun.status !== "Running") return;
    let cancelled = false;
    const tick = async () => {
      try {
        const snapshot = await getRunEventsSnapshot(activeRunId);
        if (cancelled) return;
        const missing = snapshot.filter(
          (e) => !seenIds.current.has(e.event_id),
        );
        if (missing.length) {
          for (const e of missing) seenIds.current.add(e.event_id);
          setEvents((prev) => [...prev, ...missing]);
        }
      } catch {
        // ignore — SSE remains the primary source of truth.
      }
    };
    const id = setInterval(tick, 1500);
    tick();
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [activeRunId, activeRun?.status]);

  // ---- Form state lifted up so the toolbar Run button can trigger ----
  const [formInput, setFormInput] = useState<Record<string, unknown> | null>(null);

  const runMutation = useMutation({
    mutationFn: (input: Record<string, unknown>) => createRun(wfId, input),
    onSuccess: async (run: RunSummary) => {
      await qc.refetchQueries({ queryKey: ["runs", { workflow_id: wfId }] });
      setActiveRunId(run.run_id);
    },
  });

  useEffect(() => {
    if (!wf) return;
    if (wf.mode === "event_driven" && wf.session_run_id) {
      // Switch only if we're not already showing a run, OR
      // we're showing a stale (no longer active) one.
      if (activeRunId !== wf.session_run_id) {
        setActiveRunId(wf.session_run_id);
      }
    }
    if (wf.mode === "normal" && activeRunId === wf.session_run_id) {
      // Session ended — clear the lock.
      setActiveRunId(null);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [wf?.mode, wf?.session_run_id]);

  const cancelMutation = useMutation({
    mutationFn: (run_id: string) => cancelRun(run_id),
    onSuccess: async () => {
      await qc.refetchQueries({ queryKey: ["runs", { workflow_id: wfId }] });
    },
  });

  const deleteWfMutation = useMutation({
    mutationFn: () => deleteWorkflow(wfId),
    onSuccess: () => {
      // The detail URL is now invalid — leave it IMMEDIATELY. `replace`
      // so the back button doesn't return to the deleted workflow.
      // Do cache cleanup in the background: never block (or risk losing)
      // the redirect behind an awaited refetch — and drop this workflow's
      // own polling query so it stops 404-ing after deletion.
      navigate("/", { replace: true });
      qc.removeQueries({ queryKey: ["workflow", wfId] });
      qc.invalidateQueries({ queryKey: ["workflows"] });
    },
    onError: (e) => {
      alert(`Failed to delete workflow: ${(e as Error).message}`);
    },
  });

  const removeAgentMutation = useMutation({
    mutationFn: (key: string) => removeAgent(wfId, key),
    onSuccess: async (_summary, key) => {
      // Force an immediate refetch of workflow detail (and the
      // top-level list, which shows agent counts). ``invalidateQueries``
      // marks stale but defers the refetch until the next render
      // tick — under React Strict Mode + refetchInterval that can
      // visibly delay the topology update until something else
      // triggers a refetch (e.g. adding a new agent). ``refetchQueries``
      // forces it immediately.
      await Promise.all([
        qc.refetchQueries({ queryKey: ["workflow", wfId] }),
        qc.refetchQueries({ queryKey: ["workflows"] }),
      ]);
      // If the drawer was open for the agent we just removed, close it.
      if (openAgentKey === key) {
        setOpenAgentKey(null);
      }
      if (selectedAgentKey === key) {
        setSelectedAgentKey(null);
      }
    },
    onError: (e) => {
      // Surface the failure — otherwise the user sees nothing happen.
      alert(`Failed to remove agent: ${(e as Error).message}`);
    },
  });

  /** Remove an external source by name (Delete key on selected ext node). */
  const removeExtSourceMutation = useMutation({
    mutationFn: (name: string) => removeExternalSource(wfId, name),
    onSuccess: () => {
      qc.refetchQueries({ queryKey: ["external-io", wfId] });
      qc.refetchQueries({ queryKey: ["workflow", wfId] });
      setSelectedAgentKey(null);
    },
    onError: (e) => alert(`Remove failed: ${(e as Error).message}`),
  });
  const removeExtSinkMutation = useMutation({
    mutationFn: (name: string) => removeExternalSink(wfId, name),
    onSuccess: () => {
      qc.refetchQueries({ queryKey: ["external-io", wfId] });
      qc.refetchQueries({ queryKey: ["workflow", wfId] });
      setSelectedAgentKey(null);
    },
    onError: (e) => alert(`Remove failed: ${(e as Error).message}`),
  });

  /** Undo the last topology mutation (add / remove / update). */
  const undoMutation = useMutation({
    mutationFn: () => undoWorkflow(wfId),
    onSuccess: async () => {
      await qc.refetchQueries({ queryKey: ["workflow", wfId] });
      // Clear stale selection — the agent we had selected may not
      // exist in the restored snapshot.
      setSelectedAgentKey(null);
    },
    onError: (e) => {
      alert(`Undo failed: ${(e as Error).message}`);
    },
  });

  const modeMutation = useMutation({
    mutationFn: (mode: "normal" | "event_driven") => setWorkflowMode(wfId, mode),
    onSuccess: () => qc.refetchQueries({ queryKey: ["workflow", wfId] }),
    onError: (e) => alert(`Mode switch failed: ${(e as Error).message}`),
  });

  // ---- Keyboard shortcuts (graph-level) ----
  useEffect(() => {
    const handler = async (e: KeyboardEvent) => {
      // Don't intercept while typing inside a form / contenteditable.
      const t = e.target as HTMLElement | null;
      if (t && (
        t.tagName === "INPUT" || t.tagName === "TEXTAREA" ||
        t.tagName === "SELECT" || t.isContentEditable
      )) return;

      const cmd = e.metaKey || e.ctrlKey;

      // Cmd+Z = undo (works without a selection too).
      if (cmd && e.key.toLowerCase() === "z" && !e.shiftKey) {
        e.preventDefault();
        if ((wf?.undo_depth ?? 0) > 0) undoMutation.mutate();
        return;
      }

      if (e.key === "Escape") {
        setSelectedAgentKey(null);
        return;
      }

      // The remaining shortcuts need a selected node.
      if (!selectedAgentKey || !wf) return;

      if (e.key === "Delete" || e.key === "Backspace") {
        e.preventDefault();
        // ext source / sink — virtual nodes, but they support delete too.
        if (selectedAgentKey.startsWith("__ext_source__")) {
          const name = selectedAgentKey.slice("__ext_source__".length);
          if (wf.mode !== "event_driven") {
            alert("Switch to event_driven mode first to remove external sources.");
            return;
          }
          if (confirm(`Remove external source "${name}"?`)) {
            removeExtSourceMutation.mutate(name);
          }
          return;
        }
        if (selectedAgentKey.startsWith("__ext_sink__")) {
          const name = selectedAgentKey.slice("__ext_sink__".length);
          if (wf.mode !== "event_driven") {
            alert("Switch to event_driven mode first to remove external sinks.");
            return;
          }
          if (confirm(`Remove external sink "${name}"?`)) {
            removeExtSinkMutation.mutate(name);
          }
          return;
        }
        // Regular agent.
        if (confirm(`Remove agent ${selectedAgentKey}?`)) {
          removeAgentMutation.mutate(selectedAgentKey);
        }
      } else if (cmd && e.key.toLowerCase() === "c") {
        e.preventDefault();
        const meta = wf.agents.find((a) => a.template_key === selectedAgentKey);
        if (!meta) return;
        try {
          const cfg = await getAgentConfig(wfId, selectedAgentKey);
          setClipboard({ meta, config: cfg });
          // Optional: small in-page hint (we use a transient title flash).
          flashClipboardToast(`Copied ${selectedAgentKey}`);
        } catch (err) {
          alert(`Copy failed: ${(err as Error).message}`);
        }
      } else if (cmd && e.key.toLowerCase() === "v") {
        e.preventDefault();
        if (clipboard) {
          setShowDuplicateAgent(true);
        }
      }
    };

    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedAgentKey, wf?.undo_depth, clipboard, wfId]);

  // ---- Derived: visited / active node sets, like RunDetail ----
  // CRITICAL: these feed WorkflowGraph's `rfNodes` memo. We compute a
  // stable CONTENT KEY first and only build a new Set when the membership
  // actually changes — otherwise every 200ms tick / event handed React
  // Flow a brand-new node array, which (controlled mode) resets node
  // measurement to 0×0 → invisible nodes → the event-driven blank.
  const visitedKey = useMemo(() => {
    if (!wf) return "";
    const set = new Set<string>();
    for (const env of events) {
      for (const a of wf.agents) {
        if (a.subscribe.includes(env.topic) || a.publish.includes(env.topic)) {
          set.add(a.template_key);
        }
      }
    }
    if (activeRun?.status === "Succeeded") set.add("__end__");
    if (activeRun?.status === "Failed") set.add("__error__");
    return [...set].sort().join("|");
  }, [events, wf, activeRun?.status]);
  const visited = useMemo(
    () => new Set(visitedKey ? visitedKey.split("|") : []),
    [visitedKey],
  );

  // 200 ms tick so the active-node halo fades smoothly as the
  // 1.5 s window slides forward.
  const [tick, setTick] = useState(0);
  useEffect(() => {
    if (activeRun?.status !== "Running") return;
    const id = setInterval(() => setTick((t) => t + 1), 200);
    return () => clearInterval(id);
  }, [activeRun?.status]);

  const activeKey = useMemo(() => {
    if (!wf || activeRun?.status !== "Running") return "";
    const set = new Set<string>();
    const cutoff = Date.now() - 1500;
    for (const env of events.slice(-30)) {
      const ts = new Date(env.created_at).getTime();
      if (ts < cutoff) continue;
      for (const a of wf.agents) {
        if (a.subscribe.includes(env.topic) || a.publish.includes(env.topic)) {
          set.add(a.template_key);
        }
      }
    }
    return [...set].sort().join("|");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [events, wf, activeRun?.status, tick]);
  const activeNodes = useMemo(
    () => new Set(activeKey ? activeKey.split("|") : []),
    [activeKey],
  );

  // ---- Stable graph props (memoized so the graph layout isn't rebuilt
  //       on every render / highlight tick — see WorkflowGraph). ----
  // All graph inputs are memoized on the TOPOLOGY identity (`ir_hash`),
  // NOT on the `wf` object. The workflow query refetches every 4s and
  // immediately on a mode switch, returning new `nodes`/`edges` array refs
  // even when the topology is unchanged. Keying on `ir_hash` keeps these
  // references stable across those refetches so WorkflowGraph's `layout`
  // memo doesn't rebuild — otherwise React Flow (controlled) is handed a
  // brand-new node array and blanks the canvas. That blank was racing the
  // fitView fired by the runStatus change on the event-driven switch,
  // hence the *probabilistic* white screen.
  const topoKey = wf?.ir_hash ?? `${wf?.id ?? ""}:${wf?.version ?? ""}`;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const graphNodes = useMemo(() => wf?.nodes ?? [], [topoKey]);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const graphEdges = useMemo(() => wf?.edges ?? [], [topoKey]);
  const agentsBySubscribeTopic = useMemo(() => {
    const m: Record<string, string[]> = {};
    for (const a of wf?.agents ?? [])
      for (const t of a.subscribe) (m[t] = m[t] || []).push(a.template_key);
    return m;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [topoKey]);
  const agentsByPublishTopic = useMemo(() => {
    const m: Record<string, string[]> = {};
    for (const a of wf?.agents ?? [])
      for (const t of a.publish) (m[t] = m[t] || []).push(a.template_key);
    return m;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [topoKey]);
  const nodeDescriptions = useMemo<Record<string, string>>(() => ({
    __start__: "Workflow entry — Orchestrator publishes the user's input here when a Run starts. Click to see the input contract.",
    __end__:   "Workflow output — any event on a topic wired to __end__ marks the Run as Succeeded. Click to see last run's outputs.",
    __error__: "Failure sink — every agent has an auto-injected edge here. The dashed gray lines are NOT the normal flow; they only fire when the agent's handler fails after retries.",
    ...Object.fromEntries(
      (wf?.agents ?? []).map((a) => [
        a.template_key,
        `${a.role}\n${a.description || "(no description)"}`,
      ]),
    ),
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [topoKey]);
  // External I/O comes from a separate polling query — stabilize on its
  // content too so an ext-io refetch doesn't rebuild the layout either.
  const extSrcKey = JSON.stringify(extIO?.sources ?? []);
  const extSinkKey = JSON.stringify(extIO?.sinks ?? []);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const graphExtSources = useMemo(() => extIO?.sources ?? [], [extSrcKey]);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const graphExtSinks = useMemo(() => extIO?.sinks ?? [], [extSinkKey]);

  // ---- Handlers ----
  const triggerRunWith = (input: Record<string, unknown>) => {
    setFormInput(input);
    runMutation.mutate(input);
  };

  /** Toolbar Run button — uses the form's current values, falls back
      to a sensible default if the user hasn't touched the form yet. */
  const triggerRunFromToolbar = () => {
    const input = formInput ?? { q: "" };
    runMutation.mutate(input);
  };

  if (isLoading) return <div className="text-slate-500">Loading…</div>;
  // Only hard-fail when there is NO cached data to show. A *background*
  // refetch (the 4s poll, or the mode-toggle's refetchQueries) can fail
  // transiently — e.g. while the server is busy tearing down an
  // event-driven session on stop — and in TanStack Query v5 `error`
  // stays populated even though the previous good `wf` is retained.
  // Blanking the whole page (graph included) on such a hiccup is the
  // bug behind "the graph disappears with no console error". Keep
  // rendering the cached `wf` (stale-while-revalidate); a small banner
  // below signals the transient failure.
  if (error && !wf)
    return <div className="text-rose-600">Failed: {(error as Error).message}</div>;
  if (!wf) return null;

  // Discover the inferred prompt template for the FIRST agent so the
  // PromptInputForm can render input fields. We pull it from the
  // agent's config endpoint asynchronously — for the toolbar default
  // we fall back to {"q": ""}.
  const firstAgent = wf.agents[0];

  const isRunning = activeRun?.status === "Running";

  // Static prompt-field validation (from the backend). Non-empty ⇒ a
  // prompt references a payload.<field> nothing can produce → not runnable.
  const promptErrors = wf.prompt_field_errors ?? [];
  const runnable = promptErrors.length === 0;

  return (
    <div className="space-y-6">
      {promptErrors.length > 0 && (
        <div className="rounded-lg border border-rose-300 bg-rose-50 px-4 py-3">
          <div className="text-sm font-semibold text-rose-800">
            ⚠ This workflow can't run — {promptErrors.length} unresolved prompt field
            {promptErrors.length > 1 ? "s" : ""}
          </div>
          <ul className="mt-1.5 space-y-1 text-xs text-rose-700 list-disc list-inside">
            {promptErrors.map((e, i) => (
              <li key={i}>
                <code className="font-mono">{e.agent}</code> references{" "}
                <code className="font-mono">payload.{e.field}</code> — no start-input
                field or upstream output produces it.
              </li>
            ))}
          </ul>
          <div className="mt-1.5 text-[11px] text-rose-600">
            Fix the prompt (use an existing field) or add the field to the input schema, then Run is re-enabled.
          </div>
        </div>
      )}
      {/* Non-blocking notice: a background refetch failed but we're
          still showing the last-known-good workflow. Does NOT replace
          the graph (that was the disappearing-graph bug). */}
      {error && (
        <div className="text-xs text-amber-700 bg-amber-50 border border-amber-200
                        rounded px-2 py-1">
          ⚠ 刷新暂时失败，正在重试，显示的是最近一次数据…
        </div>
      )}
      <div>
        <Link to="/" className="text-sm text-slate-500 hover:text-slate-900">
          ← Workflows
        </Link>
        <div className="mt-1 flex items-baseline justify-between gap-3">
          <h1 className="text-2xl font-mono font-semibold">{wf.id}</h1>
          <button
            onClick={() => {
              if (confirm(`Delete workflow ${wf.id}? This is permanent.`)) {
                deleteWfMutation.mutate();
              }
            }}
            disabled={deleteWfMutation.isPending}
            className="text-xs px-2 py-1 rounded text-rose-600 hover:bg-rose-50"
          >
            Delete workflow
          </button>
        </div>
        {wf.description && (
          <p className="mt-1 text-slate-600">{wf.description}</p>
        )}
        <div className="mt-1 text-xs text-slate-500 tabular-nums">
          v{wf.version} · ir_hash <code>{wf.ir_hash}</code> ·{" "}
          {wf.agents.length} agents · {wf.edges.length} edges · project{" "}
          <code>{wf.project_id}</code>
        </div>
      </div>

      {/* ============================================================
           Topology — header tools + LIVE graph (visited / active)
         ============================================================ */}
      <section>
        <div className="flex items-center justify-between mb-2">
          <h2 className="text-sm font-semibold text-slate-700 uppercase tracking-wide flex items-center gap-2">
            Topology
            {wf?.mode === "event_driven" && (
              <span className="normal-case font-normal text-[11px] bg-violet-100 text-violet-700
                               border border-violet-200 px-1.5 py-0.5 rounded inline-flex items-center gap-1">
                <span className="w-1.5 h-1.5 rounded-full bg-violet-500 animate-pulse" />
                event-driven
              </span>
            )}
            <span className="text-slate-400 font-normal normal-case ml-1">
              — click any node to inspect
              {activeRunId && (
                <span className="ml-2 text-xs">
                  · live run{" "}
                  <Link
                    to={`/runs/${encodeURIComponent(activeRunId)}`}
                    className="text-blue-600 hover:underline font-mono"
                  >
                    {activeRunId.slice(0, 14)}…
                  </Link>{" "}
                  {activeRun && <StatusBadge status={activeRun.status} />}
                </span>
              )}
            </span>
          </h2>
          <div className="flex items-center gap-2">
            <button
              onClick={() => undoMutation.mutate()}
              disabled={
                (wf?.undo_depth ?? 0) === 0 || undoMutation.isPending
              }
              className="text-xs px-2.5 py-1 rounded inline-flex items-center gap-1
                         bg-slate-100 hover:bg-slate-200 text-slate-700
                         disabled:opacity-40 disabled:cursor-not-allowed"
              title={
                (wf?.undo_depth ?? 0) === 0
                  ? "Nothing to undo"
                  : `Undo last topology change (${wf?.undo_depth} in stack) — Cmd+Z`
              }
            >
              ↶ Undo
              {(wf?.undo_depth ?? 0) > 0 && (
                <span className="text-[10px] text-slate-500">
                  ({wf?.undo_depth})
                </span>
              )}
            </button>
            <button
              onClick={() => setShowAddAgent(true)}
              className="text-xs px-2.5 py-1 bg-emerald-600 hover:bg-emerald-700
                         text-white rounded inline-flex items-center gap-1"
              title="Add a new Agent to this workflow"
            >
              + Agent
            </button>
            <button
              onClick={() => setExtModal({ dir: "source" })}
              className={
                "text-xs px-2.5 py-1 rounded inline-flex items-center gap-1 " +
                (wf?.mode === "event_driven"
                  ? "bg-violet-600 hover:bg-violet-700 text-white"
                  : "bg-slate-200 hover:bg-slate-300 text-slate-600")
              }
              title={
                wf?.mode === "event_driven"
                  ? "Listen to an external system and inject events"
                  : "View / disabled — switch to event-driven mode to edit"
              }
            >
              + External Source
            </button>
            <button
              onClick={() => setExtModal({ dir: "sink" })}
              className={
                "text-xs px-2.5 py-1 rounded inline-flex items-center gap-1 " +
                (wf?.mode === "event_driven"
                  ? "bg-amber-600 hover:bg-amber-700 text-white"
                  : "bg-slate-200 hover:bg-slate-300 text-slate-600")
              }
              title={
                wf?.mode === "event_driven"
                  ? "Push agent output to an external system"
                  : "View / disabled — switch to event-driven mode to edit"
              }
            >
              + External Sink
            </button>

            {/* Run / Cancel / Listen toolbar — mode-aware */}
            {wf?.mode === "event_driven" ? (
              <>
                <span
                  className="text-xs px-2 py-1 bg-violet-100 text-violet-700
                             rounded inline-flex items-center gap-1"
                  title="External events automatically spawn Runs while listening"
                >
                  📡 Listening · {extIO?.sources.length ?? 0} src · {extIO?.sinks.length ?? 0} sinks
                </span>
                <button
                  onClick={() => modeMutation.mutate("normal")}
                  disabled={modeMutation.isPending}
                  className="text-xs px-2 py-1 bg-rose-100 hover:bg-rose-200
                             disabled:opacity-40 disabled:cursor-not-allowed
                             text-rose-700 rounded inline-flex items-center gap-1"
                  title="Stop listening — sources/sinks pause but their configuration is preserved"
                >
                  ■ Stop listening
                </button>
              </>
            ) : isRunning ? (
              <button
                onClick={() => cancelMutation.mutate(activeRunId!)}
                disabled={cancelMutation.isPending}
                className="text-xs px-2 py-1 bg-rose-100 hover:bg-rose-200
                           text-rose-700 rounded inline-flex items-center gap-1"
                title="Cancel the in-flight Run"
              >
                ■ Cancel
              </button>
            ) : (
              <>
                <button
                  onClick={triggerRunFromToolbar}
                  disabled={runMutation.isPending || !runnable}
                  className="text-xs px-2 py-1 bg-blue-600 hover:bg-blue-700
                             disabled:bg-blue-400 disabled:cursor-not-allowed
                             text-white rounded inline-flex items-center gap-1"
                  title={
                    runnable
                      ? "Trigger a Run with the current form input"
                      : "Disabled — fix the unresolved prompt field(s) above first"
                  }
                >
                  ▶ Run
                </button>
                <button
                  onClick={() => modeMutation.mutate("event_driven")}
                  disabled={modeMutation.isPending}
                  className="text-xs px-2 py-1 bg-violet-600 hover:bg-violet-700
                             text-white rounded inline-flex items-center gap-1"
                  title="Switch to event-driven mode — the workflow will continuously listen for external events instead of running once"
                >
                  📡 Event-driven
                </button>
              </>
            )}
            <button
              disabled
              title="Pause is not yet implemented (runtime support pending)"
              className="text-xs px-2 py-1 bg-slate-100 text-slate-400 rounded
                         inline-flex items-center gap-1 cursor-not-allowed"
            >
              ⏸ Pause
            </button>
          </div>
        </div>

        <WorkflowGraph
          nodes={graphNodes}
          edges={graphEdges}
          visitedNodes={visited}
          activeNodes={activeNodes}
          externalSources={graphExtSources}
          externalSinks={graphExtSinks}
          agentsBySubscribeTopic={agentsBySubscribeTopic}
          agentsByPublishTopic={agentsByPublishTopic}
          runStatus={activeRun?.status ?? null}
          selectedNodeId={selectedAgentKey}
          descriptions={nodeDescriptions}
          onNodeClick={(id) => {
            // Single click = select. Works for agents AND ext nodes.
            setSelectedAgentKey(id);
          }}
          onNodeDoubleClick={(id) => {
            // Double click = open editor.
            if (id.startsWith("__ext_source__")) {
              setExtModal({ dir: "source", editName: id.slice("__ext_source__".length) });
              return;
            }
            if (id.startsWith("__ext_sink__")) {
              setExtModal({ dir: "sink", editName: id.slice("__ext_sink__".length) });
              return;
            }
            setOpenAgentKey(id);
          }}
          onTerminalClick={(kind) =>
            kind === "error" ? null : setOpenTerminal(kind)}
        />

        {/* Selection / clipboard status hint. */}
        <div className="mt-2 text-[11px] text-slate-500 flex items-center gap-3 flex-wrap">
          {selectedAgentKey ? (
            <span className="text-indigo-700">
              ● Selected: <code className="font-mono">{selectedAgentKey}</code>
              <span className="text-slate-500">
                {" "}— <kbd className="kbd-tiny">Delete</kbd> remove ·{" "}
                <kbd className="kbd-tiny">⌘C</kbd> copy ·{" "}
                <kbd className="kbd-tiny">Esc</kbd> deselect ·{" "}
                double-click to edit
              </span>
            </span>
          ) : (
            <span>
              Click to select · double-click to edit ·{" "}
              <kbd className="kbd-tiny">⌘Z</kbd> undo
            </span>
          )}
          {clipboard && (
            <span className="text-emerald-700">
              📋 Clipboard:{" "}
              <code className="font-mono">{clipboard.meta.template_key}</code>
              {" "}— <kbd className="kbd-tiny">⌘V</kbd> paste copy
            </span>
          )}
        </div>
      </section>

      {/* ============================================================
           Live Event Timeline (only when a run is active / recent)
         ============================================================ */}
      {activeRunId && (
        <section>
          <div className="flex items-center justify-between mb-2">
            <h2 className="text-sm font-semibold text-slate-700 uppercase tracking-wide">
              Live Events ({events.length})
            </h2>
            <Link
              to={`/runs/${encodeURIComponent(activeRunId)}`}
              className="text-xs text-blue-600 hover:underline"
            >
              View full run page →
            </Link>
          </div>
          <div className="bg-white border border-slate-200 rounded-lg
                          divide-y divide-slate-100 max-h-[280px]
                          overflow-y-auto thin-scroll">
            {events.length === 0 && (
              <div className="text-slate-500 italic px-4 py-3 text-sm">
                Waiting for events…
              </div>
            )}
            {events.map((e) => {
              const isExtSrc = e.topic.startsWith("system.ext.source.");
              const isExtSink = e.topic.startsWith("system.ext.sink.");
              const p = (e.payload as Record<string, unknown>) || {};
              const ok = isExtSink ? !!p.ok : true;
              const badge =
                isExtSrc ? "🔌 source"
                : isExtSink ? (ok ? "📤 sink ok" : "📤 sink ✗")
                : null;
              return (
                <details key={e.event_id} className="px-4 py-2 group">
                  <summary className="cursor-pointer flex justify-between items-baseline gap-3 list-none">
                    <span className="flex items-baseline gap-2 truncate">
                      <code className="text-xs text-slate-500 tabular-nums">
                        {new Date(e.created_at).toLocaleTimeString()}
                      </code>
                      {badge && (
                        <span className={
                          "text-[10px] px-1.5 py-0.5 rounded font-medium border " +
                          (isExtSrc
                            ? "bg-violet-50 text-violet-700 border-violet-200"
                            : ok
                              ? "bg-emerald-50 text-emerald-700 border-emerald-200"
                              : "bg-rose-50 text-rose-700 border-rose-200")
                        }>{badge}</span>
                      )}
                      <code className={
                        "text-xs font-medium truncate " +
                        (isExtSrc ? "text-violet-700"
                          : isExtSink ? (ok ? "text-emerald-700" : "text-rose-700")
                          : "text-blue-700")
                      }>
                        {e.topic}
                      </code>
                      {(isExtSrc || isExtSink) && p.preview ? (
                        <span className="text-[11px] text-slate-500 truncate italic">
                          — {String(p.preview).slice(0, 80)}
                        </span>
                      ) : null}
                    </span>
                    <span className="text-xs text-slate-400 group-open:rotate-90 transition-transform">
                      ▶
                    </span>
                  </summary>
                  <pre className="mt-1.5 text-xs bg-slate-50 rounded p-2 overflow-x-auto thin-scroll">
                    {JSON.stringify(e.payload, null, 2)}
                  </pre>
                </details>
              );
            })}
          </div>
        </section>
      )}

      {/* ============================================================
           Run trigger form + Agents list
         ============================================================ */}
      <section className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div>
          <h2 className="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-2">
            Trigger Run
          </h2>
          <PromptInputForm
            promptTemplate={
              wf.start_input_fields
                .map((f) => `{{ payload.${f} }}`)
                .join(" ")
            }
            firstAgentKey={firstAgent?.template_key}
            workflowId={wf.id}
            savedFields={wf.start_input_fields}
            workflowAgents={wf.agents}
            onSubmit={triggerRunWith}
            onValuesChange={setFormInput}
            isSubmitting={runMutation.isPending}
            runDisabled={!runnable}
            runDisabledReason="Fix the unresolved prompt field(s) shown above first"
          />
          <p className="text-[10px] text-slate-500 mt-1.5">
            Tip: the toolbar <code>▶ Run</code> button uses these values too.
          </p>
        </div>

        <div>
          <h2 className="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-2">
            Agents
          </h2>
          <div className="bg-white border border-slate-200 rounded-lg divide-y
                          divide-slate-100 max-h-[280px] overflow-y-auto thin-scroll">
            {wf.agents.map((a) => (
              <div
                key={a.template_key}
                className="px-4 py-2.5 group flex items-center justify-between"
              >
                <div className="flex-1 min-w-0">
                  <div className="flex justify-between items-baseline">
                    <button
                      onClick={() => setOpenAgentKey(a.template_key)}
                      className="font-mono font-medium text-blue-700 hover:underline truncate"
                    >
                      {a.template_key}
                    </button>
                    <span className="text-xs text-slate-500">{a.role}</span>
                  </div>
                  <div className="text-xs text-slate-500 truncate">
                    {a.description || "(no description)"}
                  </div>
                </div>
                <button
                  onClick={() => {
                    if (confirm(`Remove agent ${a.template_key}?`)) {
                      removeAgentMutation.mutate(a.template_key);
                    }
                  }}
                  className="opacity-0 group-hover:opacity-100 ml-2 text-xs
                             text-rose-600 hover:bg-rose-50 px-2 py-1 rounded"
                >
                  Remove
                </button>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ============================================================
           Workflow-level Run Quota Limits
         ============================================================ */}
      <WorkflowQuotaPanel wf={wf} />

      {/* ============================================================
           Recent Runs table
         ============================================================ */}
      <section>
        <h2 className="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-2">
          Recent Runs
        </h2>
        <div className="bg-white border border-slate-200 rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-slate-50 text-slate-600 text-xs uppercase tracking-wide">
              <tr>
                <th className="text-left px-4 py-2 font-medium">Run</th>
                <th className="text-left px-4 py-2 font-medium">Status</th>
                <th className="text-left px-4 py-2 font-medium">Started</th>
                <th className="text-left px-4 py-2 font-medium">Ended</th>
                <th className="text-left px-4 py-2 font-medium">Export</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {runs?.length === 0 && (
                <tr>
                  <td colSpan={5} className="text-center text-slate-500 py-6 italic">
                    No runs yet — trigger one above.
                  </td>
                </tr>
              )}
              {runs?.map((r) => {
                const live =
                  r.status === "Running" || r.status === "Pending";
                return (
                <tr key={r.run_id} className="hover:bg-slate-50">
                  <td className="px-4 py-2 font-mono text-xs">
                    <Link
                      to={`/runs/${encodeURIComponent(r.run_id)}`}
                      className="text-blue-600 hover:underline"
                    >
                      {r.run_id.slice(0, 24)}…
                    </Link>
                  </td>
                  <td className="px-4 py-2"><StatusBadge status={r.status} /></td>
                  <td className="px-4 py-2 text-slate-600 text-xs tabular-nums">
                    {r.started_at ? new Date(r.started_at).toLocaleTimeString() : "—"}
                  </td>
                  <td className="px-4 py-2 text-slate-600 text-xs tabular-nums">
                    {r.ended_at ? new Date(r.ended_at).toLocaleTimeString() : "—"}
                  </td>
                  <td className="px-4 py-2 text-xs">
                    <div className="flex items-center gap-1">
                      <button
                        type="button"
                        onClick={() => downloadRunExport(r.run_id, "json")}
                        disabled={live}
                        title={
                          live
                            ? "Wait until the run finishes to export"
                            : "Download run + all events as JSON"
                        }
                        className="px-2 py-0.5 bg-slate-100 hover:bg-slate-200
                                   text-slate-700 rounded
                                   disabled:opacity-40 disabled:cursor-not-allowed"
                      >
                        ↓ JSON
                      </button>
                      <button
                        type="button"
                        onClick={() => downloadRunExport(r.run_id, "md")}
                        disabled={live}
                        title={
                          live
                            ? "Wait until the run finishes to export"
                            : "Download run as a Markdown report"
                        }
                        className="px-2 py-0.5 bg-slate-100 hover:bg-slate-200
                                   text-slate-700 rounded
                                   disabled:opacity-40 disabled:cursor-not-allowed"
                      >
                        ↓ MD
                      </button>
                    </div>
                  </td>
                </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>

      {/* ============================================================
           Drawers + Modals
         ============================================================ */}
      <AgentDrawer
        workflowId={wfId}
        workflow={wf}
        agentKey={openAgentKey}
        agentMeta={wf.agents.find((a) => a.template_key === openAgentKey)}
        runEvents={events}
        onClose={() => setOpenAgentKey(null)}
      />
      <StartEndDrawer
        workflow={wf}
        kind={
          openTerminal === "start" || openTerminal === "end"
            ? openTerminal
            : null
        }
        runEvents={events}
        onClose={() => setOpenTerminal(null)}
      />
      {extModal && (
        <ExternalIOModal
          workflowId={wfId}
          open={true}
          direction={extModal.dir}
          mode={wf?.mode ?? "normal"}
          agentPublishTopics={
            wf ? Array.from(new Set(wf.agents.flatMap((a) => a.publish))).sort() : []
          }
          agentOutputFieldByPublishTopic={
            wf
              ? Object.fromEntries(
                  wf.agents.flatMap((a) =>
                    a.output_field
                      ? a.publish.map((t) => [t, a.output_field!])
                      : [],
                  ),
                )
              : {}
          }
          agentKeyByPublishTopic={
            wf
              ? Object.fromEntries(
                  wf.agents.flatMap((a) =>
                    a.publish.map((t) => [t, a.template_key]),
                  ),
                )
              : {}
          }
          prefilledEditName={extModal.editName ?? null}
          onClose={() => setExtModal(null)}
        />
      )}
      <AddAgentModal
        workflow={wf}
        open={showAddAgent}
        onClose={() => setShowAddAgent(false)}
      />
      {/* Duplicate-from-clipboard modal — opened by Cmd+V after Cmd+C. */}
      {clipboard && (
        <AddAgentModal
          workflow={wf}
          open={showDuplicateAgent}
          onClose={() => setShowDuplicateAgent(false)}
          mode="duplicate"
          existing={{
            meta: clipboard.meta,
            prompt: clipboard.config.prompt,
            system_prompt: clipboard.config.system_prompt,
            output_field: clipboard.config.output_field,
            max_retries: clipboard.config.max_retries,
          }}
        />
      )}
    </div>
  );
}

/** Show a brief in-page toast indicating something was copied. We
 *  use a transient title attribute on the document for low-effort
 *  feedback — replace with a proper toast component later. */
function flashClipboardToast(msg: string) {
  // Lightweight: write to document.title for 1.2s, then restore.
  const original = document.title;
  document.title = `📋 ${msg}`;
  setTimeout(() => { document.title = original; }, 1200);
}

/** Trigger a browser download for the run's export endpoint. We use
 *  a programmatic anchor click so the browser respects the backend's
 *  ``Content-Disposition: attachment`` header and saves the file
 *  with the suggested name (``run_<id>.<ext>``). */
function downloadRunExport(runId: string, format: "json" | "md") {
  const url =
    `/api/runs/${encodeURIComponent(runId)}/export?format=${format}`;
  const a = document.createElement("a");
  a.href = url;
  // Hint the filename in case the response headers are stripped by
  // some intermediate proxy. The backend's Content-Disposition takes
  // precedence when present.
  a.download = `run_${runId}.${format}`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}
/* -----------------------------------------------------------------------
   WorkflowQuotaPanel — workflow-level run quota caps editor.
   Inline in WorkflowDetail.tsx to keep the page self-contained.
   ----------------------------------------------------------------------- */
function WorkflowQuotaPanel({ wf }: { wf: import("../lib/api").WorkflowDetail }) {
  const qc = useQueryClient();
  const [open, setOpen] = useState(false);

  const saved = wf.workflow_guardrail;
  const [maxTokens, setMaxTokens] = useState<number>(saved?.max_total_tokens ?? 200_000);
  const [maxCycles, setMaxCycles] = useState<number>(saved?.max_cycles_per_run ?? 200);

  // Keep form in sync if the server value changes (e.g. after save).
  useEffect(() => {
    setMaxTokens(saved?.max_total_tokens ?? 200_000);
    setMaxCycles(saved?.max_cycles_per_run ?? 200);
  }, [saved?.max_total_tokens, saved?.max_cycles_per_run]);

  const mutation = useMutation({
    mutationFn: () =>
      saveWorkflowGuardrail(wf.id, {
        max_total_tokens: maxTokens,
        max_cycles_per_run: maxCycles,
      }),
    onSuccess: () =>
      qc.refetchQueries({ queryKey: ["workflow", wf.id] }),
  });

  const clearMutation = useMutation({
    mutationFn: () =>
      saveWorkflowGuardrail(wf.id, {
        max_total_tokens: null,
        max_cycles_per_run: null,
      }),
    onSuccess: () => {
      qc.refetchQueries({ queryKey: ["workflow", wf.id] });
    },
  });

  const isDefault =
    !saved ||
    (saved.max_total_tokens === 200_000 && saved.max_cycles_per_run === 200);

  return (
    <section>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center justify-between text-sm font-semibold
                   text-slate-700 uppercase tracking-wide mb-2 hover:text-slate-900"
      >
        <span className="flex items-center gap-2">
          🛡 Run Quota Limits
          {!isDefault && (
            <span className="normal-case font-normal text-[11px] bg-amber-100
                             text-amber-700 border border-amber-200 px-1.5 py-0.5 rounded">
              {(saved!.max_total_tokens).toLocaleString()} tokens · {saved!.max_cycles_per_run} cycles
            </span>
          )}
          {isDefault && (
            <span className="normal-case font-normal text-[11px] text-slate-400">
              (using framework defaults)
            </span>
          )}
        </span>
        <span className="text-slate-400 normal-case font-normal text-xs">
          {open ? "▲ collapse" : "▼ configure"}
        </span>
      </button>

      {open && (
        <div className="bg-white border border-slate-200 rounded-lg p-4 space-y-4">
          <p className="text-xs text-slate-500">
            These caps apply to <strong>every Run</strong> of this workflow.
            They narrow the framework defaults (200 000 tokens · 200 cycles) but cannot exceed them.
            Individual agents can be further restricted via their own guardrail panel.
          </p>

          <div className="grid grid-cols-2 gap-4">
            {/* Max total tokens */}
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1">
                Max total tokens / run
              </label>
              <input
                type="number" min={1} max={10_000_000} step={10_000}
                value={maxTokens}
                onChange={(e) => setMaxTokens(Math.max(1, Number(e.target.value)))}
                className="w-full border border-slate-300 rounded px-2.5 py-1.5 text-sm
                           focus:outline-none focus:ring-2 focus:ring-blue-400"
              />
              <p className="text-[10px] text-slate-400 mt-0.5">
                Cumulative tokens consumed across all agents in one run.
                Framework default: 200 000.
              </p>
            </div>

            {/* Max cycles per run */}
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1">
                Max cycles / run
              </label>
              <input
                type="number" min={1} max={100_000} step={10}
                value={maxCycles}
                onChange={(e) => setMaxCycles(Math.max(1, Number(e.target.value)))}
                className="w-full border border-slate-300 rounded px-2.5 py-1.5 text-sm
                           focus:outline-none focus:ring-2 focus:ring-blue-400"
              />
              <p className="text-[10px] text-slate-400 mt-0.5">
                Total agent invocations across all agents in one run.
                Framework default: 200.
              </p>
            </div>
          </div>

          {/* Action bar */}
          <div className="flex items-center justify-between pt-1">
            <button
              type="button"
              disabled={isDefault || clearMutation.isPending}
              onClick={() => clearMutation.mutate()}
              className="text-xs text-slate-500 hover:text-rose-600 disabled:opacity-30
                         disabled:cursor-not-allowed underline"
            >
              {clearMutation.isPending ? "Clearing…" : "Reset to defaults"}
            </button>
            <div className="flex items-center gap-3">
              {mutation.isSuccess && (
                <span className="text-xs text-emerald-600">✓ Saved</span>
              )}
              {mutation.isError && (
                <span className="text-xs text-rose-600">
                  ✗ {(mutation.error as Error).message}
                </span>
              )}
              <button
                type="button"
                onClick={() => mutation.mutate()}
                disabled={mutation.isPending}
                className="px-3 py-1.5 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400
                           text-white text-xs font-medium rounded"
              >
                {mutation.isPending ? "Saving…" : "Save quota limits"}
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
