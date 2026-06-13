import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { AgentSummary, EnvelopeOut, WorkflowDetail } from "../lib/api";
import { AddAgentModal } from "./AddAgentModal";

interface AgentConfig {
  template_key: string;
  prompt: string | null;
  system_prompt: string | null;
  output_field: string;
  max_retries: number;
  retry_backoff_s: number;
  fallback_response: Record<string, unknown> | null;
  json_output: boolean;
  json_unwrap: boolean;
  python_script: string | null;
}

interface Props {
  /** Pass null to close. */
  workflowId: string;
  /** Full workflow detail — needed by the embedded edit modal so it
   *  can show subscribe/publish dropdowns of other agents. */
  workflow?: WorkflowDetail;
  agentKey: string | null;
  /** Static metadata from /workflows/{id} (subscribe / publish topics). */
  agentMeta: AgentSummary | undefined;
  /** Optional run-context events to filter for I/O history. */
  runEvents?: EnvelopeOut[];
  onClose: () => void;
}

/**
 * Slide-in drawer for inspecting + live-editing one Agent.
 *
 * Sections:
 *   - Header (template_key + role)
 *   - Static metadata (subscribe / publish / llm)
 *   - Editable handler config (prompt / system_prompt / max_retries / ...)
 *   - In/Out history (events on this agent's topics from current Run)
 */
export function AgentDrawer({
  workflowId, workflow, agentKey, agentMeta, runEvents = [], onClose,
}: Props) {
  const open = !!agentKey;
  const qc = useQueryClient();
  const [editOpen, setEditOpen] = useState(false);

  // ---- Fetch live config ----
  const { data: cfg, isLoading: cfgLoading } = useQuery<AgentConfig>({
    queryKey: ["agent-config", workflowId, agentKey],
    queryFn: async () => {
      const r = await fetch(
        `/api/workflows/${encodeURIComponent(workflowId)}` +
        `/agents/${encodeURIComponent(agentKey!)}/config`,
      );
      if (!r.ok) throw new Error(await r.text());
      return r.json();
    },
    enabled: !!agentKey,
    refetchOnWindowFocus: false,
  });

  // ---- Local form state, hydrates from cfg ----
  const [prompt, setPrompt] = useState("");
  const [systemPrompt, setSystemPrompt] = useState("");
  const [maxRetries, setMaxRetries] = useState(0);
  const [jsonOutput, setJsonOutput] = useState(false);
  const [editError, setEditError] = useState<string | null>(null);
  const [savedFlash, setSavedFlash] = useState(false);

  useEffect(() => {
    if (!cfg) return;
    setPrompt(cfg.prompt ?? "");
    setSystemPrompt(cfg.system_prompt ?? "");
    setMaxRetries(cfg.max_retries);
    setJsonOutput(cfg.json_output);
    setEditError(null);
  }, [cfg]);

  // ---- Save mutation ----
  const saveMutation = useMutation({
    mutationFn: async () => {
      const body: Record<string, unknown> = {
        prompt: prompt || null,
        system_prompt: systemPrompt || null,
        max_retries: maxRetries,
        json_output: jsonOutput,
      };
      const r = await fetch(
        `/api/workflows/${encodeURIComponent(workflowId)}` +
        `/agents/${encodeURIComponent(agentKey!)}`,
        {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
      );
      if (!r.ok) throw new Error(await r.text());
      return r.json() as Promise<AgentConfig>;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["agent-config", workflowId, agentKey] });
      setSavedFlash(true);
      setTimeout(() => setSavedFlash(false), 1500);
    },
    onError: (e) => setEditError((e as Error).message),
  });

  // ---- Filter run events to those touching this agent's topics ----
  const myTopics = new Set([
    ...(agentMeta?.subscribe ?? []),
    ...(agentMeta?.publish ?? []),
  ]);
  const ioEvents = runEvents.filter((e) => myTopics.has(e.topic));

  // ---- Esc to close ----
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  if (!open) return null;
  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-slate-900/20 z-40"
        onClick={onClose}
      />
      {/* Drawer */}
      <div
        className="fixed top-0 right-0 h-full w-full sm:w-[480px] z-50
                   bg-white border-l border-slate-200 shadow-xl
                   flex flex-col"
      >
        {/* Header */}
        <div className="flex items-start justify-between px-5 py-4 border-b border-slate-200">
          <div>
            <div className="text-xs text-slate-500 uppercase tracking-wide">
              Agent
            </div>
            <div className="font-mono text-lg font-semibold">{agentKey}</div>
            {agentMeta && (
              <div className="text-xs text-slate-500 mt-0.5">
                {agentMeta.role}
                {agentMeta.description && ` — ${agentMeta.description}`}
              </div>
            )}
            {workflow && agentMeta && cfg && (
              <button
                onClick={() => setEditOpen(true)}
                className="mt-2 text-xs px-2 py-1 bg-slate-700 hover:bg-slate-900
                           text-white rounded inline-flex items-center gap-1"
                title="Open the full edit form (subscribe / publish / LLM / prompt / aggregate)"
              >
                ✎ Edit topology / LLM
              </button>
            )}
          </div>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-700 text-2xl leading-none px-2"
            aria-label="Close drawer"
          >
            ×
          </button>
        </div>

        {/* Body — scrollable */}
        <div className="flex-1 overflow-y-auto thin-scroll px-5 py-4 space-y-5">
          {/* ---- Static topology metadata ---- */}
          {agentMeta && (
            <section>
              <h3 className="text-xs font-semibold text-slate-700 uppercase tracking-wide mb-2">
                Topology
              </h3>
              <div className="text-xs text-slate-700 space-y-1 tabular-nums">
                <KV label="subscribe">
                  {agentMeta.subscribe.length === 0 ? "—" :
                    agentMeta.subscribe.map((t) => (
                      <code key={t} className="block">{t}</code>
                    ))}
                </KV>
                <KV label="publish">
                  {agentMeta.publish.length === 0 ? "—" :
                    agentMeta.publish.map((t) => (
                      <code key={t} className="block">{t}</code>
                    ))}
                </KV>
                {agentMeta.llm && (
                  <KV label="llm">
                    <code>
                      {(agentMeta.llm.provider as string) ?? "?"}/
                      {(agentMeta.llm.model as string) ?? "?"}
                    </code>
                  </KV>
                )}
              </div>
              {agentMeta.publish.length === 0 && cfg?.prompt && (
                <div className="mt-2 px-3 py-2 bg-rose-50 border border-rose-200
                                rounded text-xs text-rose-900">
                  <strong>⚠ This agent has no publish topic.</strong>{" "}
                  The default handler can't emit its LLM result anywhere — every
                  event raises{" "}
                  <code className="font-mono bg-rose-100 px-1 rounded">
                    needs at least one publish topic
                  </code>{" "}
                  and the agent transitions to <strong>Failure</strong>.
                  <br />
                  <br />
                  Subscribe / publish topics are IR-level and not yet
                  live-editable. Delete this agent (Agents list → Remove) and
                  recreate it with a publish topic.
                </div>
              )}
            </section>
          )}

          {/* ---- Live editable config ---- */}
          <section>
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-xs font-semibold text-slate-700 uppercase tracking-wide">
                Handler config (live editable)
              </h3>
              {savedFlash && (
                <span className="text-xs text-emerald-600">✓ Saved</span>
              )}
            </div>
            {cfgLoading && (
              <div className="text-slate-500 text-sm italic">Loading…</div>
            )}
            {cfg && (
              <div className="space-y-3">
                <FormField label="System prompt">
                  <textarea
                    value={systemPrompt}
                    onChange={(e) => setSystemPrompt(e.target.value)}
                    placeholder="(none — defaults to no system prompt)"
                    rows={3}
                    className="w-full font-mono text-xs border border-slate-300 rounded
                               px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-blue-400"
                  />
                </FormField>
                <FormField label="Prompt template (Jinja2)">
                  <textarea
                    value={prompt}
                    onChange={(e) => setPrompt(e.target.value)}
                    placeholder="(none — handler in pass-through mode)"
                    rows={6}
                    className="w-full font-mono text-xs border border-slate-300 rounded
                               px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-blue-400"
                  />
                  <p className="text-[10px] text-slate-400 mt-1">
                    Variables: <code>payload</code>, <code>event</code>,{" "}
                    <code>topic</code>, plus payload keys at top level.
                  </p>
                </FormField>
                <div className="grid grid-cols-2 gap-3">
                  <FormField label="Max retries">
                    <input
                      type="number"
                      value={maxRetries}
                      min={0}
                      max={10}
                      onChange={(e) => setMaxRetries(Number(e.target.value))}
                      className="w-full text-sm border border-slate-300 rounded
                                 px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-blue-400"
                    />
                  </FormField>
                  <FormField label="JSON output">
                    <label className="flex items-center gap-2 mt-1.5 text-sm">
                      <input
                        type="checkbox"
                        checked={jsonOutput}
                        onChange={(e) => setJsonOutput(e.target.checked)}
                      />
                      <span>Force JSON parse</span>
                    </label>
                  </FormField>
                </div>

                {editError && (
                  <p className="text-rose-600 text-xs">{editError}</p>
                )}

                <button
                  onClick={() => saveMutation.mutate()}
                  disabled={saveMutation.isPending}
                  className="w-full px-4 py-2 bg-blue-600 hover:bg-blue-700
                             disabled:bg-blue-400 text-white text-sm font-medium
                             rounded transition"
                >
                  {saveMutation.isPending ? "Saving…" : "Apply changes"}
                </button>
                <p className="text-[10px] text-slate-400">
                  Takes effect on the next event this agent consumes — no draining.
                </p>
              </div>
            )}
          </section>

          {/* ---- In/Out history (run context only) ---- */}
          {runEvents.length > 0 && (
            <section>
              <h3 className="text-xs font-semibold text-slate-700 uppercase tracking-wide mb-2">
                Inputs / Outputs ({ioEvents.length})
              </h3>
              {ioEvents.length === 0 ? (
                <div className="text-slate-500 text-xs italic">
                  This agent didn't process any events in this run.
                </div>
              ) : (
                <div className="space-y-1.5">
                  {ioEvents.map((e) => {
                    const direction = agentMeta?.publish.includes(e.topic)
                      ? "out" : "in";
                    return (
                      <details
                        key={e.event_id}
                        className="bg-slate-50 rounded border border-slate-200 px-2 py-1.5"
                      >
                        <summary className="cursor-pointer flex items-baseline gap-2 list-none">
                          <span
                            className={
                              direction === "in"
                                ? "text-amber-700 text-[10px] font-medium uppercase"
                                : "text-emerald-700 text-[10px] font-medium uppercase"
                            }
                          >
                            {direction}
                          </span>
                          <code className="text-[11px] text-slate-500 tabular-nums">
                            {new Date(e.created_at).toLocaleTimeString()}
                          </code>
                          <code className="text-xs text-blue-700 truncate flex-1">
                            {e.topic}
                          </code>
                        </summary>
                        <pre className="mt-1.5 text-[11px] bg-white rounded p-2
                                        overflow-x-auto thin-scroll">
                          {JSON.stringify(e.payload, null, 2)}
                        </pre>
                      </details>
                    );
                  })}
                </div>
              )}
            </section>
          )}
        </div>
      </div>

      {/* Full edit modal — opened by the button above. */}
      {workflow && agentMeta && cfg && (
        <AddAgentModal
          workflow={workflow}
          open={editOpen}
          onClose={() => setEditOpen(false)}
          mode="edit"
          existing={{
            meta: agentMeta,
            prompt: cfg.prompt,
            system_prompt: cfg.system_prompt,
            output_field: cfg.output_field,
            max_retries: cfg.max_retries,
            python_script: cfg.python_script,
            aggregate: agentMeta.aggregate,
          }}
        />
      )}
    </>
  );
}

function KV({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex gap-2">
      <span className="inline-block w-20 text-slate-400">{label}</span>
      <div className="flex-1">{children}</div>
    </div>
  );
}

function FormField({
  label, children,
}: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <label className="block text-xs font-medium text-slate-600 mb-1">
        {label}
      </label>
      {children}
    </div>
  );
}
