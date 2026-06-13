import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  addAgent, getLlmModels, listWorkflowExternal, updateAgent,
  type WorkflowDetail, type AgentSummary,
} from "../lib/api";
import { PromptEditor } from "./PromptEditor";

interface ExistingAgentConfig {
  /** Static topology (from /workflows/{id}). */
  meta: AgentSummary;
  /** Live handler config (from /agents/{key}/config). */
  prompt: string | null;
  system_prompt: string | null;
  output_field: string;
  max_retries: number;
  /** Aggregate config from spec — current backend doesn't return this
   *  yet; pass null until we extend the GET endpoint. */
  aggregate?: { threshold: number; required: string[] } | null;
  /** Python script source (mutually exclusive with prompt+llm). */
  python_script?: string | null;
}

/** Sentinel value for the LLM provider dropdown that selects
 *  "Python script" mode instead of an actual LLM provider. */
const PYTHON_PROVIDER = "python_script";

interface Props {
  workflow: WorkflowDetail;
  open: boolean;
  onClose: () => void;
  /** "create" (default) → POST /agents.
   *  "edit"     → PUT /agents/{key} (replace).
   *  "duplicate" → POST /agents pre-filled from ``existing``. */
  mode?: "create" | "edit" | "duplicate";
  /** Pre-fill source. Required when mode === "edit" or "duplicate". */
  existing?: ExistingAgentConfig;
}

/**
 * Modal for creating a new Agent in the current workflow.
 *
 * Smart inputs:
 *   - Subscribe topic(s): MULTI-SELECT chip list. Pick from any other
 *     agent's PUBLISH topics + add custom topics.
 *   - Output topic: free text, defaults to ``agent.<key>.out``.
 *   - LLM provider/model: dropdown sourced from /api/system/llm-models.
 *   - Prompt: textarea + variable hint.
 *   - Aggregate (only when subscribe ≥ 2): threshold + required-topic
 *     checkboxes.
 */
export function AddAgentModal({ workflow, open, onClose, mode = "create", existing }: Props) {
  const qc = useQueryClient();
  const { data: llmModels } = useQuery({
    queryKey: ["llm-models"],
    queryFn: getLlmModels,
    staleTime: 60_000,
  });

  // Pull external sources so their publish topics show up as
  // candidates in the subscribe list.  External sinks aren't
  // suggested for ``publish`` because the sink is the consumer
  // and the agent's publish topic is wholly the agent's choice.
  const { data: extIO } = useQuery({
    queryKey: ["external-io", workflow.id],
    queryFn: () => listWorkflowExternal(workflow.id),
    enabled: open,
    staleTime: 4_000,
  });

  // ---- Form state ----
  const [templateKey, setTemplateKey] = useState("");
  const [role, setRole] = useState("thinking");
  const [description, setDescription] = useState("");

  // Multiple subscribe topics (chip list).
  const [subTopics, setSubTopics] = useState<string[]>([]);
  const [newSubTopic, setNewSubTopic] = useState("");

  // Single output topic — this agent's own emit channel.
  const [pubTopic, setPubTopic] = useState("");

  const [provider, setProvider] = useState("");
  const [model, setModel] = useState("");
  const [customModelMode, setCustomModelMode] = useState(false);
  const [prompt, setPrompt] = useState("");
  const [systemPrompt, setSystemPrompt] = useState("");
  const [pythonScript, setPythonScript] = useState("");
  const [outputField, setOutputField] = useState("result");
  const [maxRetries, setMaxRetries] = useState(0);
  const [connectStart, setConnectStart] = useState(false);
  const [connectEnd, setConnectEnd] = useState(false);

  // Aggregator config (only relevant when len(subTopics) > 1).
  const [aggregateThreshold, setAggregateThreshold] = useState(0); // 0 = all
  const [requiredTopics, setRequiredTopics] = useState<string[]>([]);

  // Per-agent guardrail caps (None = inherit workflow/framework defaults).
  const [guardrailEnabled, setGuardrailEnabled] = useState(false);
  const [maxTokensPerCall, setMaxTokensPerCall] = useState(8000);
  const [maxCycles, setMaxCycles] = useState(5);

  // The subscribe topic auto-managed by the "Wire from __start__"
  // checkbox. We track it so we can replace it cleanly when
  // template_key changes (and not leave a stale chip behind).
  const [autoStartTopic, setAutoStartTopic] = useState<string | null>(null);

  const [error, setError] = useState<string | null>(null);

  // Guard: the form should be initialized from props ONCE per "open"
  // session. Without this, parent re-renders that pass a fresh
  // ``existing={{...}}`` object literal trigger the init effect every
  // render and clobber state the user (or our auto-detect effect)
  // just changed — most visibly the customModelMode flag.
  const initializedForOpen = useRef(false);

  // Reset on open.
  useEffect(() => {
    if (!open) {
      initializedForOpen.current = false;
      return;
    }
    if (initializedForOpen.current) return;
    initializedForOpen.current = true;

    if ((mode === "edit" || mode === "duplicate") && existing) {
      // Pre-fill from the existing agent.
      const m = existing.meta;
      // For duplicate, start with a unique key. For edit, keep the
      // original key (it's read-only in the edit UI anyway).
      if (mode === "duplicate") {
        setTemplateKey(suggestUniqueKey(m.template_key, workflow));
      } else {
        setTemplateKey(m.template_key);
      }
      setRole(m.role);
      setDescription(m.description);
      setSubTopics([...m.subscribe]);
      setNewSubTopic("");
      setPubTopic(m.publish[0] ?? "");
      // If existing has a python_script, prefer that as the "provider".
      if (existing.python_script && existing.python_script.trim()) {
        setProvider(PYTHON_PROVIDER);
        setModel("");
        setPythonScript(existing.python_script);
        setPrompt("");
      } else {
        setProvider((m.llm?.provider as string) ?? "");
        setModel((m.llm?.model as string) ?? "");
        setPrompt(existing.prompt ?? "");
        setPythonScript("");
      }
      setSystemPrompt(existing.system_prompt ?? "");
      setOutputField(existing.output_field);
      setMaxRetries(existing.max_retries);
      // Custom-model auto-detect: if the loaded model is NOT in the
      // curated list, the user must have typed it manually before —
      // open the form in custom mode so we don't silently lose it.
      // (We can't read llmModels here because the query may not have
      // resolved yet; the effect below covers that case.)
      setCustomModelMode(false);
      // Detect existing connect_to_start/end from current edges.
      // CRITICAL: ignore synthetic edges injected by the backend
      // ``_auto_wire_external_start_edges`` (id prefix ``e_ext_start_``).
      // Those bridge __start__ → agent only to satisfy IR reachability
      // when the agent's only subscribe topic is fed by an external
      // source. Letting them flip ``connectStart=true`` would (a) lock
      // the checkbox in a confusing forced-on state, (b) cause the
      // unrelated "auto-managed agent.<key>.in subscribe chip" effect
      // below to inject a phantom subscribe topic the user never asked
      // for. See bugs.md §"connect_to_start ghost-checked for ext-source agents".
      setConnectStart(
        workflow.edges.some(
          (e) =>
            e.source === "__start__" &&
            e.target === m.template_key &&
            !e.id.startsWith("e_ext_start_"),
        ),
      );
      setConnectEnd(
        workflow.edges.some(
          (e) => e.source === m.template_key && e.target === "__end__",
        ),
      );
      setAggregateThreshold(existing.aggregate?.threshold ?? 0);
      setRequiredTopics(existing.aggregate?.required ?? []);
      // Per-agent guardrail
      const ag = existing.meta.agent_guardrail;
      if (ag) {
        setGuardrailEnabled(true);
        setMaxTokensPerCall(ag.max_tokens_per_call ?? 8000);
        setMaxCycles(ag.max_cycles ?? 5);
      } else {
        setGuardrailEnabled(false);
        setMaxTokensPerCall(8000);
        setMaxCycles(5);
      }
      setAutoStartTopic(null);
      setError(null);
      return;
    }
    // Create mode — empty form.
    setTemplateKey("");
    setRole("thinking");
    setDescription("");
    setSubTopics([]);
    setNewSubTopic("");
    setPubTopic("");
    setProvider("");
    setModel("");
    setCustomModelMode(false);
    setPrompt("");
    setSystemPrompt("");
    setPythonScript("");
    setOutputField("result");
    setMaxRetries(0);
    setConnectStart(false);
    setConnectEnd(false);
    setAggregateThreshold(0);
    setRequiredTopics([]);
    setGuardrailEnabled(false);
    setMaxTokensPerCall(8000);
    setMaxCycles(5);
    setAutoStartTopic(null);
    setError(null);
  }, [open, mode, existing, workflow.edges]);

  // Auto-fill output topic + suggest subscribe topic placeholder when
  // template_key changes.
  useEffect(() => {
    if (!templateKey) return;
    setPubTopic((cur) => {
      // Don't override if user has typed something custom.
      if (!cur || cur.match(/^agent\.[^.]+\.out$/)) {
        return `agent.${templateKey}.out`;
      }
      return cur;
    });
  }, [templateKey]);

  // Manage the auto-added "agent.<key>.in" chip for the
  // "Wire from __start__" checkbox. Without a concrete subscribe
  // topic the orchestrator has nowhere to publish the run input,
  // so the framework requires one. We add it transparently and
  // let the user remove it (they'll lose the start wiring).
  useEffect(() => {
    if (!templateKey) {
      // Remove a stale auto chip if user cleared the key.
      if (autoStartTopic) {
        setSubTopics((prev) => prev.filter((t) => t !== autoStartTopic));
        setAutoStartTopic(null);
      }
      return;
    }
    if (!connectStart) {
      // Checkbox unchecked → drop our auto chip but leave manual ones.
      if (autoStartTopic) {
        setSubTopics((prev) => prev.filter((t) => t !== autoStartTopic));
        setAutoStartTopic(null);
      }
      return;
    }
    const desired = `agent.${templateKey}.in`;
    if (autoStartTopic === desired) return; // already in sync
    // Replace previous auto chip (if any) with the new one.
    setSubTopics((prev) => {
      const without = autoStartTopic
        ? prev.filter((t) => t !== autoStartTopic)
        : prev;
      if (without.includes(desired)) return without;
      return [desired, ...without];
    });
    setAutoStartTopic(desired);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connectStart, templateKey]);

  // After llmModels loads (or provider changes), if the current model
  // value isn't in the curated list, auto-flip to custom mode so the
  // value is preserved + the input is shown.
  useEffect(() => {
    if (!provider || provider === PYTHON_PROVIDER) return;
    if (!model) return;
    const curated = llmModels?.[provider] ?? [];
    if (!curated.includes(model)) {
      setCustomModelMode(true);
    }
  }, [provider, model, llmModels]);

  // Available "publish topics from other agents" — what subscribe can
  // pick from. Helps user avoid typos when chaining.
  const subscribeOptions = useMemo(() => {
    const set = new Set<string>();
    // Topics published by other agents — the typical chaining target.
    for (const a of workflow.agents) a.publish.forEach((t) => set.add(t));
    // Topics fed by external sources — pick one of these to wire
    // an agent directly to a Telegram / IMAP / custom feed.
    for (const s of extIO?.sources ?? []) set.add(s.topic);
    return Array.from(set).sort();
  }, [workflow, extIO?.sources]);

  // Drop required topics that aren't in the current subscribe list.
  useEffect(() => {
    setRequiredTopics((cur) => cur.filter((t) => subTopics.includes(t)));
  }, [subTopics]);

  // ---- Edge preview ----
  const edgePreview = useMemo(() => {
    const out: { from: string; to: string; via: string; reason: string }[] = [];
    if (!templateKey) return out;
    for (const sub of subTopics) {
      for (const a of workflow.agents) {
        if (a.publish.includes(sub)) {
          out.push({
            from: a.template_key, to: templateKey, via: sub,
            reason: "auto: matches existing publish",
          });
        }
      }
    }
    if (pubTopic) {
      for (const a of workflow.agents) {
        if (a.subscribe.includes(pubTopic)) {
          out.push({
            from: templateKey, to: a.template_key, via: pubTopic,
            reason: "auto: matches existing subscribe",
          });
        }
      }
    }
    if (connectStart && subTopics.length > 0) {
      out.push({
        from: "__start__", to: templateKey, via: subTopics[0],
        reason: "from 'Wire from __start__' checkbox",
      });
    }
    if (connectEnd && pubTopic) {
      out.push({
        from: templateKey, to: "__end__", via: pubTopic,
        reason: "from 'Wire to __end__' checkbox",
      });
    }
    return out;
  }, [workflow, templateKey, subTopics, pubTopic, connectStart, connectEnd]);

  const submit = useMutation({
    mutationFn: () => {
      const isPythonMode = provider === PYTHON_PROVIDER;
      const body = {
        role,
        description,
        subscribe_topics: subTopics,
        publish_topics: pubTopic.trim() ? [pubTopic.trim()] : [],
        llm:
          isPythonMode || !provider || !model
            ? null
            : { provider, model },
        prompt: isPythonMode ? null : (prompt || null),
        system_prompt: systemPrompt || null,
        python_script: isPythonMode ? pythonScript : null,
        output_field: outputField,
        max_retries: maxRetries,
        connect_to_start: connectStart,
        connect_to_end: connectEnd,
        aggregate_threshold: subTopics.length >= 2 ? aggregateThreshold : 0,
        aggregate_required_topics:
          subTopics.length >= 2 ? requiredTopics : [],
        agent_guardrail: guardrailEnabled
          ? { max_tokens_per_call: maxTokensPerCall, max_cycles: maxCycles }
          : null,
      };
      if (mode === "edit") {
        return updateAgent(workflow.id, templateKey, body);
      }
      return addAgent(workflow.id, {
        ...body,
        template_key: templateKey.trim(),
      });
    },
    onSuccess: () => {
      // Close immediately — don't block the modal behind awaited refetches
      // (that made "Add agent" feel very slow). Invalidate in the
      // background; the workflow detail view (refetchInterval) and any
      // mounted list refresh themselves a beat later.
      onClose();
      qc.invalidateQueries({ queryKey: ["workflow", workflow.id] });
      qc.invalidateQueries({ queryKey: ["workflows"] });
      // Agent-config (handler-level: prompt etc.) is also stale after edit.
      qc.invalidateQueries({
        queryKey: ["agent-config", workflow.id, templateKey],
      });
    },
    onError: (e) => setError((e as Error).message),
  });

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  if (!open) return null;

  const addSubTopic = (topic: string) => {
    const t = topic.trim();
    if (!t || subTopics.includes(t)) return;
    setSubTopics((prev) => [...prev, t]);
  };

  const isAggregator = subTopics.length >= 2;

  return (
    <>
      <div className="fixed inset-0 bg-slate-900/40 z-40" onClick={onClose} />
      <div
        className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50
                   w-[640px] max-w-[95vw] max-h-[90vh] overflow-y-auto thin-scroll
                   bg-white rounded-lg shadow-2xl"
      >
        <div className="flex items-center justify-between px-5 py-4 border-b border-slate-200">
          <h2 className="text-lg font-semibold">
            {mode === "edit"
              ? <>Edit <code className="font-mono">{templateKey}</code></>
              : mode === "duplicate"
                ? <>Duplicate <code className="font-mono">{existing?.meta.template_key}</code></>
                : "Add Agent"}
          </h2>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-700 text-2xl leading-none"
          >×</button>
        </div>

        <div className="px-5 py-4 space-y-4">
          {/* ---- Identity ---- */}
          <div className="grid grid-cols-2 gap-3">
            <Field label="Template key (unique)" required>
              <input
                value={templateKey}
                onChange={(e) =>
                  setTemplateKey(
                    e.target.value.replace(/[^a-z0-9_]/gi, "_").toLowerCase(),
                  )
                }
                placeholder="my_agent"
                disabled={mode === "edit"}
                className="form-input font-mono disabled:bg-slate-100 disabled:text-slate-500"
              />
            </Field>
            <Field label="Role">
              <select
                value={role}
                onChange={(e) => setRole(e.target.value)}
                className="form-input"
              >
                <optgroup label="Common">
                  <option value="thinking">thinking — LLM reasoning</option>
                  <option value="fetch">fetch — pull data from external sources</option>
                  <option value="judge">judge — score / classify / route</option>
                  <option value="tool">tool — call APIs / run code</option>
                </optgroup>
                <optgroup label="Advanced">
                  <option value="aggregator">aggregator — combine fan-in inputs</option>
                  <option value="memory">memory — read/write Run state</option>
                  <option value="guard">guard — policy check / escalation</option>
                  <option value="human">human — wait for human approval</option>
                </optgroup>
              </select>
            </Field>
          </div>
          <Field label="Description">
            <input
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="What does this agent do?"
              className="form-input"
            />
          </Field>

          {/* ---- Subscribe (multi) ---- */}
          <Field label="Subscribe topics — fan-in from upstream agents" required>
            <div className="flex flex-wrap gap-1.5 min-h-[36px] p-1.5 border border-slate-300 rounded">
              {subTopics.map((t) => {
                const isAuto = t === autoStartTopic;
                return (
                  <span
                    key={t}
                    className={
                      "inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-mono " +
                      (isAuto
                        ? "bg-amber-100 text-amber-900 border border-amber-300"
                        : "bg-blue-100 text-blue-800")
                    }
                    title={
                      isAuto
                        ? "Auto-added because 'Wire from __start__' is checked. " +
                          "Uncheck the box to remove."
                        : ""
                    }
                  >
                    {isAuto && <span className="text-[10px]">▶</span>}
                    {t}
                    <button
                      onClick={() => {
                        setSubTopics((prev) => prev.filter((x) => x !== t));
                        if (isAuto) {
                          setAutoStartTopic(null);
                          setConnectStart(false);
                        }
                      }}
                      className={
                        isAuto
                          ? "text-amber-700 hover:text-amber-900"
                          : "text-blue-600 hover:text-blue-900"
                      }
                    >×</button>
                  </span>
                );
              })}
              {subTopics.length === 0 && (
                <span className="text-xs text-slate-400 italic px-1">
                  No topics — add one below or check "Wire from __start__"
                </span>
              )}
            </div>

            {/* Picker row: dropdown of existing publishes + custom input */}
            <div className="mt-1.5 flex gap-2">
              <select
                value=""
                onChange={(e) => {
                  if (e.target.value) {
                    addSubTopic(e.target.value);
                    e.target.value = "";
                  }
                }}
                className="form-input flex-1 text-xs"
              >
                <option value="">— pick from existing publish topics or external sources —</option>
                {subscribeOptions
                  .filter((t) => !subTopics.includes(t))
                  .map((t) => {
                    const matchingSrc = (extIO?.sources ?? []).find((s) => s.topic === t);
                    if (matchingSrc) {
                      const out = (matchingSrc.config.output_field as string) || "text";
                      return (
                        <option key={t} value={t}>
                          {`🔌 ${t}  — external (${matchingSrc.kind}, payload.${out})`}
                        </option>
                      );
                    }
                    return <option key={t} value={t}>{t}</option>;
                  })}
              </select>
              <input
                value={newSubTopic}
                onChange={(e) => setNewSubTopic(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && newSubTopic.trim()) {
                    e.preventDefault();
                    addSubTopic(newSubTopic);
                    setNewSubTopic("");
                  }
                }}
                placeholder="or type custom topic + Enter"
                className="form-input flex-1 font-mono text-xs"
              />
            </div>
            <p className="text-[10px] text-slate-500 mt-1">
              Pick multiple to make this a fan-in agent.
            </p>
          </Field>

          {/* ---- Output topic ---- */}
          <Field label="Output topic — this agent's own emit channel" required>
            <input
              value={pubTopic}
              onChange={(e) => setPubTopic(e.target.value)}
              placeholder={
                templateKey ? `agent.${templateKey}.out` : "agent.<key>.out"
              }
              className="form-input font-mono text-xs"
            />
            <p className="text-[10px] text-slate-500 mt-1">
              Every agent owns one output topic where it publishes its result.
              Other agents can subscribe to it. Routing to{" "}
              <code>__end__</code> is via the checkbox below — there is no{" "}
              <code>__end__</code> topic.
            </p>
          </Field>

          {/* ---- Wire to start/end ---- */}
          <div className="bg-slate-50 border border-slate-200 rounded-lg p-3">
            <div className="text-xs font-semibold text-slate-700 uppercase tracking-wide mb-2">
              Where does this output flow?
            </div>
            <div className="space-y-1.5">
              <label className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox" checked={connectStart}
                  onChange={(e) => setConnectStart(e.target.checked)}
                />
                <span>
                  Wire from <code className="font-mono text-xs bg-slate-200 px-1 rounded">__start__</code>
                  <span className="text-xs text-slate-500 ml-1">
                    (this agent receives the user's initial input)
                  </span>
                </span>
              </label>
              <label className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox" checked={connectEnd}
                  onChange={(e) => setConnectEnd(e.target.checked)}
                />
                <span>
                  Wire to <code className="font-mono text-xs bg-slate-200 px-1 rounded">__end__</code>
                  <span className="text-xs text-slate-500 ml-1">
                    (this agent's output marks the run as Succeeded)
                  </span>
                </span>
              </label>
            </div>
            <p className="text-[10px] text-slate-500 pt-1">
              Other connections are auto-wired when your topics overlap with existing agents.
            </p>
          </div>

          {/* ---- Aggregator config (only when fan-in) ---- */}
          {isAggregator && (
            <div className="bg-violet-50 border border-violet-200 rounded-lg p-3">
              <div className="text-xs font-semibold text-violet-900 uppercase tracking-wide mb-2">
                Fan-in gating ({subTopics.length} sources)
              </div>
              <p className="text-xs text-violet-800 mb-3">
                With multiple subscribe topics, this agent buffers events
                per Run and fires the handler once with a <strong>merged
                payload</strong> (union of all upstream payloads + an{" "}
                <code className="bg-violet-100 px-1 rounded">_inputs</code>{" "}
                map keyed by topic). Pick a threshold below — the default
                <strong> All</strong> waits for every upstream to emit.
                Set to <strong>1</strong> for per-event dispatch (no
                aggregation, fires on every arriving event).
              </p>

              <div className="grid grid-cols-2 gap-3">
                <Field label="Threshold (min topics to fire)">
                  <select
                    value={aggregateThreshold}
                    onChange={(e) => setAggregateThreshold(Number(e.target.value))}
                    className="form-input"
                  >
                    <option value={0}>All ({subTopics.length})</option>
                    {Array.from({ length: subTopics.length }, (_, i) => i + 1)
                      .map((n) => (
                        <option key={n} value={n}>{n}</option>
                      ))}
                  </select>
                </Field>
                <Field label="Behaviour">
                  <div className="text-xs text-slate-700 mt-1.5">
                    {aggregateThreshold === 0 || aggregateThreshold === subTopics.length
                      ? "Fires once after ALL upstream events arrive."
                      : aggregateThreshold === 1
                      ? "Fires on every arriving event (per-event mode)."
                      : `Fires once after ≥${aggregateThreshold} of ${subTopics.length} arrived.`}
                  </div>
                </Field>
              </div>

              <div className="mt-3">
                <label className="block text-xs font-medium text-slate-700 mb-1">
                  Required topics (must be present regardless of threshold)
                </label>
                <div className="space-y-0.5">
                  {subTopics.map((t) => (
                    <label
                      key={t}
                      className="flex items-center gap-2 text-xs"
                    >
                      <input
                        type="checkbox"
                        checked={requiredTopics.includes(t)}
                        onChange={(e) => {
                          if (e.target.checked) {
                            setRequiredTopics((p) => [...p, t]);
                          } else {
                            setRequiredTopics((p) => p.filter((x) => x !== t));
                          }
                        }}
                      />
                      <code className="font-mono">{t}</code>
                    </label>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* ---- LLM ---- */}
          {prompt.trim() && (!provider || (!model && provider !== PYTHON_PROVIDER)) && provider !== PYTHON_PROVIDER && (
            <div className="bg-amber-50 border border-amber-300 rounded p-3">
              <div className="text-xs font-semibold text-amber-900 mb-1">
                ⚠ LLM provider and model are required
              </div>
              <p className="text-xs text-amber-900">
                You've written a prompt template — pick a provider and
                model below. The default handler will invoke{" "}
                <code className="font-mono bg-amber-100 px-1 rounded">
                  ctx.llm.chat()
                </code>{" "}
                and needs both. Or clear the prompt to keep the agent
                as a pure pass-through.
              </p>
            </div>
          )}
          <div className="grid grid-cols-2 gap-3">
            <Field label={<>LLM provider {prompt.trim() && provider !== PYTHON_PROVIDER && <span className="text-rose-600">*</span>}</>}>
              <select
                value={provider}
                onChange={(e) => {
                  setProvider(e.target.value);
                  setModel("");
                }}
                className={
                  "form-input " +
                  (prompt.trim() && !provider && provider !== PYTHON_PROVIDER
                    ? "border-amber-400 ring-1 ring-amber-200"
                    : "")
                }
              >
                <option value="">— none (pass-through) —</option>
                <option value={PYTHON_PROVIDER}>🐍 Python script (no LLM)</option>
                {llmModels && Object.keys(llmModels).map((p) => (
                  <option key={p} value={p}>{p}</option>
                ))}
              </select>
            </Field>
            <Field label={<>Model {prompt.trim() && provider !== PYTHON_PROVIDER && <span className="text-rose-600">*</span>}</>}>
              {customModelMode ? (
                <div className="flex gap-1">
                  <input
                    type="text"
                    value={model}
                    onChange={(e) => setModel(e.target.value)}
                    placeholder="e.g. gpt-5.2-pro / claude-opus-4-8"
                    disabled={!provider || provider === PYTHON_PROVIDER}
                    spellCheck={false}
                    className={
                      "form-input font-mono text-xs disabled:bg-slate-100 " +
                      "disabled:text-slate-400 flex-1 " +
                      (prompt.trim() && provider && !model && provider !== PYTHON_PROVIDER
                        ? "border-amber-400 ring-1 ring-amber-200"
                        : "")
                    }
                  />
                  <button
                    type="button"
                    onClick={() => {
                      setCustomModelMode(false);
                      // Clear the custom value so the select goes back
                      // to "— select —" instead of being inconsistent.
                      setModel("");
                    }}
                    title="Switch back to the curated model list"
                    className="text-xs px-2 py-1 bg-slate-100 hover:bg-slate-200
                               text-slate-600 rounded shrink-0"
                  >
                    ← list
                  </button>
                </div>
              ) : (
                <select
                  value={model}
                  onChange={(e) => {
                    if (e.target.value === "__custom__") {
                      setCustomModelMode(true);
                      setModel("");
                    } else {
                      setModel(e.target.value);
                    }
                  }}
                  disabled={!provider || provider === PYTHON_PROVIDER}
                  className={
                    "form-input disabled:bg-slate-100 disabled:text-slate-400 " +
                    (prompt.trim() && provider && !model && provider !== PYTHON_PROVIDER
                      ? "border-amber-400 ring-1 ring-amber-200"
                      : "")
                  }
                >
                  <option value="">
                    {provider === PYTHON_PROVIDER ? "— N/A (script mode) —" : "— select —"}
                  </option>
                  {provider && provider !== PYTHON_PROVIDER && (llmModels?.[provider] ?? []).map((m) => (
                    <option key={m} value={m}>{m}</option>
                  ))}
                  {provider && provider !== PYTHON_PROVIDER && (
                    <option value="__custom__">🔧 Custom… (type any model name)</option>
                  )}
                </select>
              )}
              {customModelMode && (
                <p className="text-[10px] text-slate-500 mt-1">
                  Free-form model name. Use this when the API has a
                  newer model not yet in the curated list.
                </p>
              )}
              {!customModelMode && model && isThinkingModel(model) && (
                <p className="text-[10px] text-amber-700 mt-1">
                  ⏱ <strong>{model}</strong> is a reasoning model — expect
                  2–15s latency per call as it thinks internally before
                  emitting output. For fast iteration / pass-through
                  flows pick a non-reasoning model
                  {fastAlternative(provider) &&
                    <> (e.g. <code className="font-mono">{fastAlternative(provider)}</code>)</>
                  }.
                </p>
              )}
            </Field>
          </div>

          {/* ---- Python script editor (only when provider == python_script) ---- */}
          {provider === PYTHON_PROVIDER && (
            <div className="bg-emerald-50 border border-emerald-200 rounded-lg p-3">
              <div className="text-xs font-semibold text-emerald-900 mb-1">
                🐍 Python handler
              </div>
              <p className="text-[11px] text-emerald-900 mb-2">
                Replace the LLM call with a Python function. Define a top-level{" "}
                <code className="font-mono bg-emerald-100 px-1 rounded">
                  def handle(payload, event=None) -&gt; dict
                </code>{" "}
                that returns a dict — its keys merge into the published
                payload. Both <strong>sync</strong> and{" "}
                <strong>async def</strong> are supported. Accessible:
                Python builtins, the input{" "}
                <code className="font-mono">payload</code> (a dict copy of{" "}
                <code className="font-mono">event.payload</code>), and the
                full <code className="font-mono">event</code> object.
                <br />
                <span className="text-amber-800">
                  ⚠ Code is exec'd in-process — no sandbox. Don't paste
                  untrusted code.
                </span>
              </p>
              <textarea
                value={pythonScript}
                onChange={(e) => setPythonScript(e.target.value)}
                spellCheck={false}
                rows={10}
                placeholder={`def handle(payload):
    # Compute whatever you want; return a dict.
    n = int(payload.get("number", 0))
    op = payload.get("op", "+")
    if op == "+":
        return {"result": n + 1}
    return {"result": n}`}
                className="form-input font-mono text-xs whitespace-pre"
                style={{ tabSize: 4, fontFamily: 'ui-monospace, "SF Mono", Consolas, monospace' }}
              />
              {pythonScript.trim() && !/\bdef\s+handle\b/.test(pythonScript) && (
                <p className="mt-1 text-[11px] text-rose-600">
                  Script must define a top-level{" "}
                  <code className="font-mono">def handle(...)</code> function.
                </p>
              )}
            </div>
          )}

          {/* ---- Prompt — hidden in python script mode ---- */}
          {provider !== PYTHON_PROVIDER && (
            <>
              <Field label="System prompt (optional)">
                <textarea
                  value={systemPrompt}
                  onChange={(e) => setSystemPrompt(e.target.value)}
                  rows={2}
                  className="form-input font-mono text-xs"
                />
              </Field>
              <PromptEditor
                value={prompt}
                onChange={setPrompt}
                subscribeTopics={subTopics}
                workflow={workflow}
                connectStart={connectStart}
                externalSources={extIO?.sources ?? []}
              />
            </>
          )}
          <div className="grid grid-cols-2 gap-3">
            <Field label="Output field">
              <input
                value={outputField}
                onChange={(e) => setOutputField(e.target.value)}
                className="form-input font-mono text-xs"
              />
            </Field>
            <Field label="Max retries">
              <input
                type="number" min={0} max={10}
                value={maxRetries}
                onChange={(e) => setMaxRetries(Number(e.target.value))}
                className="form-input"
              />
            </Field>
          </div>

          {error && <p className="text-rose-600 text-sm">{error}</p>}
        </div>

        {/* ---- Guardrails ---- */}
        <div className="border border-slate-200 rounded-lg overflow-hidden">
          <button
            type="button"
            onClick={() => setGuardrailEnabled((v) => !v)}
            className="w-full flex items-center justify-between px-4 py-2.5 bg-slate-50
                       hover:bg-slate-100 text-sm font-medium text-slate-700 transition-colors"
          >
            <span className="flex items-center gap-2">
              🛡 Per-agent token guardrails
              {guardrailEnabled && (
                <span className="text-[11px] bg-amber-100 text-amber-700 border border-amber-200
                                 px-1.5 py-0.5 rounded font-normal">
                  {maxTokensPerCall.toLocaleString()} tok/call · {maxCycles} cycles
                </span>
              )}
            </span>
            <span className="text-slate-400 text-xs">{guardrailEnabled ? "▲ hide" : "▼ configure"}</span>
          </button>
          {guardrailEnabled && (
            <div className="px-4 py-3 space-y-3 bg-white border-t border-slate-200">
              <p className="text-[11px] text-slate-500 leading-snug">
                Caps applied to <strong>this agent only</strong>.{" "}
                Leave disabled to inherit the workflow-level defaults.
              </p>
              <div className="grid grid-cols-2 gap-3">
                <Field label="Max tokens / call">
                  <input
                    type="number" min={1} max={200000} step={1000}
                    value={maxTokensPerCall}
                    onChange={(e) => setMaxTokensPerCall(Math.max(1, Number(e.target.value)))}
                    className="w-full border border-slate-300 rounded px-2.5 py-1.5 text-sm
                               focus:outline-none focus:ring-2 focus:ring-blue-400"
                  />
                  <p className="text-[10px] text-slate-400 mt-0.5">
                    Single LLM call upper bound (framework default: 8 000).
                  </p>
                </Field>
                <Field label="Max cycles (per run)">
                  <input
                    type="number" min={1} max={10000} step={1}
                    value={maxCycles}
                    onChange={(e) => setMaxCycles(Math.max(1, Number(e.target.value)))}
                    className="w-full border border-slate-300 rounded px-2.5 py-1.5 text-sm
                               focus:outline-none focus:ring-2 focus:ring-blue-400"
                  />
                  <p className="text-[10px] text-slate-400 mt-0.5">
                    Total invocations this agent may run (framework default: 5).
                  </p>
                </Field>
              </div>
            </div>
          )}
        </div>

        {/* ---- Edge preview ---- */}
        {edgePreview.length > 0 && (
          <div className="px-5 pb-3">
            <div className="bg-blue-50 border border-blue-200 rounded p-3 text-xs">
              <div className="font-semibold text-blue-900 mb-1.5">
                On submit, these edges will be added:
              </div>
              <ul className="space-y-0.5 font-mono">
                {edgePreview.map((e, i) => (
                  <li key={i} className="text-blue-900">
                    <span className="text-blue-700">{e.from}</span>
                    {" → "}
                    <span className="text-blue-700">{e.to}</span>
                    <span className="text-slate-500"> via </span>
                    <span className="text-slate-700">{e.via}</span>
                    <span className="text-[10px] text-slate-500 ml-2">
                      ({e.reason})
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          </div>
        )}

        <div className="flex justify-end gap-2 px-5 py-3 border-t border-slate-200">
          <button onClick={onClose} className="px-4 py-2 text-sm hover:bg-slate-100 rounded">
            Cancel
          </button>
          <button
            onClick={() => submit.mutate()}
            disabled={
              !templateKey.trim() ||
              subTopics.length === 0 ||
              !pubTopic.trim() ||
              // Prompt requires both provider and model.
              (!!prompt.trim() && (!provider || !model) && provider !== PYTHON_PROVIDER) ||
              // Python script mode requires a non-empty script with `def handle`.
              (provider === PYTHON_PROVIDER &&
                (!pythonScript.trim() || !/\bdef\s+handle\b/.test(pythonScript))) ||
              submit.isPending
            }
            className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400
                       text-white text-sm font-medium rounded"
            title={
              !!prompt.trim() && (!provider || !model) && provider !== PYTHON_PROVIDER
                ? "Pick LLM provider + model, or clear the prompt"
                : provider === PYTHON_PROVIDER && (!pythonScript.trim() || !/\bdef\s+handle\b/.test(pythonScript))
                  ? "Python script must define a top-level `def handle(...)`"
                  : ""
            }
          >
            {submit.isPending
              ? (mode === "edit" ? "Saving…" : (mode === "duplicate" ? "Creating copy…" : "Creating…"))
              : (mode === "edit" ? "Save changes" : (mode === "duplicate" ? "Create copy" : "Create Agent"))}
          </button>
        </div>
      </div>
    </>
  );
}

function Field({
  label, required = false, children,
}: { label: React.ReactNode; required?: boolean; children: React.ReactNode }) {
  return (
    <div>
      <label className="block text-xs font-medium text-slate-600 mb-1">
        {label}
        {required && <span className="text-rose-500 ml-0.5">*</span>}
      </label>
      {children}
    </div>
  );
}

/** Suggest a non-colliding template key for a duplicated agent.
 *  Tries ``<base>_copy``, then ``<base>_copy_2``, etc. */
function suggestUniqueKey(base: string, workflow: WorkflowDetail): string {
  const taken = new Set(workflow.agents.map((a) => a.template_key));
  let candidate = `${base}_copy`;
  if (!taken.has(candidate)) return candidate;
  for (let i = 2; i < 100; i++) {
    candidate = `${base}_copy_${i}`;
    if (!taken.has(candidate)) return candidate;
  }
  return `${base}_copy_${Date.now()}`;
}

/** Heuristic: does this model use internal reasoning / thinking?
 *  Used to warn the user about latency in the AddAgentModal.
 *  Conservative — only flags models we have direct evidence are slow. */
function isThinkingModel(model: string): boolean {
  const m = model.toLowerCase();
  // Gemini reasoning models (3.5-flash uses thinking by default; flash-lite + 2.5-pro do not).
  if (m === "gemini-3.5-flash") return true;
  // GPT-5 family — all reasoning.
  if (m.startsWith("gpt-5")) return true;
  // Claude opus / sonnet (large reasoning models).
  if (m.startsWith("claude-opus") || m.startsWith("claude-sonnet")) return true;
  // DeepSeek reasoner
  if (m === "deepseek-reasoner" || m === "deepseek-v4-pro") return true;
  // Qwen Max (full reasoning)
  if (m.startsWith("qwen3.7-max") || m === "qwen-max") return true;
  return false;
}

/** Suggest a fast non-reasoning model for the given provider — shown
 *  in the latency hint as a pointer. */
function fastAlternative(provider: string): string | null {
  switch (provider) {
    case "gemini":   return "gemini-3.1-flash-lite";
    case "openai":   return "gpt-4.1";
    case "anthropic":return "claude-haiku-4-5";
    case "deepseek": return "deepseek-chat";
    case "qwen":     return "qwen3.6-plus";
    default: return null;
  }
}
