/**
 * Modal to add / list / remove external sources or sinks.
 *
 * The form is rendered dynamically from the kind's ``fields`` schema
 * returned by GET /api/external/kinds — adding a new adapter on the
 * backend automatically surfaces it here, no UI code changes required.
 */
import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  addExternalSink, addExternalSource, listExternalKinds, listWorkflowExternal,
  removeExternalSink, removeExternalSource,
  type ExternalIOItem, type ExternalKindMeta,
} from "../lib/api";

interface Props {
  workflowId: string;
  open: boolean;
  /** "source" → outside-→bus.   "sink" → bus-→outside. */
  direction: "source" | "sink";
  /** Workflow execution mode. ``normal`` greys out the form so the
   *  user can read the configuration but not mutate it. */
  mode?: "normal" | "event_driven";
  /** Optional — drives the sink-topic dropdown so users pick from
   *  agents' actual publish topics instead of typing free-form. */
  agentPublishTopics?: string[];
  /** topic → output_field map.  When the sink picks a topic that
   *  matches one of these, we suggest that agent's output_field as
   *  the body / text field default — preventing the "sink forwarded
   *  raw text instead of agent output" foot-gun. */
  agentOutputFieldByPublishTopic?: Record<string, string>;
  /** topic → agent template_key. Used for the "Body field" chips so
   *  we can label each suggestion with the agent it comes from. */
  agentKeyByPublishTopic?: Record<string, string>;
  /** When set on open, pre-loads that item into the edit form
   *  (used by the topology graph's double-click-to-edit). */
  prefilledEditName?: string | null;
  onClose: () => void;
}

export function ExternalIOModal({
  workflowId, open, direction, mode = "event_driven",
  agentPublishTopics = [], agentOutputFieldByPublishTopic = {},
  agentKeyByPublishTopic = {},
  prefilledEditName = null, onClose,
}: Props) {
  const readOnly = mode !== "event_driven";
  const qc = useQueryClient();

  const { data: kindsAll = [] } = useQuery({
    queryKey: ["external-kinds"],
    queryFn: listExternalKinds,
    staleTime: 60_000,
  });

  const { data: existing = { sources: [], sinks: [] } } = useQuery({
    queryKey: ["external-io", workflowId],
    queryFn: () => listWorkflowExternal(workflowId),
    enabled: open && !!workflowId,
    refetchInterval: open ? 3_000 : false,
  });

  const items = direction === "source" ? existing.sources : existing.sinks;
  const kinds = useMemo(
    () => kindsAll.filter((k) => k.direction === direction),
    [kindsAll, direction],
  );

  // ---- Form state -----
  const [name, setName] = useState("");
  const [kind, setKind] = useState("");
  const [topic, setTopic] = useState("");

  // For sinks: look up the upstream agent for the currently-
  // selected subscribe topic so we can show suggestion chips
  // for the Body field.
  const upstreamAgentForSink = useMemo(() => {
    if (direction !== "sink" || !topic) return null;
    const key = agentKeyByPublishTopic[topic];
    const outputField = agentOutputFieldByPublishTopic[topic];
    if (!key || !outputField) return null;
    return { key, outputField };
  }, [direction, topic, agentKeyByPublishTopic, agentOutputFieldByPublishTopic]);
  const [config, setConfig] = useState<Record<string, unknown>>({});
  const [error, setError] = useState<string | null>(null);

  // Track whether the user has manually edited the topic — once
  // they do, we stop the auto-derive-from-name behaviour so we don't
  // clobber their input.  Reset on every modal open.
  const [topicManuallyEdited, setTopicManuallyEdited] = useState(false);
  /** When non-null we're editing an existing item — name is locked,
   *  the form is pre-filled.  Submitting POSTs which is upsert. */
  const [editingName, setEditingName] = useState<string | null>(null);

  // Reset form when modal opens or direction switches.
  useEffect(() => {
    if (!open) return;
    setEditingName(null);
    setName("");
    setKind(kinds[0]?.kind ?? "");
    setTopic("");
    setTopicManuallyEdited(false);
    setConfig({});
    setError(null);
  }, [open, direction, kinds.length]);

  /** Load an existing item's config into the form.  Secret fields
   *  arrive redacted ("***xxxx") from the backend; we leave them as-is —
   *  the manager treats redacted values on POST as "keep prior". */
  const startEdit = (item: ExternalIOItem) => {
    setEditingName(item.name);
    setName(item.name);
    setKind(item.kind);
    setTopic(item.topic);
    setTopicManuallyEdited(true);   // freeze auto-derive
    setConfig(item.config);
    setError(null);
  };

  // Honor the parent's "double-clicked node X — open in edit form" hint.
  // Re-runs whenever the items list refreshes so a freshly-loaded query
  // doesn't lose the request.
  useEffect(() => {
    if (!open || !prefilledEditName || editingName === prefilledEditName) return;
    const target = items.find((i) => i.name === prefilledEditName);
    if (target) startEdit(target);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, prefilledEditName, items.length]);

  // Auto-derive a unique default topic from the name + kind so multiple
  // sources don't all collide on "ext.in".
  useEffect(() => {
    if (topicManuallyEdited) return;
    const safeName = name.trim().replace(/[^a-zA-Z0-9_-]/g, "_");
    if (!safeName || !kind) {
      setTopic("");
      return;
    }
    setTopic(direction === "source" ? `ext.${kind}.${safeName}.in` : `ext.${kind}.${safeName}.out`);
  }, [name, kind, direction, topicManuallyEdited]);

  // When the sink's subscribe topic matches an agent's publish
  // topic and the user hasn't typed a custom text_field yet, suggest
  // that agent's output_field so the sink forwards agent output by
  // default.
  useEffect(() => {
    if (direction !== "sink" || !topic) return;
    const suggested = agentOutputFieldByPublishTopic[topic];
    if (!suggested) return;
    setConfig((cur) => {
      // Don't overwrite if user already customized it (and it's not
      // one of the historic defaults we know we set).
      const prev = (cur.text_field as string) || "";
      if (prev && prev !== "text" && prev !== "result") return cur;
      return { ...cur, text_field: suggested };
    });
  }, [direction, topic, agentOutputFieldByPublishTopic]);

  // Re-default config defaults whenever kind changes.
  const currentKind: ExternalKindMeta | undefined =
    kinds.find((k) => k.kind === kind);
  useEffect(() => {
    if (!currentKind) return;
    const initial: Record<string, unknown> = {};
    for (const [key, schema] of Object.entries(currentKind.fields)) {
      if (schema.default !== undefined) initial[key] = schema.default;
    }
    setConfig(initial);
  }, [currentKind?.kind]);

  const addMut = useMutation({
    mutationFn: () => {
      if (readOnly) throw new Error("workflow is in normal mode — switch to event_driven first");
      const body = { name: name.trim(), kind, topic: topic.trim(), config };
      return direction === "source"
        ? addExternalSource(workflowId, body)
        : addExternalSink(workflowId, body);
    },
    onSuccess: () => {
      qc.refetchQueries({ queryKey: ["external-io", workflowId] });
      qc.refetchQueries({ queryKey: ["workflow", workflowId] });
      setError(null);
      setEditingName(null);
      setName("");
      setTopicManuallyEdited(false);
    },
    onError: (e) => setError((e as Error).message),
  });

  const removeMut = useMutation({
    mutationFn: (name: string) =>
      direction === "source"
        ? removeExternalSource(workflowId, name)
        : removeExternalSink(workflowId, name),
    onSuccess: () => {
      qc.refetchQueries({ queryKey: ["external-io", workflowId] });
      qc.refetchQueries({ queryKey: ["workflow", workflowId] });
      setError(null);
    },
    onError: (e) => setError(`Remove failed: ${(e as Error).message}`),
  });

  if (!open) return null;

  const valid =
    !!name.trim() &&
    !!kind &&
    !!topic.trim() &&
    (currentKind
      ? Object.entries(currentKind.fields).every(
          ([k, s]) => !s.required || !!config[k],
        )
      : false);

  return (
    <>
      <div
        className="fixed inset-0 bg-black/40 z-30"
        onClick={onClose}
      />
      <div
        className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2
                   w-[680px] max-h-[88vh] bg-white rounded-lg shadow-2xl
                   z-40 flex flex-col"
      >
        <div className="px-5 py-3 border-b border-slate-200 flex items-center justify-between">
          <h2 className="font-semibold text-slate-800">
            {direction === "source"
              ? "🔌 External Sources — outside world → workflow"
              : "📤 External Sinks — workflow → outside world"}
          </h2>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-700"
            title="Close"
          >
            ✕
          </button>
        </div>

        <div className={"flex-1 overflow-y-auto px-5 py-4 space-y-5 " +
                          (readOnly ? "opacity-70" : "")}>
          {readOnly && (
            <div className="bg-amber-50 border border-amber-200 text-amber-800
                            text-xs px-3 py-2 rounded">
              <strong>Read-only:</strong> the workflow is in <code>normal</code>{" "}
              mode. External I/O configuration is preserved but the adapters
              are paused. Switch the workflow to <code>event_driven</code> to
              edit / remove these — or to start listening again.
            </div>
          )}
          {/* ---- Existing items ---- */}
          {items.length > 0 && (
            <section>
              <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-2">
                Configured ({items.length})
              </h3>
              <div className="border border-slate-200 rounded divide-y divide-slate-100">
                {items.map((it) => (
                  <ExternalRow
                    key={it.name}
                    item={it}
                    isEditing={editingName === it.name}
                    disabled={readOnly}
                    removing={removeMut.isPending && removeMut.variables === it.name}
                    onEdit={() => startEdit(it)}
                    onRemove={() => {
                      if (confirm(`Remove ${direction} "${it.name}"?`))
                        removeMut.mutate(it.name);
                    }}
                  />
                ))}
              </div>
            </section>
          )}

          {/* ---- Add form ---- */}
          <section>
            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-2">
              Add new {direction}
            </h3>

            <div className="grid grid-cols-2 gap-3">
              <Field label="Name" required>
                <input
                  className="form-input"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  disabled={!!editingName}
                  placeholder="e.g. tg_main"
                  title={editingName ? "Cannot rename — remove + re-add to change name" : undefined}
                />
              </Field>
              <Field label="Kind" required>
                <select
                  className="form-input"
                  value={kind}
                  onChange={(e) => setKind(e.target.value)}
                >
                  {kinds.map((k) => (
                    <option key={k.kind} value={k.kind}>
                      {k.label}
                    </option>
                  ))}
                </select>
              </Field>
              <Field
                label={direction === "source" ? "Publish topic" : "Subscribe topic"}
                hint={
                  direction === "source"
                    ? "Agents subscribe to this topic to receive messages from this source."
                    : "Sink consumes any envelope on this topic — pick an agent's publish topic."
                }
                required
              >
                {direction === "sink" ? (
                  <>
                    <input
                      list="ext-sink-topic-suggestions"
                      className="form-input font-mono"
                      value={topic}
                      onChange={(e) => {
                        setTopic(e.target.value);
                        setTopicManuallyEdited(true);
                      }}
                      placeholder="agent.<name>.out  (or click ▾ to pick)"
                    />
                    <datalist id="ext-sink-topic-suggestions">
                      {agentPublishTopics.map((t) => (
                        <option key={t} value={t} />
                      ))}
                    </datalist>
                    {agentPublishTopics.length > 0 && (
                      <div className="mt-1 flex flex-wrap gap-1">
                        {agentPublishTopics.map((t) => (
                          <button
                            key={t}
                            type="button"
                            onClick={() => {
                              setTopic(t);
                              setTopicManuallyEdited(true);
                            }}
                            className={
                              "text-[10px] font-mono px-1.5 py-0.5 rounded border " +
                              (t === topic
                                ? "bg-blue-100 border-blue-300 text-blue-700"
                                : "bg-slate-50 border-slate-200 text-slate-600 hover:bg-slate-100")
                            }
                          >
                            {t}
                          </button>
                        ))}
                      </div>
                    )}
                  </>
                ) : (
                  <input
                    className="form-input font-mono"
                    value={topic}
                    onChange={(e) => {
                      setTopic(e.target.value);
                      setTopicManuallyEdited(true);
                    }}
                    placeholder="ext.<name>.in"
                  />
                )}
              </Field>
            </div>

            {currentKind && (
              <p className="text-[11px] text-slate-500 mt-1.5 mb-3">
                {currentKind.description}
              </p>
            )}

            {/* Dynamic config fields */}
            {currentKind && (
              <div className="space-y-2">
                {Object.entries(currentKind.fields).map(([key, schema]) => (
                  <div key={key}>
                    <DynamicField
                      fieldKey={key}
                      schema={schema}
                      value={config[key]}
                      onChange={(v) =>
                        setConfig((cur) => ({ ...cur, [key]: v }))
                      }
                    />
                    {direction === "sink" && key === "text_field"
                      && upstreamAgentForSink && (
                      <BodyFieldChips
                        agentKey={upstreamAgentForSink.key}
                        outputField={upstreamAgentForSink.outputField}
                        currentValue={(config.text_field as string) || ""}
                        onPick={(v) =>
                          setConfig((cur) => ({ ...cur, text_field: v }))
                        }
                      />
                    )}
                  </div>
                ))}
              </div>
            )}

            {error && (
              <p className="mt-2 text-xs text-rose-600">✗ {error}</p>
            )}
          </section>
        </div>

        <div className="px-5 py-3 border-t border-slate-200 flex justify-end gap-2">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm hover:bg-slate-100 rounded"
          >
            Close
          </button>
          <button
            disabled={readOnly || !valid || addMut.isPending}
            onClick={() => addMut.mutate()}
            className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400
                       text-white text-sm font-medium rounded
                       disabled:cursor-not-allowed"
            title={readOnly ? "Switch to event_driven mode first" : ""}
          >
            {addMut.isPending
              ? "Saving…"
              : editingName
              ? `Update ${direction}`
              : items.some((i) => i.name === name.trim())
              ? `Replace ${direction}`
              : `Add ${direction}`}
          </button>
        </div>
      </div>
    </>
  );
}

function ExternalRow({
  item, onEdit, onRemove, isEditing, disabled, removing,
}: {
  item: ExternalIOItem;
  isEditing: boolean;
  disabled: boolean;
  removing: boolean;
  onEdit: () => void;
  onRemove: () => void;
}) {
  return (
    <div className="px-3 py-2 flex items-center gap-3 group">
      <span
        className={`inline-block w-2 h-2 rounded-full ${
          item.health.running ? "bg-emerald-500" : "bg-slate-300"
        }`}
        title={item.health.running ? "running" : "stopped"}
      />
      <div className="flex-1 min-w-0">
        <div className="flex items-baseline gap-2 text-sm">
          <span className="font-mono font-medium">{item.name}</span>
          <span className="text-xs text-slate-500">{item.kind}</span>
          <span className="text-xs text-slate-400">·</span>
          <span className="text-xs font-mono text-slate-500 truncate">
            {item.topic}
          </span>
        </div>
        <div className="text-[11px] text-slate-500">
          {item.health.events_total} event(s)
          {item.health.last_event_at &&
            `, last: ${new Date(item.health.last_event_at).toLocaleTimeString()}`}
          {item.health.last_error && (
            <span className="text-rose-600 ml-2">
              error: {item.health.last_error}
            </span>
          )}
        </div>
      </div>
      <button
        onClick={onEdit}
        disabled={disabled}
        className={
          "text-xs px-2 py-1 rounded " +
          (isEditing
            ? "bg-blue-100 text-blue-700 font-medium"
            : "text-blue-600 hover:bg-blue-50 disabled:opacity-30 disabled:cursor-not-allowed")
        }
        title={
          disabled ? "Switch to event_driven to edit" :
          isEditing ? "Currently editing — fill the form below" : "Load into form to edit"
        }
      >
        {isEditing ? "✎ editing" : "Edit"}
      </button>
      <button
        onClick={onRemove}
        disabled={disabled || removing}
        className="text-xs text-rose-600 hover:bg-rose-50
                   disabled:opacity-30 disabled:cursor-not-allowed px-2 py-1 rounded"
        title={disabled ? "Switch to event_driven to remove" : "Remove this item"}
      >
        {removing ? "Removing…" : "Remove"}
      </button>
    </div>
  );
}

function DynamicField({
  fieldKey, schema, value, onChange,
}: {
  fieldKey: string;
  schema: ExternalKindMeta["fields"][string];
  value: unknown;
  onChange: (v: unknown) => void;
}) {
  const label = (
    <span>
      {schema.label}
      {schema.required && <span className="text-rose-500 ml-0.5">*</span>}
    </span>
  );
  if (schema.type === "bool") {
    return (
      <label className="flex items-center gap-2 text-sm">
        <input
          type="checkbox"
          checked={!!value}
          onChange={(e) => onChange(e.target.checked)}
        />
        {label}
        {schema.help && (
          <span className="text-[10px] text-slate-400">— {schema.help}</span>
        )}
      </label>
    );
  }
  if (schema.type === "code") {
    return (
      <Field label={label} hint={schema.help}>
        <textarea
          className="form-input font-mono text-xs"
          rows={10}
          spellCheck={false}
          value={(value as string) ?? ""}
          onChange={(e) => onChange(e.target.value)}
          placeholder={
            fieldKey === "script"
              ? "async def stream(ctx):\n    while True:\n        await asyncio.sleep(60)\n        yield {\"hello\": \"world\"}"
              : ""
          }
        />
      </Field>
    );
  }
  if (schema.type === "number") {
    return (
      <Field label={label} hint={schema.help}>
        <input
          type="number"
          className="form-input"
          value={(value as number | string) ?? ""}
          onChange={(e) =>
            onChange(e.target.value === "" ? "" : Number(e.target.value))
          }
        />
      </Field>
    );
  }
  return (
    <Field label={label} hint={schema.help}>
      <input
        type={schema.type === "secret" ? "password" : "text"}
        className="form-input font-mono"
        autoComplete="off"
        value={(value as string) ?? ""}
        onChange={(e) => onChange(e.target.value)}
      />
    </Field>
  );
}

function Field({
  label, hint, required, children,
}: { label: React.ReactNode; hint?: string; required?: boolean; children: React.ReactNode }) {
  return (
    <div>
      <label className="block text-xs font-medium text-slate-600 mb-1">
        {label}
        {required && <span className="text-rose-500 ml-0.5">*</span>}
      </label>
      {children}
      {hint && <p className="text-[10px] text-slate-400 mt-0.5">{hint}</p>}
    </div>
  );
}


/** Suggestion chips shown under the sink's "Body field" input.  We
 *  emit one chip per "obvious" path: ``<output_field>``, ``<output_field>.<sub>``
 *  (if the field is structured), and a fallback ``q`` for the original
 *  user input. The user clicks a chip to fill the input. */
function BodyFieldChips({
  agentKey, outputField, currentValue, onPick,
}: {
  agentKey: string;
  outputField: string;
  currentValue: string;
  onPick: (path: string) => void;
}) {
  // Common shapes we can suggest without knowing the actual payload:
  // 1. The agent's primary output_field (90% case)
  // 2. ``<output_field>.comment`` / ``.text`` for known dict outputs
  //    (best-effort; user can ignore if not applicable).
  const suggestions: { path: string; label: string }[] = [
    { path: outputField, label: `payload.${outputField}` },
    { path: `${outputField}.comment`, label: `payload.${outputField}.comment` },
    { path: `${outputField}.text`,    label: `payload.${outputField}.text` },
    { path: "q",                      label: "payload.q (original input)" },
  ];
  return (
    <div className="mt-1 flex flex-wrap items-center gap-1.5">
      <span className="text-[10px] text-slate-500">From agent <code className="font-mono">{agentKey}</code>:</span>
      {suggestions.map((s) => (
        <button
          key={s.path}
          type="button"
          onClick={() => onPick(s.path)}
          className={
            "text-[10px] font-mono px-1.5 py-0.5 rounded border " +
            (s.path === currentValue
              ? "bg-blue-100 border-blue-300 text-blue-700"
              : "bg-slate-50 border-slate-200 text-slate-600 hover:bg-slate-100")
          }
        >
          {s.label}
        </button>
      ))}
    </div>
  );
}
