import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { getAgentConfig, saveStartInputFields } from "../lib/api";
import {
  RenameRefactorDialog,
  type AffectedAgent,
} from "./RenameRefactorDialog";

interface Props {
  /** First agent's prompt — used to seed/suggest fields. */
  promptTemplate?: string | null;
  /** Workflow's first agent template key (for hint). */
  firstAgentKey?: string;
  /** Initial values (re-runs / pre-fill). Overrides prompt-derived seed. */
  initial?: Record<string, string>;
  /** Called when the user clicks Run. */
  onSubmit: (values: Record<string, unknown>) => void;
  /** Called whenever the user edits values — lets the parent
   *  trigger a Run from a remote button using the same input. */
  onValuesChange?: (values: Record<string, unknown>) => void;
  isSubmitting?: boolean;
  /** Workflow id — needed for the "save schema" PUT. */
  workflowId?: string;
  /** Currently-saved field names from the workflow. Used to compute
   *  the dirty state of the Save button. */
  savedFields?: string[];
  /** Workflow agents — scanned after a schema save for prompt references
   *  the new schema can't satisfy, so the user can be offered an
   *  auto-rename refactor. ``output_field`` lets us treat a field a
   *  downstream agent produces as "available". */
  workflowAgents?: { template_key: string; output_field?: string | null }[];
}

/** A single editable field row. */
interface Field {
  /** Stable row id, used as React key. */
  id: string;
  /** Payload field name (the ``X`` in ``payload.X``). */
  key: string;
  value: string;
}

/**
 * Editable input form for triggering a Run.
 *
 * Behaviour:
 *   - Auto-seeds the field list from the first agent's prompt (parses
 *     ``{{ payload.X }}`` references) on first render, OR from the
 *     ``initial`` prop if provided.
 *   - User can ADD fields (any name they choose) and REMOVE fields
 *     (down to a minimum of 1 — the form is never allowed to be empty).
 *   - User can rename a field's key inline.
 *   - Validates: non-empty keys, unique keys, valid JS-identifier-ish
 *     names (the runtime ultimately just dict-indexes by string so
 *     anything non-empty works, but we surface a soft warning if the
 *     name has unusual characters).
 *   - "Switch to JSON" toggle for users who want to paste a literal
 *     JSON object (e.g. with nested values).
 *
 * The form starts as: one row per detected prompt variable, OR a
 * single empty ``q`` row if the prompt has no ``payload.X`` references.
 */
export function PromptInputForm({
  promptTemplate, firstAgentKey, initial, onSubmit, onValuesChange, isSubmitting,
  workflowId, savedFields, workflowAgents = [],
}: Props) {
  const promptVars = useMemo(
    () => extractPayloadVariables(promptTemplate ?? ""),
    [promptTemplate],
  );

  // Seed fields once. After that the user owns the list.
  const [fields, setFields] = useState<Field[]>(
    () => seedFields(initial, savedFields, promptVars),
  );
  const seededRef = useRef(true);

  // When the prompt changes (user edits agent prompt), append any new
  // referenced variable that isn't already in the form. Don't ever
  // remove user-added fields — they may be intentional.
  useEffect(() => {
    if (seededRef.current) {
      seededRef.current = false;
      return;
    }
    setFields((prev) => {
      const have = new Set(prev.map((f) => f.key));
      const additions = promptVars
        .filter((v) => !have.has(v))
        .map((v) => ({ id: newId(), key: v, value: "" }));
      return additions.length ? [...prev, ...additions] : prev;
    });
  }, [promptVars]);

  const [rawMode, setRawMode] = useState(false);
  const [rawJson, setRawJson] = useState(
    () => JSON.stringify(buildObject(seedFields(initial, savedFields, promptVars)).obj, null, 2),
  );
  const [submitError, setSubmitError] = useState<string | null>(null);

  // Field-level validation (computed every render).
  const errors = useMemo(() => validate(fields), [fields]);

  // Forward parsed values to parent so the toolbar Run button can
  // re-use the same input without re-prompting the user.
  useEffect(() => {
    if (!onValuesChange) return;
    if (rawMode) {
      try { onValuesChange(JSON.parse(rawJson)); }
      catch { /* parse error — submit will surface it */ }
      return;
    }
    if (errors.size === 0) {
      onValuesChange(buildObject(fields).obj);
    }
  }, [fields, rawJson, rawMode, errors, onValuesChange]);

  const minOneFieldGuard = fields.length <= 1;

  const addField = () =>
    setFields((prev) => [
      ...prev,
      { id: newId(), key: suggestNextKey(prev), value: "" },
    ]);

  const removeField = (id: string) => {
    setFields((prev) => (prev.length <= 1 ? prev : prev.filter((f) => f.id !== id)));
  };

  const updateField = (id: string, patch: Partial<Pick<Field, "key" | "value">>) => {
    setFields((prev) => prev.map((f) => (f.id === id ? { ...f, ...patch } : f)));
  };

  const submit = () => {
    setSubmitError(null);
    if (rawMode) {
      try {
        onSubmit(JSON.parse(rawJson));
      } catch (e) {
        setSubmitError(`Invalid JSON: ${(e as Error).message}`);
      }
      return;
    }
    if (errors.size > 0) {
      setSubmitError("Fix the highlighted fields before running.");
      return;
    }
    onSubmit(buildObject(fields).obj);
  };

  // Soft hint: prompt references a var that's not in the form.
  const promptVarsNotInForm = useMemo(() => {
    const have = new Set(fields.map((f) => f.key));
    return promptVars.filter((v) => !have.has(v));
  }, [fields, promptVars]);

  // Compute dirty state for the "Save schema" button: are the current
  // valid keys different from what's saved on the workflow?
  const currentKeys = useMemo(
    () => fields.map((f) => f.key).filter((k) => k.trim()),
    [fields],
  );
  const isDirty = useMemo(() => {
    if (!savedFields) return false;
    if (currentKeys.length !== savedFields.length) return true;
    for (let i = 0; i < currentKeys.length; i++) {
      if (currentKeys[i] !== savedFields[i]) return true;
    }
    return false;
  }, [currentKeys, savedFields]);

  const qc = useQueryClient();
  const [savedFlash, setSavedFlash] = useState(false);

  // Refactor dialog state — populated after a save that removes
  // referenced fields.
  const [refactorState, setRefactorState] = useState<{
    removed: string[];
    newFields: string[];
    affected: AffectedAgent[];
  } | null>(null);

  const saveMutation = useMutation({
    mutationFn: () => {
      if (!workflowId) throw new Error("no workflow id");
      return saveStartInputFields(workflowId, currentKeys);
    },
    onSuccess: async () => {
      if (workflowId) {
        await qc.refetchQueries({ queryKey: ["workflow", workflowId] });
      }
      setSavedFlash(true);
      setTimeout(() => setSavedFlash(false), 1800);

      // ---- Refactor: scan agents for prompt refs that the new schema can
      // no longer satisfy. We flag ANY `{{ payload.<field> }}` that is not
      // produced by the new start schema OR by an upstream agent's output —
      // not just fields that were explicitly *removed* this save. This is
      // what catches a prompt referencing `payload.topic` when the schema
      // is `[intent]` (the field was never the "removed" one, so the old
      // removed-only scan missed it and it blew up at runtime). ----
      if (!workflowId || workflowAgents.length === 0) return;

      // A field is "available" if it's a start-input field or any agent's
      // output_field (preserved through the merged payload downstream).
      const available = new Set<string>(currentKeys);
      for (const a of workflowAgents) {
        if (a.output_field) available.add(a.output_field);
      }

      // Fetch each agent's live config so we can check prompt + system_prompt.
      const configs = await Promise.all(
        workflowAgents.map((a) =>
          getAgentConfig(workflowId, a.template_key).catch(() => null),
        ),
      );
      const affected: AffectedAgent[] = [];
      const danglingAll = new Set<string>();
      for (const cfg of configs) {
        if (!cfg) continue;
        const refs = Array.from(new Set([
          ...extractPayloadDotRefs(cfg.prompt ?? ""),
          ...extractPayloadDotRefs(cfg.system_prompt ?? ""),
        ]));
        const dangling = refs.filter((r) => !available.has(r));
        if (dangling.length > 0) {
          dangling.forEach((d) => danglingAll.add(d));
          affected.push({
            template_key: cfg.template_key,
            config: cfg,
            referenced: dangling,
          });
        }
      }
      if (affected.length > 0) {
        setRefactorState({
          removed: Array.from(danglingAll),
          newFields: [...currentKeys],
          affected,
        });
      }
    },
    onError: (e) => {
      alert(`Failed to save input schema: ${(e as Error).message}`);
    },
  });

  return (
    <>
      {/* The dialog is always rendered but invisible unless open. */}
      {refactorState && workflowId && (
        <RenameRefactorDialog
          open={!!refactorState}
          workflowId={workflowId}
          removed={refactorState.removed}
          newFields={refactorState.newFields}
          affected={refactorState.affected}
          onClose={async () => {
            // Refresh affected agents' configs so AddAgentModal /
            // AgentDrawer reads the rewritten prompts.
            for (const a of refactorState.affected) {
              qc.invalidateQueries({
                queryKey: ["agent-config", workflowId, a.template_key],
              });
            }
            setRefactorState(null);
          }}
        />
      )}

      <div className="bg-white border border-slate-200 rounded-lg p-4">
        <div className="flex items-center justify-between mb-3">
          <span className="text-xs font-medium text-slate-600">
            {rawMode
              ? "Input (raw JSON)"
              : `Input — ${fields.length} field${fields.length === 1 ? "" : "s"}`}
            {!rawMode && firstAgentKey && (
              <span className="text-slate-400">
                {" "}· seeded from {firstAgentKey}'s prompt
              </span>
            )}
          </span>
          <button
            type="button"
            onClick={() => {
              if (!rawMode) {
                // Switching TO json: dump current fields.
                setRawJson(JSON.stringify(buildObject(fields).obj, null, 2));
              } else {
                // Switching FROM json: try to parse and rebuild fields.
                try {
                  const parsed = JSON.parse(rawJson);
                  if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
                    const next = Object.entries(parsed).map(([k, v]) => ({
                      id: newId(),
                      key: k,
                      value: typeof v === "string" ? v : JSON.stringify(v),
                    }));
                    setFields(next.length ? next : [{ id: newId(), key: "q", value: "" }]);
                  }
                } catch { /* leave fields as-is */ }
              }
              setRawMode((r) => !r);
            }}
            className="text-xs text-blue-600 hover:underline"
          >
            {rawMode ? "Switch to form" : "Switch to JSON"}
          </button>
        </div>

        {rawMode ? (
          <textarea
            value={rawJson}
            onChange={(e) => setRawJson(e.target.value)}
            spellCheck={false}
            rows={6}
            className="w-full font-mono text-xs border border-slate-300 rounded
                     px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-400"
          />
        ) : (
          <>
            <div className="space-y-1.5">
              {fields.map((f, i) => {
                const err = errors.get(f.id);
                return (
                  <div key={f.id} className="flex items-start gap-2">
                    <div className="flex-shrink-0 w-32">
                      <input
                        value={f.key}
                        onChange={(e) => updateField(f.id, { key: e.target.value })}
                        placeholder="field"
                        autoFocus={i === fields.length - 1 && f.key === "" && f.value === ""}
                        className={
                          "w-full text-xs font-mono border rounded px-2 py-1.5 " +
                          "focus:outline-none focus:ring-2 focus:ring-blue-400 " +
                          (err
                            ? "border-rose-400 ring-1 ring-rose-200"
                            : "border-slate-300")
                        }
                      />
                    </div>
                    <span className="text-slate-400 text-sm pt-1.5">=</span>
                    <input
                      value={f.value}
                      onChange={(e) => updateField(f.id, { value: e.target.value })}
                      placeholder={`value for ${f.key || "field"}…`}
                      className="flex-1 text-sm border border-slate-300 rounded
                               px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-blue-400"
                    />
                    <button
                      type="button"
                      onClick={() => removeField(f.id)}
                      disabled={minOneFieldGuard}
                      title={
                        minOneFieldGuard
                          ? "At least one field is required"
                          : `Remove field '${f.key || "(unnamed)"}'`
                      }
                      className="flex-shrink-0 px-2 py-1 text-rose-500 hover:bg-rose-50
                               rounded text-sm leading-none
                               disabled:opacity-30 disabled:cursor-not-allowed"
                    >
                      ✕
                    </button>
                  </div>
                );
              })}
            </div>

            {/* Per-row error messages — shown below the rows so they don't
                jiggle the layout. */}
            {Array.from(errors.values()).length > 0 && (
              <ul className="mt-1.5 text-[11px] text-rose-600 list-disc list-inside">
                {Array.from(new Set(errors.values())).map((msg) => (
                  <li key={msg}>{msg}</li>
                ))}
              </ul>
            )}

            <div className="mt-2 flex items-center gap-3 flex-wrap">
              <button
                type="button"
                onClick={addField}
                className="text-xs px-2 py-1 border border-dashed border-slate-300
                         rounded hover:bg-slate-50 text-slate-700"
              >
                + Add field
              </button>

              {workflowId && (
                <button
                  type="button"
                  onClick={() => saveMutation.mutate()}
                  disabled={
                    !isDirty ||
                    errors.size > 0 ||
                    saveMutation.isPending ||
                    currentKeys.length === 0
                  }
                  title={
                    errors.size > 0
                      ? "Fix the highlighted fields first"
                      : !isDirty
                        ? "These fields already match the saved schema"
                        : "Save these field names as the workflow's input schema. " +
                          "PromptEditor's Beginner mode will then suggest them as " +
                          "payload.<name> chips for downstream agents."
                  }
                  className="text-xs px-2 py-1 bg-emerald-600 hover:bg-emerald-700
                           text-white rounded
                           disabled:bg-slate-300 disabled:cursor-not-allowed
                           transition"
                >
                  {saveMutation.isPending ? "Saving…" : "💾 Save as input schema"}
                </button>
              )}

              {savedFlash && (
                <span className="text-xs text-emerald-700">✓ Saved</span>
              )}
              {isDirty && !savedFlash && savedFields && (
                <span className="text-[11px] text-amber-700">
                  Modified · saved schema:{" "}
                  <code className="font-mono">
                    [{savedFields.join(", ")}]
                  </code>
                </span>
              )}

              {promptVarsNotInForm.length > 0 && (
                <span className="text-[11px] text-amber-700">
                  Prompt references{" "}
                  {promptVarsNotInForm.map((v, i) => (
                    <span key={v}>
                      <button
                        type="button"
                        onClick={() =>
                          setFields((p) => [...p, { id: newId(), key: v, value: "" }])
                        }
                        className="font-mono underline hover:text-amber-900"
                        title={`Add field '${v}'`}
                      >
                        payload.{v}
                      </button>
                      {i < promptVarsNotInForm.length - 1 ? ", " : ""}
                    </span>
                  ))}
                  {" "}— click to add.
                </span>
              )}
            </div>
          </>
        )}

        {submitError && (
          <p className="text-rose-600 text-xs mt-2">{submitError}</p>
        )}

        <button
          onClick={submit}
          disabled={isSubmitting || (!rawMode && errors.size > 0)}
          className="mt-3 w-full px-4 py-2 bg-blue-600 hover:bg-blue-700
                     disabled:bg-blue-400 text-white text-sm font-medium
                     rounded transition flex items-center justify-center gap-1.5"
        >
          {isSubmitting ? "Starting…" : <>▶ Run</>}
        </button>
      </div>
    </>
  );
}

// ----------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------

/** Cheap unique id for React keys — collision-resistant enough for a
 *  handful of fields per run. */
function newId(): string {
  return `f_${Math.random().toString(36).slice(2, 9)}_${Date.now() % 100000}`;
}

/** Build the initial seed: prefer ``initial`` if given, else one row
 *  per prompt variable, else a single ``q`` row. */
function seedFields(
  initial: Record<string, string> | undefined,
  savedFields: string[] | undefined,
  promptVars: string[],
): Field[] {
  // 1) Explicit re-run / pre-fill values win.
  if (initial && Object.keys(initial).length > 0) {
    return Object.entries(initial).map(([k, v]) => ({
      id: newId(),
      key: k,
      value: typeof v === "string" ? v : JSON.stringify(v),
    }));
  }
  // 2) The PERSISTED input schema is authoritative — seed straight from it.
  //    (Parsing it back out of a synthesized prompt loses fields named
  //    `topic` / `event` / `payload`, which `extractPayloadVariables`
  //    treats as reserved — that's the bug where a saved `[topic]` schema
  //    fell back to the default `q`.)
  if (savedFields && savedFields.length > 0) {
    return savedFields.map((k) => ({ id: newId(), key: k, value: "" }));
  }
  // 3) Otherwise infer from the prompt's `{{ payload.X }}` references.
  if (promptVars.length > 0) {
    return promptVars.map((v) => ({ id: newId(), key: v, value: "" }));
  }
  // 4) Last resort: the conventional single `q` field.
  return [{ id: newId(), key: "q", value: "" }];
}

/** Suggest a name for a brand-new field that doesn't collide. */
function suggestNextKey(existing: Field[]): string {
  const have = new Set(existing.map((f) => f.key));
  if (!have.has("q")) return "q";
  for (let i = 1; i < 100; i++) {
    const candidate = `field_${i}`;
    if (!have.has(candidate)) return candidate;
  }
  return "";
}

/** Convert the field array to a plain object. Returns the obj plus
 *  the list of duplicate keys (for caller-side validation). */
function buildObject(fields: Field[]): {
  obj: Record<string, string>;
  duplicates: string[];
} {
  const obj: Record<string, string> = {};
  const duplicates: string[] = [];
  for (const f of fields) {
    if (!f.key.trim()) continue;
    if (Object.prototype.hasOwnProperty.call(obj, f.key)) {
      duplicates.push(f.key);
    } else {
      obj[f.key] = f.value;
    }
  }
  return { obj, duplicates };
}

/** Returns a Map(rowId → error message) for any field that's invalid. */
function validate(fields: Field[]): Map<string, string> {
  const errs = new Map<string, string>();
  const seen = new Set<string>();
  const duplicates = new Set<string>();
  for (const f of fields) {
    if (seen.has(f.key) && f.key.trim()) duplicates.add(f.key);
    seen.add(f.key);
  }
  for (const f of fields) {
    if (!f.key.trim()) {
      errs.set(f.id, "Field name cannot be empty.");
    } else if (duplicates.has(f.key)) {
      errs.set(f.id, `Duplicate field name '${f.key}'.`);
    } else if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(f.key)) {
      errs.set(
        f.id,
        `'${f.key}' has unusual characters — use letters/digits/underscore for safe Jinja2 access.`,
      );
    }
  }
  return errs;
}

// ----------------------------------------------------------------
// Template parsing
// ----------------------------------------------------------------

/**
 * Pull out every distinct ``payload.X`` reference from a Jinja2
 * template string. Handles both ``{{ payload.foo }}`` and bare
 * ``{{ foo }}`` (since the renderer also exposes payload keys at top
 * level).
 */
function extractPayloadVariables(template: string): string[] {
  const seen = new Set<string>();
  const re = /\{\{\s*(?:payload\.)?([a-zA-Z_][a-zA-Z0-9_]*)\s*[}|]/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(template)) !== null) {
    const name = m[1];
    if (name === "payload" || name === "event" || name === "topic") continue;
    seen.add(name);
  }
  return Array.from(seen);
}

/** Extract every explicit ``{{ payload.<field> }}`` reference. Unlike
 *  {@link extractPayloadVariables}, this does NOT skip ``topic`` /
 *  ``event`` / ``payload`` — the ``payload.`` prefix makes the reference
 *  unambiguous, so a payload field literally named ``topic`` IS counted
 *  (the case that silently broke at runtime). */
export function extractPayloadDotRefs(template: string): string[] {
  const seen = new Set<string>();
  const re = /\{\{\s*payload\.([a-zA-Z_][a-zA-Z0-9_]*)\s*[}|]/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(template)) !== null) seen.add(m[1]);
  return Array.from(seen);
}
