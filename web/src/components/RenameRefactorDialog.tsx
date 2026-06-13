import { useEffect, useMemo, useState } from "react";
import { patchAgentConfig, type AgentConfig } from "../lib/api";

/** A single agent that has at least one prompt referencing a removed field. */
export interface AffectedAgent {
  template_key: string;
  /** Live handler config (we fetched it before opening the dialog so
   *  we can apply replacements to ``prompt`` and ``system_prompt``). */
  config: AgentConfig;
  /** Which removed fields show up in this agent's prompts. */
  referenced: string[];
}

interface Props {
  open: boolean;
  workflowId: string;
  /** Field names that disappeared from the schema. */
  removed: string[];
  /** Field names available in the new schema (for the "replace with" picker). */
  newFields: string[];
  /** Pre-computed list of agents that reference any removed field. */
  affected: AffectedAgent[];
  onClose: () => void;
}

/** Special sentinel for "leave references as-is" in the per-field picker. */
const KEEP = "__keep__";

/**
 * Modal that appears after the user saves a new trigger-input schema
 * which removes/renames fields that downstream agent prompts referenced.
 *
 * UX:
 *   - One row per removed field with a dropdown of new fields
 *     (plus "Leave as-is" option). Defaults to the only new field
 *     when the count matches 1-to-1.
 *   - Below the mappings, a list of affected agents with which old
 *     fields they reference (so the user knows what's at stake).
 *   - "Apply" runs PATCH on every affected agent in parallel,
 *     rewriting both ``prompt`` and ``system_prompt`` via regex
 *     replacement of ``{{ payload.<old> }}`` → ``{{ payload.<new> }}``.
 *   - "Skip" closes without changes (user will fix manually).
 */
export function RenameRefactorDialog({
  open, workflowId, removed, newFields, affected, onClose,
}: Props) {
  // mapping: removedField -> newField | KEEP
  const initialMapping = useMemo<Record<string, string>>(() => {
    const m: Record<string, string> = {};
    for (const old of removed) {
      // Default: pick the first new field if there's exactly one new
      // field per removed (most common rename case). Otherwise KEEP.
      if (newFields.length === 1) m[old] = newFields[0];
      else m[old] = KEEP;
    }
    return m;
  }, [removed, newFields]);

  const [mapping, setMapping] = useState<Record<string, string>>(initialMapping);
  useEffect(() => setMapping(initialMapping), [initialMapping]);

  const [applying, setApplying] = useState(false);
  const [results, setResults] = useState<
    { key: string; ok: boolean; error?: string }[] | null
  >(null);

  if (!open) return null;

  const willChange = Object.values(mapping).some((v) => v !== KEEP);

  const apply = async () => {
    setApplying(true);
    setResults(null);
    const out: { key: string; ok: boolean; error?: string }[] = [];

    // Patch each affected agent. Sequential keeps the order stable in
    // the result list — this is small N (typically 1-5) so latency is fine.
    for (const a of affected) {
      try {
        const newPrompt = rewriteTemplate(a.config.prompt ?? "", mapping);
        const newSystem = rewriteTemplate(a.config.system_prompt ?? "", mapping);

        const patch: Record<string, string | null> = {};
        if (newPrompt !== (a.config.prompt ?? "")) {
          patch.prompt = newPrompt || null;
        }
        if (newSystem !== (a.config.system_prompt ?? "")) {
          patch.system_prompt = newSystem || null;
        }
        if (Object.keys(patch).length === 0) {
          out.push({ key: a.template_key, ok: true });
          continue;
        }

        await patchAgentConfig(workflowId, a.template_key, patch);
        out.push({ key: a.template_key, ok: true });
      } catch (e) {
        out.push({
          key: a.template_key,
          ok: false,
          error: (e as Error).message,
        });
      }
    }
    setResults(out);
    setApplying(false);

    // Auto-close on full success after a short pause to flash success.
    if (out.every((r) => r.ok)) {
      setTimeout(() => onClose(), 1200);
    }
  };

  return (
    <>
      <div className="fixed inset-0 bg-slate-900/40 z-50" onClick={onClose} />
      <div
        className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50
                   w-[640px] max-w-[95vw] max-h-[90vh] overflow-y-auto thin-scroll
                   bg-white rounded-lg shadow-2xl"
      >
        <div className="px-5 py-4 border-b border-slate-200">
          <h2 className="text-lg font-semibold">
            Update agent prompts?
          </h2>
          <p className="text-xs text-slate-500 mt-1">
            You changed the workflow's trigger-input schema —{" "}
            <strong>{affected.length}</strong> agent(s) reference removed
            field name(s) in their prompts. Pick a replacement per field
            below, or skip to fix manually.
          </p>
        </div>

        <div className="px-5 py-4 space-y-4">
          {/* ---- Mapping rows ---- */}
          <div>
            <div className="text-xs font-semibold text-slate-700 uppercase tracking-wide mb-1.5">
              Field replacements
            </div>
            <div className="space-y-1.5">
              {removed.map((old) => (
                <div key={old} className="flex items-center gap-2 text-sm">
                  <code className="font-mono px-2 py-1 bg-rose-50
                                   border border-rose-200 rounded text-rose-700
                                   text-xs flex-shrink-0">
                    payload.{old}
                  </code>
                  <span className="text-slate-400 text-xs">→</span>
                  <select
                    value={mapping[old] ?? KEEP}
                    onChange={(e) =>
                      setMapping((m) => ({ ...m, [old]: e.target.value }))
                    }
                    className="form-input text-xs flex-1"
                    disabled={applying}
                  >
                    <option value={KEEP}>Leave as-is (keep payload.{old})</option>
                    {newFields.map((nf) => (
                      <option key={nf} value={nf}>
                        Replace with payload.{nf}
                      </option>
                    ))}
                  </select>
                </div>
              ))}
            </div>
          </div>

          {/* ---- Affected agents ---- */}
          <div>
            <div className="text-xs font-semibold text-slate-700 uppercase tracking-wide mb-1.5">
              Affected agents ({affected.length})
            </div>
            <ul className="space-y-1 text-xs">
              {affected.map((a) => {
                const r = results?.find((x) => x.key === a.template_key);
                return (
                  <li
                    key={a.template_key}
                    className="flex items-start gap-2 px-2 py-1.5 rounded bg-slate-50"
                  >
                    <code className="font-mono font-semibold flex-shrink-0">
                      {a.template_key}
                    </code>
                    <span className="text-slate-500 flex-1">
                      references{" "}
                      {a.referenced.map((f, i) => (
                        <span key={f}>
                          <code className="font-mono">payload.{f}</code>
                          {i < a.referenced.length - 1 ? ", " : ""}
                        </span>
                      ))}
                    </span>
                    {r?.ok && (
                      <span className="text-emerald-600 flex-shrink-0">✓</span>
                    )}
                    {r?.ok === false && (
                      <span
                        className="text-rose-600 flex-shrink-0"
                        title={r.error}
                      >
                        ✗
                      </span>
                    )}
                  </li>
                );
              })}
            </ul>
          </div>

          {/* ---- Result summary ---- */}
          {results && results.some((r) => !r.ok) && (
            <div className="text-xs text-rose-700 bg-rose-50 border border-rose-200 p-2 rounded">
              Some agents failed to update. Check the ✗ icons for details.
              You can close and retry, or fix manually.
            </div>
          )}
        </div>

        <div className="flex justify-end gap-2 px-5 py-3 border-t border-slate-200">
          <button
            onClick={onClose}
            disabled={applying}
            className="px-4 py-2 text-sm hover:bg-slate-100 rounded disabled:opacity-50"
          >
            {results?.every((r) => r.ok) ? "Close" : "Skip (fix manually)"}
          </button>
          <button
            onClick={apply}
            disabled={applying || !willChange || !!results?.every((r) => r.ok)}
            className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-300
                       text-white text-sm font-medium rounded"
            title={
              !willChange
                ? "Pick at least one replacement (or close to skip)"
                : ""
            }
          >
            {applying ? "Updating…" : "Apply to all"}
          </button>
        </div>
      </div>
    </>
  );
}

// ----------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------

/** Return the regex that matches ``{{ payload.<field> }}`` references
 *  with surrounding whitespace tolerated. We deliberately do NOT match
 *  bare ``{{ field }}`` form to avoid false positives — only the
 *  ``payload.`` form is unambiguously a payload reference. */
function referenceRegex(field: string): RegExp {
  // Escape regex metachars in the field name (defensive — our validator
  // already restricts to identifier chars).
  const escaped = field.replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&");
  return new RegExp(
    `\\{\\{\\s*payload\\.${escaped}(\\s*[}|])`,
    "g",
  );
}

/** Replace ``{{ payload.<old> }}`` with ``{{ payload.<new> }}`` in the
 *  template string, for every ``old → new`` entry in mapping. Mapping
 *  values equal to ``__keep__`` (KEEP sentinel) are skipped. */
export function rewriteTemplate(
  template: string,
  mapping: Record<string, string>,
): string {
  let out = template;
  for (const [old, replacement] of Object.entries(mapping)) {
    if (replacement === KEEP) continue;
    out = out.replace(referenceRegex(old), `{{ payload.${replacement}$1`);
  }
  return out;
}

/** Scan a prompt for which removed fields it references. Empty list
 *  means the prompt is unaffected. */
export function findReferencedFields(
  template: string,
  candidates: string[],
): string[] {
  const out: string[] = [];
  for (const f of candidates) {
    if (referenceRegex(f).test(template)) out.push(f);
  }
  return out;
}
