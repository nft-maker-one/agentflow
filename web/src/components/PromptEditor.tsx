import { useMemo, useRef, useState } from "react";
import type { WorkflowDetail } from "../lib/api";

interface Props {
  /** Current prompt template (Jinja2 source). */
  value: string;
  /** Setter from parent. */
  onChange: (next: string) => void;
  /** Subscribe topics this agent listens to — drives variable picker. */
  subscribeTopics: string[];
  /** Other agents in the workflow (sources of upstream output_field). */
  workflow: WorkflowDetail;
  /** Whether the agent is auto-wired from __start__ (controls `q` hint). */
  connectStart?: boolean;
  /** External sources whose publish_topic the agent might subscribe
   *  to — surfaces their ``output_field`` as a variable chip. */
  externalSources?: { name: string; kind: string; topic: string; config: Record<string, unknown> }[];
  /** Display label in form (default: "Prompt template"). */
  label?: string;
  rows?: number;
}

interface Variable {
  /** Visible chip label (the dot-path / bracket-path the user sees). */
  label: string;
  /** Exact text inserted at the cursor (with the {{ }} braces). */
  insert: string;
  /** Where this field comes from (e.g. "tagger", "start input"). */
  source: string;
  /** Tooltip explaining the choice. */
  hint: string;
  /** Whether this is a per-source ``_inputs`` accessor (visual cue). */
  ambiguous: boolean;
}

/**
 * Prompt template editor with Pro / Beginner modes.
 *
 * Beginner mode analyzes the agent's subscribe topics + the workflow
 * agents to infer the available ``payload.<field>`` variables and
 * exposes them as click-to-insert chips.
 *
 * Pro mode is a plain textarea with a brief reminder of the available
 * Jinja2 variables.
 *
 * The actual stored value is always Jinja2 — beginner mode just
 * helps users construct it without typing the curly braces.
 *
 * **Field-name collision handling:** when multiple upstream agents
 * write a field with the same name (e.g. both have
 * ``output_field="result"``), a flat ``payload.result`` would only
 * carry the last writer's value. The picker detects the collision
 * and emits **per-source** chips of the form
 * ``payload._inputs['<topic>'].result`` so every contribution is
 * distinctly accessible.
 */
export function PromptEditor({
  value, onChange, subscribeTopics, workflow,
  connectStart = false,
  externalSources = [],
  label = "Prompt template (Jinja2)",
  rows = 5,
}: Props) {
  const [mode, setMode] = useState<"beginner" | "pro">("beginner");
  const textareaRef = useRef<HTMLTextAreaElement | null>(null);

  const variables = useMemo(
    () => deriveVariables(subscribeTopics, workflow, connectStart, externalSources),
    [subscribeTopics, workflow, connectStart, externalSources],
  );

  const insertAtCursor = (text: string) => {
    const ta = textareaRef.current;
    if (!ta) {
      onChange(value + text);
      return;
    }
    const start = ta.selectionStart ?? value.length;
    const end = ta.selectionEnd ?? value.length;
    const next = value.slice(0, start) + text + value.slice(end);
    onChange(next);
    requestAnimationFrame(() => {
      ta.focus();
      const pos = start + text.length;
      ta.setSelectionRange(pos, pos);
    });
  };

  // Split chips into "clean" (single-source) and "ambiguous" (per-source
  // _inputs accessors) so the user can see at a glance which fields
  // had a name collision.
  const cleanVars = variables.filter((v) => !v.ambiguous);
  const ambiguousVars = variables.filter((v) => v.ambiguous);

  return (
    <div>
      <div className="flex items-center justify-between mb-1">
        <label className="block text-xs font-medium text-slate-600">
          {label}
        </label>
        <div className="text-xs">
          <button
            type="button"
            onClick={() => setMode("beginner")}
            className={
              "px-2 py-0.5 rounded-l border " +
              (mode === "beginner"
                ? "bg-blue-600 text-white border-blue-600"
                : "bg-white text-slate-600 border-slate-300 hover:bg-slate-50")
            }
          >
            Beginner
          </button>
          <button
            type="button"
            onClick={() => setMode("pro")}
            className={
              "px-2 py-0.5 rounded-r border-y border-r " +
              (mode === "pro"
                ? "bg-blue-600 text-white border-blue-600"
                : "bg-white text-slate-600 border-slate-300 hover:bg-slate-50")
            }
          >
            Pro (Jinja2)
          </button>
        </div>
      </div>

      <textarea
        ref={textareaRef}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={
          mode === "beginner"
            ? "Type instructions in plain text. Click a variable below to insert it."
            : "Summarize: {{ payload.q }}"
        }
        rows={rows}
        className="form-input font-mono text-xs"
      />

      {mode === "beginner" && (
        <div className="mt-2 bg-emerald-50 border border-emerald-200 rounded p-2.5">
          <div className="text-[11px] font-semibold text-emerald-900 mb-1.5">
            Click a variable to insert it at the cursor
          </div>
          {variables.length === 0 ? (
            <p className="text-xs text-slate-500 italic">
              No upstream variables detected. Add a subscribe topic
              that matches another agent's output, or check{" "}
              <code className="font-mono bg-slate-200 px-1 rounded">
                Wire from __start__
              </code>{" "}
              for the user input.
            </p>
          ) : (
            <>
              <div className="flex flex-wrap gap-1.5">
                {cleanVars.map((v) => (
                  <button
                    key={`${v.source}|${v.label}`}
                    type="button"
                    onClick={() => insertAtCursor(v.insert)}
                    title={v.hint}
                    className="inline-flex items-center gap-1 px-2 py-0.5
                               bg-white hover:bg-emerald-100 border border-emerald-300
                               rounded text-xs"
                  >
                    <code className="font-mono text-emerald-800">
                      {v.label}
                    </code>
                    <span className="text-[10px] text-slate-500">
                      ({v.source})
                    </span>
                  </button>
                ))}
              </div>

              {ambiguousVars.length > 0 && (
                <div className="mt-2 pt-2 border-t border-emerald-200">
                  <div className="text-[10px] text-amber-900 mb-1">
                    ⚠ <strong>Name collisions</strong> — multiple upstream
                    agents have a field with the same name. The flat
                    <code className="font-mono"> payload.&lt;name&gt; </code>
                    would only carry the last writer's value, so use these
                    per-source accessors instead:
                  </div>
                  <div className="flex flex-wrap gap-1.5">
                    {ambiguousVars.map((v) => (
                      <button
                        key={`${v.source}|${v.label}`}
                        type="button"
                        onClick={() => insertAtCursor(v.insert)}
                        title={v.hint}
                        className="inline-flex items-center gap-1 px-2 py-0.5
                                   bg-amber-50 hover:bg-amber-100 border border-amber-300
                                   rounded text-xs"
                      >
                        <code className="font-mono text-amber-900 truncate max-w-[300px]">
                          {v.label}
                        </code>
                        <span className="text-[10px] text-slate-500">
                          ({v.source})
                        </span>
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </>
          )}
          <div className="mt-2 text-[10px] text-emerald-800">
            Below, the editor still stores Jinja2 — switch to{" "}
            <strong>Pro</strong> any time to write conditional logic
            (<code className="font-mono">{"{% if ... %}"}</code>),
            filters, or loops.
          </div>
        </div>
      )}

      {mode === "pro" && (
        <p className="text-[10px] text-slate-500 mt-1">
          Variables: <code>payload.&lt;field&gt;</code>,{" "}
          <code>event</code>, <code>topic</code>. Filters:{" "}
          <code>{"{{ payload.x | upper }}"}</code>. Conditionals:{" "}
          <code>{"{% if ... %}...{% endif %}"}</code>. Per-source
          access for fan-in agents:{" "}
          <code>{"{{ payload._inputs['agent.x.out'].field }}"}</code>.
        </p>
      )}
    </div>
  );
}

// ----------------------------------------------------------------
// Variable derivation
// ----------------------------------------------------------------

interface Contribution {
  agentKey: string;
  field: string;
  /** The publish topic that carried this contribution into the merge. */
  topic: string;
}

/**
 * Derive available variables from subscribe topics.
 *
 * Walks upstream from each subscribe topic, collecting (agent, field,
 * topic) triples. Then groups by field name:
 *
 *   - **Single source** → emit ``payload.<field>``
 *   - **Multiple sources (collision)** → emit one chip per source as
 *     ``payload._inputs['<topic>'].<field>`` — the runtime keeps every
 *     upstream payload addressable in the ``_inputs`` map even when
 *     the flat merge clobbered same-named keys.
 */
function deriveVariables(
  subscribeTopics: string[],
  workflow: WorkflowDetail,
  connectStart: boolean,
  externalSources: { name: string; kind: string; topic: string; config: Record<string, unknown> }[],
): Variable[] {
  // Walk upstream once with a global visited set so each agent's
  // contribution is recorded at most once.
  const visited = new Set<string>();
  const collected: Contribution[] = [];

  // For each agent, find which of OUR direct subscribe topics
  // carries that agent's payload (transitively). This lets us emit
  // the ``_inputs[<topic>]`` accessor pointing at the right key.
  const directTopicForAgent = new Map<string, string>();

  const walk = (topic: string, directTopic: string) => {
    for (const agent of workflow.agents) {
      if (!agent.publish.includes(topic)) continue;
      if (visited.has(agent.template_key)) continue;
      visited.add(agent.template_key);

      // Anchor this agent's contribution to the direct subscribe
      // topic of OUR aggregator (which is what _inputs is keyed by).
      directTopicForAgent.set(agent.template_key, directTopic);

      if (agent.output_field) {
        collected.push({
          agentKey: agent.template_key,
          field: agent.output_field,
          topic: directTopic,
        });
      }
      for (const upTopic of agent.subscribe) {
        walk(upTopic, directTopic);
      }
    }
  };

  for (const sub of subscribeTopics) {
    walk(sub, sub);
  }

  // Inject contributions from external sources whose publish topic
  // matches any of OUR subscribe topics.  Each source's output_field
  // shows up just like an agent's output_field would.
  for (const sub of subscribeTopics) {
    for (const s of externalSources) {
      if (s.topic !== sub) continue;
      const field = (s.config.output_field as string) || "text";
      collected.push({
        agentKey: `🔌 ${s.name}`,
        field,
        topic: sub,
      });
    }
  }

  // Group by field name to detect collisions.
  const byField = new Map<string, Contribution[]>();
  for (const c of collected) {
    if (!byField.has(c.field)) byField.set(c.field, []);
    byField.get(c.field)!.push(c);
  }

  const out: Variable[] = [];

  // ① The convention `q` for user input from __start__.
  if (connectStart || subscribeTopics.some((t) => /\.in(\.|$)/.test(t))) {
    const startFields = workflow.start_input_fields?.length
      ? workflow.start_input_fields
      : ["q"];
    for (const name of startFields) {
      out.push({
        label: `payload.${name}`,
        insert: `{{ payload.${name} }}`,
        source: "start input",
        hint:
          `User-supplied input from __start__ (preserved through the chain). ` +
          `Configured as the workflow's trigger-input schema.`,
        ambiguous: false,
      });
    }
  }

  // ② One chip per (field, source) tuple. If a field has 2+ sources,
  //    emit per-source ``_inputs`` accessors instead of the flat form.
  for (const [field, group] of byField) {
    if (group.length === 1) {
      const c = group[0];
      out.push({
        label: `payload.${field}`,
        insert: `{{ payload.${field} }}`,
        source: c.agentKey,
        hint: `Field '${field}' added by '${c.agentKey}'.`,
        ambiguous: false,
      });
    } else {
      for (const c of group) {
        out.push({
          label: `payload._inputs['${c.topic}'].${field}`,
          insert: `{{ payload._inputs['${c.topic}'].${field} }}`,
          source: c.agentKey,
          hint:
            `Multiple upstream agents have a field named '${field}'. ` +
            `Using the per-source accessor for '${c.agentKey}' (via '${c.topic}') ` +
            `so its value isn't clobbered by other writers in the merged payload.`,
          ambiguous: true,
        });
      }
    }
  }

  return out;
}
