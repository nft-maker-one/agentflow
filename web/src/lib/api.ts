/* REST + SSE client for the AgentKit control plane. */

export interface WorkflowSummary {
  id: string;
  version: number;
  description: string;
  ir_hash: string;
  n_agents: number;
  n_edges: number;
  project_id: string;
}

export interface AgentSummary {
  template_key: string;
  role: string;
  description: string;
  subscribe: string[];
  publish: string[];
  llm: Record<string, unknown> | null;
  output_field: string | null;
  agent_guardrail: { max_tokens_per_call: number; max_cycles: number } | null;
  /** Fan-in aggregator gating; null when subscribe < 2 topics. */
  aggregate: { threshold: number; required: string[] } | null;
}

export interface GraphNode {
  id: string;
  label: string;
  kind: "start" | "end" | "error" | "agent";
  role: string | null;
}

export interface GraphEdge {
  id: string;
  source: string;
  target: string;
  via: string | null;
  is_switch: boolean;
  case: string | null;
}

export interface WorkflowDetail {
  id: string;
  version: number;
  description: string;
  ir_hash: string;
  agents: AgentSummary[];
  nodes: GraphNode[];
  edges: GraphEdge[];
  project_id: string;
  start_input_fields: string[];
  undo_depth: number;
  /** Workflow-level run-quota caps. null = use project/framework defaults. */
  workflow_guardrail: { max_total_tokens: number; max_cycles_per_run: number } | null;
  /** Execution mode: "normal" = one Run per POST, "event_driven"
   *  = continuous, external sources auto-spawn Runs. */
  mode: "normal" | "event_driven";
  /** Event-driven session run_id; empty in normal mode. */
  session_run_id: string;
}

export interface BranchEvent {
  edge_id: string;
  chosen: string;
  by: string;
  reason: string | null;
}

export interface RunSummary {
  run_id: string;
  workflow_id: string;
  status: string;
  started_at: string | null;
  ended_at: string | null;
  failure_reason: string | null;
  trace_id: string | null;
}

export interface RunDetail extends RunSummary {
  input: Record<string, unknown>;
  output: Record<string, unknown> | null;
  current_node: string | null;
  branch_log: BranchEvent[];
  // Topology/agents as they were when this run executed (snapshot).
  snapshot_nodes: GraphNode[];
  snapshot_edges: GraphEdge[];
  snapshot_agents: AgentSummary[];
}

export interface EnvelopeOut {
  event_id: string;
  topic: string;
  payload: Record<string, unknown>;
  from_template: string | null;
  from_agent: string | null;
  causation_id: string | null;
  created_at: string;
}

// Same-origin in production; Vite proxies /api in dev.
const BASE = "";

async function jsonFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  // Tolerate empty / no-content responses (e.g. 204 from DELETE).
  // `res.json()` on an empty body throws "Unexpected end of JSON input",
  // which made a successful DELETE look like a failure (and blocked the
  // post-delete redirect, since the mutation fell into onError).
  if (res.status === 204) return undefined as T;
  const text = await res.text();
  return (text ? JSON.parse(text) : undefined) as T;
}

// ---- Workflows ------------------------------------------------

export const listWorkflows = () =>
  jsonFetch<WorkflowSummary[]>("/api/workflows");

export const getWorkflow = (id: string) =>
  jsonFetch<WorkflowDetail>(`/api/workflows/${encodeURIComponent(id)}`);

// ---- Runs -----------------------------------------------------

export const listRuns = (params: {
  workflow_id?: string;
  status?: string;
  limit?: number;
} = {}) => {
  const q = new URLSearchParams();
  if (params.workflow_id) q.set("workflow_id", params.workflow_id);
  if (params.status) q.set("status", params.status);
  if (params.limit) q.set("limit", String(params.limit));
  const qs = q.toString();
  return jsonFetch<RunSummary[]>(`/api/runs${qs ? `?${qs}` : ""}`);
};

export const getRun = (id: string) =>
  jsonFetch<RunDetail>(`/api/runs/${encodeURIComponent(id)}`);

export const createRun = (workflow_id: string, input: Record<string, unknown>) =>
  jsonFetch<RunSummary>("/api/runs", {
    method: "POST",
    body: JSON.stringify({ workflow_id, input }),
  });

export const cancelRun = (id: string) =>
  jsonFetch<RunSummary>(
    `/api/runs/${encodeURIComponent(id)}/cancel`,
    { method: "POST" },
  );

// ---- Workflow CRUD ------------------------------------------

export const createWorkflow = (
  id: string, project_id: string, description = "",
) =>
  jsonFetch<WorkflowSummary>("/api/workflows", {
    method: "POST",
    body: JSON.stringify({ id, project_id, description }),
  });

export const deleteWorkflow = (id: string) =>
  jsonFetch<void>(`/api/workflows/${encodeURIComponent(id)}`, {
    method: "DELETE",
  });

/** Persist the workflow's start-input schema (the field names that
 *  __start__ injects into payload when a Run is triggered). */
export const saveStartInputFields = (wf_id: string, fields: string[]) =>
  jsonFetch<{ workflow_id: string; start_input_fields: string[] }>(
    `/api/workflows/${encodeURIComponent(wf_id)}/start-input`,
    {
      method: "PUT",
      body: JSON.stringify({ fields }),
    },
  );


// ============================================================
// External I/O — sources / sinks (Telegram, IMAP, SMTP, Python)
// ============================================================

export interface ExternalKindMeta {
  kind: string;
  direction: "source" | "sink";
  label: string;
  description: string;
  fields: Record<string, {
    type: "string" | "secret" | "number" | "bool" | "code";
    label: string;
    required?: boolean;
    default?: unknown;
    help?: string;
  }>;
}

export interface ExternalIOItem {
  name: string;
  kind: string;
  direction: "source" | "sink";
  topic: string;
  config: Record<string, unknown>;
  health: {
    running: boolean;
    started_at: string | null;
    last_event_at: string | null;
    events_total: number;
    last_error: string | null;
  };
}

export interface ExternalIOList {
  sources: ExternalIOItem[];
  sinks: ExternalIOItem[];
}

export const listExternalKinds = () =>
  jsonFetch<ExternalKindMeta[]>(`/api/external/kinds`);

export const listWorkflowExternal = (workflowId: string) =>
  jsonFetch<ExternalIOList>(
    `/api/workflows/${encodeURIComponent(workflowId)}/external`,
  );

export const addExternalSource = (
  workflowId: string,
  body: { name: string; kind: string; topic: string; config: Record<string, unknown> },
) =>
  jsonFetch<ExternalIOList>(
    `/api/workflows/${encodeURIComponent(workflowId)}/external/sources`,
    { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) },
  );

export const addExternalSink = (
  workflowId: string,
  body: { name: string; kind: string; topic: string; config: Record<string, unknown> },
) =>
  jsonFetch<ExternalIOList>(
    `/api/workflows/${encodeURIComponent(workflowId)}/external/sinks`,
    { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) },
  );

export const removeExternalSource = (workflowId: string, name: string) =>
  fetch(
    `/api/workflows/${encodeURIComponent(workflowId)}/external/sources/${encodeURIComponent(name)}`,
    { method: "DELETE" },
  ).then((r) => {
    if (!r.ok && r.status !== 204) throw new Error(`${r.status}`);
  });

export const removeExternalSink = (workflowId: string, name: string) =>
  fetch(
    `/api/workflows/${encodeURIComponent(workflowId)}/external/sinks/${encodeURIComponent(name)}`,
    { method: "DELETE" },
  ).then((r) => {
    if (!r.ok && r.status !== 204) throw new Error(`${r.status}`);
  });


export const setWorkflowMode = (
  workflowId: string,
  mode: "normal" | "event_driven",
) =>
  jsonFetch<{ workflow_id: string; mode: string }>(
    `/api/workflows/${encodeURIComponent(workflowId)}/mode`,
    { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ mode }) },
  );
export const saveWorkflowGuardrail = (
  workflowId: string,
  body: { max_total_tokens: number | null; max_cycles_per_run: number | null },
) =>
  jsonFetch<{ id: string; version: number }>(
    `/api/workflows/${encodeURIComponent(workflowId)}/guardrail`,
    {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    },
  );

/** Pop the most recent topology snapshot. Returns 409 if nothing
 *  to undo. */
export const undoWorkflow = (wf_id: string) =>
  jsonFetch<{
    workflow_id: string;
    history_depth: number;
    restored_agents: string[];
    ir_hash: string;
  }>(
    `/api/workflows/${encodeURIComponent(wf_id)}/undo`,
    { method: "POST" },
  );

/** Handler-level config for a class-based Agent. Same shape as the
 *  ``AgentConfig`` Pydantic model in ``agent_edit.py``. */
export interface AgentConfig {
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

/** Fetch the live handler config for an agent. */
export const getAgentConfig = (wf_id: string, key: string) =>
  jsonFetch<AgentConfig>(
    `/api/workflows/${encodeURIComponent(wf_id)}` +
    `/agents/${encodeURIComponent(key)}/config`,
  );

/** Patch the live handler config of a class-based Agent (prompt,
 *  system_prompt, etc). Only fields explicitly present are touched. */
export const patchAgentConfig = (
  wf_id: string,
  key: string,
  patch: Partial<Pick<
    AgentConfig,
    "prompt" | "system_prompt" | "output_field" | "max_retries"
    | "retry_backoff_s" | "json_output" | "json_unwrap" | "python_script"
  >>,
) =>
  jsonFetch<AgentConfig>(
    `/api/workflows/${encodeURIComponent(wf_id)}` +
    `/agents/${encodeURIComponent(key)}`,
    {
      method: "PATCH",
      body: JSON.stringify(patch),
    },
  );

// ---- Agent CRUD (within an existing workflow) ---------------

export interface AddAgentRequest {
  template_key: string;
  role: string;
  description: string;
  subscribe_topics: string[];
  publish_topics: string[];
  llm: { provider: string; model: string } | null;
  prompt: string | null;
  system_prompt: string | null;
  python_script?: string | null;
  output_field: string;
  max_retries: number;
  connect_to_start?: boolean;
  connect_to_end?: boolean;
  aggregate_threshold?: number;
  aggregate_required_topics?: string[];
}

export const addAgent = (wf_id: string, body: AddAgentRequest) =>
  jsonFetch<WorkflowSummary>(
    `/api/workflows/${encodeURIComponent(wf_id)}/agents`,
    { method: "POST", body: JSON.stringify(body) },
  );

export const removeAgent = (wf_id: string, key: string) =>
  jsonFetch<WorkflowSummary>(
    `/api/workflows/${encodeURIComponent(wf_id)}/agents/${encodeURIComponent(key)}`,
    { method: "DELETE" },
  );

export interface UpdateAgentRequest {
  role: string;
  description: string;
  subscribe_topics: string[];
  publish_topics: string[];
  llm: { provider: string; model: string } | null;
  prompt: string | null;
  system_prompt: string | null;
  python_script?: string | null;
  output_field: string;
  max_retries: number;
  connect_to_start?: boolean;
  connect_to_end?: boolean;
  aggregate_threshold?: number;
  aggregate_required_topics?: string[];
}

export const updateAgent = (
  wf_id: string, key: string, body: UpdateAgentRequest,
) =>
  jsonFetch<WorkflowSummary>(
    `/api/workflows/${encodeURIComponent(wf_id)}/agents/${encodeURIComponent(key)}`,
    { method: "PUT", body: JSON.stringify(body) },
  );

// ---- Projects -----------------------------------------------

export interface ProjectOut {
  id: string;
  name: string;
  description: string;
  created_at: string;
  n_workflows: number;
}

export const listProjects = () =>
  jsonFetch<ProjectOut[]>("/api/projects");

export const createProject = (name: string, description = "") =>
  jsonFetch<ProjectOut>("/api/projects", {
    method: "POST",
    body: JSON.stringify({ name, description }),
  });

export const deleteProject = (id: string) =>
  fetch(`/api/projects/${encodeURIComponent(id)}`, { method: "DELETE" })
    .then((r) => {
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    });

// ---- System info --------------------------------------------

export interface ProviderInfo {
  name: string;
  adapter: string;
  compat: string | null;
  base_url: string | null;
  has_api_key: boolean;
  available_models: string[];
}

export interface SystemInfo {
  version: string;
  n_projects: number;
  n_workflows: number;
  n_agents: number;
  n_active_runs: number;
  providers: ProviderInfo[];
}

export const getSystemInfo = () => jsonFetch<SystemInfo>("/api/system");

export const getLlmModels = () =>
  jsonFetch<Record<string, string[]>>("/api/system/llm-models");

// ---- Health -------------------------------------------------

export interface HealthResponse {
  ok: boolean;
  version: string;
  deployed_workflows: string[];
}

export const getHealth = () => jsonFetch<HealthResponse>("/api/health");

// ---- SSE event stream ----------------------------------------

/**
 * Subscribe to a Run's live envelope stream via Server-Sent Events.
 * Returns a cleanup function that closes the connection.
 */
/** REST fallback for the SSE stream — returns the current buffered
 *  envelope list for a run.  Polled by the UI to recover from
 *  SSE proxies that buffer chunks (Vite default) so the topology
 *  view eventually catches up. */
export const getRunEventsSnapshot = (runId: string) =>
  jsonFetch<EnvelopeOut[]>(
    `/api/runs/${encodeURIComponent(runId)}/events/snapshot`,
  );

export function streamRunEvents(
  runId: string,
  onEnvelope: (env: EnvelopeOut) => void,
  onError?: (err: Event) => void,
): () => void {
  const url = `${BASE}/api/runs/${encodeURIComponent(runId)}/events`;
  const es = new EventSource(url);
  es.addEventListener("envelope", (e) => {
    try {
      const env = JSON.parse((e as MessageEvent).data) as EnvelopeOut;
      onEnvelope(env);
    } catch {
      // Drop malformed payloads silently.
    }
  });
  if (onError) es.addEventListener("error", onError);
  return () => es.close();
}

// ============================================================
// Inbox notifications
// ============================================================

export type InboxCategory =
  | "run_succeeded"
  | "run_failed"
  | "run_cancelled"
  | "ext_source"
  | "ext_sink_ok"
  | "ext_sink_error"
  | "error";

export interface InboxItem {
  id: string;
  workflow_id: string;
  category: InboxCategory;
  title: string;
  body: string;
  payload: Record<string, unknown> | null;
  ts: string;
  read: boolean;
  archived: boolean;
}

export interface InboxResponse {
  items: InboxItem[];
  total: number;
  unread: number;
}

export const listInbox = (params: {
  workflow_id?: string;
  include_archived?: boolean;
  unread_only?: boolean;
  limit?: number;
} = {}) => {
  const q = new URLSearchParams();
  if (params.workflow_id) q.set("workflow_id", params.workflow_id);
  if (params.include_archived) q.set("include_archived", "true");
  if (params.unread_only) q.set("unread_only", "true");
  if (params.limit) q.set("limit", String(params.limit));
  return jsonFetch<InboxResponse>(`/api/inbox?${q.toString()}`);
};

export const inboxMarkRead = (id: string) =>
  fetch(`/api/inbox/${encodeURIComponent(id)}/read`, { method: "POST" });

export const inboxMarkAllRead = (workflowId?: string) => {
  const q = workflowId ? `?workflow_id=${encodeURIComponent(workflowId)}` : "";
  return jsonFetch<{ marked: number }>(`/api/inbox/read-all${q}`, { method: "POST" });
};

export const inboxArchive = (id: string) =>
  fetch(`/api/inbox/${encodeURIComponent(id)}/archive`, { method: "POST" });

export const inboxDelete = (id: string) =>
  fetch(`/api/inbox/${encodeURIComponent(id)}`, { method: "DELETE" });

export const inboxClear = (params: {
  workflow_id?: string;
  archived_only?: boolean;
} = {}) => {
  const q = new URLSearchParams();
  if (params.workflow_id) q.set("workflow_id", params.workflow_id);
  if (params.archived_only) q.set("archived_only", "true");
  return jsonFetch<{ removed: number }>(`/api/inbox/clear?${q.toString()}`, {
    method: "POST",
  });
};
