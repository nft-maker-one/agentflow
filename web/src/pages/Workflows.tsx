import { useState, useMemo } from "react";
import { Link, useNavigate } from "react-router-dom";
import {
  useMutation, useQuery, useQueryClient,
} from "@tanstack/react-query";
import {
  createProject, createWorkflow, deleteProject, listProjects, listWorkflows,
  type ProjectOut,
} from "../lib/api";

export default function WorkflowsPage() {
  const qc = useQueryClient();
  const navigate = useNavigate();
  const [activeProject, setActiveProject] = useState<string>("default");
  const [showCreateWf, setShowCreateWf] = useState(false);
  const [showCreateProj, setShowCreateProj] = useState(false);

  const { data: projects } = useQuery({
    queryKey: ["projects"],
    queryFn: listProjects,
    refetchInterval: 5_000,
  });

  const { data: workflows, isLoading, error } = useQuery({
    queryKey: ["workflows"],
    queryFn: listWorkflows,
    refetchInterval: 5_000,
  });

  const filtered = useMemo(
    () => (workflows ?? []).filter((w) => w.project_id === activeProject),
    [workflows, activeProject],
  );

  return (
    <div>
      {/* ---- Top bar ---- */}
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-semibold">Workflows</h1>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setShowCreateWf(true)}
            className="px-3 py-1.5 bg-blue-600 hover:bg-blue-700 text-white
                       text-sm font-medium rounded inline-flex items-center gap-1"
          >
            + New workflow
          </button>
        </div>
      </div>

      {/* ---- Project tabs ---- */}
      <ProjectTabs
        projects={projects ?? []}
        active={activeProject}
        onSelect={setActiveProject}
        onAddProject={() => setShowCreateProj(true)}
        onDeleteProject={(id) => {
          if (id === "default") return;
          if (!confirm(`Delete project? Workflows in it move to Default.`)) return;
          deleteProject(id).then(() => {
            qc.invalidateQueries({ queryKey: ["projects"] });
            qc.invalidateQueries({ queryKey: ["workflows"] });
            if (activeProject === id) setActiveProject("default");
          });
        }}
      />

      {/* ---- Workflow grid ---- */}
      {isLoading && <div className="text-slate-500">Loading…</div>}
      {error && (
        <div className="text-rose-600">
          Failed: {(error as Error).message}
        </div>
      )}
      {filtered.length === 0 && !isLoading && (
        <div className="text-slate-500 italic mt-2">
          {activeProject === "default" && (workflows ?? []).length === 0 ? (
            <>No workflows yet. Click <strong>+ New workflow</strong> above.</>
          ) : (
            <>No workflows in this project yet.</>
          )}
        </div>
      )}
      <div className="grid gap-3 mt-2">
        {filtered.map((w) => (
          <Link
            key={w.id}
            to={`/workflows/${encodeURIComponent(w.id)}`}
            className="block p-4 bg-white rounded-lg border border-slate-200
                       hover:border-blue-400 hover:shadow-sm transition"
          >
            <div className="flex justify-between items-baseline">
              <span className="text-lg font-mono font-medium text-slate-900">
                {w.id}
              </span>
              <span className="text-xs text-slate-500 tabular-nums">
                v{w.version} · ir_hash <code>{w.ir_hash}</code>
              </span>
            </div>
            {w.description && (
              <p className="mt-1 text-slate-600 text-sm">{w.description}</p>
            )}
            <div className="mt-2 flex gap-3 text-xs text-slate-500 tabular-nums">
              <span>{w.n_agents} agents</span>
              <span>·</span>
              <span>{w.n_edges} edges</span>
            </div>
          </Link>
        ))}
      </div>

      {/* ---- Modals ---- */}
      <CreateWorkflowModal
        open={showCreateWf}
        projectId={activeProject}
        onClose={() => setShowCreateWf(false)}
        onCreated={(wfId) => {
          qc.invalidateQueries({ queryKey: ["workflows"] });
          setShowCreateWf(false);
          // Client-side navigation — NOT window.location.href, which forced
          // a full-page reload (re-download the bundle, re-init React,
          // re-run every query) and made the redirect feel very slow.
          navigate(`/workflows/${encodeURIComponent(wfId)}`);
        }}
      />
      <CreateProjectModal
        open={showCreateProj}
        onClose={() => setShowCreateProj(false)}
        onCreated={(p) => {
          qc.invalidateQueries({ queryKey: ["projects"] });
          setActiveProject(p.id);
          setShowCreateProj(false);
        }}
      />
    </div>
  );
}

// ----------------------------------------------------------------
// Project tab strip
// ----------------------------------------------------------------

function ProjectTabs({
  projects, active, onSelect, onAddProject, onDeleteProject,
}: {
  projects: ProjectOut[];
  active: string;
  onSelect: (id: string) => void;
  onAddProject: () => void;
  onDeleteProject: (id: string) => void;
}) {
  return (
    <div className="flex items-center gap-1 mb-4 border-b border-slate-200">
      {projects.map((p) => {
        const isActive = p.id === active;
        return (
          <div key={p.id} className="relative group">
            <button
              onClick={() => onSelect(p.id)}
              className={
                "px-3 py-2 text-sm font-medium border-b-2 -mb-px transition " +
                (isActive
                  ? "border-blue-500 text-blue-700"
                  : "border-transparent text-slate-600 hover:text-slate-900")
              }
            >
              {p.name}
              <span className="ml-1.5 text-xs text-slate-400 tabular-nums">
                {p.n_workflows}
              </span>
            </button>
            {p.id !== "default" && (
              <button
                onClick={() => onDeleteProject(p.id)}
                className="absolute -right-1 -top-0.5 opacity-0 group-hover:opacity-100
                           text-slate-400 hover:text-rose-600 text-xs"
                title="Delete project"
              >
                ×
              </button>
            )}
          </div>
        );
      })}
      <button
        onClick={onAddProject}
        className="px-3 py-2 text-sm text-slate-500 hover:text-blue-600 border-b-2
                   border-transparent -mb-px"
      >
        + project
      </button>
    </div>
  );
}

// ----------------------------------------------------------------
// Create workflow modal
// ----------------------------------------------------------------

function CreateWorkflowModal({
  open, projectId, onClose, onCreated,
}: {
  open: boolean;
  projectId: string;
  onClose: () => void;
  onCreated: (wfId: string) => void;
}) {
  const [id, setId] = useState("");
  const [description, setDescription] = useState("");
  const [error, setError] = useState<string | null>(null);

  const submit = useMutation({
    mutationFn: () => createWorkflow(id.trim(), projectId, description),
    onSuccess: (wf) => onCreated(wf.id),
    onError: (e) => setError((e as Error).message),
  });

  if (!open) return null;
  return (
    <>
      <div className="fixed inset-0 bg-slate-900/40 z-40" onClick={onClose} />
      <div className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2
                      bg-white rounded-lg shadow-2xl w-[420px] z-50">
        <div className="px-5 py-4 border-b border-slate-200">
          <h2 className="text-lg font-semibold">New workflow</h2>
          <p className="text-xs text-slate-500">
            Will be added to project <code>{projectId}</code>.
            A placeholder agent is generated; edit it from the topology.
          </p>
        </div>
        <div className="px-5 py-4 space-y-3">
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">
              Workflow id <span className="text-rose-500">*</span>
            </label>
            <input
              value={id}
              onChange={(e) => setId(e.target.value.replace(/[^a-z0-9_]/gi, "_").toLowerCase())}
              placeholder="wf_my_pipeline"
              className="form-input font-mono"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">
              Description
            </label>
            <input
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="What is this workflow for?"
              className="form-input"
            />
          </div>
          {error && <p className="text-rose-600 text-sm">{error}</p>}
        </div>
        <div className="flex justify-end gap-2 px-5 py-3 border-t border-slate-200">
          <button onClick={onClose} className="px-4 py-2 text-sm hover:bg-slate-100 rounded">
            Cancel
          </button>
          <button
            onClick={() => submit.mutate()}
            disabled={!id.trim() || submit.isPending}
            className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400
                       text-white text-sm font-medium rounded"
          >
            {submit.isPending ? "Creating…" : "Create"}
          </button>
        </div>
      </div>
    </>
  );
}

// ----------------------------------------------------------------
// Create project modal
// ----------------------------------------------------------------

function CreateProjectModal({
  open, onClose, onCreated,
}: {
  open: boolean;
  onClose: () => void;
  onCreated: (p: ProjectOut) => void;
}) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [error, setError] = useState<string | null>(null);

  const submit = useMutation({
    mutationFn: () => createProject(name.trim(), description),
    onSuccess: onCreated,
    onError: (e) => setError((e as Error).message),
  });

  if (!open) return null;
  return (
    <>
      <div className="fixed inset-0 bg-slate-900/40 z-40" onClick={onClose} />
      <div className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2
                      bg-white rounded-lg shadow-2xl w-[420px] z-50">
        <div className="px-5 py-4 border-b border-slate-200">
          <h2 className="text-lg font-semibold">New project</h2>
          <p className="text-xs text-slate-500">
            Group related workflows. Phase 2.x: in-memory only — restart loses projects.
          </p>
        </div>
        <div className="px-5 py-4 space-y-3">
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">
              Name <span className="text-rose-500">*</span>
            </label>
            <input
              value={name} onChange={(e) => setName(e.target.value)}
              placeholder="my-team-workflows"
              className="form-input"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">
              Description
            </label>
            <input
              value={description} onChange={(e) => setDescription(e.target.value)}
              className="form-input"
            />
          </div>
          {error && <p className="text-rose-600 text-sm">{error}</p>}
        </div>
        <div className="flex justify-end gap-2 px-5 py-3 border-t border-slate-200">
          <button onClick={onClose} className="px-4 py-2 text-sm hover:bg-slate-100 rounded">
            Cancel
          </button>
          <button
            onClick={() => submit.mutate()}
            disabled={!name.trim() || submit.isPending}
            className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400
                       text-white text-sm font-medium rounded"
          >
            {submit.isPending ? "Creating…" : "Create"}
          </button>
        </div>
      </div>
    </>
  );
}
