import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { getLlmModels, getSystemInfo } from "../lib/api";

export default function SettingsPage() {
  const { data: sys } = useQuery({
    queryKey: ["system-info"],
    queryFn: getSystemInfo,
    refetchInterval: 5_000,
  });
  const { data: models } = useQuery({
    queryKey: ["llm-models"],
    queryFn: getLlmModels,
  });

  return (
    <div className="space-y-6">
      <div>
        <Link to="/" className="text-sm text-slate-500 hover:text-slate-900">
          ← Workflows
        </Link>
        <h1 className="mt-1 text-2xl font-semibold">Settings</h1>
        <p className="text-sm text-slate-500">
          Read-only system view.
        </p>
      </div>

      {/* ---- Counters ---- */}
      <section>
        <h2 className="text-xs font-semibold text-slate-700 uppercase tracking-wide mb-2">
          Inventory
        </h2>
        <div className="grid grid-cols-4 gap-3">
          <Stat label="Version" value={sys?.version ?? "…"} />
          <Stat label="Projects" value={sys?.n_projects ?? "…"} />
          <Stat label="Workflows" value={sys?.n_workflows ?? "…"} />
          <Stat label="Agents" value={sys?.n_agents ?? "…"} />
        </div>
      </section>

      {/* ---- Connected providers ---- */}
      <section>
        <h2 className="text-xs font-semibold text-slate-700 uppercase tracking-wide mb-2">
          Connected LLM Providers
        </h2>
        <div className="bg-white border border-slate-200 rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-slate-50 text-slate-600 text-xs uppercase tracking-wide">
              <tr>
                <th className="text-left px-4 py-2 font-medium">Name</th>
                <th className="text-left px-4 py-2 font-medium">Adapter</th>
                <th className="text-left px-4 py-2 font-medium">Compat</th>
                <th className="text-left px-4 py-2 font-medium">API key</th>
                <th className="text-left px-4 py-2 font-medium">Models</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {sys?.providers.length === 0 && (
                <tr>
                  <td colSpan={5} className="px-4 py-6 text-center text-slate-500 italic">
                    No providers connected.
                  </td>
                </tr>
              )}
              {sys?.providers.map((p) => (
                <tr key={p.name} className="hover:bg-slate-50">
                  <td className="px-4 py-2 font-mono">{p.name}</td>
                  <td className="px-4 py-2">{p.adapter}</td>
                  <td className="px-4 py-2 text-slate-500">{p.compat ?? "—"}</td>
                  <td className="px-4 py-2">
                    {p.has_api_key ? (
                      <span className="text-emerald-600 text-xs">✓ set</span>
                    ) : (
                      <span className="text-slate-400 text-xs">—</span>
                    )}
                  </td>
                  <td className="px-4 py-2 text-xs text-slate-600">
                    {p.available_models.length === 0
                      ? "—"
                      : p.available_models.join(", ")}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {/* ---- Curated catalogue ---- */}
      <section>
        <h2 className="text-xs font-semibold text-slate-700 uppercase tracking-wide mb-2">
          Available Models (catalogue)
        </h2>
        <div className="bg-white border border-slate-200 rounded-lg p-4 grid grid-cols-2 gap-x-6 gap-y-3">
          {models && Object.entries(models).map(([provider, list]) => (
            <div key={provider}>
              <div className="text-xs font-mono font-semibold text-slate-700">
                {provider}
              </div>
              <ul className="text-xs text-slate-600 mt-1 space-y-0.5">
                {list.map((m) => <li key={m}>· {m}</li>)}
              </ul>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

function Stat({
  label, value,
}: { label: string; value: string | number }) {
  return (
    <div className="bg-white border border-slate-200 rounded-lg p-4">
      <div className="text-xs text-slate-500 uppercase tracking-wide">{label}</div>
      <div className="text-2xl font-semibold tabular-nums mt-1">{value}</div>
    </div>
  );
}
