/**
 * Global Inbox panel — slides in from the right when the bell button
 * in the header is clicked.
 *
 * Behaviour:
 *   - Polls GET /api/inbox every 3s (lightweight; backend is in-memory).
 *   - When unread count grows, plays a short beep (Web Audio API) so
 *     the user notices new arrivals even when the panel is closed.
 *   - Filter dropdown picks one workflow_id to scope the list.
 *   - Per-item: ✓ archive, ✕ delete; mark-as-read on click.
 *   - Footer: "Mark all read" + "Clear archived" bulk actions.
 *
 * The panel is mounted globally in App.tsx so it works regardless of
 * which page the user is on.
 */
import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import {
  inboxArchive, inboxClear, inboxDelete, inboxMarkAllRead, inboxMarkRead,
  listInbox, listWorkflows,
  type InboxCategory, type InboxItem,
} from "../lib/api";

interface Props {
  open: boolean;
  onClose: () => void;
}

export function InboxPanel({ open, onClose }: Props) {
  const qc = useQueryClient();
  const navigate = useNavigate();

  // Filter state
  const [workflowFilter, setWorkflowFilter] = useState<string>("");
  const [showArchived, setShowArchived] = useState(false);

  // Workflows for the filter dropdown.
  const { data: wfs = [] } = useQuery({
    queryKey: ["workflows"],
    queryFn: listWorkflows,
    staleTime: 30_000,
  });

  const { data } = useQuery({
    queryKey: ["inbox", workflowFilter, showArchived],
    queryFn: () =>
      listInbox({
        workflow_id: workflowFilter || undefined,
        include_archived: showArchived,
        limit: 200,
      }),
    refetchInterval: 3_000,
  });

  const items = data?.items ?? [];

  // Track unread count for sound alerts (used by the *header* button
  // even when the panel is closed; the panel itself just shows them).
  // ChimeOnNewUnread lives in the Header, not here, so we just keep
  // this query running.

  // ── Per-item actions ──
  const markRead = useMutation({
    mutationFn: (id: string) => inboxMarkRead(id),
    onSuccess: () => qc.refetchQueries({ queryKey: ["inbox"] }),
  });
  const archive = useMutation({
    mutationFn: (id: string) => inboxArchive(id),
    onSuccess: () => qc.refetchQueries({ queryKey: ["inbox"] }),
  });
  const removeOne = useMutation({
    mutationFn: (id: string) => inboxDelete(id),
    onSuccess: () => qc.refetchQueries({ queryKey: ["inbox"] }),
  });

  // ── Bulk actions ──
  const markAll = useMutation({
    mutationFn: () => inboxMarkAllRead(workflowFilter || undefined),
    onSuccess: () => qc.refetchQueries({ queryKey: ["inbox"] }),
  });
  const clearAll = useMutation({
    mutationFn: () =>
      inboxClear({
        workflow_id: workflowFilter || undefined,
        archived_only: true,
      }),
    onSuccess: () => qc.refetchQueries({ queryKey: ["inbox"] }),
  });

  // ── Click-anywhere row navigation ──
  // Marks the item read and routes the user to the relevant workflow
  // page, closing the panel so the workflow detail view becomes the
  // user's primary surface.
  const onItemClick = (it: InboxItem) => {
    if (!it.read) markRead.mutate(it.id);
    if (it.workflow_id) {
      navigate(`/workflows/${encodeURIComponent(it.workflow_id)}`);
      onClose();
    }
  };

  if (!open) return null;
  return (
    <>
      {/* Backdrop — click closes */}
      <div className="fixed inset-0 bg-black/30 z-30" onClick={onClose} />

      {/* Drawer */}
      <div
        className="fixed right-0 top-0 h-full w-[420px] bg-white shadow-2xl
                   border-l border-slate-200 z-40 flex flex-col"
      >
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-slate-200">
          <h2 className="font-semibold text-slate-800 flex items-center gap-2">
            📬 Inbox
            {(data?.unread ?? 0) > 0 && (
              <span className="text-[10px] bg-rose-500 text-white px-1.5 py-0.5 rounded-full">
                {data?.unread} unread
              </span>
            )}
          </h2>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-700"
            title="Close"
          >
            ✕
          </button>
        </div>

        {/* Filter row */}
        <div className="flex items-center gap-2 px-4 py-2 border-b border-slate-100 bg-slate-50">
          <select
            value={workflowFilter}
            onChange={(e) => setWorkflowFilter(e.target.value)}
            className="form-input text-xs flex-1"
          >
            <option value="">All workflows</option>
            {wfs.map((w) => (
              <option key={w.id} value={w.id}>{w.id}</option>
            ))}
          </select>
          <label className="flex items-center gap-1 text-[11px] text-slate-600">
            <input
              type="checkbox"
              checked={showArchived}
              onChange={(e) => setShowArchived(e.target.checked)}
            />
            archived
          </label>
        </div>

        {/* List */}
        <div className="flex-1 overflow-y-auto thin-scroll">
          {items.length === 0 && (
            <div className="text-center text-slate-500 italic py-12 text-sm">
              No notifications {workflowFilter && `for ${workflowFilter}`}.
            </div>
          )}
          <ul className="divide-y divide-slate-100">
            {items.map((it) => (
              <InboxRow
                key={it.id}
                item={it}
                onOpen={() => onItemClick(it)}
                onArchive={() => archive.mutate(it.id)}
                onDelete={() => removeOne.mutate(it.id)}
              />
            ))}
          </ul>
        </div>

        {/* Footer bulk actions */}
        <div className="px-4 py-2 border-t border-slate-200 flex items-center justify-between text-xs text-slate-600">
          <span>
            {items.length} shown · {data?.total ?? 0} total
          </span>
          <div className="flex gap-2">
            <button
              onClick={() => markAll.mutate()}
              disabled={markAll.isPending || (data?.unread ?? 0) === 0}
              className="text-blue-600 hover:underline disabled:opacity-30"
            >
              Mark all read
            </button>
            <button
              onClick={() => clearAll.mutate()}
              disabled={clearAll.isPending}
              className="text-rose-600 hover:underline disabled:opacity-30"
              title="Permanently delete archived items"
            >
              Clear archived
            </button>
          </div>
        </div>
      </div>
    </>
  );
}

function InboxRow({
  item, onOpen, onArchive, onDelete,
}: {
  item: InboxItem;
  onOpen: () => void;
  onArchive: () => void;
  onDelete: () => void;
}) {
  const meta = CATEGORY_META[item.category] ?? CATEGORY_META.error;
  return (
    <li
      className={
        "px-4 py-2.5 hover:bg-blue-50/60 group cursor-pointer relative " +
        "transition-colors " +
        (item.read ? "" : "bg-blue-50/40")
      }
      onClick={onOpen}
      title="Open the workflow this notification belongs to"
    >
      <div className="flex gap-2">
        <span className={"text-base shrink-0 " + meta.iconColor}>{meta.icon}</span>
        <div className="flex-1 min-w-0">
          <div className="flex items-baseline justify-between gap-2">
            <span className={
              "text-sm truncate " + (item.read ? "text-slate-700" : "font-semibold text-slate-900")
            }>{item.title}</span>
            <span className="text-[10px] text-slate-400 tabular-nums shrink-0">
              {timeAgo(new Date(item.ts))}
            </span>
          </div>
          <div className="text-[11px] text-slate-500 truncate">{item.body}</div>
          <div className="mt-1 flex items-center gap-2">
            <Link
              to={`/workflows/${encodeURIComponent(item.workflow_id)}`}
              className="text-[10px] text-blue-600 hover:underline font-mono truncate"
              onClick={(e) => e.stopPropagation()}
            >
              {item.workflow_id}
            </Link>
            <span className={"text-[10px] px-1.5 py-0.5 rounded border " + meta.badgeClass}>
              {meta.label}
            </span>
            {item.archived && (
              <span className="text-[10px] text-slate-400">📁 archived</span>
            )}
          </div>
        </div>
      </div>
      {/* Hover actions */}
      <div
        className="absolute top-1 right-2 opacity-0 group-hover:opacity-100 flex gap-1"
        onClick={(e) => e.stopPropagation()}
      >
        {!item.archived && (
          <button
            onClick={onArchive}
            className="text-[10px] text-slate-500 hover:bg-slate-200 px-1.5 py-0.5 rounded"
            title="Archive"
          >
            📁
          </button>
        )}
        <button
          onClick={onDelete}
          className="text-[10px] text-rose-500 hover:bg-rose-50 px-1.5 py-0.5 rounded"
          title="Delete"
        >
          ✕
        </button>
      </div>
    </li>
  );
}

const CATEGORY_META: Record<
  InboxCategory,
  { icon: string; iconColor: string; badgeClass: string; label: string }
> = {
  run_succeeded: {
    icon: "✓", iconColor: "text-emerald-600",
    badgeClass: "bg-emerald-50 text-emerald-700 border-emerald-200",
    label: "run ✓",
  },
  run_failed: {
    icon: "✗", iconColor: "text-rose-600",
    badgeClass: "bg-rose-50 text-rose-700 border-rose-200",
    label: "run ✗",
  },
  run_cancelled: {
    icon: "⊘", iconColor: "text-slate-500",
    badgeClass: "bg-slate-100 text-slate-600 border-slate-300",
    label: "cancelled",
  },
  ext_source: {
    icon: "🔌", iconColor: "text-violet-600",
    badgeClass: "bg-violet-50 text-violet-700 border-violet-200",
    label: "ext source",
  },
  ext_sink_ok: {
    icon: "📤", iconColor: "text-emerald-600",
    badgeClass: "bg-emerald-50 text-emerald-700 border-emerald-200",
    label: "ext sink",
  },
  ext_sink_error: {
    icon: "📤", iconColor: "text-rose-600",
    badgeClass: "bg-rose-50 text-rose-700 border-rose-200",
    label: "sink ✗",
  },
  error: {
    icon: "⚠", iconColor: "text-amber-600",
    badgeClass: "bg-amber-50 text-amber-700 border-amber-200",
    label: "error",
  },
};

function timeAgo(d: Date): string {
  const s = Math.floor((Date.now() - d.getTime()) / 1000);
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return d.toLocaleDateString();
}
