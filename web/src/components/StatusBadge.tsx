interface Props {
  status: string;
}

const STATUS_STYLE: Record<string, string> = {
  Running:        "bg-blue-100 text-blue-700",
  Pending:        "bg-slate-100 text-slate-700",
  Succeeded:      "bg-emerald-100 text-emerald-700",
  Failed:         "bg-rose-100 text-rose-700",
  Cancelled:      "bg-amber-100 text-amber-700",
  AwaitingHuman:  "bg-violet-100 text-violet-700",
  Degraded:       "bg-orange-100 text-orange-700",
};

/** Coloured pill rendering a Run status. */
export function StatusBadge({ status }: Props) {
  const cls = STATUS_STYLE[status] ?? "bg-slate-100 text-slate-700";
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls}`}
    >
      {status}
    </span>
  );
}
