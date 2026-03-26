import { cn } from "@/lib/utils";

type Status = "pending" | "running" | "completed" | "failed" | "partial";

const STATUS_STYLES: Record<Status, string> = {
  pending:   "bg-blue-500/10 text-blue-400 ring-1 ring-inset ring-blue-500/20",
  running:   "bg-indigo-500/10 text-indigo-400 ring-1 ring-inset ring-indigo-500/20 animate-pulse",
  completed: "bg-emerald-500/10 text-emerald-400 ring-1 ring-inset ring-emerald-500/20",
  failed:    "bg-red-500/10 text-red-400 ring-1 ring-inset ring-red-500/20",
  partial:   "bg-amber-500/10 text-amber-400 ring-1 ring-inset ring-amber-500/20",
};

const STATUS_LABELS: Record<Status, string> = {
  pending: "Pending",
  running: "Running",
  completed: "Completed",
  failed: "Failed",
  partial: "Partial",
};

export function StatusBadge({ status }: { status: string }) {
  const s = status as Status;
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium",
        STATUS_STYLES[s] ?? "bg-muted text-muted-foreground ring-1 ring-inset ring-border"
      )}
    >
      {STATUS_LABELS[s] ?? status}
    </span>
  );
}
