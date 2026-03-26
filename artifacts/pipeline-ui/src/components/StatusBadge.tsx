import { cn } from "@/lib/utils";

type Status = "pending" | "running" | "completed" | "failed" | "partial";

const STATUS_STYLES: Record<Status, string> = {
  pending: "bg-gray-100 text-gray-600",
  running: "bg-blue-100 text-blue-700 animate-pulse",
  completed: "bg-green-100 text-green-700",
  failed: "bg-red-100 text-red-700",
  partial: "bg-yellow-100 text-yellow-700",
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
        STATUS_STYLES[s] ?? "bg-gray-100 text-gray-600"
      )}
    >
      {STATUS_LABELS[s] ?? status}
    </span>
  );
}
