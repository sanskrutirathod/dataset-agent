import { cn } from "@/lib/utils";

interface MetricCardProps {
  label: string;
  value: string | number;
  sub?: string;
  color?: "default" | "green" | "blue" | "yellow" | "red";
}

const COLOR_STYLES: Record<string, string> = {
  default: "bg-card border-card-border",
  green:   "bg-emerald-500/5 border-emerald-500/20",
  blue:    "bg-blue-500/5 border-blue-500/20",
  yellow:  "bg-amber-500/5 border-amber-500/20",
  red:     "bg-red-500/5 border-red-500/20",
};

const VALUE_STYLES: Record<string, string> = {
  default: "text-foreground",
  green:   "text-emerald-400",
  blue:    "text-blue-400",
  yellow:  "text-amber-400",
  red:     "text-red-400",
};

export function MetricCard({ label, value, sub, color = "default" }: MetricCardProps) {
  return (
    <div className={cn("rounded-xl border p-4", COLOR_STYLES[color])}>
      <p className="text-xs text-muted-foreground font-medium uppercase tracking-wider">{label}</p>
      <p className={cn("mt-1 text-2xl font-bold tabular-nums", VALUE_STYLES[color])}>{value}</p>
      {sub && <p className="mt-0.5 text-xs text-muted-foreground">{sub}</p>}
    </div>
  );
}
