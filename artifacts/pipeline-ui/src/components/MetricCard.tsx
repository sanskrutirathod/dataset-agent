import { cn } from "@/lib/utils";

interface MetricCardProps {
  label: string;
  value: string | number;
  sub?: string;
  color?: "default" | "green" | "blue" | "yellow" | "red";
}

const COLOR_STYLES = {
  default: "bg-white border-gray-200",
  green: "bg-green-50 border-green-200",
  blue: "bg-blue-50 border-blue-200",
  yellow: "bg-yellow-50 border-yellow-200",
  red: "bg-red-50 border-red-200",
};

export function MetricCard({ label, value, sub, color = "default" }: MetricCardProps) {
  return (
    <div className={cn("rounded-xl border p-4", COLOR_STYLES[color])}>
      <p className="text-xs text-gray-500 font-medium uppercase tracking-wide">{label}</p>
      <p className="mt-1 text-2xl font-bold text-gray-900">{value}</p>
      {sub && <p className="mt-0.5 text-xs text-gray-500">{sub}</p>}
    </div>
  );
}
