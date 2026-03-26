import { useQuery } from "@tanstack/react-query";
import { useParams, Link } from "wouter";
import { getRun, downloadUrl } from "@/lib/pipeline-api";
import { StatusBadge } from "@/components/StatusBadge";
import { StageTimeline } from "@/components/StageTimeline";
import { MetricCard } from "@/components/MetricCard";
import { Button } from "@/components/ui/button";
import { ArrowLeft, Download } from "lucide-react";
import { formatDistanceToNow } from "date-fns";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
} from "recharts";

const CHART_COLORS = ["#6366f1", "#818cf8", "#a5b4fc", "#c7d2fe", "#4f46e5", "#4338ca", "#3730a3", "#312e81"];

export default function RunDetailPage() {
  const { id } = useParams<{ id: string }>();

  const { data: run, isLoading } = useQuery({
    queryKey: ["run", id],
    queryFn: () => getRun(id!),
    enabled: !!id,
    refetchInterval: (query) => {
      const s = query.state.data?.status;
      return s === "running" || s === "pending" ? 3000 : false;
    },
  });

  if (isLoading) {
    return (
      <div className="p-6 max-w-5xl mx-auto space-y-4">
        {[...Array(4)].map((_, i) => (
          <div key={i} className="h-24 bg-card rounded-xl animate-pulse border border-card-border" />
        ))}
      </div>
    );
  }

  if (!run) {
    return (
      <div className="p-6 text-center text-muted-foreground">Run not found</div>
    );
  }

  const stageChartData = (run.stage_metrics ?? []).map((s, i) => ({
    name: s.stage,
    latency: parseFloat((s.latency_ms / 1000).toFixed(3)),
    out: s.output_count,
    color: CHART_COLORS[i % CHART_COLORS.length],
  }));

  const isDone = run.status === "completed" || run.status === "partial";

  return (
    <div className="p-6 max-w-5xl mx-auto space-y-6">
      <div className="flex items-center gap-3">
        <Link href="/">
          <Button variant="ghost" size="sm" className="gap-1.5 text-muted-foreground hover:text-foreground">
            <ArrowLeft className="w-4 h-4" />
            Runs
          </Button>
        </Link>
        <div className="h-4 w-px bg-border" />
        <h1 className="text-xl font-bold text-foreground">{run.run_name}</h1>
        <StatusBadge status={run.status} />
      </div>

      <div className="flex items-center gap-4 text-sm text-muted-foreground">
        <span className="font-mono text-xs bg-muted px-2 py-0.5 rounded">{run.run_id}</span>
        <span>·</span>
        <span>Created {formatDistanceToNow(new Date(run.created_at), { addSuffix: true })}</span>
        {run.error && (
          <>
            <span>·</span>
            <span className="text-red-400 text-xs">{run.error}</span>
          </>
        )}
      </div>

      {run.metrics && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <MetricCard
            label="Total Records"
            value={run.metrics.total_records ?? 0}
            color={run.metrics.total_records ? "green" : "default"}
            sub={`${run.metrics.drop_count ?? 0} dropped`}
          />
          <MetricCard
            label="Avg Score"
            value={run.metrics.avg_final_score != null ? (run.metrics.avg_final_score * 100).toFixed(0) + "%" : "—"}
            color="blue"
            sub={`Ingest: ${run.metrics.ingest_success_rate != null ? (run.metrics.ingest_success_rate * 100).toFixed(0) + "%" : "—"}`}
          />
          <MetricCard
            label="Validation Pass"
            value={run.metrics.validation_pass_rate != null ? (run.metrics.validation_pass_rate * 100).toFixed(0) + "%" : "—"}
            color="yellow"
            sub={`Gen yield: ${run.metrics.generation_yield != null ? (run.metrics.generation_yield * 100).toFixed(0) + "%" : "—"}`}
          />
          <MetricCard
            label="Avg Chunk Tokens"
            value={run.metrics.avg_chunk_tokens?.toFixed(0) ?? "—"}
            sub={`Dedup: ${run.metrics.dedup_ratio != null ? (run.metrics.dedup_ratio * 100).toFixed(0) + "%" : "—"}`}
          />
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
        <div className="lg:col-span-1">
          <div className="bg-card rounded-xl border border-card-border p-5">
            <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-4">Stage Progress</h2>
            <StageTimeline
              stageMetrics={run.stage_metrics ?? []}
              status={run.status}
            />
          </div>
        </div>

        <div className="lg:col-span-2 space-y-4">
          {stageChartData.length > 0 && (
            <div className="bg-card rounded-xl border border-card-border p-5">
              <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-4">Stage Latency (seconds)</h2>
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={stageChartData} margin={{ top: 0, right: 0, left: -20, bottom: 0 }}>
                  <XAxis
                    dataKey="name"
                    tick={{ fontSize: 11, fill: "hsl(215 20% 55%)" }}
                    axisLine={{ stroke: "hsl(216 34% 17%)" }}
                    tickLine={false}
                  />
                  <YAxis
                    tick={{ fontSize: 11, fill: "hsl(215 20% 55%)" }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "hsl(222 47% 14%)",
                      border: "1px solid hsl(216 34% 20%)",
                      borderRadius: "8px",
                      color: "hsl(213 31% 91%)",
                      fontSize: "12px",
                    }}
                    formatter={(v: number) => [`${v}s`, "Latency"]}
                  />
                  <Bar dataKey="latency" radius={[4, 4, 0, 0]}>
                    {stageChartData.map((entry, i) => (
                      <Cell key={i} fill={entry.color} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {isDone && run.metrics?.total_records && run.metrics.total_records > 0 ? (
            <div className="bg-card rounded-xl border border-card-border p-5">
              <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-4">Download Dataset</h2>
              <div className="flex flex-wrap gap-2">
                {(["jsonl", "csv", "report"] as const).map((fmt) => (
                  <a key={fmt} href={downloadUrl(run.run_id, fmt)} download>
                    <Button variant="outline" size="sm" className="gap-2 border-border text-foreground hover:bg-accent">
                      <Download className="w-3.5 h-3.5" />
                      {fmt.toUpperCase()}
                    </Button>
                  </a>
                ))}
              </div>
            </div>
          ) : isDone && (
            <div className="bg-card rounded-xl border border-card-border p-5">
              <p className="text-sm text-muted-foreground">No records to download — check the run configuration and source quality.</p>
            </div>
          )}

          {run.config && (
            <div className="bg-card rounded-xl border border-card-border p-5">
              <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Configuration</h2>
              <pre className="text-xs text-muted-foreground bg-background rounded-lg p-3 overflow-auto max-h-48 border border-border">
                {JSON.stringify(run.config, null, 2)}
              </pre>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
