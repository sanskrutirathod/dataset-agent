import { useQuery } from "@tanstack/react-query";
import { useParams, Link } from "wouter";
import { getRun, downloadUrl } from "@/lib/pipeline-api";
import { StatusBadge } from "@/components/StatusBadge";
import { StageTimeline } from "@/components/StageTimeline";
import { MetricCard } from "@/components/MetricCard";
import { Button } from "@/components/ui/button";
import { ArrowLeft, Download } from "lucide-react";
import { formatDistanceToNow } from "date-fns";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";

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
          <div key={i} className="h-24 bg-gray-100 rounded-xl animate-pulse" />
        ))}
      </div>
    );
  }

  if (!run) {
    return (
      <div className="p-6 text-center text-gray-500">Run not found</div>
    );
  }

  const stageChartData = (run.stage_metrics ?? []).map((s) => ({
    name: s.stage,
    latency: parseFloat((s.latency_ms / 1000).toFixed(3)),
    out: s.output_count,
  }));

  const isDone = run.status === "completed" || run.status === "partial";

  return (
    <div className="p-6 max-w-5xl mx-auto space-y-6">
      <div className="flex items-center gap-3">
        <Link href="/">
          <Button variant="ghost" size="sm" className="gap-1 text-gray-500">
            <ArrowLeft className="w-4 h-4" />
            Runs
          </Button>
        </Link>
        <div className="h-4 w-px bg-gray-300" />
        <h1 className="text-xl font-bold text-gray-900">{run.run_name}</h1>
        <StatusBadge status={run.status} />
      </div>

      <div className="flex items-center gap-4 text-sm text-gray-500">
        <span className="font-mono">{run.run_id}</span>
        <span>·</span>
        <span>Created {formatDistanceToNow(new Date(run.created_at), { addSuffix: true })}</span>
        {run.error && (
          <>
            <span>·</span>
            <span className="text-red-600">{run.error}</span>
          </>
        )}
      </div>

      {run.metrics && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
          <MetricCard
            label="Total Records"
            value={run.metrics.total_records ?? 0}
            color={run.metrics.total_records ? "green" : "default"}
          />
          <MetricCard
            label="Avg Score"
            value={run.metrics.avg_final_score != null ? (run.metrics.avg_final_score * 100).toFixed(0) + "%" : "—"}
            color="blue"
          />
          <MetricCard
            label="Validation Pass"
            value={run.metrics.validation_pass_rate != null ? (run.metrics.validation_pass_rate * 100).toFixed(0) + "%" : "—"}
            sub={`${run.metrics.drop_count ?? 0} dropped`}
            color="yellow"
          />
          <MetricCard
            label="Avg Chunk Tokens"
            value={run.metrics.avg_chunk_tokens?.toFixed(0) ?? "—"}
            sub={`Dedup: ${run.metrics.dedup_ratio != null ? (run.metrics.dedup_ratio * 100).toFixed(0) + "%" : "—"}`}
          />
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-1">
          <div className="bg-white rounded-xl border border-gray-200 p-5">
            <h2 className="text-sm font-semibold text-gray-700 mb-4">Stage Progress</h2>
            <StageTimeline
              stageMetrics={run.stage_metrics ?? []}
              status={run.status}
            />
          </div>
        </div>

        <div className="lg:col-span-2 space-y-4">
          {stageChartData.length > 0 && (
            <div className="bg-white rounded-xl border border-gray-200 p-5">
              <h2 className="text-sm font-semibold text-gray-700 mb-4">Stage Latency (seconds)</h2>
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={stageChartData} margin={{ top: 0, right: 0, left: -20, bottom: 0 }}>
                  <XAxis dataKey="name" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} />
                  <Tooltip formatter={(v: number) => [`${v}s`, "Latency"]} />
                  <Bar dataKey="latency" radius={[4, 4, 0, 0]}>
                    {stageChartData.map((_, i) => (
                      <Cell key={i} fill={i === stageChartData.length - 1 ? "#6366f1" : "#a5b4fc"} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {isDone && run.metrics?.total_records && run.metrics.total_records > 0 ? (
            <div className="bg-white rounded-xl border border-gray-200 p-5">
              <h2 className="text-sm font-semibold text-gray-700 mb-4">Download Dataset</h2>
              <div className="flex flex-wrap gap-3">
                {(["jsonl", "csv", "report"] as const).map((fmt) => (
                  <a key={fmt} href={downloadUrl(run.run_id, fmt)} download>
                    <Button variant="outline" className="gap-2">
                      <Download className="w-4 h-4" />
                      {fmt.toUpperCase()}
                    </Button>
                  </a>
                ))}
              </div>
            </div>
          ) : isDone && (
            <div className="bg-gray-50 rounded-xl border border-gray-200 p-5 text-sm text-gray-500">
              No records to download — check the run configuration.
            </div>
          )}

          {run.config && (
            <div className="bg-white rounded-xl border border-gray-200 p-5">
              <h2 className="text-sm font-semibold text-gray-700 mb-3">Configuration</h2>
              <pre className="text-xs text-gray-600 bg-gray-50 rounded-lg p-3 overflow-auto max-h-48">
                {JSON.stringify(run.config, null, 2)}
              </pre>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
