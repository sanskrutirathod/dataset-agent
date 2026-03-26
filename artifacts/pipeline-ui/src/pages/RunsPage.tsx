import { useQuery } from "@tanstack/react-query";
import { Link } from "wouter";
import { listRuns } from "@/lib/pipeline-api";
import { StatusBadge } from "@/components/StatusBadge";
import { Button } from "@/components/ui/button";
import { Plus, RefreshCw, Zap } from "lucide-react";
import { formatDistanceToNow } from "date-fns";

const ALL_STAGES = ["ingest", "clean", "dedup", "chunk", "generate", "validate", "score", "export"];

function getCurrentStage(run: { status: string; stage_metrics?: Array<{ stage: string }> }): string {
  const metrics = run.stage_metrics ?? [];
  if (run.status === "completed" || run.status === "partial") {
    return `${metrics.length}/${ALL_STAGES.length}`;
  }
  if (run.status === "failed") {
    const last = metrics[metrics.length - 1];
    return last ? last.stage : "—";
  }
  if (run.status === "running") {
    const last = metrics[metrics.length - 1];
    const lastIdx = last ? ALL_STAGES.indexOf(last.stage) : -1;
    const next = ALL_STAGES[lastIdx + 1];
    return next ?? last?.stage ?? "starting";
  }
  return "—";
}

export default function RunsPage() {
  const { data: runs, isLoading, refetch, isFetching } = useQuery({
    queryKey: ["runs"],
    queryFn: listRuns,
    refetchInterval: (query) => {
      const hasActiveRun = query.state.data?.some(
        (r) => r.status === "running" || r.status === "pending"
      );
      return hasActiveRun ? 3000 : false;
    },
  });

  const sortedRuns = runs ? [...runs].reverse() : [];

  return (
    <div className="p-6 max-w-5xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-foreground">Pipeline Runs</h1>
          <p className="text-sm text-muted-foreground mt-1">Monitor and manage your dataset generation runs</p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => refetch()}
            disabled={isFetching}
            className="gap-1.5 border-border text-muted-foreground hover:text-foreground"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isFetching ? "animate-spin" : ""}`} />
            Refresh
          </Button>
          <Link href="/new">
            <Button size="sm" className="gap-1.5 bg-primary text-primary-foreground hover:bg-primary/90">
              <Plus className="w-3.5 h-3.5" />
              New Run
            </Button>
          </Link>
        </div>
      </div>

      {isLoading ? (
        <div className="space-y-2">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="h-16 rounded-xl bg-card animate-pulse border border-card-border" />
          ))}
        </div>
      ) : !runs || runs.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-20 text-center">
          <div className="w-16 h-16 rounded-2xl bg-primary/10 flex items-center justify-center mb-4">
            <Zap className="w-8 h-8 text-primary" />
          </div>
          <h2 className="text-lg font-semibold text-foreground">No runs yet</h2>
          <p className="text-sm text-muted-foreground mt-1 mb-4">Start your first dataset generation run</p>
          <Link href="/new">
            <Button className="bg-primary text-primary-foreground hover:bg-primary/90">Create New Run</Button>
          </Link>
        </div>
      ) : (
        <div className="bg-card rounded-xl border border-card-border overflow-hidden">
          <table className="min-w-full divide-y divide-border">
            <thead className="bg-muted/30">
              <tr>
                <th className="px-5 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">Run</th>
                <th className="px-5 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">Status</th>
                <th className="px-5 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">Stage</th>
                <th className="px-5 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">Records</th>
                <th className="px-5 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">Ingest</th>
                <th className="px-5 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">Val. Pass</th>
                <th className="px-5 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">Avg Score</th>
                <th className="px-5 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider">Created</th>
                <th className="px-5 py-3" />
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {sortedRuns.map((run) => (
                <tr key={run.run_id} className="hover:bg-accent/40 transition-colors">
                  <td className="px-5 py-4">
                    <div className="text-sm font-medium text-foreground">{run.run_name}</div>
                    <div className="text-xs text-muted-foreground font-mono mt-0.5">{run.run_id}</div>
                  </td>
                  <td className="px-5 py-4">
                    <StatusBadge status={run.status} />
                  </td>
                  <td className="px-5 py-4 text-sm text-muted-foreground font-mono tabular-nums">
                    {getCurrentStage(run)}
                  </td>
                  <td className="px-5 py-4 text-sm text-foreground tabular-nums">
                    {run.metrics?.total_records ?? <span className="text-muted-foreground">—</span>}
                  </td>
                  <td className="px-5 py-4 text-sm tabular-nums">
                    {run.metrics?.ingest_success_rate != null
                      ? <span className="text-foreground">{(run.metrics.ingest_success_rate * 100).toFixed(0)}%</span>
                      : <span className="text-muted-foreground">—</span>}
                  </td>
                  <td className="px-5 py-4 text-sm tabular-nums">
                    {run.metrics?.validation_pass_rate != null
                      ? <span className="text-foreground">{(run.metrics.validation_pass_rate * 100).toFixed(0)}%</span>
                      : <span className="text-muted-foreground">—</span>}
                  </td>
                  <td className="px-5 py-4 text-sm tabular-nums">
                    {run.metrics?.avg_final_score != null
                      ? <span className="text-foreground">{(run.metrics.avg_final_score * 100).toFixed(0)}%</span>
                      : <span className="text-muted-foreground">—</span>}
                  </td>
                  <td className="px-5 py-4 text-sm text-muted-foreground whitespace-nowrap">
                    {formatDistanceToNow(new Date(run.created_at), { addSuffix: true })}
                  </td>
                  <td className="px-5 py-4 text-right">
                    <Link href={`/runs/${run.run_id}`}>
                      <Button variant="ghost" size="sm" className="text-primary hover:text-primary/80 hover:bg-primary/10 text-xs">
                        View →
                      </Button>
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
