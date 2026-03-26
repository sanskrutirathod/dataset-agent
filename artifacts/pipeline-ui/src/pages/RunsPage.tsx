import { useQuery } from "@tanstack/react-query";
import { Link } from "wouter";
import { listRuns } from "@/lib/pipeline-api";
import { StatusBadge } from "@/components/StatusBadge";
import { Button } from "@/components/ui/button";
import { Plus, RefreshCw } from "lucide-react";
import { formatDistanceToNow } from "date-fns";

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

  return (
    <div className="p-6 max-w-5xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Pipeline Runs</h1>
          <p className="text-sm text-gray-500 mt-1">Monitor and manage your dataset generation runs</p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => refetch()}
            disabled={isFetching}
            className="gap-1"
          >
            <RefreshCw className={`w-4 h-4 ${isFetching ? "animate-spin" : ""}`} />
            Refresh
          </Button>
          <Link href="/new">
            <Button size="sm" className="gap-1 bg-indigo-600 hover:bg-indigo-700">
              <Plus className="w-4 h-4" />
              New Run
            </Button>
          </Link>
        </div>
      </div>

      {isLoading ? (
        <div className="space-y-3">
          {[...Array(3)].map((_, i) => (
            <div key={i} className="h-20 rounded-xl bg-gray-100 animate-pulse" />
          ))}
        </div>
      ) : !runs || runs.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-20 text-center">
          <div className="w-16 h-16 rounded-2xl bg-indigo-50 flex items-center justify-center mb-4">
            <Plus className="w-8 h-8 text-indigo-400" />
          </div>
          <h2 className="text-lg font-semibold text-gray-900">No runs yet</h2>
          <p className="text-sm text-gray-500 mt-1 mb-4">Start your first dataset generation run</p>
          <Link href="/new">
            <Button className="bg-indigo-600 hover:bg-indigo-700">Create New Run</Button>
          </Link>
        </div>
      ) : (
        <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Run</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Records</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Score</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Created</th>
                <th className="px-6 py-3"></th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-100">
              {[...runs].reverse().map((run) => (
                <tr key={run.run_id} className="hover:bg-gray-50 transition-colors">
                  <td className="px-6 py-4">
                    <div className="text-sm font-medium text-gray-900">{run.run_name}</div>
                    <div className="text-xs text-gray-400 font-mono">{run.run_id}</div>
                  </td>
                  <td className="px-6 py-4">
                    <StatusBadge status={run.status} />
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-700">
                    {run.metrics?.total_records ?? "—"}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-700">
                    {run.metrics?.avg_final_score != null
                      ? (run.metrics.avg_final_score * 100).toFixed(0) + "%"
                      : "—"}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-500">
                    {formatDistanceToNow(new Date(run.created_at), { addSuffix: true })}
                  </td>
                  <td className="px-6 py-4 text-right">
                    <Link href={`/runs/${run.run_id}`}>
                      <Button variant="ghost" size="sm" className="text-indigo-600 hover:text-indigo-700">
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
