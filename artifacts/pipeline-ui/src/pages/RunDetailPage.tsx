import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useParams, Link, useLocation } from "wouter";
import { getRun, downloadUrl, pushToHub, getHubStatus, deleteRun } from "@/lib/pipeline-api";
import { StatusBadge } from "@/components/StatusBadge";
import { StageTimeline } from "@/components/StageTimeline";
import { MetricCard } from "@/components/MetricCard";
import { Button } from "@/components/ui/button";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { ArrowLeft, Download, Upload, ExternalLink, X, Eye, EyeOff, Trash2 } from "lucide-react";
import { formatDistanceToNow } from "date-fns";
import { useState, useEffect, useRef } from "react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
} from "recharts";

const CHART_COLORS = ["#6366f1", "#818cf8", "#a5b4fc", "#c7d2fe", "#4f46e5", "#4338ca", "#3730a3", "#312e81"];

const DISTILLATION_LABELS: Record<string, string> = {
  cot: "Chain-of-Thought (CoT)",
  dpo: "Direct Preference (DPO)",
  sft: "Knowledge Distillation (SFT)",
};

function getDistillationMode(config: Record<string, unknown> | undefined): string | null {
  if (!config) return null;
  const generation = config.generation as Record<string, unknown> | undefined;
  return (generation?.distillation_mode as string) || null;
}

function sseUrl(runId: string): string {
  const base = import.meta.env.BASE_URL.replace(/\/$/, "");
  return `${base}/api/v1/runs/${runId}/stream`;
}

function PushToHubPanel({ runId, onClose }: { runId: string; onClose: () => void }) {
  const queryClient = useQueryClient();
  const [repoId, setRepoId] = useState("");
  const [isPrivate, setIsPrivate] = useState(false);
  const [description, setDescription] = useState("");
  const [split, setSplit] = useState("train");
  const [apiError, setApiError] = useState<string | null>(null);

  const mutation = useMutation({
    mutationFn: () => pushToHub(runId, { repo_id: repoId, private: isPrivate, description, split }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["run", runId] });
    },
    onError: (err: unknown) => {
      const msg = err instanceof Error ? err.message : String(err);
      setApiError(msg);
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setApiError(null);
    if (!repoId.trim()) return;
    mutation.mutate();
  };

  return (
    <div className="bg-card rounded-xl border border-card-border p-5">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
          Push to HuggingFace Hub
        </h2>
        <button
          onClick={onClose}
          className="text-muted-foreground hover:text-foreground transition-colors"
          aria-label="Close"
        >
          <X className="w-4 h-4" />
        </button>
      </div>

      <form onSubmit={handleSubmit} className="space-y-4">
        <div>
          <label className="block text-xs text-muted-foreground mb-1.5">
            Repository ID <span className="text-red-400">*</span>
          </label>
          <input
            type="text"
            value={repoId}
            onChange={(e) => setRepoId(e.target.value)}
            placeholder="username/my-dataset"
            className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
            required
          />
          <p className="text-xs text-muted-foreground mt-1">Format: username/dataset-name</p>
        </div>

        <div>
          <label className="block text-xs text-muted-foreground mb-1.5">Split name</label>
          <input
            type="text"
            value={split}
            onChange={(e) => setSplit(e.target.value)}
            placeholder="train"
            className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
          />
        </div>

        <div>
          <label className="block text-xs text-muted-foreground mb-1.5">Description (optional)</label>
          <textarea
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            rows={2}
            placeholder="A brief description of this dataset..."
            className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring resize-none"
          />
        </div>

        <div>
          <button
            type="button"
            onClick={() => setIsPrivate(!isPrivate)}
            className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground transition-colors"
          >
            {isPrivate ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
            {isPrivate ? "Private repository" : "Public repository"}
          </button>
        </div>

        {apiError && (
          <div className="rounded-lg bg-red-950/30 border border-red-800/40 px-3 py-2 text-xs text-red-400">
            {apiError}
          </div>
        )}

        <Button
          type="submit"
          size="sm"
          disabled={mutation.isPending || !repoId.trim()}
          className="gap-2 w-full"
        >
          <Upload className="w-3.5 h-3.5" />
          {mutation.isPending ? "Uploading…" : "Push to Hub"}
        </Button>
      </form>
    </div>
  );
}

function HubStatusCard({ runId, hfStatus, hfRepoUrl }: {
  runId: string;
  hfStatus: string | null | undefined;
  hfRepoUrl: string | null | undefined;
}) {
  const queryClient = useQueryClient();
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    if (hfStatus === "uploading") {
      pollingRef.current = setInterval(async () => {
        try {
          const res = await getHubStatus(runId);
          if (res.hf_status !== "uploading") {
            queryClient.invalidateQueries({ queryKey: ["run", runId] });
            if (pollingRef.current) clearInterval(pollingRef.current);
          }
        } catch {
        }
      }, 3000);
    }
    return () => {
      if (pollingRef.current) clearInterval(pollingRef.current);
    };
  }, [hfStatus, runId, queryClient]);

  if (!hfStatus) return null;

  if (hfStatus === "uploading") {
    return (
      <div className="bg-card rounded-xl border border-card-border p-5">
        <div className="flex items-center gap-3">
          <div className="w-4 h-4 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
          <span className="text-sm text-muted-foreground">Uploading to HuggingFace Hub…</span>
        </div>
      </div>
    );
  }

  if (hfStatus === "done" && hfRepoUrl) {
    return (
      <div className="bg-card rounded-xl border border-card-border p-5">
        <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
          HuggingFace Hub
        </h2>
        <a
          href={hfRepoUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-2 text-sm text-indigo-400 hover:text-indigo-300 transition-colors font-medium"
        >
          <ExternalLink className="w-4 h-4" />
          View on HuggingFace
        </a>
      </div>
    );
  }

  if (hfStatus === "error") {
    return (
      <div className="bg-card rounded-xl border border-red-800/30 p-5">
        <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2">
          HuggingFace Hub Upload Failed
        </h2>
        <p className="text-xs text-red-400">Upload encountered an error. Check that your HUGGINGFACE_TOKEN is valid and try again.</p>
      </div>
    );
  }

  return null;
}

export default function RunDetailPage() {
  const { id } = useParams<{ id: string }>();
  const [, setLocation] = useLocation();
  const queryClient = useQueryClient();
  const [showPushPanel, setShowPushPanel] = useState(false);
  const [sseActive, setSseActive] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const eventSourceRef = useRef<EventSource | null>(null);

  const { data: run, isLoading } = useQuery({
    queryKey: ["run", id],
    queryFn: () => getRun(id!),
    enabled: !!id,
    refetchInterval: (query) => {
      const s = query.state.data?.status;
      const hf = query.state.data?.hf_status;
      if (s === "running" || s === "pending") {
        return sseActive ? false : 3000;
      }
      return hf === "uploading" ? 3000 : false;
    },
  });

  useEffect(() => {
    if (!id || !run) return;
    const isActive = run.status === "running" || run.status === "pending";

    if (!isActive) {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
        setSseActive(false);
      }
      return;
    }

    if (eventSourceRef.current) return;

    if (typeof EventSource === "undefined") {
      return;
    }

    const es = new EventSource(sseUrl(id));
    eventSourceRef.current = es;
    setSseActive(true);

    es.onmessage = (e) => {
      try {
        const event = JSON.parse(e.data);
        if (event.event === "done") {
          queryClient.invalidateQueries({ queryKey: ["run", id] });
          es.close();
          eventSourceRef.current = null;
          setSseActive(false);
        } else if (event.event === "stage_complete") {
          queryClient.invalidateQueries({ queryKey: ["run", id] });
        }
      } catch {
        // ignore
      }
    };

    es.onerror = () => {
      es.close();
      eventSourceRef.current = null;
      setSseActive(false);
    };

    return () => {
      es.close();
      eventSourceRef.current = null;
      setSseActive(false);
    };
  }, [id, run?.status]);

  async function handleDelete() {
    if (!id) return;
    setDeleting(true);
    setDeleteError(null);
    try {
      await deleteRun(id);
      queryClient.invalidateQueries({ queryKey: ["runs"] });
      setLocation("/");
    } catch (err: unknown) {
      setDeleteError(err instanceof Error ? err.message : "Failed to delete run");
      setDeleting(false);
    }
  }

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
  const isActive = run.status === "running" || run.status === "pending";
  const distillMode = getDistillationMode(run.config);
  const isDpo = distillMode === "dpo";
  const hasRecords = (run.metrics?.total_records ?? 0) > 0;
  const canPush = isDone && hasRecords && !run.hf_status;
  const isUploading = run.hf_status === "uploading";
  const teacherModel = run.config
    ? String((run.config.generation as Record<string, unknown> | undefined)?.teacher_model ?? "")
    : "";

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
        {distillMode && (
          <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-violet-500/15 text-violet-400 border border-violet-500/25">
            Distillation · {DISTILLATION_LABELS[distillMode] ?? distillMode}
          </span>
        )}
        {sseActive && (
          <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-xs font-semibold bg-green-500/10 text-green-400 border border-green-500/20">
            <span className="w-1.5 h-1.5 rounded-full bg-green-400 animate-pulse" />
            Live
          </span>
        )}
        <div className="ml-auto">
          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button
                variant="ghost"
                size="sm"
                className="gap-1.5 text-muted-foreground hover:text-red-400 hover:bg-red-500/10"
                disabled={deleting || isActive}
                title={isActive ? "Cannot delete a running pipeline" : "Delete this run"}
              >
                <Trash2 className="w-4 h-4" />
                Delete
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>Delete run?</AlertDialogTitle>
                <AlertDialogDescription>
                  This will permanently delete <strong>{run.run_name}</strong> and all associated data on disk. This action cannot be undone.
                </AlertDialogDescription>
              </AlertDialogHeader>
              {deleteError && (
                <p className="text-sm text-red-400">{deleteError}</p>
              )}
              <AlertDialogFooter>
                <AlertDialogCancel>Cancel</AlertDialogCancel>
                <AlertDialogAction
                  onClick={handleDelete}
                  className="bg-red-600 hover:bg-red-700 text-white"
                >
                  Delete
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        </div>
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

          {isDone && hasRecords ? (
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
                {isDpo && (
                  <a href={downloadUrl(run.run_id, "dpo_jsonl")} download>
                    <Button variant="outline" size="sm" className="gap-2 border-violet-500/40 text-violet-400 hover:bg-violet-500/10">
                      <Download className="w-3.5 h-3.5" />
                      DPO JSONL
                    </Button>
                  </a>
                )}
                {canPush && !showPushPanel && (
                  <Button
                    variant="outline"
                    size="sm"
                    className="gap-2 border-indigo-700/60 text-indigo-300 hover:bg-indigo-900/30 hover:border-indigo-600"
                    onClick={() => setShowPushPanel(true)}
                  >
                    <Upload className="w-3.5 h-3.5" />
                    Push to HuggingFace
                  </Button>
                )}
                {isUploading && (
                  <Button variant="outline" size="sm" disabled className="gap-2 border-indigo-700/60 text-indigo-400">
                    <div className="w-3.5 h-3.5 border-2 border-indigo-400 border-t-transparent rounded-full animate-spin" />
                    Uploading…
                  </Button>
                )}
                {run.hf_status === "done" && run.hf_repo_url && (
                  <a href={run.hf_repo_url} target="_blank" rel="noopener noreferrer">
                    <Button variant="outline" size="sm" className="gap-2 border-indigo-700/60 text-indigo-300 hover:bg-indigo-900/30">
                      <ExternalLink className="w-3.5 h-3.5" />
                      View on HuggingFace
                    </Button>
                  </a>
                )}
              </div>
              {isDpo && (
                <p className="text-xs text-muted-foreground mt-2">
                  DPO JSONL is compatible with TRL's <code className="text-violet-400">DPOTrainer</code> format (prompt / chosen / rejected).
                </p>
              )}
            </div>
          ) : isDone && (
            <div className="bg-card rounded-xl border border-card-border p-5">
              <p className="text-sm text-muted-foreground">No records to download — check the run configuration and source quality.</p>
            </div>
          )}

          {showPushPanel && canPush && (
            <PushToHubPanel runId={run.run_id} onClose={() => setShowPushPanel(false)} />
          )}

          {run.hf_status && (
            <HubStatusCard
              runId={run.run_id}
              hfStatus={run.hf_status}
              hfRepoUrl={run.hf_repo_url}
            />
          )}

          {run.config && (
            <div className="bg-card rounded-xl border border-card-border p-5">
              <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Configuration</h2>
              {distillMode && (
                <div className="mb-3 flex items-center gap-2 text-xs text-muted-foreground">
                  <span>Distillation Mode:</span>
                  <span className="font-semibold text-violet-400">{DISTILLATION_LABELS[distillMode] ?? distillMode}</span>
                  {teacherModel && (
                    <>
                      <span>·</span>
                      <span>Teacher Model:</span>
                      <span className="font-mono text-foreground">{teacherModel}</span>
                    </>
                  )}
                </div>
              )}
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
