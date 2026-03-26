import { CheckCircle, Clock, Loader2 } from "lucide-react";
import type { StageMetrics } from "@/lib/pipeline-api";

const STAGE_LABELS: Record<string, string> = {
  ingest: "Ingest",
  clean: "Clean",
  dedup: "Dedup",
  chunk: "Chunk",
  generate: "Generate",
  validate: "Validate",
  score: "Score",
  export: "Export",
};

const ALL_STAGES = ["ingest", "clean", "dedup", "chunk", "generate", "validate", "score", "export"];

interface StageTimelineProps {
  stageMetrics: StageMetrics[];
  status: string;
}

function fmt(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

export function StageTimeline({ stageMetrics, status }: StageTimelineProps) {
  const completed = new Set(stageMetrics.map((m) => m.stage));
  const lastCompleted = stageMetrics.length > 0 ? stageMetrics[stageMetrics.length - 1].stage : null;

  return (
    <div className="flow-root">
      <ul className="-mb-8">
        {ALL_STAGES.map((stage, idx) => {
          const metric = stageMetrics.find((m) => m.stage === stage);
          const isDone = completed.has(stage);
          const isActive = !isDone && lastCompleted === ALL_STAGES[idx - 1] && status === "running";
          const isLast = idx === ALL_STAGES.length - 1;

          return (
            <li key={stage}>
              <div className="relative pb-7">
                {!isLast && (
                  <span
                    className={`absolute left-4 top-4 -ml-px h-full w-0.5 ${
                      isDone ? "bg-primary/30" : "bg-border"
                    }`}
                    aria-hidden="true"
                  />
                )}
                <div className="relative flex items-start space-x-3">
                  <div className="relative">
                    {isDone ? (
                      <span className="flex h-8 w-8 items-center justify-center rounded-full bg-primary/10 ring-2 ring-background ring-offset-0">
                        <CheckCircle className="h-4 w-4 text-primary" />
                      </span>
                    ) : isActive ? (
                      <span className="flex h-8 w-8 items-center justify-center rounded-full bg-blue-500/10 ring-2 ring-background">
                        <Loader2 className="h-4 w-4 text-blue-400 animate-spin" />
                      </span>
                    ) : (
                      <span className="flex h-8 w-8 items-center justify-center rounded-full bg-muted ring-2 ring-background">
                        <Clock className="h-4 w-4 text-muted-foreground" />
                      </span>
                    )}
                  </div>
                  <div className="min-w-0 flex-1 pt-1.5">
                    <div className="flex items-center justify-between">
                      <p className={`text-sm font-medium ${isDone ? "text-foreground" : "text-muted-foreground"}`}>
                        {STAGE_LABELS[stage] ?? stage}
                      </p>
                      {metric && metric.latency_ms > 0 && (
                        <span className="text-xs text-muted-foreground tabular-nums">{fmt(metric.latency_ms)}</span>
                      )}
                    </div>
                    {metric && (
                      <p className="text-xs text-muted-foreground mt-0.5">
                        {metric.input_count} in → {metric.output_count} out
                        {metric.notes ? ` · ${metric.notes}` : ""}
                      </p>
                    )}
                  </div>
                </div>
              </div>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
