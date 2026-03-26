import { CheckCircle, XCircle, Clock, Loader2 } from "lucide-react";
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
          const isActive = !isDone && lastCompleted === ALL_STAGES[idx - 1] && (status === "running");
          const isLast = idx === ALL_STAGES.length - 1;

          return (
            <li key={stage}>
              <div className="relative pb-8">
                {!isLast && (
                  <span
                    className={`absolute left-4 top-4 -ml-px h-full w-0.5 ${isDone ? "bg-indigo-200" : "bg-gray-200"}`}
                    aria-hidden="true"
                  />
                )}
                <div className="relative flex items-start space-x-3">
                  <div className="relative">
                    {isDone ? (
                      <span className="flex h-8 w-8 items-center justify-center rounded-full bg-indigo-100 ring-4 ring-white">
                        <CheckCircle className="h-5 w-5 text-indigo-600" />
                      </span>
                    ) : isActive ? (
                      <span className="flex h-8 w-8 items-center justify-center rounded-full bg-blue-100 ring-4 ring-white">
                        <Loader2 className="h-5 w-5 text-blue-600 animate-spin" />
                      </span>
                    ) : (
                      <span className="flex h-8 w-8 items-center justify-center rounded-full bg-gray-100 ring-4 ring-white">
                        <Clock className="h-5 w-5 text-gray-400" />
                      </span>
                    )}
                  </div>
                  <div className="min-w-0 flex-1 pt-1.5">
                    <div className="flex items-center justify-between">
                      <p className={`text-sm font-medium ${isDone ? "text-gray-900" : "text-gray-400"}`}>
                        {STAGE_LABELS[stage] ?? stage}
                      </p>
                      {metric && (
                        <span className="text-xs text-gray-400">{fmt(metric.latency_ms)}</span>
                      )}
                    </div>
                    {metric && (
                      <p className="text-xs text-gray-500">
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
