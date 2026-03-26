import { apiFetch } from "./api";

export interface SourceConfig {
  type: "url" | "text" | "file";
  value: string;
  title?: string;
}

export interface PipelineConfig {
  run_name: string;
  sources: SourceConfig[];
  chunk?: { target_tokens?: number; overlap?: number };
  generation?: { mode?: "qa" | "instruction" | "summary" | "chat"; max_records_per_chunk?: number };
  validation?: { min_length?: number; max_length?: number; score_threshold?: number; grounding_min_overlap?: number };
  limits?: { max_records?: number; max_per_source?: number };
}

export interface PipelineRunResponse {
  run_id: string;
  status: string;
  message?: string;
}

export interface RunMetrics {
  ingest_success_rate?: number;
  dedup_ratio?: number;
  avg_chunk_tokens?: number;
  generation_yield?: number;
  validation_pass_rate?: number;
  avg_final_score?: number;
  total_records?: number;
  drop_count?: number;
}

export interface StageMetrics {
  stage: string;
  input_count: number;
  output_count: number;
  latency_ms: number;
  notes?: string;
}

export interface RunListItem {
  run_id: string;
  run_name: string;
  status: "pending" | "running" | "completed" | "failed" | "partial";
  created_at: string;
  updated_at: string;
  metrics?: RunMetrics;
  stage_metrics?: StageMetrics[];
}

export interface RunDetail extends RunListItem {
  config?: Record<string, unknown>;
  error?: string | null;
}

export async function startPipelineRun(config: PipelineConfig): Promise<PipelineRunResponse> {
  return apiFetch<PipelineRunResponse>("/pipeline/run", {
    method: "POST",
    body: JSON.stringify({ config }),
  });
}

export async function listRuns(): Promise<RunListItem[]> {
  return apiFetch<RunListItem[]>("/pipeline/runs");
}

export async function getRun(runId: string): Promise<RunDetail> {
  return apiFetch<RunDetail>(`/pipeline/runs/${runId}`);
}

export function downloadUrl(runId: string, format: "jsonl" | "csv" | "report"): string {
  const base = import.meta.env.BASE_URL.replace(/\/$/, "");
  return `${base}/pipeline/runs/${runId}/download?format=${format}`;
}
