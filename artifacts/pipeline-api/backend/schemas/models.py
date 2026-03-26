from __future__ import annotations
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field
from datetime import datetime


class SourceType(str, Enum):
    web = "web"
    file = "file"
    text = "text"


class Source(BaseModel):
    source_id: str
    type: SourceType
    title: str = ""
    uri: str = ""
    raw_text: str
    meta: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class Chunk(BaseModel):
    chunk_id: str
    source_id: str
    idx: int
    text: str
    tokens: int
    overlap: int = 80
    section: str = ""
    hash: str
    flags: list[str] = Field(default_factory=list)


class DatasetRecordScores(BaseModel):
    relevance: float = 0.0
    clarity: float = 0.0
    grounding: float = 0.0
    diversity: float = 0.0
    final: float = 0.0


class DatasetRecord(BaseModel):
    id: str
    type: str
    instruction: str = ""
    input: str = ""
    output: str
    provenance: dict[str, str] = Field(default_factory=dict)
    scores: DatasetRecordScores = Field(default_factory=DatasetRecordScores)
    meta: dict[str, Any] = Field(default_factory=dict)


class ChunkConfig(BaseModel):
    target_tokens: int = 600
    overlap: int = 80


class GenerationConfig(BaseModel):
    mode: str = "qa"
    temperature: float = 0.2
    max_records_per_chunk: int = 1


class ValidationConfig(BaseModel):
    min_length: int = 50
    max_length: int = 4096
    score_threshold: float = 0.75
    grounding_min_overlap: float = 0.1


class LimitsConfig(BaseModel):
    max_records: int = 5000
    max_per_source: int = 1000


class PipelineConfig(BaseModel):
    run_name: str = "default_run"
    sources: list[dict[str, Any]] = Field(default_factory=list)
    chunk: ChunkConfig = Field(default_factory=ChunkConfig)
    generation: GenerationConfig = Field(default_factory=GenerationConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    limits: LimitsConfig = Field(default_factory=LimitsConfig)


class StageMetrics(BaseModel):
    stage: str
    input_count: int = 0
    output_count: int = 0
    latency_ms: float = 0.0
    notes: str = ""


class RunMetrics(BaseModel):
    ingest_success_rate: float = 0.0
    dedup_ratio: float = 0.0
    avg_chunk_tokens: float = 0.0
    generation_yield: float = 0.0
    validation_pass_rate: float = 0.0
    avg_final_score: float = 0.0
    total_records: int = 0
    drop_count: int = 0


class RunStatus(str, Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"
    partial = "partial"


class IngestRequest(BaseModel):
    sources: list[dict[str, Any]]


class IngestResponse(BaseModel):
    source_ids: list[str]
    count: int


class PipelineRunRequest(BaseModel):
    config: PipelineConfig


class PipelineRunResponse(BaseModel):
    run_id: str
    status: RunStatus
    message: str = ""


class RunListItem(BaseModel):
    run_id: str
    run_name: str
    status: RunStatus
    created_at: str
    updated_at: str
    metrics: Optional[RunMetrics] = None
    stage_metrics: list[StageMetrics] = Field(default_factory=list)


class RunDetail(BaseModel):
    run_id: str
    run_name: str
    status: RunStatus
    created_at: str
    updated_at: str
    config: Optional[dict[str, Any]] = None
    metrics: Optional[RunMetrics] = None
    stage_metrics: list[StageMetrics] = Field(default_factory=list)
    error: Optional[str] = None


class DownloadFormat(str, Enum):
    jsonl = "jsonl"
    csv = "csv"
    report = "report"
