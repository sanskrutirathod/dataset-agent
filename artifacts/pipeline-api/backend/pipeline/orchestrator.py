"""Pipeline orchestrator: linear pipeline with checkpointing."""
from __future__ import annotations
import json
import logging
import time
from pathlib import Path
from datetime import datetime

from ..schemas import (
    PipelineConfig, RunStatus, RunMetrics, StageMetrics, Source, Chunk, DatasetRecord
)
from ..modules.ingest import run_ingest
from ..modules.clean import run_clean
from ..modules.dedup import run_dedup
from ..modules.chunk import run_chunk
from ..modules.generate import run_generate
from ..modules.validate import run_validate
from ..modules.score import run_score_and_filter
from ..modules.export import run_export
from .db import create_run, update_run_status, update_run_metrics

logger = logging.getLogger(__name__)

DATA_BASE = Path("data")
VERSIONS_BASE = DATA_BASE / "versions"


def _checkpoint_path(run_dir: Path, stage: str) -> Path:
    return run_dir / f"checkpoint_{stage}.done"


def _is_done(run_dir: Path, stage: str) -> bool:
    return _checkpoint_path(run_dir, stage).exists()


def _mark_done(run_dir: Path, stage: str) -> None:
    _checkpoint_path(run_dir, stage).write_text(datetime.utcnow().isoformat())


def _load_sources(path: Path) -> list[Source]:
    if not path.exists():
        return []
    results = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                results.append(Source.model_validate_json(line))
    return results


def _load_chunks(path: Path) -> list[Chunk]:
    if not path.exists():
        return []
    results = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                results.append(Chunk.model_validate_json(line))
    return results


def _load_records(path: Path) -> list[DatasetRecord]:
    if not path.exists():
        return []
    results = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                results.append(DatasetRecord.model_validate_json(line))
    return results


def run_pipeline(run_id: str, config: PipelineConfig) -> None:
    """Execute the full pipeline linearly with per-stage checkpoints."""
    run_dir = VERSIONS_BASE / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    (run_dir / "config.json").write_text(config.model_dump_json(indent=2))

    stage_metrics: list[StageMetrics] = []

    def timed_stage(name: str, fn, *args, **kwargs):
        t0 = time.time()
        result = fn(*args, **kwargs)
        ms = (time.time() - t0) * 1000
        return result, ms

    update_run_status(run_id, RunStatus.running)

    try:
        # Stage 1: Ingest
        raw_dir = run_dir / "raw"
        if not _is_done(run_dir, "ingest"):
            sources, ms = timed_stage(
                "ingest", run_ingest, config.sources, raw_dir
            )
            _mark_done(run_dir, "ingest")
        else:
            sources = _load_sources(raw_dir / "sources.jsonl")
            ms = 0.0
        stage_metrics.append(StageMetrics(
            stage="ingest",
            input_count=len(config.sources),
            output_count=len(sources),
            latency_ms=ms,
        ))
        logger.info(f"[{run_id}] ingest: {len(sources)} sources")

        if not sources:
            update_run_status(run_id, RunStatus.failed, "No sources ingested")
            return

        # Stage 2: Clean
        processed_dir = run_dir / "processed"
        if not _is_done(run_dir, "clean"):
            cleaned, ms = timed_stage("clean", run_clean, sources, processed_dir)
            _mark_done(run_dir, "clean")
        else:
            cleaned = _load_sources(processed_dir / "sources.jsonl")
            ms = 0.0
        stage_metrics.append(StageMetrics(
            stage="clean",
            input_count=len(sources),
            output_count=len(cleaned),
            latency_ms=ms,
        ))

        # Stage 3: Dedup
        dedup_dir = run_dir / "dedup"
        if not _is_done(run_dir, "dedup"):
            deduped, ms = timed_stage("dedup", run_dedup, cleaned, dedup_dir)
            _mark_done(run_dir, "dedup")
        else:
            deduped = _load_sources(dedup_dir / "sources.jsonl")
            ms = 0.0
        stage_metrics.append(StageMetrics(
            stage="dedup",
            input_count=len(cleaned),
            output_count=len(deduped),
            latency_ms=ms,
        ))

        # Stage 4: Chunk
        chunk_dir = run_dir / "chunks"
        if not _is_done(run_dir, "chunk"):
            chunks, ms = timed_stage("chunk", run_chunk, deduped, config.chunk, chunk_dir)
            _mark_done(run_dir, "chunk")
        else:
            chunks = _load_chunks(chunk_dir / "chunks.jsonl")
            ms = 0.0
        avg_tokens = sum(c.tokens for c in chunks) / max(len(chunks), 1)
        stage_metrics.append(StageMetrics(
            stage="chunk",
            input_count=len(deduped),
            output_count=len(chunks),
            latency_ms=ms,
            notes=f"avg_tokens={avg_tokens:.1f}",
        ))

        if not chunks:
            update_run_status(run_id, RunStatus.failed, "No chunks produced")
            return

        # Stage 5: Generate
        gen_dir = run_dir / "generated"
        if not _is_done(run_dir, "generate"):
            records, ms = timed_stage(
                "generate", run_generate, chunks, config.generation, gen_dir,
                config.limits.max_records
            )
            _mark_done(run_dir, "generate")
        else:
            records = _load_records(gen_dir / "records.jsonl")
            ms = 0.0
        stage_metrics.append(StageMetrics(
            stage="generate",
            input_count=len(chunks),
            output_count=len(records),
            latency_ms=ms,
        ))

        if not records:
            update_run_status(run_id, RunStatus.partial, "No records generated")
            _compute_and_save_metrics(run_id, sources, cleaned, deduped, chunks, records, [], stage_metrics)
            return

        # Stage 6: Validate
        val_dir = run_dir / "validated"
        if not _is_done(run_dir, "validate"):
            valid_records, ms = timed_stage(
                "validate", run_validate, records, chunks, config.validation, val_dir
            )
            _mark_done(run_dir, "validate")
        else:
            valid_records = _load_records(val_dir / "records.jsonl")
            ms = 0.0
        stage_metrics.append(StageMetrics(
            stage="validate",
            input_count=len(records),
            output_count=len(valid_records),
            latency_ms=ms,
        ))

        # Stage 7: Score + Filter
        scored_dir = run_dir / "scored"
        if not _is_done(run_dir, "score"):
            final_records, ms = timed_stage(
                "score", run_score_and_filter,
                valid_records, chunks, config.validation, scored_dir,
                config.limits.max_per_source
            )
            _mark_done(run_dir, "score")
        else:
            final_records = _load_records(scored_dir / "records.jsonl")
            ms = 0.0
        stage_metrics.append(StageMetrics(
            stage="score",
            input_count=len(valid_records),
            output_count=len(final_records),
            latency_ms=ms,
        ))

        # Stage 8: Export
        export_dir = run_dir / "export"
        if not _is_done(run_dir, "export"):
            run_export(final_records, stage_metrics, export_dir, run_id, config.run_name)
            _mark_done(run_dir, "export")
        stage_metrics.append(StageMetrics(
            stage="export",
            input_count=len(final_records),
            output_count=len(final_records),
        ))

        _compute_and_save_metrics(
            run_id, sources, cleaned, deduped, chunks, records, final_records,
            stage_metrics, attempted_source_count=len(config.sources)
        )
        update_run_status(run_id, RunStatus.completed)
        logger.info(f"[{run_id}] pipeline completed: {len(final_records)} final records")

    except Exception as e:
        logger.exception(f"[{run_id}] Pipeline failed: {e}")
        update_run_status(run_id, RunStatus.failed, str(e))


def _compute_and_save_metrics(
    run_id: str,
    sources, cleaned, deduped, chunks, generated, final_records,
    stage_metrics: list[StageMetrics],
    attempted_source_count: int = 0,
) -> None:
    from .db import update_run_metrics
    n_sources_attempted = max(attempted_source_count, len(sources), 1)
    n_sources_ingested = len(sources)
    n_deduped = len(deduped)
    n_chunks = max(len(chunks), 1)
    n_generated = len(generated)
    n_final = len(final_records)

    avg_chunk_tokens = sum(c.tokens for c in chunks) / n_chunks if chunks else 0.0
    avg_score = sum(r.scores.final for r in final_records) / max(n_final, 1) if final_records else 0.0

    metrics = RunMetrics(
        ingest_success_rate=n_sources_ingested / n_sources_attempted,
        dedup_ratio=1.0 - (n_deduped / max(n_sources_ingested, 1)),
        avg_chunk_tokens=avg_chunk_tokens,
        generation_yield=n_generated / n_chunks,
        validation_pass_rate=n_final / max(n_generated, 1),
        avg_final_score=round(avg_score, 4),
        total_records=n_final,
        drop_count=max(n_generated - n_final, 0),
    )
    update_run_metrics(run_id, metrics, stage_metrics)
