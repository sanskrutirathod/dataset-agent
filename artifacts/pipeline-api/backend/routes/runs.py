"""Pipeline run routes."""
from __future__ import annotations
import asyncio
import json
import logging
import os
import shutil
import threading
from pathlib import Path
from typing import Optional, AsyncGenerator

import yaml
from fastapi import APIRouter, HTTPException, BackgroundTasks, Request
from fastapi.responses import FileResponse, StreamingResponse

from ..schemas import (
    PipelineRunRequest, PipelineRunResponse, RunListItem, RunDetail,
    RunStatus, RunMetrics, StageMetrics, DownloadFormat, PipelineConfig,
    PushToHubRequest, HubStatusResponse,
)
from ..pipeline.db import (
    create_run, get_run, list_runs, update_run_hf_status, delete_run,
    get_aggregate_stats, get_total_records_generated, get_avg_pipeline_latency_ms
)
from ..pipeline.orchestrator import run_pipeline
from ..pipeline.job_queue import submit_job, get_status as get_queue_status
from ..pipeline import event_bus
from ..utils.ids import new_id

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/pipeline", tags=["pipeline"])
v1_router = APIRouter(prefix="/api/v1", tags=["v1"])

VERSIONS_BASE = Path("data/versions")


def _parse_metrics(row: dict) -> Optional[RunMetrics]:
    if not row.get("metrics_json"):
        return None
    try:
        return RunMetrics.model_validate_json(row["metrics_json"])
    except Exception:
        return None


def _parse_stage_metrics(row: dict) -> list[StageMetrics]:
    if not row.get("stage_metrics_json"):
        return []
    try:
        data = json.loads(row["stage_metrics_json"])
        return [StageMetrics.model_validate(sm) for sm in data]
    except Exception:
        return []


async def _parse_run_request(request: Request) -> PipelineConfig:
    """Parse pipeline run config from JSON or YAML body."""
    content_type = request.headers.get("content-type", "")
    body = await request.body()
    if not body:
        raise HTTPException(status_code=422, detail="Request body is required")

    if "yaml" in content_type or "text/plain" in content_type:
        try:
            data = yaml.safe_load(body.decode("utf-8"))
        except yaml.YAMLError as e:
            raise HTTPException(status_code=422, detail=f"Invalid YAML: {e}")
    else:
        try:
            data = json.loads(body)
        except json.JSONDecodeError as e:
            try:
                data = yaml.safe_load(body.decode("utf-8"))
            except Exception:
                raise HTTPException(status_code=422, detail=f"Invalid JSON: {e}")

    if not isinstance(data, dict):
        raise HTTPException(status_code=422, detail="Request body must be an object")

    config_data = data.get("config", data)
    try:
        return PipelineConfig.model_validate(config_data)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Config validation error: {e}")


def _start_run(config: PipelineConfig) -> PipelineRunResponse:
    run_id = new_id("run")
    create_run(run_id, config.run_name, config.model_dump())
    submit_job(run_pipeline, run_id, config)
    return PipelineRunResponse(
        run_id=run_id,
        status=RunStatus.running,
        message=f"Pipeline started with run_id={run_id}",
    )


@router.post("/run", response_model=PipelineRunResponse)
async def start_pipeline_run(
    request: Request,
    background_tasks: BackgroundTasks,
) -> PipelineRunResponse:
    """Start a new dataset pipeline run.

    Accepts either JSON or YAML body. The body may be either a full
    `{config: {...}}` wrapper or the config object directly.
    """
    config = await _parse_run_request(request)
    return _start_run(config)


def _make_run_list_item(row: dict) -> RunListItem:
    return RunListItem(
        run_id=row["run_id"],
        run_name=row["run_name"],
        status=RunStatus(row["status"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        metrics=_parse_metrics(row),
        stage_metrics=_parse_stage_metrics(row),
    )


def _make_run_detail(row: dict) -> RunDetail:
    config_data = None
    if row.get("config_json"):
        try:
            config_data = json.loads(row["config_json"])
        except Exception:
            pass

    return RunDetail(
        run_id=row["run_id"],
        run_name=row["run_name"],
        status=RunStatus(row["status"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        config=config_data,
        metrics=_parse_metrics(row),
        stage_metrics=_parse_stage_metrics(row),
        error=row.get("error") or None,
        hf_status=row.get("hf_status") or None,
        hf_repo_url=row.get("hf_repo_url") or None,
    )


def _do_push_to_hub(run_id: str, body: PushToHubRequest, token: str) -> None:
    from ..modules.hf_upload import push_to_hub

    export_dir = VERSIONS_BASE / run_id / "export"
    result = push_to_hub(
        run_id=run_id,
        repo_id=body.repo_id,
        token=token,
        private=body.private,
        split=body.split,
        description=body.description,
        export_dir=export_dir,
    )
    update_run_hf_status(
        run_id=run_id,
        hf_status=result["status"],
        hf_repo_url=result.get("url"),
    )


def _build_file_response(run_id: str, format: DownloadFormat) -> FileResponse:
    row = get_run(run_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    if row["status"] not in (RunStatus.completed.value, RunStatus.partial.value):
        raise HTTPException(status_code=409, detail=f"Run {run_id} is not complete yet")

    export_dir = VERSIONS_BASE / run_id / "export"
    if format == DownloadFormat.jsonl:
        path = export_dir / "dataset.jsonl"
        media_type = "application/x-ndjson"
        filename = f"dataset_{run_id}.jsonl"
    elif format == DownloadFormat.csv:
        path = export_dir / "dataset.csv"
        media_type = "text/csv"
        filename = f"dataset_{run_id}.csv"
    elif format == DownloadFormat.dpo_jsonl:
        path = export_dir / "dataset_dpo.jsonl"
        media_type = "application/x-ndjson"
        filename = f"dataset_dpo_{run_id}.jsonl"
    else:
        path = export_dir / "report.md"
        media_type = "text/markdown"
        filename = f"report_{run_id}.md"

    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Export file not found for run {run_id}")

    return FileResponse(
        path=str(path),
        media_type=media_type,
        filename=filename,
    )


@router.get("/runs", response_model=list[RunListItem])
async def get_runs() -> list[RunListItem]:
    """List all pipeline runs."""
    return [_make_run_list_item(row) for row in list_runs()]


@router.get("/runs/{run_id}", response_model=RunDetail)
async def get_run_detail(run_id: str) -> RunDetail:
    """Get details for a specific run."""
    row = get_run(run_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return _make_run_detail(row)


@router.post("/runs/{run_id}/push-to-hub", response_model=HubStatusResponse)
async def push_run_to_hub(run_id: str, body: PushToHubRequest) -> HubStatusResponse:
    """Trigger an async upload of the run's dataset to HuggingFace Hub."""
    row = get_run(run_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    if row["status"] not in (RunStatus.completed.value, RunStatus.partial.value):
        raise HTTPException(status_code=409, detail="Run is not complete yet")

    token = os.environ.get("HUGGINGFACE_TOKEN", "")
    if not token:
        raise HTTPException(status_code=400, detail="HUGGINGFACE_TOKEN environment variable not set")

    update_run_hf_status(run_id, "uploading")
    t = threading.Thread(target=_do_push_to_hub, args=(run_id, body, token), daemon=True)
    t.start()

    return HubStatusResponse(run_id=run_id, hf_status="uploading")


@router.get("/runs/{run_id}/hub-status", response_model=HubStatusResponse)
async def get_run_hub_status(run_id: str) -> HubStatusResponse:
    """Poll the HuggingFace upload status for a run."""
    row = get_run(run_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return HubStatusResponse(
        run_id=run_id,
        hf_status=row.get("hf_status"),
        hf_repo_url=row.get("hf_repo_url"),
    )


@router.get("/runs/{run_id}/download")
async def download_dataset(
    run_id: str,
    format: DownloadFormat = DownloadFormat.jsonl,
) -> FileResponse:
    """Download the dataset export for a run (via run-scoped path)."""
    return _build_file_response(run_id, format)


# ── datasets_router ──────────────────────────────────────────────────────────
datasets_router = APIRouter(prefix="/datasets", tags=["datasets"])


@datasets_router.get("", response_model=list[RunListItem])
async def list_datasets() -> list[RunListItem]:
    """List all datasets (alias for /pipeline/runs)."""
    return [_make_run_list_item(row) for row in list_runs()]


@datasets_router.get("/{dataset_id}", response_model=RunDetail)
async def get_dataset(dataset_id: str) -> RunDetail:
    """Get a dataset by run_id (alias for /pipeline/runs/{id})."""
    row = get_run(dataset_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
    return _make_run_detail(row)


@datasets_router.get("/{dataset_id}/download")
async def download_dataset_by_id(
    dataset_id: str,
    format: DownloadFormat = DownloadFormat.jsonl,
) -> FileResponse:
    """Download dataset export (alias for /pipeline/runs/{id}/download)."""
    return _build_file_response(dataset_id, format)


# ── top-level runs_router (legacy alias) ─────────────────────────────────────
runs_router = APIRouter(prefix="/runs", tags=["runs"])


@runs_router.get("", response_model=list[RunListItem])
async def list_runs_alias() -> list[RunListItem]:
    """List all runs (top-level alias for /pipeline/runs)."""
    return [_make_run_list_item(row) for row in list_runs()]


@runs_router.get("/{run_id}", response_model=RunDetail)
async def get_run_alias(run_id: str) -> RunDetail:
    """Get run detail (top-level alias for /pipeline/runs/{id})."""
    row = get_run(run_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return _make_run_detail(row)


# ── /api/v1/ routes ──────────────────────────────────────────────────────────

@v1_router.post(
    "/runs",
    response_model=PipelineRunResponse,
    summary="Start a pipeline run",
    description="Start a new dataset pipeline run. Accepts JSON or YAML body.",
)
async def v1_start_run(request: Request) -> PipelineRunResponse:
    """Start a new dataset pipeline run (v1)."""
    config = await _parse_run_request(request)
    return _start_run(config)


@v1_router.get(
    "/runs",
    response_model=list[RunListItem],
    summary="List pipeline runs",
)
async def v1_list_runs() -> list[RunListItem]:
    """List all pipeline runs."""
    return [_make_run_list_item(row) for row in list_runs()]


@v1_router.get(
    "/runs/{run_id}",
    response_model=RunDetail,
    summary="Get run detail",
)
async def v1_get_run(run_id: str) -> RunDetail:
    """Get details for a specific run."""
    row = get_run(run_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return _make_run_detail(row)


@v1_router.delete(
    "/runs/{run_id}",
    summary="Delete a run",
    description="Delete a run and its associated data on disk.",
)
async def v1_delete_run(run_id: str) -> dict:
    """Delete a run record and its data directory."""
    row = get_run(run_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    deleted = delete_run(run_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    run_dir = VERSIONS_BASE / run_id
    if run_dir.exists():
        shutil.rmtree(run_dir, ignore_errors=True)

    return {"deleted": True, "run_id": run_id}


@v1_router.get(
    "/runs/{run_id}/download",
    summary="Download run dataset",
)
async def v1_download_run(
    run_id: str,
    format: DownloadFormat = DownloadFormat.jsonl,
) -> FileResponse:
    """Download the dataset export for a run."""
    return _build_file_response(run_id, format)


async def _sse_generator(run_id: str) -> AsyncGenerator[str, None]:
    """Yield SSE-formatted events from the event bus for a run."""
    q = event_bus.subscribe(run_id)
    try:
        while True:
            try:
                event = await asyncio.wait_for(q.get(), timeout=30.0)
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
                continue

            if event.get("event") == "done":
                yield f"data: {json.dumps({'event': 'done'})}\n\n"
                break

            yield f"data: {json.dumps(event)}\n\n"
    finally:
        event_bus.unsubscribe(run_id, q)


@v1_router.get(
    "/runs/{run_id}/stream",
    summary="Stream run stage events (SSE)",
    description="Server-sent events stream for live pipeline stage completion updates.",
    response_class=StreamingResponse,
)
async def v1_stream_run(run_id: str) -> StreamingResponse:
    """SSE endpoint: stream stage_complete events for an active run."""
    row = get_run(run_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    return StreamingResponse(
        _sse_generator(run_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@v1_router.get(
    "/metrics",
    summary="System metrics",
    description="Aggregate stats: total runs, completed runs, total records generated, avg pipeline latency.",
)
async def v1_metrics() -> dict:
    """Return system-level aggregate metrics."""
    stats = get_aggregate_stats()
    total_records = get_total_records_generated()
    avg_latency = get_avg_pipeline_latency_ms()
    queue = get_queue_status()
    return {
        "total_runs": stats.get("total_runs", 0),
        "completed_runs": stats.get("completed_runs", 0),
        "running_runs": stats.get("running_runs", 0),
        "failed_runs": stats.get("failed_runs", 0),
        "total_records_generated": total_records,
        "avg_pipeline_latency_ms": avg_latency,
        "queue": queue,
    }


@v1_router.get(
    "/queue",
    summary="Job queue status",
    description="Returns current queue depth and active worker count.",
)
async def v1_queue_status() -> dict:
    """Return current job queue status."""
    return get_queue_status()
