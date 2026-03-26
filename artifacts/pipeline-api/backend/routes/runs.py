"""Pipeline run routes."""
from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Optional

import yaml
from fastapi import APIRouter, HTTPException, BackgroundTasks, Request
from fastapi.responses import FileResponse

from ..schemas import (
    PipelineRunRequest, PipelineRunResponse, RunListItem, RunDetail,
    RunStatus, RunMetrics, StageMetrics, DownloadFormat, PipelineConfig
)
from ..pipeline.db import create_run, get_run, list_runs
from ..pipeline.orchestrator import run_pipeline
from ..utils.ids import new_id

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/pipeline", tags=["pipeline"])

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
    run_id = new_id("run")

    create_run(run_id, config.run_name, config.model_dump())

    background_tasks.add_task(run_pipeline, run_id, config)

    return PipelineRunResponse(
        run_id=run_id,
        status=RunStatus.running,
        message=f"Pipeline started with run_id={run_id}",
    )


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


@router.get("/runs/{run_id}/download")
async def download_dataset(
    run_id: str,
    format: DownloadFormat = DownloadFormat.jsonl,
) -> FileResponse:
    """Download the dataset export for a run (via run-scoped path)."""
    return _build_file_response(run_id, format)


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
