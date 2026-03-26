"""Pipeline run routes."""
from __future__ import annotations
import json
import logging
import threading
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, PlainTextResponse

from ..schemas import (
    PipelineRunRequest, PipelineRunResponse, RunListItem, RunDetail,
    RunStatus, RunMetrics, StageMetrics, DownloadFormat
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


@router.post("/run", response_model=PipelineRunResponse)
async def start_pipeline_run(
    body: PipelineRunRequest,
    background_tasks: BackgroundTasks,
) -> PipelineRunResponse:
    """Start a new pipeline run."""
    run_id = new_id("run")
    config = body.config

    create_run(run_id, config.run_name, config.model_dump())

    background_tasks.add_task(run_pipeline, run_id, config)

    return PipelineRunResponse(
        run_id=run_id,
        status=RunStatus.running,
        message=f"Pipeline started with run_id={run_id}",
    )


@router.get("/runs", response_model=list[RunListItem])
async def get_runs() -> list[RunListItem]:
    """List all pipeline runs."""
    rows = list_runs()
    result = []
    for row in rows:
        result.append(RunListItem(
            run_id=row["run_id"],
            run_name=row["run_name"],
            status=RunStatus(row["status"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            metrics=_parse_metrics(row),
            stage_metrics=_parse_stage_metrics(row),
        ))
    return result


@router.get("/runs/{run_id}", response_model=RunDetail)
async def get_run_detail(run_id: str) -> RunDetail:
    """Get details for a specific run."""
    row = get_run(run_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

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


@router.get("/runs/{run_id}/download")
async def download_dataset(
    run_id: str,
    format: DownloadFormat = DownloadFormat.jsonl,
) -> FileResponse:
    """Download the dataset export for a run."""
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
