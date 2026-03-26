"""Ingest routes."""
from __future__ import annotations
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException

from ..schemas import IngestRequest, IngestResponse, Source
from ..modules.ingest import ingest_url, ingest_text

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/pipeline/ingest", tags=["ingest"])
ingest_alias_router = APIRouter(prefix="/ingest", tags=["ingest"])


async def _process_ingest(body: IngestRequest) -> IngestResponse:
    sources: list[Source] = []
    for cfg in body.sources:
        src_type = cfg.get("type", "text")
        if src_type == "url":
            url = cfg.get("value", "")
            if not url:
                continue
            try:
                source = ingest_url(url)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
        elif src_type in ("text", "file"):
            text = cfg.get("value", "") or cfg.get("text", "")
            title = cfg.get("title", "text_input")
            source = ingest_text(text, title=title)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown source type: {src_type}")
        if source:
            sources.append(source)

    return IngestResponse(
        source_ids=[s.source_id for s in sources],
        count=len(sources),
    )


@router.post("", response_model=IngestResponse)
async def ingest_sources(body: IngestRequest) -> IngestResponse:
    """Ingest sources (URL or text) and return source IDs."""
    return await _process_ingest(body)


@ingest_alias_router.post("", response_model=IngestResponse)
async def ingest_sources_alias(body: IngestRequest) -> IngestResponse:
    """Ingest sources — top-level alias for POST /pipeline/ingest."""
    return await _process_ingest(body)
