"""Ingest routes."""
from __future__ import annotations
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException

from ..schemas import IngestRequest, IngestResponse, Source
from ..modules.ingest import ingest_url, ingest_text

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/pipeline/ingest", tags=["ingest"])


@router.post("", response_model=IngestResponse)
async def ingest_sources(body: IngestRequest) -> IngestResponse:
    """Ingest sources (URL or text) and return source IDs."""
    sources: list[Source] = []
    for cfg in body.sources:
        src_type = cfg.get("type", "text")
        if src_type == "url":
            url = cfg.get("value", "")
            if not url:
                continue
            source = ingest_url(url)
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
