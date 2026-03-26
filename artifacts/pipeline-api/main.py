"""WEBSPACEAI Dataset Engine — FastAPI entry point."""
from __future__ import annotations
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.pipeline.db import init_db
from backend.routes.ingest import router as ingest_router, ingest_alias_router
from backend.routes.runs import router as pipeline_router, datasets_router, runs_router as top_runs_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    logger.info("Dataset Engine started")
    yield
    logger.info("Dataset Engine shutting down")


app = FastAPI(
    title="WEBSPACEAI Dataset Engine",
    description="Production-grade dataset factory for AI training data",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ingest_router)
app.include_router(ingest_alias_router)
app.include_router(pipeline_router)
app.include_router(datasets_router)
app.include_router(top_runs_router)


@app.get("/pipeline/healthz", tags=["health"])
async def health():
    return {"status": "ok", "service": "dataset-engine"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PIPELINE_PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
