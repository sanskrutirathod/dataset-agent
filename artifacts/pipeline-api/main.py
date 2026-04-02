"""WEBSPACEAI Dataset Engine — FastAPI entry point."""
from __future__ import annotations
import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi

from backend.pipeline.db import init_db
from backend.pipeline import event_bus
from backend.routes.ingest import router as ingest_router, ingest_alias_router
from backend.routes.runs import (
    router as pipeline_router,
    datasets_router,
    runs_router as top_runs_router,
    v1_router,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

_API_KEY = os.environ.get("API_KEY", "")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    event_bus.set_loop(asyncio.get_event_loop())
    logger.info("Dataset Engine started")
    yield
    logger.info("Dataset Engine shutting down")


app = FastAPI(
    title="WEBSPACEAI Dataset Engine",
    description=(
        "Production-grade dataset factory for AI training data.\n\n"
        "## Authentication\n\n"
        "When the server is started with the `API_KEY` environment variable set, "
        "all `/api/v1/` routes require an `Authorization: Bearer <key>` header.\n\n"
        "The `/pipeline/healthz` and `/api/v1/metrics` endpoints are always public."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def api_key_middleware(request: Request, call_next):
    """Require Authorization: Bearer <API_KEY> on /api/v1/* routes when API_KEY is set."""
    if not _API_KEY:
        return await call_next(request)

    path = request.url.path
    if not path.startswith("/api/v1/"):
        return await call_next(request)

    public_v1_paths = {"/api/v1/metrics", "/api/v1/queue"}
    if path in public_v1_paths:
        return await call_next(request)

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return JSONResponse(
            status_code=401,
            content={"detail": "Missing or invalid Authorization header. Use 'Bearer <API_KEY>'."},
        )

    token = auth_header[len("Bearer "):]
    if token != _API_KEY:
        return JSONResponse(
            status_code=401,
            content={"detail": "Invalid API key."},
        )

    return await call_next(request)


app.include_router(ingest_router)
app.include_router(ingest_alias_router)
app.include_router(pipeline_router)
app.include_router(datasets_router)
app.include_router(top_runs_router)
app.include_router(v1_router)


@app.get("/pipeline/healthz", tags=["health"])
async def health():
    return {"status": "ok", "service": "dataset-engine"}


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    schema["components"]["securitySchemes"] = {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "API Key",
            "description": "Set API_KEY environment variable to enable. Pass as 'Authorization: Bearer <key>'.",
        }
    }
    for path_item in schema.get("paths", {}).values():
        for operation in path_item.values():
            if isinstance(operation, dict):
                path_str = next(
                    (p for p in schema.get("paths", {}) if schema["paths"][p] is path_item),
                    ""
                )
                if "/api/v1/" in str(path_str):
                    operation.setdefault("security", [{"BearerAuth": []}])
    app.openapi_schema = schema
    return schema


app.openapi = custom_openapi


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PIPELINE_PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
