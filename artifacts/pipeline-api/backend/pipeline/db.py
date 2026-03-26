"""SQLite metadata store for pipeline runs."""
from __future__ import annotations
import json
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..schemas import RunStatus, RunMetrics, StageMetrics

logger = logging.getLogger(__name__)

DB_PATH = Path("data/pipeline.db")


def get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                run_name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                config_json TEXT,
                metrics_json TEXT,
                stage_metrics_json TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
        """)
        conn.commit()
    logger.info("Database initialized")


def create_run(run_id: str, run_name: str, config: dict) -> None:
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO runs (run_id, run_name, status, config_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (run_id, run_name, RunStatus.pending.value, json.dumps(config), now, now)
        )
        conn.commit()


def update_run_status(run_id: str, status: RunStatus, error: str = "") -> None:
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE runs SET status=?, error=?, updated_at=? WHERE run_id=?",
            (status.value, error, now, run_id)
        )
        conn.commit()


def update_run_metrics(
    run_id: str,
    metrics: RunMetrics,
    stage_metrics: list[StageMetrics],
) -> None:
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE runs SET metrics_json=?, stage_metrics_json=?, updated_at=? WHERE run_id=?",
            (
                metrics.model_dump_json(),
                json.dumps([sm.model_dump() for sm in stage_metrics]),
                now,
                run_id,
            )
        )
        conn.commit()


def get_run(run_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if not row:
            return None
        return dict(row)


def list_runs() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM runs ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
