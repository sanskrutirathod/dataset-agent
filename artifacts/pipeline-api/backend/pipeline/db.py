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
                updated_at TEXT NOT NULL,
                hf_status TEXT,
                hf_repo_url TEXT
            );
        """)
        conn.commit()

    with get_conn() as conn:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(runs)").fetchall()}
        if "hf_status" not in cols:
            conn.execute("ALTER TABLE runs ADD COLUMN hf_status TEXT")
        if "hf_repo_url" not in cols:
            conn.execute("ALTER TABLE runs ADD COLUMN hf_repo_url TEXT")
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


def update_run_hf_status(
    run_id: str,
    hf_status: str,
    hf_repo_url: Optional[str] = None,
) -> None:
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE runs SET hf_status=?, hf_repo_url=?, updated_at=? WHERE run_id=?",
            (hf_status, hf_repo_url, now, run_id),
        )
        conn.commit()
