"""SQLite metadata store for pipeline runs."""
from __future__ import annotations
import json
import sqlite3
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..schemas import RunStatus, RunMetrics, StageMetrics

logger = logging.getLogger(__name__)

DB_PATH = Path("data/pipeline.db")

_RETRY_ATTEMPTS = 5
_RETRY_DELAY = 0.1


def _retry(fn):
    """Decorator: retry on OperationalError (busy/locked)."""
    def wrapper(*args, **kwargs):
        for attempt in range(_RETRY_ATTEMPTS):
            try:
                return fn(*args, **kwargs)
            except sqlite3.OperationalError as e:
                if attempt < _RETRY_ATTEMPTS - 1 and ("locked" in str(e) or "busy" in str(e)):
                    time.sleep(_RETRY_DELAY * (attempt + 1))
                else:
                    raise
    return wrapper


def get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA busy_timeout=5000;
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


@_retry
def create_run(run_id: str, run_name: str, config: dict) -> None:
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO runs (run_id, run_name, status, config_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (run_id, run_name, RunStatus.pending.value, json.dumps(config), now, now)
        )
        conn.commit()


@_retry
def update_run_status(run_id: str, status: RunStatus, error: str = "") -> None:
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "UPDATE runs SET status=?, error=?, updated_at=? WHERE run_id=?",
            (status.value, error, now, run_id)
        )
        conn.commit()


@_retry
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


@_retry
def get_run(run_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if not row:
            return None
        return dict(row)


@_retry
def list_runs() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM runs ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]


@_retry
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


@_retry
def delete_run(run_id: str) -> bool:
    with get_conn() as conn:
        cursor = conn.execute("DELETE FROM runs WHERE run_id=?", (run_id,))
        conn.commit()
        return cursor.rowcount > 0


@_retry
def get_aggregate_stats() -> dict:
    with get_conn() as conn:
        row = conn.execute("""
            SELECT
                COUNT(*) as total_runs,
                SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed_runs,
                SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) as running_runs,
                SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed_runs
            FROM runs
        """).fetchone()
        return dict(row) if row else {}


@_retry
def get_total_records_generated() -> int:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT metrics_json FROM runs WHERE metrics_json IS NOT NULL"
        ).fetchall()
        total = 0
        for r in rows:
            try:
                m = json.loads(r["metrics_json"])
                total += m.get("total_records", 0) or 0
            except Exception:
                pass
        return total


@_retry
def get_avg_pipeline_latency_ms() -> Optional[float]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT stage_metrics_json FROM runs WHERE stage_metrics_json IS NOT NULL AND status='completed'"
        ).fetchall()
        totals = []
        for r in rows:
            try:
                stages = json.loads(r["stage_metrics_json"])
                total = sum(s.get("latency_ms", 0) for s in stages)
                if total > 0:
                    totals.append(total)
            except Exception:
                pass
        if not totals:
            return None
        return sum(totals) / len(totals)
