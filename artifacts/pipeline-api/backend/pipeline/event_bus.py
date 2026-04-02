"""In-process event bus for pipeline stage completion events."""
from __future__ import annotations
import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

_queues: dict[str, list[asyncio.Queue]] = {}


def subscribe(run_id: str) -> asyncio.Queue:
    """Subscribe to events for a run. Returns a queue that receives event dicts."""
    q: asyncio.Queue = asyncio.Queue()
    if run_id not in _queues:
        _queues[run_id] = []
    _queues[run_id].append(q)
    return q


def unsubscribe(run_id: str, q: asyncio.Queue) -> None:
    """Remove a subscriber queue."""
    if run_id in _queues:
        try:
            _queues[run_id].remove(q)
        except ValueError:
            pass
        if not _queues[run_id]:
            del _queues[run_id]


def publish(run_id: str, event: dict[str, Any]) -> None:
    """Publish an event to all subscribers of a run (thread-safe, sync)."""
    if run_id not in _queues:
        return
    for q in list(_queues[run_id]):
        try:
            q.put_nowait(event)
        except Exception as e:
            logger.warning(f"Failed to put event in queue: {e}")


def close_run(run_id: str) -> None:
    """Signal run completion by sending a sentinel None to all subscribers."""
    publish(run_id, {"event": "done"})
    if run_id in _queues:
        del _queues[run_id]
