"""In-process event bus for pipeline stage completion events."""
from __future__ import annotations
import asyncio
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

_queues: dict[str, list[asyncio.Queue]] = {}
_loop: Optional[asyncio.AbstractEventLoop] = None


def set_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Store the running event loop so worker threads can schedule callbacks safely."""
    global _loop
    _loop = loop


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
    """Publish an event to all subscribers of a run.

    Thread-safe: may be called from ThreadPoolExecutor worker threads.
    Uses loop.call_soon_threadsafe so the waiting asyncio coroutine is
    properly woken up even when called from outside the event loop.
    """
    if run_id not in _queues:
        return
    for q in list(_queues[run_id]):
        try:
            if _loop is not None and _loop.is_running():
                _loop.call_soon_threadsafe(q.put_nowait, event)
            else:
                q.put_nowait(event)
        except Exception as e:
            logger.warning(f"Failed to put event in queue: {e}")


def close_run(run_id: str) -> None:
    """Signal run completion by sending a sentinel 'done' event to all subscribers.

    Thread-safe: may be called from ThreadPoolExecutor worker threads.
    The queue registry entry is removed AFTER scheduling the final event so
    that subscribers always receive it before the entry disappears.
    """
    if run_id not in _queues:
        return
    queues_snapshot = list(_queues[run_id])
    done_event: dict[str, Any] = {"event": "done"}
    for q in queues_snapshot:
        try:
            if _loop is not None and _loop.is_running():
                _loop.call_soon_threadsafe(q.put_nowait, done_event)
            else:
                q.put_nowait(done_event)
        except Exception as e:
            logger.warning(f"Failed to send done event to queue: {e}")
    if run_id in _queues:
        del _queues[run_id]
