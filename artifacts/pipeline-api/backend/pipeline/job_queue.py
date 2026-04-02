"""ThreadPoolExecutor-based job queue for pipeline runs."""
from __future__ import annotations
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any

logger = logging.getLogger(__name__)

_CONCURRENCY = int(os.environ.get("PIPELINE_CONCURRENCY", "4"))

_executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=_CONCURRENCY)
_active_count = 0
_queue_depth = 0
_lock = __import__("threading").Lock()


def get_status() -> dict:
    with _lock:
        return {
            "active_workers": _active_count,
            "queue_depth": _queue_depth,
            "max_concurrency": _CONCURRENCY,
        }


def submit_job(fn: Callable, *args: Any, **kwargs: Any) -> None:
    global _queue_depth
    with _lock:
        _queue_depth += 1

    def _wrapped():
        global _active_count, _queue_depth
        with _lock:
            _active_count += 1
            _queue_depth -= 1
        try:
            fn(*args, **kwargs)
        finally:
            with _lock:
                _active_count -= 1

    _executor.submit(_wrapped)
