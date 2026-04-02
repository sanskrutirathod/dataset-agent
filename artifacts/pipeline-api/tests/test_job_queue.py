"""Tests for the job queue module - specifically thread safety."""
from __future__ import annotations
import pytest
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from backend.pipeline.job_queue import get_status, submit_job, get_status as get_queue_status


class TestJobQueueStatus:
    """Tests for job queue status."""

    def test_get_status_returns_valid_structure(self):
        """Should return a dict with expected keys."""
        status = get_queue_status()
        assert isinstance(status, dict)
        assert "active_workers" in status
        assert "queue_depth" in status
        assert "max_concurrency" in status

    def test_get_status_is_thread_safe(self):
        """Concurrent calls to get_status should not cause race conditions."""
        results = []
        errors = []

        def read_status():
            try:
                for _ in range(100):
                    status = get_queue_status()
                    results.append(status)
            except Exception as e:
                errors.append(e)

        def modify_state():
            # Submit many jobs that complete quickly
            for _ in range(50):
                submit_job(lambda: time.sleep(0.001))

        # Run multiple readers and writers concurrently
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Start readers
            readers = [executor.submit(read_status) for _ in range(5)]
            # Start a writer
            writer = executor.submit(modify_state)
            
            # Wait for all to complete
            for r in readers:
                r.result()
            writer.result()

        # No errors should have occurred
        assert len(errors) == 0, f"Errors occurred: {errors}"
        
        # All reads should have returned valid data
        assert len(results) == 500  # 5 readers * 100 reads each
        
        for status in results:
            assert isinstance(status, dict)
            assert "active_workers" in status
            assert "queue_depth" in status
            assert status["active_workers"] >= 0
            assert status["queue_depth"] >= 0
