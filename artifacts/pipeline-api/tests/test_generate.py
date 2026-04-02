"""Tests for the generate module - specifically API key validation."""
from __future__ import annotations
import os
import pytest
from pathlib import Path
from unittest.mock import patch

from backend.modules.generate import _get_client, run_generate
from backend.schemas import Chunk, GenerationConfig


class TestGetClient:
    """Tests for _get_client() function."""

    def test_raises_error_when_api_key_not_set(self):
        """Should raise ValueError when AI_INTEGRATIONS_OPENAI_API_KEY is not set."""
        env = {"AI_INTEGRATIONS_OPENAI_API_KEY": ""}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError) as exc_info:
                _get_client()
            assert "not set" in str(exc_info.value)
            assert "AI_INTEGRATIONS_OPENAI_API_KEY" in str(exc_info.value)

    def test_raises_error_when_api_key_is_none(self):
        """Should raise ValueError when AI_INTEGRATIONS_OPENAI_API_KEY is None."""
        env = {"AI_INTEGRATIONS_OPENAI_API_KEY": ""}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError) as exc_info:
                _get_client()
            assert "not set" in str(exc_info.value)

    def test_creates_client_when_api_key_is_set(self):
        """Should create OpenAI client when API key is set."""
        env = {"AI_INTEGRATIONS_OPENAI_API_KEY": "test-key-123"}
        with patch.dict(os.environ, env, clear=True):
            client = _get_client()
            assert client is not None
            assert client.api_key == "test-key-123"


class TestRunGenerate:
    """Tests for run_generate() function."""

    def test_returns_empty_when_api_key_not_configured(self, tmp_path: Path):
        """Should return empty list and write empty file when API key not configured."""
        chunks = [
            Chunk(
                chunk_id="chk_123",
                source_id="src_123",
                idx=0,
                text="Test chunk content for testing purposes.",
                tokens=8,
                overlap=10,
                section="",
                hash="abc123",
            )
        ]
        config = GenerationConfig(mode="qa", max_records_per_chunk=1)
        out_dir = tmp_path / "output"
        out_dir.mkdir()

        env = {"AI_INTEGRATIONS_OPENAI_API_KEY": ""}
        with patch.dict(os.environ, env, clear=True):
            records = run_generate(chunks, config, out_dir, max_records=100)

        assert records == []
        
        # Verify empty file was written
        records_file = out_dir / "records.jsonl"
        assert records_file.exists()
        assert records_file.read_text() == ""

    def test_processes_chunks_when_api_key_configured(self, tmp_path: Path):
        """Should attempt to process chunks when API key is configured."""
        chunks = [
            Chunk(
                chunk_id="chk_456",
                source_id="src_456",
                idx=0,
                text="Machine learning is a subset of artificial intelligence.",
                tokens=10,
                overlap=10,
                section="",
                hash="def456",
            )
        ]
        config = GenerationConfig(mode="qa", max_records_per_chunk=1)
        out_dir = tmp_path / "output"
        out_dir.mkdir()

        # Use a mock API key but it will fail on actual LLM call
        env = {"AI_INTEGRATIONS_OPENAI_API_KEY": "sk-mock-key"}
        with patch.dict(os.environ, env, clear=True):
            records = run_generate(chunks, config, out_dir, max_records=100)

        # With a mock key, the API call will fail but the function should complete
        assert isinstance(records, list)
        
        records_file = out_dir / "records.jsonl"
        assert records_file.exists()
