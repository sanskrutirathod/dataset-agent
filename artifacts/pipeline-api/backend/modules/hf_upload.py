"""HuggingFace Hub dataset upload module."""
from __future__ import annotations

import json
import logging
import statistics
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def _build_dataset_card(
    run_id: str,
    run_name: str,
    repo_id: str,
    description: str,
    split: str,
    record_count: int,
    avg_score: float,
    min_score: float,
    max_score: float,
    generation_mode: str,
) -> str:
    now = datetime.utcnow().strftime("%Y-%m-%d")
    visibility_note = "This dataset was generated automatically by the WEBSPACEAI Dataset Engine."
    desc_section = f"\n{description}\n" if description else ""
    return f"""---
license: mit
task_categories:
- text-generation
language:
- en
size_categories:
- {'n<1K' if record_count < 1000 else '1K<n<10K' if record_count < 10000 else '10K<n<100K'}
---

# {run_name}
{desc_section}
{visibility_note}

## Dataset Details

| Field | Value |
|-------|-------|
| Run ID | `{run_id}` |
| Repository | `{repo_id}` |
| Split | `{split}` |
| Generation Mode | `{generation_mode}` |
| Record Count | {record_count} |
| Created | {now} |
| Pipeline Version | 1.0.0 |

## Score Statistics

| Metric | Value |
|--------|-------|
| Average Final Score | {avg_score:.4f} |
| Min Score | {min_score:.4f} |
| Max Score | {max_score:.4f} |

## Format

Each record in the dataset follows this schema:

```json
{{
  "instruction": "...",
  "input": "...",
  "output": "..."
}}
```

Compatible with the HuggingFace `datasets` library schema for instruction-tuning datasets.

## Usage

```python
from datasets import load_dataset

ds = load_dataset("{repo_id}", split="{split}")
```
"""


def push_to_hub(
    run_id: str,
    repo_id: str,
    token: str,
    private: bool,
    split: str,
    description: str,
    export_dir: Path,
) -> dict:
    """
    Upload dataset to HuggingFace Hub.

    Returns a dict with keys: url, status, error.
    """
    try:
        from huggingface_hub import HfApi, CommitOperationAdd
    except ImportError:
        return {
            "url": None,
            "status": "error",
            "error": "huggingface_hub package is not installed",
        }

    jsonl_path = export_dir / "dataset.jsonl"
    if not jsonl_path.exists():
        return {
            "url": None,
            "status": "error",
            "error": f"Export file not found: {jsonl_path}",
        }

    try:
        api = HfApi(token=token)

        api.create_repo(
            repo_id=repo_id,
            repo_type="dataset",
            private=private,
            exist_ok=True,
        )
        logger.info("HF repo ensured: %s (private=%s)", repo_id, private)

        records = []
        with jsonl_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rec = json.loads(line)
                        records.append({
                            "instruction": rec.get("instruction", ""),
                            "input": rec.get("input", ""),
                            "output": rec.get("output", ""),
                        })
                    except json.JSONDecodeError:
                        pass

        record_count = len(records)
        scores_raw = []
        with jsonl_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rec = json.loads(line)
                        s = rec.get("scores", {})
                        final = s.get("final", 0.0) if isinstance(s, dict) else 0.0
                        if final > 0:
                            scores_raw.append(final)
                    except json.JSONDecodeError:
                        pass

        avg_score = statistics.mean(scores_raw) if scores_raw else 0.0
        min_score = min(scores_raw) if scores_raw else 0.0
        max_score = max(scores_raw) if scores_raw else 0.0

        run_row = None
        try:
            from ..pipeline.db import get_run
            run_row = get_run(run_id)
        except Exception:
            pass

        run_name = run_row["run_name"] if run_row else run_id
        generation_mode = "unknown"
        if run_row and run_row.get("config_json"):
            try:
                cfg = json.loads(run_row["config_json"])
                generation_mode = cfg.get("generation", {}).get("mode", "unknown")
            except Exception:
                pass

        card_content = _build_dataset_card(
            run_id=run_id,
            run_name=run_name,
            repo_id=repo_id,
            description=description,
            split=split,
            record_count=record_count,
            avg_score=avg_score,
            min_score=min_score,
            max_score=max_score,
            generation_mode=generation_mode,
        )

        hf_jsonl_content = ""
        for rec in records:
            hf_jsonl_content += json.dumps(rec) + "\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            hf_jsonl_path = tmp / f"{split}.jsonl"
            hf_jsonl_path.write_text(hf_jsonl_content, encoding="utf-8")
            card_path = tmp / "README.md"
            card_path.write_text(card_content, encoding="utf-8")

            operations = [
                CommitOperationAdd(
                    path_in_repo=f"data/{split}.jsonl",
                    path_or_fileobj=str(hf_jsonl_path),
                ),
                CommitOperationAdd(
                    path_in_repo="README.md",
                    path_or_fileobj=str(card_path),
                ),
            ]

            api.create_commit(
                repo_id=repo_id,
                repo_type="dataset",
                operations=operations,
                commit_message=f"Upload dataset from WEBSPACEAI run {run_id} ({record_count} records)",
            )

        url = f"https://huggingface.co/datasets/{repo_id}"
        logger.info("Dataset uploaded to %s (%d records)", url, record_count)
        return {"url": url, "status": "done", "error": None}

    except Exception as exc:
        logger.exception("HuggingFace upload failed for run %s", run_id)
        return {"url": None, "status": "error", "error": str(exc)}
