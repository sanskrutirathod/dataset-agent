"""Export and versioning module."""
from __future__ import annotations
import csv
import json
import logging
import statistics
from pathlib import Path

from ..schemas import DatasetRecord, RunMetrics, StageMetrics

logger = logging.getLogger(__name__)


def export_jsonl(records: list[DatasetRecord], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(rec.model_dump_json(exclude_none=True) + "\n")


def export_dpo_jsonl(records: list[DatasetRecord], path: Path) -> None:
    """Export DPO records in TRL DPOTrainer-compatible format."""
    path.parent.mkdir(parents=True, exist_ok=True)
    dpo_records = [r for r in records if r.type == "distillation_dpo" and r.chosen and r.rejected]
    with path.open("w", encoding="utf-8") as f:
        for rec in dpo_records:
            dpo_entry = {
                "prompt": rec.instruction,
                "chosen": rec.chosen,
                "rejected": rec.rejected,
            }
            f.write(json.dumps(dpo_entry) + "\n")
    logger.info(f"Exported {len(dpo_records)} DPO records to {path}")


def export_csv(records: list[DatasetRecord], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not records:
        path.write_text("")
        return
    fields = ["id", "type", "instruction", "input", "output",
              "thinking", "chosen", "rejected",
              "score_relevance", "score_clarity", "score_grounding",
              "score_diversity", "score_final", "source_id", "chunk_id"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for rec in records:
            writer.writerow({
                "id": rec.id,
                "type": rec.type,
                "instruction": rec.instruction,
                "input": rec.input,
                "output": rec.output,
                "thinking": rec.thinking or "",
                "chosen": rec.chosen or "",
                "rejected": rec.rejected or "",
                "score_relevance": rec.scores.relevance,
                "score_clarity": rec.scores.clarity,
                "score_grounding": rec.scores.grounding,
                "score_diversity": rec.scores.diversity,
                "score_final": rec.scores.final,
                "source_id": rec.provenance.get("source_id", ""),
                "chunk_id": rec.provenance.get("chunk_id", ""),
            })


def export_report(
    records: list[DatasetRecord],
    stage_metrics: list[StageMetrics],
    run_id: str,
    run_name: str,
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    scores = [r.scores.final for r in records if r.scores.final > 0]
    avg_score = statistics.mean(scores) if scores else 0.0
    min_score = min(scores) if scores else 0.0
    max_score = max(scores) if scores else 0.0

    type_counts: dict[str, int] = {}
    for rec in records:
        type_counts[rec.type] = type_counts.get(rec.type, 0) + 1

    lines = [
        f"# Dataset Export Report",
        f"",
        f"**Run ID**: `{run_id}`",
        f"**Run Name**: {run_name}",
        f"**Total Records**: {len(records)}",
        f"",
        f"## Score Distribution",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Avg Final Score | {avg_score:.4f} |",
        f"| Min Score | {min_score:.4f} |",
        f"| Max Score | {max_score:.4f} |",
        f"",
        f"## Record Types",
        f"",
    ]
    for t, count in type_counts.items():
        lines.append(f"- **{t}**: {count}")

    lines += ["", "## Pipeline Stage Metrics", ""]
    if stage_metrics:
        lines.append("| Stage | Input | Output | Latency (ms) |")
        lines.append("|-------|-------|--------|--------------|")
        for sm in stage_metrics:
            lines.append(
                f"| {sm.stage} | {sm.input_count} | {sm.output_count} | {sm.latency_ms:.0f} |"
            )

    path.write_text("\n".join(lines), encoding="utf-8")


def run_export(
    records: list[DatasetRecord],
    stage_metrics: list[StageMetrics],
    version_dir: Path,
    run_id: str,
    run_name: str,
) -> dict[str, str]:
    version_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = version_dir / "dataset.jsonl"
    csv_path = version_dir / "dataset.csv"
    report_path = version_dir / "report.md"

    export_jsonl(records, jsonl_path)
    export_csv(records, csv_path)
    export_report(records, stage_metrics, run_id, run_name, report_path)

    result = {
        "jsonl": str(jsonl_path),
        "csv": str(csv_path),
        "report": str(report_path),
    }

    dpo_records = [r for r in records if r.type == "distillation_dpo" and r.chosen and r.rejected]
    if dpo_records:
        dpo_path = version_dir / "dataset_dpo.jsonl"
        export_dpo_jsonl(records, dpo_path)
        result["dpo_jsonl"] = str(dpo_path)

    logger.info(
        f"Exported {len(records)} records to {version_dir}: "
        f"JSONL={jsonl_path.stat().st_size}b, CSV={csv_path.stat().st_size}b"
    )
    return result
