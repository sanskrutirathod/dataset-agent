"""Validate module: schema, length, grounding, and safety checks."""
from __future__ import annotations
import logging
import re
from pathlib import Path

from ..schemas import DatasetRecord, Chunk, ValidationConfig

logger = logging.getLogger(__name__)

SAFETY_KEYWORDS = [
    r"\b(bomb|explosive|weapon)\b",
    r"\b(synthesize|manufacture)\s+(drug|meth|heroin|cocaine)\b",
    r"\b(child\s+porn|csam)\b",
    r"\b(self.?harm|suicide\s+method)\b",
]
SAFETY_PATTERNS = [re.compile(p, re.I) for p in SAFETY_KEYWORDS]


def _ngram_overlap(text1: str, text2: str, n: int = 4) -> float:
    def ngrams(t: str) -> set[str]:
        words = t.lower().split()
        return {" ".join(words[i:i+n]) for i in range(len(words) - n + 1)}
    a = ngrams(text1)
    b = ngrams(text2)
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _safety_check(text: str) -> bool:
    for pat in SAFETY_PATTERNS:
        if pat.search(text):
            return False
    return True


def validate_record(
    record: DatasetRecord,
    source_chunk: Chunk | None,
    config: ValidationConfig,
) -> tuple[bool, list[str]]:
    flags: list[str] = []

    output_len = len(record.output)
    if output_len < config.min_length:
        flags.append(f"output_too_short:{output_len}")
    if output_len > config.max_length:
        flags.append(f"output_too_long:{output_len}")

    if not _safety_check(record.output):
        flags.append("safety_filter")
    if record.instruction and not _safety_check(record.instruction):
        flags.append("safety_filter_instruction")
    if record.thinking and not _safety_check(record.thinking):
        flags.append("safety_filter_thinking")
    if record.rejected and not _safety_check(record.rejected):
        flags.append("safety_filter_rejected")

    if source_chunk:
        overlap = _ngram_overlap(record.output, source_chunk.text)
        if overlap < config.grounding_min_overlap:
            flags.append(f"low_grounding:{overlap:.3f}")

    if not record.output.strip():
        flags.append("empty_output")

    return len(flags) == 0, flags


def run_validate(
    records: list[DatasetRecord],
    chunks: list[Chunk],
    config: ValidationConfig,
    out_dir: Path,
) -> list[DatasetRecord]:
    out_dir.mkdir(parents=True, exist_ok=True)
    chunk_map: dict[str, Chunk] = {c.chunk_id: c for c in chunks}

    valid_records = []
    invalid_count = 0

    for record in records:
        chunk_id = record.provenance.get("chunk_id", "")
        chunk = chunk_map.get(chunk_id)
        passed, flags = validate_record(record, chunk, config)
        if passed:
            valid_records.append(record)
        else:
            invalid_count += 1
            logger.debug(f"Record {record.id} failed validation: {flags}")

    out_file = out_dir / "records.jsonl"
    with out_file.open("w", encoding="utf-8") as f:
        for rec in valid_records:
            f.write(rec.model_dump_json() + "\n")

    pass_rate = len(valid_records) / len(records) if records else 0
    logger.info(
        f"Validation: {len(valid_records)} passed, {invalid_count} failed "
        f"(pass_rate={pass_rate:.2%})"
    )
    return valid_records
