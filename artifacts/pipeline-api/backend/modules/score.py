"""Score and filter module: heuristic quality scoring."""
from __future__ import annotations
import logging
import re
from pathlib import Path

from ..schemas import DatasetRecord, DatasetRecordScores, Chunk, ValidationConfig

logger = logging.getLogger(__name__)

WEIGHTS = {
    "relevance": 0.25,
    "clarity": 0.25,
    "grounding": 0.30,
    "diversity": 0.20,
}


def _ngram_overlap_ratio(text1: str, text2: str, n: int = 3) -> float:
    def ngrams(t: str) -> set[str]:
        words = t.lower().split()
        return {" ".join(words[i:i+n]) for i in range(max(0, len(words) - n + 1))}
    a = ngrams(text1)
    b = ngrams(text2)
    if not b:
        return 0.0
    return len(a & b) / len(b)


def _score_relevance(record: DatasetRecord, chunk: Chunk | None) -> float:
    if not chunk:
        return 0.5
    combined = (record.instruction + " " + record.input).strip()
    return min(1.0, _ngram_overlap_ratio(combined, chunk.text) * 3)


def _score_clarity(record: DatasetRecord) -> float:
    text = record.output
    if not text:
        return 0.0
    score = 0.0
    length = len(text.split())
    if 20 <= length <= 500:
        score += 0.4
    elif length > 500:
        score += 0.2
    elif length >= 10:
        score += 0.3

    if re.search(r"[.!?]$", text.strip()):
        score += 0.3

    upper_ratio = sum(1 for c in text if c.isupper()) / max(len(text), 1)
    if upper_ratio < 0.3:
        score += 0.3

    return min(1.0, score)


def _score_grounding(record: DatasetRecord, chunk: Chunk | None) -> float:
    if not chunk:
        return 0.5
    return min(1.0, _ngram_overlap_ratio(record.output, chunk.text) * 2.5)


def _score_diversity(record: DatasetRecord, seen_outputs: list[str]) -> float:
    if not seen_outputs:
        return 1.0
    output_words = set(record.output.lower().split())
    max_sim = 0.0
    for prev in seen_outputs[-50:]:
        prev_words = set(prev.lower().split())
        union = output_words | prev_words
        if union:
            sim = len(output_words & prev_words) / len(union)
            max_sim = max(max_sim, sim)
    return 1.0 - max_sim


def score_record(
    record: DatasetRecord,
    chunk: Chunk | None,
    seen_outputs: list[str],
) -> DatasetRecord:
    relevance = _score_relevance(record, chunk)
    clarity = _score_clarity(record)
    grounding = _score_grounding(record, chunk)
    diversity = _score_diversity(record, seen_outputs)
    final = (
        WEIGHTS["relevance"] * relevance
        + WEIGHTS["clarity"] * clarity
        + WEIGHTS["grounding"] * grounding
        + WEIGHTS["diversity"] * diversity
    )
    scored = record.model_copy()
    object.__setattr__(scored, "scores", DatasetRecordScores(
        relevance=round(relevance, 4),
        clarity=round(clarity, 4),
        grounding=round(grounding, 4),
        diversity=round(diversity, 4),
        final=round(final, 4),
    ))
    return scored


def run_score_and_filter(
    records: list[DatasetRecord],
    chunks: list[Chunk],
    config: ValidationConfig,
    out_dir: Path,
    max_per_source: int = 1000,
) -> list[DatasetRecord]:
    out_dir.mkdir(parents=True, exist_ok=True)
    chunk_map: dict[str, Chunk] = {c.chunk_id: c for c in chunks}
    seen_outputs: list[str] = []
    source_counts: dict[str, int] = {}
    scored_all: list[DatasetRecord] = []
    dropped_score = 0
    dropped_source = 0

    for record in records:
        chunk_id = record.provenance.get("chunk_id", "")
        source_id = record.provenance.get("source_id", "")
        chunk = chunk_map.get(chunk_id)

        scored = score_record(record, chunk, seen_outputs)

        if scored.scores.final < config.score_threshold:
            dropped_score += 1
            continue

        source_count = source_counts.get(source_id, 0)
        if source_count >= max_per_source:
            dropped_source += 1
            continue

        source_counts[source_id] = source_count + 1
        seen_outputs.append(record.output)
        scored_all.append(scored)

    out_file = out_dir / "records.jsonl"
    with out_file.open("w", encoding="utf-8") as f:
        for rec in scored_all:
            f.write(rec.model_dump_json() + "\n")

    avg_score = (
        sum(r.scores.final for r in scored_all) / len(scored_all)
        if scored_all else 0.0
    )
    logger.info(
        f"Score/filter: {len(scored_all)} kept, {dropped_score} below threshold, "
        f"{dropped_source} exceeded source cap, avg_score={avg_score:.3f}"
    )
    return scored_all
