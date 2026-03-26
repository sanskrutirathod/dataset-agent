"""Dedup module: exact + near-duplicate detection."""
from __future__ import annotations
import logging
from pathlib import Path

from rapidfuzz import fuzz

from ..schemas import Source
from ..utils.ids import sha256_hash

logger = logging.getLogger(__name__)

SIMILARITY_THRESHOLD = 85.0


def _ngrams(text: str, n: int = 5) -> set[str]:
    words = text.lower().split()
    return {" ".join(words[i:i+n]) for i in range(len(words) - n + 1)}


def run_dedup(sources: list[Source], out_dir: Path) -> list[Source]:
    out_dir.mkdir(parents=True, exist_ok=True)
    seen_hashes: set[str] = set()
    seen_texts: list[str] = []
    unique: list[Source] = []
    dup_count = 0

    for src in sources:
        h = sha256_hash(src.raw_text)
        if h in seen_hashes:
            dup_count += 1
            logger.debug(f"Exact dup skipped: {src.source_id}")
            continue

        is_near_dup = False
        for prev_text in seen_texts:
            score = fuzz.token_set_ratio(src.raw_text[:2000], prev_text[:2000])
            if score >= SIMILARITY_THRESHOLD:
                is_near_dup = True
                dup_count += 1
                logger.debug(f"Near-dup skipped: {src.source_id} (score={score})")
                break

        if not is_near_dup:
            seen_hashes.add(h)
            seen_texts.append(src.raw_text[:2000])
            unique.append(src)

    out_file = out_dir / "sources.jsonl"
    with out_file.open("w", encoding="utf-8") as f:
        for src in unique:
            f.write(src.model_dump_json() + "\n")

    logger.info(f"Dedup: {len(unique)} unique / {dup_count} duplicates removed")
    return unique
