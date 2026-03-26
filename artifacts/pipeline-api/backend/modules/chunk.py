"""Chunk module: sliding window chunking with semantic splits."""
from __future__ import annotations
import hashlib
import logging
import re
from pathlib import Path
from typing import Optional

import tiktoken

from ..schemas import Chunk, Source, ChunkConfig
from ..utils.ids import new_id, sha256_hash

logger = logging.getLogger(__name__)

HEADING_RE = re.compile(r"^#{1,6}\s+.+|^[A-Z][A-Z\s]{3,}$", re.M)

try:
    _enc = tiktoken.get_encoding("cl100k_base")
except Exception:
    _enc = None


def count_tokens(text: str) -> int:
    if _enc:
        return len(_enc.encode(text))
    return len(text.split())


def _split_on_semantics(text: str) -> list[str]:
    """Split on headings and double newlines."""
    parts = re.split(r"\n\n+", text)
    result = []
    for part in parts:
        if HEADING_RE.match(part.strip()):
            result.append(part)
        else:
            result.append(part)
    return [p.strip() for p in result if p.strip()]


def chunk_text(
    source: Source,
    config: ChunkConfig,
) -> list[Chunk]:
    """Sliding window chunking with semantic hints."""
    target = config.target_tokens
    overlap = config.overlap
    text = source.raw_text

    semantic_parts = _split_on_semantics(text)
    words = []
    section_map: dict[int, str] = {}
    current_section = ""

    for part in semantic_parts:
        if HEADING_RE.match(part):
            current_section = part[:80]
        part_words = part.split()
        for w in part_words:
            section_map[len(words)] = current_section
            words.append(w)

    chunks = []
    idx = 0
    pos = 0
    token_buf: list[str] = []
    token_count = 0

    while pos < len(words):
        word = words[pos]
        wt = count_tokens(word + " ")
        token_buf.append(word)
        token_count += wt

        if token_count >= target:
            chunk_text_str = " ".join(token_buf)
            section = section_map.get(pos - len(token_buf) + 1, "")
            h = sha256_hash(chunk_text_str)
            chunk = Chunk(
                chunk_id=new_id("chk"),
                source_id=source.source_id,
                idx=idx,
                text=chunk_text_str,
                tokens=token_count,
                overlap=overlap,
                section=section,
                hash=h,
            )
            chunks.append(chunk)
            idx += 1

            overlap_tokens = 0
            overlap_words = []
            for w in reversed(token_buf):
                overlap_tokens += count_tokens(w + " ")
                if overlap_tokens >= overlap:
                    break
                overlap_words.insert(0, w)

            token_buf = overlap_words
            token_count = sum(count_tokens(w + " ") for w in token_buf)

        pos += 1

    if token_buf and token_count > 10:
        chunk_text_str = " ".join(token_buf)
        section = section_map.get(len(words) - len(token_buf), "")
        h = sha256_hash(chunk_text_str)
        chunk = Chunk(
            chunk_id=new_id("chk"),
            source_id=source.source_id,
            idx=idx,
            text=chunk_text_str,
            tokens=token_count,
            overlap=overlap,
            section=section,
            hash=h,
        )
        chunks.append(chunk)

    return chunks


def run_chunk(
    sources: list[Source],
    config: ChunkConfig,
    out_dir: Path,
) -> list[Chunk]:
    out_dir.mkdir(parents=True, exist_ok=True)
    all_chunks: list[Chunk] = []
    for src in sources:
        chunks = chunk_text(src, config)
        all_chunks.extend(chunks)

    out_file = out_dir / "chunks.jsonl"
    with out_file.open("w", encoding="utf-8") as f:
        for chunk in all_chunks:
            f.write(chunk.model_dump_json() + "\n")

    total_tokens = sum(c.tokens for c in all_chunks)
    avg_tokens = total_tokens / len(all_chunks) if all_chunks else 0
    logger.info(f"Chunking: {len(all_chunks)} chunks, avg_tokens={avg_tokens:.1f}")
    return all_chunks
