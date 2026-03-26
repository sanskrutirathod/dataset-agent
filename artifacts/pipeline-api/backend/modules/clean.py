"""Clean module: normalize and clean source text."""
from __future__ import annotations
import re
import logging
from pathlib import Path

from ..schemas import Source

logger = logging.getLogger(__name__)

_BOILERPLATE_PATTERNS = [
    re.compile(r"cookie(s)? policy|privacy policy|terms of service|all rights reserved", re.I),
    re.compile(r"subscribe to our newsletter", re.I),
    re.compile(r"click here to (read|learn|see) more", re.I),
    re.compile(r"loading\.\.\.", re.I),
    re.compile(r"^(home|about|contact|sitemap|menu|search)\s*$", re.I | re.M),
]


def _normalize_whitespace(text: str) -> str:
    text = re.sub(r"\r\n|\r", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _remove_boilerplate(text: str) -> str:
    lines = text.split("\n")
    cleaned = []
    for line in lines:
        skip = False
        for pat in _BOILERPLATE_PATTERNS:
            if pat.search(line):
                skip = True
                break
        if not skip:
            cleaned.append(line)
    return "\n".join(cleaned)


def clean_source(source: Source) -> Source | None:
    """Clean a source's raw_text."""
    text = source.raw_text
    if not text or len(text.strip()) < 50:
        logger.debug(f"Source {source.source_id} too short, skipping")
        return None
    text = _remove_boilerplate(text)
    text = _normalize_whitespace(text)
    if len(text) < 50:
        return None
    cleaned = source.model_copy()
    object.__setattr__(cleaned, "raw_text", text)
    return cleaned


def run_clean(sources: list[Source], out_dir: Path) -> list[Source]:
    out_dir.mkdir(parents=True, exist_ok=True)
    cleaned = []
    for src in sources:
        result = clean_source(src)
        if result:
            cleaned.append(result)

    out_file = out_dir / "sources.jsonl"
    with out_file.open("w", encoding="utf-8") as f:
        for src in cleaned:
            f.write(src.model_dump_json() + "\n")

    logger.info(f"Clean: {len(cleaned)}/{len(sources)} sources kept")
    return cleaned
