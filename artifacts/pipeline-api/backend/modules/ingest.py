"""Ingest module: fetch URLs and accept text input, output raw Source objects."""
from __future__ import annotations
import json
import logging
import time
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter, Retry

from ..schemas import Source
from ..utils.ids import new_id

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; WebspaceAI-DataEngine/1.0; "
        "+https://webspaceai.app)"
    )
}


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


def _html_to_text(html: str, url: str = "") -> str:
    """Try trafilatura first, fallback to BeautifulSoup."""
    try:
        import trafilatura
        text = trafilatura.extract(
            html,
            include_comments=False,
            include_tables=True,
            no_fallback=False,
            favor_precision=True,
        )
        if text and len(text.strip()) > 100:
            return text.strip()
    except Exception as e:
        logger.debug(f"trafilatura failed for {url}: {e}")

    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["nav", "footer", "script", "style", "header", "aside"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        return text.strip()
    except Exception as e:
        logger.warning(f"bs4 fallback failed for {url}: {e}")
        return ""


def ingest_url(url: str) -> Source | None:
    """Fetch a URL and extract text."""
    session = _make_session()
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "")
        if "html" in content_type:
            raw_text = _html_to_text(resp.text, url)
        else:
            raw_text = resp.text.strip()
        if not raw_text:
            logger.warning(f"Empty extraction for {url}")
            return None
        raw_text = raw_text.encode("utf-8", errors="replace").decode("utf-8")
        return Source(
            source_id=new_id("src"),
            type="web",
            title=url.split("/")[-1] or url,
            uri=url,
            raw_text=raw_text,
            meta={"lang": "en", "tags": [], "url": url},
        )
    except Exception as e:
        logger.error(f"Failed to ingest URL {url}: {e}")
        return None


def ingest_text(text: str, title: str = "text_input", uri: str = "") -> Source:
    """Accept plain text input as a source."""
    text = text.encode("utf-8", errors="replace").decode("utf-8").strip()
    return Source(
        source_id=new_id("src"),
        type="text",
        title=title,
        uri=uri,
        raw_text=text,
        meta={"lang": "en", "tags": []},
    )


def run_ingest(
    sources_config: list[dict[str, Any]],
    out_dir: Path,
) -> list[Source]:
    """Run ingestion for all sources, write raw JSONL, return Source list."""
    out_dir.mkdir(parents=True, exist_ok=True)
    results: list[Source] = []
    for cfg in sources_config:
        src_type = cfg.get("type", "text")
        if src_type == "url":
            url = cfg.get("value", "")
            if not url:
                continue
            source = ingest_url(url)
        elif src_type in ("text", "file"):
            text = cfg.get("value", "") or cfg.get("text", "")
            title = cfg.get("title", "text_input")
            source = ingest_text(text, title=title)
        else:
            logger.warning(f"Unknown source type: {src_type}")
            continue
        if source:
            results.append(source)

    out_file = out_dir / "sources.jsonl"
    with out_file.open("w", encoding="utf-8") as f:
        for src in results:
            f.write(src.model_dump_json() + "\n")

    logger.info(f"Ingested {len(results)}/{len(sources_config)} sources")
    return results
