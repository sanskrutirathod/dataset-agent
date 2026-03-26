"""Generate module: LLM-backed dataset record generation."""
from __future__ import annotations
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Optional

from openai import OpenAI

from ..schemas import Chunk, DatasetRecord, DatasetRecordScores, GenerationConfig
from ..utils.ids import new_id

logger = logging.getLogger(__name__)

SEED = 42

QA_PROMPT = """You are a dataset generation assistant. Given the text below, generate {n} high-quality question-answer pairs.

RULES:
- Questions must be answerable from the text only
- Answers must be grounded in the text — no hallucination
- Output ONLY a JSON array of objects with keys: "instruction" (the question), "input" (empty string), "output" (the answer)
- No commentary, no markdown fences, just the JSON array

TEXT:
{text}

OUTPUT:"""

INSTRUCTION_PROMPT = """You are a dataset generation assistant. Given the text below, generate {n} instruction-following examples.

RULES:
- Instructions should ask the model to do something based on the text (summarize, explain, extract, classify, etc.)
- Outputs must be grounded in the text — no hallucination
- Output ONLY a JSON array of objects with keys: "instruction", "input" (relevant excerpt or empty), "output"
- No commentary, no markdown fences, just the JSON array

TEXT:
{text}

OUTPUT:"""

SUMMARY_PROMPT = """You are a dataset generation assistant. Given the text below, generate a summarization training example.

RULES:
- The instruction should ask to summarize the text
- The output should be a concise, accurate summary grounded in the text
- Output ONLY a JSON array with ONE object with keys: "instruction", "input" (the text to summarize), "output" (the summary)
- No commentary, no markdown fences, just the JSON array

TEXT:
{text}

OUTPUT:"""

CHAT_PROMPT = """You are a dataset generation assistant. Given the text below, generate a multi-turn chat conversation (2-4 turns) where a user asks questions and an assistant answers based on the text.

RULES:
- All answers must be grounded in the text — no hallucination
- Output ONLY a JSON array with ONE object with keys: "instruction" (system prompt), "input" (chat history as JSON string), "output" (final assistant response)
- No commentary, no markdown fences, just the JSON array

TEXT:
{text}

OUTPUT:"""

PROMPTS = {
    "qa": QA_PROMPT,
    "instruction": INSTRUCTION_PROMPT,
    "summary": SUMMARY_PROMPT,
    "chat": CHAT_PROMPT,
}


def _get_client() -> OpenAI:
    return OpenAI(
        base_url=os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL"),
        api_key=os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY", "dummy"),
    )


def _repair_json(text: str) -> Optional[list[dict]]:
    """Try to extract JSON array from potentially malformed LLM output."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"```(?:json)?", "", text).strip("`").strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\[.*\]", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    return None


def generate_for_chunk(
    chunk: Chunk,
    config: GenerationConfig,
    existing_hashes: set[str],
) -> list[DatasetRecord]:
    client = _get_client()
    mode = config.mode
    n = config.max_records_per_chunk
    prompt_template = PROMPTS.get(mode, QA_PROMPT)
    prompt = prompt_template.format(text=chunk.text[:4000], n=n)

    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model="gpt-5-mini",
                messages=[{"role": "user", "content": prompt}],
                max_completion_tokens=2048,
            )
            raw = response.choices[0].message.content or ""
            parsed = _repair_json(raw)
            if not parsed or not isinstance(parsed, list):
                logger.warning(f"Failed to parse JSON from LLM (attempt {attempt+1})")
                continue
            break
        except Exception as e:
            logger.warning(f"LLM call failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
            parsed = None

    if not parsed:
        return []

    records = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        instruction = str(item.get("instruction", "")).strip()
        input_text = str(item.get("input", "")).strip()
        output_text = str(item.get("output", "")).strip()

        if not output_text:
            continue

        import hashlib
        rec_hash = hashlib.sha256(f"{instruction}||{output_text}".encode()).hexdigest()
        if rec_hash in existing_hashes:
            continue
        existing_hashes.add(rec_hash)

        record = DatasetRecord(
            id=new_id("ds"),
            type=mode,
            instruction=instruction,
            input=input_text,
            output=output_text,
            provenance={
                "source_id": chunk.source_id,
                "chunk_id": chunk.chunk_id,
            },
            scores=DatasetRecordScores(),
            meta={"generator": "v1", "seed": SEED, "mode": mode},
        )
        records.append(record)

    return records


def run_generate(
    chunks: list[Chunk],
    config: GenerationConfig,
    out_dir: Path,
    max_records: int = 5000,
) -> list[DatasetRecord]:
    out_dir.mkdir(parents=True, exist_ok=True)
    all_records: list[DatasetRecord] = []
    existing_hashes: set[str] = set()

    for i, chunk in enumerate(chunks):
        if len(all_records) >= max_records:
            break
        try:
            records = generate_for_chunk(chunk, config, existing_hashes)
            all_records.extend(records)
            logger.debug(f"Chunk {i+1}/{len(chunks)}: {len(records)} records generated")
        except Exception as e:
            logger.error(f"Generation error for chunk {chunk.chunk_id}: {e}")

    out_file = out_dir / "records.jsonl"
    with out_file.open("w", encoding="utf-8") as f:
        for rec in all_records:
            f.write(rec.model_dump_json() + "\n")

    logger.info(f"Generation: {len(all_records)} records from {len(chunks)} chunks")
    return all_records
