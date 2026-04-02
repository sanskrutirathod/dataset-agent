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

from ..schemas import Chunk, DatasetRecord, DatasetRecordScores, GenerationConfig, DistillationMode
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

COT_PROMPT = """You are a teacher model generating chain-of-thought reasoning training data. Given the text below, generate {n} examples where you demonstrate step-by-step reasoning.

RULES:
- The instruction should be a question or task that requires multi-step reasoning
- The "thinking" field must contain detailed, step-by-step reasoning traces before reaching the answer
- The "output" field contains the final concise answer
- All content must be grounded in the provided text — no hallucination
- Output ONLY a JSON array of objects with keys: "instruction", "input" (empty string), "thinking" (step-by-step reasoning), "output" (final answer)
- No commentary, no markdown fences, just the JSON array

TEXT:
{text}

OUTPUT:"""

DPO_PREFERRED_PROMPT = """You are a teacher model generating the PREFERRED response for DPO training. Given the text and instruction, generate one high-quality, detailed, accurate response.

RULES:
- The response must be thorough, accurate, well-structured, and grounded in the text
- This is the "chosen" (preferred) response in a DPO pair
- Output ONLY a JSON object with keys: "instruction", "chosen"
- No commentary, no markdown fences, just the JSON object

TEXT:
{text}

INSTRUCTION:
{instruction}

OUTPUT:"""

DPO_REJECTED_PROMPT = """You are generating a REJECTED (lower-quality) response for DPO training. Given the text and instruction, generate a response that is plausible but subtly flawed — too brief, slightly off-topic, or missing key details.

RULES:
- The response should look reasonable but be clearly inferior to a high-quality answer
- Do NOT make it obviously wrong or nonsensical — it should be the kind of response a weaker model might produce
- Output ONLY a JSON object with keys: "instruction", "rejected"
- No commentary, no markdown fences, just the JSON object

TEXT:
{text}

INSTRUCTION:
{instruction}

OUTPUT:"""

DPO_INSTRUCTION_PROMPT = """You are a dataset generation assistant. Given the text below, generate {n} diverse instructions (questions or tasks) suitable for DPO training pairs.

RULES:
- Instructions should require substantive answers grounded in the text
- Output ONLY a JSON array of strings, each being one instruction
- No commentary, no markdown fences, just the JSON array

TEXT:
{text}

OUTPUT:"""

SFT_PROMPT = """You are a teacher model generating high-quality knowledge distillation training data. Given the text below, generate {n} instruction-output pairs with rich, verbose, expert-level explanations.

RULES:
- Instructions should ask for explanations, analysis, or synthesis of concepts from the text
- Outputs must be long-form, detailed, and pedagogically rich — as a knowledgeable teacher would explain
- All content must be grounded in the provided text — no hallucination
- Output ONLY a JSON array of objects with keys: "instruction", "input" (empty string), "output"
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


def _repair_json_object(text: str) -> Optional[dict]:
    """Try to extract a JSON object from potentially malformed LLM output."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"```(?:json)?", "", text).strip("`").strip()

    try:
        result = json.loads(text)
        if isinstance(result, dict):
            return result
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            result = json.loads(match.group(0))
            if isinstance(result, dict):
                return result
        except json.JSONDecodeError:
            pass

    return None


def _llm_call(client: OpenAI, model: str, prompt: str, max_tokens: int = 2048) -> Optional[str]:
    """Make a single LLM call with retry logic."""
    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_completion_tokens=max_tokens,
            )
            return response.choices[0].message.content or ""
        except Exception as e:
            logger.warning(f"LLM call failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    return None


def _generate_cot(
    chunk: Chunk,
    config: GenerationConfig,
    existing_hashes: set[str],
) -> list[DatasetRecord]:
    """Generate Chain-of-Thought records."""
    client = _get_client()
    model = config.teacher_model or "gpt-5-mini"
    n = config.max_records_per_chunk
    prompt = COT_PROMPT.format(text=chunk.text[:4000], n=n)

    raw = _llm_call(client, model, prompt, max_tokens=3000)
    if not raw:
        return []

    parsed = _repair_json(raw)
    if not parsed or not isinstance(parsed, list):
        logger.warning("Failed to parse JSON from CoT LLM response")
        return []

    import hashlib
    records = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        instruction = str(item.get("instruction", "")).strip()
        input_text = str(item.get("input", "")).strip()
        thinking = str(item.get("thinking", "")).strip()
        output_text = str(item.get("output", "")).strip()

        if not output_text or not thinking:
            continue

        rec_hash = hashlib.sha256(f"{instruction}||{output_text}".encode()).hexdigest()
        if rec_hash in existing_hashes:
            continue
        existing_hashes.add(rec_hash)

        record = DatasetRecord(
            id=new_id("ds"),
            type="distillation_cot",
            instruction=instruction,
            input=input_text,
            output=output_text,
            thinking=thinking,
            provenance={
                "source_id": chunk.source_id,
                "chunk_id": chunk.chunk_id,
            },
            scores=DatasetRecordScores(),
            meta={"generator": "v1", "seed": SEED, "mode": "distillation_cot", "teacher_model": model},
        )
        records.append(record)

    return records


def _generate_dpo(
    chunk: Chunk,
    config: GenerationConfig,
    existing_hashes: set[str],
) -> list[DatasetRecord]:
    """Generate Direct Preference Optimization (DPO) records."""
    client = _get_client()
    model = config.teacher_model or "gpt-5-mini"
    n = config.max_records_per_chunk

    instr_prompt = DPO_INSTRUCTION_PROMPT.format(text=chunk.text[:4000], n=n)
    raw_instrs = _llm_call(client, model, instr_prompt, max_tokens=1024)
    if not raw_instrs:
        return []

    instructions_raw = _repair_json(raw_instrs)
    if not instructions_raw or not isinstance(instructions_raw, list):
        logger.warning("Failed to parse DPO instructions")
        return []

    instructions = [str(i).strip() for i in instructions_raw if str(i).strip()]

    import hashlib
    records = []
    for instruction in instructions[:n]:
        preferred_prompt = DPO_PREFERRED_PROMPT.format(
            text=chunk.text[:3000], instruction=instruction
        )
        rejected_prompt = DPO_REJECTED_PROMPT.format(
            text=chunk.text[:3000], instruction=instruction
        )

        raw_preferred = _llm_call(client, model, preferred_prompt, max_tokens=1024)
        raw_rejected = _llm_call(client, model, rejected_prompt, max_tokens=512)

        if not raw_preferred or not raw_rejected:
            continue

        preferred_obj = _repair_json_object(raw_preferred)
        rejected_obj = _repair_json_object(raw_rejected)

        if not preferred_obj or not rejected_obj:
            continue

        chosen = str(preferred_obj.get("chosen", "")).strip()
        rejected = str(rejected_obj.get("rejected", "")).strip()

        if not chosen or not rejected:
            continue

        rec_hash = hashlib.sha256(f"{instruction}||{chosen}".encode()).hexdigest()
        if rec_hash in existing_hashes:
            continue
        existing_hashes.add(rec_hash)

        record = DatasetRecord(
            id=new_id("ds"),
            type="distillation_dpo",
            instruction=instruction,
            input="",
            output=chosen,
            chosen=chosen,
            rejected=rejected,
            provenance={
                "source_id": chunk.source_id,
                "chunk_id": chunk.chunk_id,
            },
            scores=DatasetRecordScores(),
            meta={"generator": "v1", "seed": SEED, "mode": "distillation_dpo", "teacher_model": model},
        )
        records.append(record)

    return records


def _generate_sft(
    chunk: Chunk,
    config: GenerationConfig,
    existing_hashes: set[str],
) -> list[DatasetRecord]:
    """Generate Knowledge Distillation SFT records with rich, verbose outputs."""
    client = _get_client()
    model = config.teacher_model or "gpt-5-mini"
    n = config.max_records_per_chunk
    prompt = SFT_PROMPT.format(text=chunk.text[:4000], n=n)

    raw = _llm_call(client, model, prompt, max_tokens=3000)
    if not raw:
        return []

    parsed = _repair_json(raw)
    if not parsed or not isinstance(parsed, list):
        logger.warning("Failed to parse JSON from SFT LLM response")
        return []

    import hashlib
    records = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        instruction = str(item.get("instruction", "")).strip()
        input_text = str(item.get("input", "")).strip()
        output_text = str(item.get("output", "")).strip()

        if not output_text:
            continue

        rec_hash = hashlib.sha256(f"{instruction}||{output_text}".encode()).hexdigest()
        if rec_hash in existing_hashes:
            continue
        existing_hashes.add(rec_hash)

        record = DatasetRecord(
            id=new_id("ds"),
            type="distillation_sft",
            instruction=instruction,
            input=input_text,
            output=output_text,
            provenance={
                "source_id": chunk.source_id,
                "chunk_id": chunk.chunk_id,
            },
            scores=DatasetRecordScores(),
            meta={"generator": "v1", "seed": SEED, "mode": "distillation_sft", "teacher_model": model},
        )
        records.append(record)

    return records


def generate_for_chunk(
    chunk: Chunk,
    config: GenerationConfig,
    existing_hashes: set[str],
) -> list[DatasetRecord]:
    if config.distillation_mode is not None:
        if config.distillation_mode == DistillationMode.cot:
            return _generate_cot(chunk, config, existing_hashes)
        elif config.distillation_mode == DistillationMode.dpo:
            return _generate_dpo(chunk, config, existing_hashes)
        elif config.distillation_mode == DistillationMode.sft:
            return _generate_sft(chunk, config, existing_hashes)

    mode = config.mode
    n = config.max_records_per_chunk
    prompt_template = PROMPTS.get(mode, QA_PROMPT)
    prompt = prompt_template.format(text=chunk.text[:4000], n=n)

    if hasattr(config, "temperature") and config.temperature is not None and config.temperature != 1.0:
        logger.debug(
            f"temperature={config.temperature} requested but the current model only supports "
            "the default value (1.0); parameter is not forwarded to the API"
        )

    client = _get_client()
    raw = _llm_call(client, "gpt-5-mini", prompt, max_tokens=2048)
    if not raw:
        return []
    parsed = _repair_json(raw)
    if not parsed or not isinstance(parsed, list):
        logger.warning("Failed to parse JSON from LLM response")
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


_CIRCUIT_BREAKER_THRESHOLD = 3


def run_generate(
    chunks: list[Chunk],
    config: GenerationConfig,
    out_dir: Path,
    max_records: int = 5000,
) -> list[DatasetRecord]:
    out_dir.mkdir(parents=True, exist_ok=True)
    all_records: list[DatasetRecord] = []
    existing_hashes: set[str] = set()
    consecutive_failures = 0

    for i, chunk in enumerate(chunks):
        if len(all_records) >= max_records:
            break
        if consecutive_failures >= _CIRCUIT_BREAKER_THRESHOLD:
            logger.warning(
                f"Circuit breaker tripped after {consecutive_failures} consecutive LLM failures — "
                f"skipping remaining {len(chunks) - i} chunks and returning partial results"
            )
            break
        try:
            records = generate_for_chunk(chunk, config, existing_hashes)
            if records:
                consecutive_failures = 0
                all_records.extend(records)
            else:
                consecutive_failures += 1
            logger.debug(f"Chunk {i+1}/{len(chunks)}: {len(records)} records generated")
        except Exception as e:
            consecutive_failures += 1
            logger.error(f"Generation error for chunk {chunk.chunk_id}: {e}")

    out_file = out_dir / "records.jsonl"
    with out_file.open("w", encoding="utf-8") as f:
        for rec in all_records:
            f.write(rec.model_dump_json() + "\n")

    logger.info(f"Generation: {len(all_records)} records from {len(chunks)} chunks")
    return all_records
