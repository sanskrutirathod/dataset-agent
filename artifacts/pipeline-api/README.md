# Dataset Engine

Production-grade dataset factory for AI training data. Transforms web pages, documents, and text into high-quality training datasets using LLM-powered generation.

## Features

- **Multi-source ingestion**: URLs, crawled websites, text files
- **Smart cleaning**: Boilerplate removal, text normalization
- **Deduplication**: Exact + near-duplicate detection (fuzzy matching)
- **Intelligent chunking**: Semantic-aware sliding window tokenization
- **LLM generation**: QA pairs, Chain-of-Thought, DPO, SFT formats
- **Quality scoring**: Multi-dimensional heuristic evaluation
- **Validation**: Length, safety, grounding checks
- **Export**: JSONL, CSV, Markdown reports, HuggingFace Hub upload

## Quick Start

### 1. Configuration

Copy the example environment file and configure your API keys:

```bash
cd artifacts/pipeline-api
cp example.env .env
# Edit .env with your actual values
```

Required environment variables:

| Variable | Description |
|----------|-------------|
| `AI_INTEGRATIONS_OPENAI_API_KEY` | OpenAI API key for LLM generation (required) |
| `AI_INTEGRATIONS_OPENAI_BASE_URL` | Custom API endpoint (optional) |
| `HUGGINGFACE_TOKEN` | HF token for dataset upload (optional) |

### 2. Installation

```bash
cd artifacts/pipeline-api
pip install -r requirements.txt
```

### 3. Run the Server

```bash
python main.py
```

The API will be available at `http://localhost:8000`

### 4. Start a Pipeline Run

```bash
curl -X POST http://localhost:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{
    "sources": [
      {"type": "url", "value": "https://example.com/article"}
    ],
    "chunk": {"target_tokens": 600, "overlap": 80},
    "generation": {"mode": "qa", "max_records_per_chunk": 1}
  }'
```

## Pipeline Stages

```
┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐
│ INGEST  │──▶│  CLEAN  │──▶│  DEDUP  │──▶│  CHUNK  │
└─────────┘   └─────────┘   └─────────┘   └─────────┘
                                              │
                                              ▼
┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐
│ EXPORT  │◀──│  SCORE  │◀──│VALIDATE │◀──│ GENERATE│
└─────────┘   └─────────┘   └─────────┘   └─────────┘
```

1. **INGEST**: Fetch URLs or crawl websites
2. **CLEAN**: Remove boilerplate, normalize text
3. **DEDUP**: Remove exact and near-duplicates
4. **CHUNK**: Split into token-bounded segments
5. **GENERATE**: LLM-powered record creation
6. **VALIDATE**: Quality and safety checks
7. **SCORE**: Multi-dimensional quality scoring
8. **EXPORT**: Output to JSONL, CSV, or push to HuggingFace

## API Reference

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/pipeline/run` | Start a pipeline run |
| `GET` | `/pipeline/runs` | List all runs |
| `GET` | `/pipeline/runs/{id}` | Get run details |
| `GET` | `/pipeline/runs/{id}/download` | Download dataset |
| `POST` | `/pipeline/runs/{id}/push-to-hub` | Upload to HuggingFace |
| `GET` | `/pipeline/runs/{id}/stream` | SSE event stream |
| `GET` | `/pipeline/healthz` | Health check |

### Run Configuration

```json
{
  "run_name": "my_dataset",
  "sources": [
    {"type": "url", "value": "https://example.com/page"},
    {"type": "crawl", "seed_url": "https://docs.example.com", "max_depth": 2, "max_pages": 50},
    {"type": "text", "value": "Your text content here", "title": "Custom Source"}
  ],
  "crawl": {
    "seed_url": "",
    "max_depth": 2,
    "max_pages": 50,
    "allowed_domains": [],
    "delay_ms": 500
  },
  "chunk": {
    "target_tokens": 600,
    "overlap": 80
  },
  "generation": {
    "mode": "qa",
    "temperature": 0.2,
    "max_records_per_chunk": 1,
    "teacher_model": "gpt-5-mini"
  },
  "validation": {
    "min_length": 50,
    "max_length": 4096,
    "score_threshold": 0.75,
    "grounding_min_overlap": 0.1
  },
  "limits": {
    "max_records": 5000,
    "max_per_source": 1000
  }
}
```

### Generation Modes

| Mode | Description |
|------|-------------|
| `qa` | Question-Answer pairs |
| `cot` | Chain-of-Thought reasoning |
| `dpo` | Direct Preference Optimization pairs |
| `sft` | Supervised Fine-Tuning format |

## Data Flow

### Object Lifecycle

```
Source (raw) → Source (cleaned) → Source (deduped) → Chunk → DatasetRecord → ValidatedRecord → ScoredRecord → Export
```

1. **Source**: Raw content from URL/text/crawl with metadata
2. **Chunk**: Token-bounded text segments with provenance
3. **DatasetRecord**: LLM-generated content (instruction/input/output)
4. **ValidatedRecord**: Passed quality and safety checks
5. **ScoredRecord**: Includes multi-dimensional quality scores

### Output Files

Each run produces:

```
data/versions/{run_id}/
├── config.json           # Pipeline configuration
├── raw/sources.jsonl     # Raw ingested sources
├── processed/sources.jsonl  # Cleaned sources
├── dedup/sources.jsonl   # Deduplicated sources
├── chunks/chunks.jsonl   # Text chunks
├── generated/records.jsonl  # LLM-generated records
├── validated/records.jsonl # Validation-passed records
├── scored/records.jsonl  # Scored and filtered records
└── export/
    ├── dataset.jsonl     # Final JSONL export
    ├── dataset.csv       # CSV export
    └── report.md         # Markdown report
```

## Environment Variables

See `example.env` for all configuration options.

| Variable | Default | Description |
|----------|---------|-------------|
| `AI_INTEGRATIONS_OPENAI_API_KEY` | - | OpenAI API key (required) |
| `AI_INTEGRATIONS_OPENAI_BASE_URL` | - | Custom API endpoint |
| `HUGGINGFACE_TOKEN` | - | HF Hub token |
| `PIPELINE_PORT` | 8000 | Server port |
| `PIPELINE_CONCURRENCY` | 4 | Max concurrent runs |
| `API_KEY` | - | API authentication key |

## Testing

```bash
cd artifacts/pipeline-api
pip install pytest
python -m pytest tests/ -v
```

## Architecture

```
backend/
├── main.py              # FastAPI application entry point
├── routes/
│   ├── runs.py          # Pipeline run endpoints
│   └── ingest.py        # Direct ingestion endpoints
├── modules/
│   ├── ingest.py        # URL/text fetching
│   ├── crawler.py       # Web crawler
│   ├── clean.py         # Text cleaning
│   ├── dedup.py         # Deduplication
│   ├── chunk.py         # Text chunking
│   ├── generate.py       # LLM generation
│   ├── validate.py      # Quality validation
│   ├── score.py         # Quality scoring
│   ├── export.py        # Export formatting
│   └── hf_upload.py     # HuggingFace upload
├── pipeline/
│   ├── orchestrator.py  # Pipeline orchestration
│   ├── db.py           # SQLite metadata store
│   ├── event_bus.py    # SSE event distribution
│   └── job_queue.py    # ThreadPoolExecutor queue
└── schemas/
    └── models.py       # Pydantic data models
```

## Error Handling

The pipeline provides clear error messages:

- **Missing API key**: `"AI_INTEGRATIONS_OPENAI_API_KEY environment variable is not set"`
- **Invalid URL**: `"URL blocked for security: <reason>"`
- **SSRF prevention**: Private/internal IP addresses are blocked
- **Malformed records**: Skipped with warning logs

## Known Issues Fixed

- ✅ API key validation now fails fast with clear error
- ✅ Temperature parameter now forwarded to LLM API
- ✅ Job queue status is thread-safe
- ✅ HF upload handles malformed JSON lines gracefully
- ✅ Export files created even when generation fails

## License

MIT
