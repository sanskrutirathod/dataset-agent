# Dataset Export Report

**Run ID**: `run_c1c13701d152`
**Run Name**: test_full_pipeline
**Total Records**: 1

## Score Distribution

| Metric | Value |
|--------|-------|
| Avg Final Score | 0.4846 |
| Min Score | 0.4846 |
| Max Score | 0.4846 |

## Record Types

- **qa**: 1

## Pipeline Stage Metrics

| Stage | Input | Output | Latency (ms) |
|-------|-------|--------|--------------|
| ingest | 1 | 1 | 0 |
| clean | 1 | 1 | 0 |
| dedup | 1 | 1 | 0 |
| chunk | 1 | 2 | 2 |
| generate | 2 | 4 | 19594 |
| validate | 4 | 1 | 1 |
| score | 1 | 1 | 1 |