# dopejobs-pipeline

Scrape, parse, and index job postings from multiple ATS platforms.

This repo is the standalone ingestion pipeline for dopejobs. It is intended to run locally or on GitHub Actions as the canonical source of truth for:

- scraping ATS job boards
- storing pipeline state in Postgres
- parsing jobs into structured metadata with an LLM
- incrementally upserting changed jobs into MeiliSearch

## Supported ATS Platforms

- Greenhouse
- Lever
- Ashby
- Jobvite

## Architecture

The pipeline has three stages:

1. Scrape
   - fetch jobs from ATS boards
   - compare against Postgres state
   - detect new, changed, unchanged, and removed jobs

2. Parse
   - parse only jobs that still need parsing
   - merge ATS-native structured data with LLM output

3. Load
   - incrementally upsert only newly parsed or changed jobs into MeiliSearch
   - delete jobs that were removed upstream

Pipeline state lives in Postgres. Search documents live in MeiliSearch.

## Required Secrets

Set these in your environment locally, or as GitHub Actions repository secrets:

- `DATABASE_URL`
- `MEILISEARCH_HOST`
- `MEILISEARCH_MASTER_KEY`
- `GEMINI_API_KEY`
- `COMPANIES_PASSPHRASE`

Depending on your parser/backend choices, you may also need:

- `OPENAI_API_KEY`

## Local Setup

```bash
uv sync
```

Create a `.env` file or export the required environment variables.

## Local Usage

Run the full pipeline on the default company list:

```bash
gpg --quiet --batch --yes --decrypt \
  --passphrase="$COMPANIES_PASSPHRASE" \
  --output /tmp/companies.txt \
  companies.txt.gpg

uv run python pipeline.py --companies /tmp/companies.txt
```

Useful variants:

```bash
uv run python pipeline.py --companies /tmp/companies.txt --skip-load
uv run python pipeline.py --companies /tmp/companies.txt --skip-scrape
uv run python pipeline.py --companies /tmp/companies.txt --skip-parse
uv run python pipeline.py --companies /tmp/companies.txt --full-load
```

## Sharding

This pipeline supports native deterministic sharding from a single canonical company list.

Each company is assigned to a shard by hashing `ats:token`. This means:

- no committed shard files are required
- new companies automatically land in a stable shard
- the same company always stays in the same shard for a given shard count

Example:

```bash
uv run python pipeline.py \
  --companies /tmp/companies.txt \
  --shard-index 0 \
  --total-shards 4
```

Run four shards in parallel by invoking shard indices `0` through `3`.

## Safety Notes

- Normal load runs are incremental. The pipeline does not reload the full MeiliSearch corpus unless `--full-load` is passed explicitly.
- For bounded test runs, be careful with `--max-per-company`. The pipeline only marks removals when a scrape appears complete for that company.
- Scheduled workflows should avoid the top of the hour because GitHub may delay or drop scheduled jobs during high load.

## GitHub Actions

The repository currently uses a bounded manual workflow so the first GitHub run is cheap and explicit.

The workflow:

- runs on `workflow_dispatch`
- decrypts `companies.txt.gpg` at runtime
- passes shard and limit flags directly to `pipeline.py`
- supports `skip-scrape`, `skip-parse`, and `skip-load`

Recommended first run:

- `shard_index=0`
- `total_shards=1`
- `max_per_company=5`
- `parse_limit=5`
- `skip_load=true`

After that succeeds, expand the shard count, raise the limits, and add the daily `schedule` trigger back to the workflow.

## Tests

Run the fast local tests:

```bash
uv run pytest tests/test_db.py tests/test_merge_api_data.py tests/test_pipeline_sharding.py -q
```

There are also live integration tests that hit real ATS APIs. Run them only intentionally.

## Files

Core pipeline:

- `pipeline.py`
- `db.py`
- `parse.py`
- `detect_boilerplate.py`
- `job_groups.py`

ATS integrations:

- `scrapers/`

Shared helpers:

- `utils/`

## Non-Goals

This repo is only the ingestion pipeline.

It should not contain:

- the user-facing app
- unrelated private product code
- experimental one-off scripts unless they become part of routine pipeline operations
