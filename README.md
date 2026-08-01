# scraw-fd-open-data-mcp

The **unified concept-driven crawler** for `fd-open-data-mcp`. Replaces the per-source
`scraw-*` projects. Two modes:

- **crawl** - a `ConceptCrawlSpider` consumes a `CrawlPlan` (from `fd-open-data-mcp plan-crawl`),
  fetches via the shared `fd_open_data_mcp` adapter registry, and writes `semantic_observations`
  (crawl warms the read-cache).
- **migrate** - reshape legacy per-source tables into `semantic_observations` (delegates to
  `fd_open_data_mcp.crawl.migrate`).

## Overview

`fd-open-data-mcp plan-crawl` emits a `CrawlPlan` (concepts + entity scope + date range).
`scraw-fd-open-data-mcp crawl plan.json` runs it: for each `(concept x entity x date)` the
adapter registry builds params, `run_upstream` fetches, and the PG pipeline idempotently
upserts into `semantic_observations` on the remote Postgres. scrapy-redis dedups by a
synthesized `(source, command, params)` request key.

## Architecture

See `docs/ARCHITECTURE.md`.

## Quickstart

```bash
cd scraw-fd-open-data-mcp
uv venv --python 3.12
uv pip install --python .venv/bin/python -e .
cp .env.example .env  # set FD_OPEN_DATA_MCP_DATABASE_URL, REDIS_URL, SCRAPYD_URL

# crawl (forward): plan then run
fd-open-data-mcp plan-crawl --concept-id 234 --entity-type stock --start 2026-07-01 --end 2026-07-10 -o plan.json
scraw-fd-open-data-mcp crawl plan.json

# migrate (legacy -> semantic_observations)
scraw-fd-open-data-mcp migrate astock --symbol 000001   # sample
```

## Configuration

See `docs/CONFIG.md`. Env: `FD_OPEN_DATA_MCP_DATABASE_URL` (canonical store),
`REDIS_URL`, `SCRAPYD_URL`, `JSONL_PATH`.

## Deploy & Schedule

See `docs/DEPLOY.md`. `./deploy.sh` builds the egg and deploys to scrapyd;
`python schedule.py concept_crawl --plan plan.json` schedules via the scrapyd API.

## Tests

```bash
.venv/bin/python -m pytest tests/test_smoke.py -v   # import + settings + scrapy list
```

## Project layout

```
scraw-fd-open-data-mcp/
├── pyproject.toml          # scrapy entry point -> settings; console script
├── scrapy.cfg              # deploy -> scrapyd
├── deploy.sh  schedule.py
├── scraw_fd_open_data_mcp/
│   ├── settings.py         # scrapy-redis + pipelines (PG@300, JSONL@400)
│   ├── items.py            # ObservationItem
│   ├── pipelines.py        # ObservationUpsertPipeline + JsonLinesPipeline
│   ├── db.py               # write_observations -> semantic_observations
│   ├── cli.py              # crawl / migrate
│   └── spiders/concept_crawl_spider.py
├── tests/test_smoke.py
└── docs/
```
