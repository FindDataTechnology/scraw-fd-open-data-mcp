# Architecture

`CrawlPlan` (from `fd-open-data-mcp plan-crawl`) -> `ConceptCrawlSpider` expands
`(concept x entity x date)` -> adapter registry `build_params` + `run_upstream` fetch
-> `ObservationItem` -> `ObservationUpsertPipeline` (idempotent upsert into
`semantic_observations` on the remote Postgres) + `JsonLinesPipeline` (audit).

scrapy-redis `RFPDupeFilter` dedups by the synthetic request URL encoding
`(source, command, params)` (design D7), so pause/resume does not re-emit items.

The store is the single canonical `semantic_observations` (unified-data-store spec):
crawl warms the read-cache; `fd-open-data-mcp read()` returns migrated/crawled rows
from cache without dispatch.
