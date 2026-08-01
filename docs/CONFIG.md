# Configuration

| Env | Purpose |
|---|---|
| `FD_OPEN_DATA_MCP_DATABASE_URL` | Canonical store (remote Postgres, where `semantic_observations` lives) |
| `REDIS_URL` | scrapy-redis (env from test env; per-project `scraw_fd_open_data_mcp:start_urls`) |
| `SCRAPYD_URL` | scrapyd deploy/schedule target |
| `JSONL_PATH` | audit JSONL output |
