"""Scrapy settings for scraw-fd-open-data-mcp (scraw-project-template conformant).

scrapy-redis scheduler + RFPDupeFilter (SCHEDULER_PERSIST, per-project REDIS_KEY);
JsonLinesPipeline@400 (audit); PG pipeline@300 -> semantic_observations.
"""
import os

BOT_NAME = "scraw_fd_open_data_mcp"

SPIDER_MODULES = ["scraw_fd_open_data_mcp.spiders"]
NEWSPIDER_MODULE = "scraw_fd_open_data_mcp.spiders"

ROBOTSTXT_OBEY = False

# fetch:// download handler -> adapter registry (lib-based fetch, not HTTP).
# Only override the fetch scheme; Scrapy keeps its defaults for http/https/data.
DOWNLOAD_HANDLERS = {
    "fetch": "scraw_fd_open_data_mcp.fetch_handler.FetchHandler",
}

# scrapy-redis scheduler + dupefilter (template-mandated)
SCHEDULER = "scrapy_redis.scheduler.Scheduler"
DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"
SCHEDULER_PERSIST = True
REDIS_URL = os.environ.get("REDIS_URL", "redis://192.168.1.4:6379/0")
REDIS_KEY = "scraw_fd_open_data_mcp:start_urls"

ITEM_PIPELINES = {
    "scraw_fd_open_data_mcp.pipelines.ObservationUpsertPipeline": 300,
    "scraw_fd_open_data_mcp.pipelines.JsonLinesPipeline": 400,
}

# outputs
JSONL_PATH = os.environ.get("JSONL_PATH", "output/items.jl")

# the mcp store (semantic_observations lives here)
FD_OPEN_DATA_MCP_DATABASE_URL = os.environ.get("FD_OPEN_DATA_MCP_DATABASE_URL")

# fetch politeness (akshare/eastmoney is flaky + rate-limited)
DOWNLOAD_DELAY = 0
CONCURRENT_REQUESTS = 4
RETRY_TIMES = 2
