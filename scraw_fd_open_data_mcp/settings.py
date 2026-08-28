"""Scrapy settings for scraw-fd-open-data-mcp.

Configured for scrapy-redis queue-based scheduling (dynamic load balancing).
Used by both price crawl and financial data extraction spiders.
"""
import os

BOT_NAME = "scraw_fd_open_data_mcp"

SPIDER_MODULES = ["scraw_fd_open_data_mcp.spiders"]
NEWSPIDER_MODULE = "scraw_fd_open_data_mcp.spiders"

ROBOTSTXT_OBEY = False

# fetch:// download handler -> adapter registry (lib-based fetch, not HTTP)
DOWNLOAD_HANDLERS = {
    "fetch": "scraw_fd_open_data_mcp.fetch_handler.FetchHandler",
}

# Scrapy-redis scheduler for dynamic task distribution
SCHEDULER = "scrapy_redis.scheduler.Scheduler"
SCHEDULER_QUEUE_CLASS = "scrapy_redis.queue.SpiderQueue"  # LIFO queue
SCHEDULER_PERSIST = True  # Keep tasks when stopped
DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"
# Canonical redis = fd-redis on guangzhou-xinru :30380, password-protected (real
# password in guangzhou-xinru:/etc/fd-open-data/db-credentials.env). From the Mac
# use the SSH tunnel (localhost:30380). Always set REDIS_URL in the env for runs.
REDIS_URL = os.environ.get("REDIS_URL", "redis://:FD_REDIS_PASSWORD@localhost:30380/0")
# Per-project redis namespace (scraw-project-template / concept-crawl-executor spec)
REDIS_KEY = "scraw_fd_open_data_mcp:start_urls"
REDIS_START_URLS_KEY = REDIS_KEY  # scrapy-redis start-queue key

# Rate limiting (polite crawling of akshare/eastmoney/yahoo)
DOWNLOAD_DELAY = 0.5
CONCURRENT_REQUESTS = 4
CONCURRENT_REQUESTS_PER_DOMAIN = 2
RETRY_TIMES = 2
DOWNLOADER_TIMEOUT = 30

# Default to INFO: urllib3 connectionpool DEBUG lines print the full request URL
# (query string included), which leaks credentials like the DC_API_KEY used by
# the datacommons adapter. Override per-run via the LOG_LEVEL env var if needed.
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# Item pipelines
ITEM_PIPELINES = {
    "scraw_fd_open_data_mcp.pipelines.ObservationUpsertPipeline": 300,
    "scraw_fd_open_data_mcp.pipelines.JsonLinesPipeline": 400,
}

JSONL_PATH = os.environ.get("JSONL_PATH", "output/items.jl")
FD_OPEN_DATA_MCP_DATABASE_URL = os.environ.get("FD_OPEN_DATA_MCP_DATABASE_URL")
