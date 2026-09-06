"""Per-job scrapy-redis queue keys (fix-shared-redis-queue).

scrapy_redis keys its request queue on the spider name alone, so every
crawl-policy Job in the fleet shared ONE ``concept_crawl:requests`` list: a
country/datacommons pod drained thousands of stale akshare requests left by
killed jobs (found live 2026-09-07: wave-25's datacommons pod starved behind
zombie stock-ETF requests). Scheduling the project's ``JobSpiderQueue``
appends the Job's ``SCRAW_JOB_REF`` to the key, so each run drains only the
requests it pushed itself. Dupefilter stays shared (cleared at startup, see
spider._clear_stale_dupefilter).
"""
from __future__ import annotations

import os

from scrapy_redis.queue import SpiderQueue


class JobSpiderQueue(SpiderQueue):
    """SpiderQueue whose key is suffixed with the run's SCRAW_JOB_REF."""

    def __init__(self, server, spider, key, serializer=None):
        job = os.environ.get("SCRAW_JOB_REF")
        if job:
            key = f"{key}:{job}"
        super().__init__(server, spider, key, serializer)
