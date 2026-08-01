"""Item pipelines: PG upsert into semantic_observations (@300) + JSONL audit (@400)."""
from __future__ import annotations

import os
from collections import defaultdict

from itemadapter import ItemAdapter


class ObservationUpsertPipeline:
    """Batch-upsert ObservationItems into semantic_observations."""

    BATCH = 500

    def __init__(self):
        self._buffer: list[dict] = []

    def process_item(self, item, spider):
        self._buffer.append(ItemAdapter(item).asdict())
        if len(self._buffer) >= self.BATCH:
            self._flush()
        return item

    def close_spider(self, spider):
        self._flush()

    def _flush(self):
        if not self._buffer:
            return
        from scraw_fd_open_data_mcp.db import write_observations

        write_observations(self._buffer)
        self._buffer.clear()


class JsonLinesPipeline:
    """Append every item to a JSON Lines file (audit/provenance sink, per template)."""

    def __init__(self, path: str):
        self.path = path
        self._fh = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get("JSONL_PATH", "output/items.jl"))

    def open_spider(self, spider):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")

    def process_item(self, item, spider):
        import json

        self._fh.write(json.dumps(ItemAdapter(item).asdict(), ensure_ascii=False, default=str) + "\n")
        self._fh.flush()
        return item

    def close_spider(self, spider):
        if self._fh:
            self._fh.close()
