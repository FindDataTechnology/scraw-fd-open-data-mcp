"""Item pipelines: PG upsert into semantic_observations (@300) + JSONL audit (@400)."""
from __future__ import annotations

import os
from collections import defaultdict

from itemadapter import ItemAdapter


class ObservationUpsertPipeline:
    """Batch-upsert ObservationItems into semantic_observations.

    Two item shapes:
    - per_date:  {concept_id, entity_type, entity_id, date, value, unit, source_used}
    - series:    {concept_id, entity_type, entity_id, unit, source_used,
                  series: {'YYYY-MM-DD': value}, start, end}
      A series item is EXPLODED into per-date observation rows here, clamped to
      [start, end] (out-of-range rows are dropped), before the standard
      idempotent upsert (design D6).
    """

    # Flush every 50 items, not 500. A long crawl (the country-macro yearly
    # sweep is ~1650 fetches / ~60min) must NOT hold all progress in memory
    # until spider-close: if the process is killed (SIGTERM/SIGKILL, OOM, pod
    # eviction), an unflushed buffer is lost entirely. 50 keeps DB round-trips
    # trivial (one execute_values per ~2min) while bounding loss-on-kill to
    # ~2min of work. close_spider still flushes the final partial batch.
    BATCH = 50

    def __init__(self):
        self._buffer: list[dict] = []
        # Yield accounting (fix-silent-zero-yield-crawls D2): cumulative
        # (attempted, inserted). Persisted on EVERY flush via report_yield so
        # a SIGKILLed pod leaves an accurate partial count — inheriting the
        # same loss-on-kill bound as the buffer itself.
        self._attempted = 0
        self._new = 0
        # 留存收尾的键：(source_used, concept_id)。累积到 close_spider 一次收尾
        # （见 db.apply_retention 的注释：T 语义在这条裸 SQL 路上从未执行过）。
        self._retention_keys: set[tuple[str, int]] = set()

    @property
    def _job_ref(self) -> str | None:
        import os
        return os.environ.get("SCRAW_JOB_REF")

    def process_item(self, item, spider):
        d = ItemAdapter(item).asdict()
        if "series" in d:
            self._explode(d, spider)
        else:
            self._buffer.append(d)
        if len(self._buffer) >= self.BATCH:
            self._flush()
        return item

    def _explode(self, d: dict, spider) -> None:
        series = d.get("series") or {}
        start, end = d.get("start") or "", d.get("end") or "9999-12-31"
        kept = dropped = 0
        for date, value in series.items():
            if not (start <= str(date) <= end):
                dropped += 1
                continue
            self._buffer.append({
                "concept_id": d["concept_id"], "entity_type": d["entity_type"],
                "entity_id": d["entity_id"], "date": str(date), "value": value,
                "unit": d.get("unit") or "", "source_used": d.get("source_used") or "",
                # the concept's granularity travels on the item so the (…, date, granularity)
                # unique key tags every exploded row (fix-observation-time-granularity)
                "granularity": d.get("granularity", "day"),
            })
            kept += 1
        spider.logger.debug(
            "series explode: concept=%s entity=%s -> %d rows (%d out-of-range dropped)",
            d.get("concept_id"), d.get("entity_id"), kept, dropped,
        )

    def close_spider(self, spider):
        self._flush()
        # 留存收尾：一次作业一次（不是每次 flush）。失败只记日志——
        # 收尾是"写完之后的清理"，不能把已经落库的爬取判成失败。
        if not self._retention_keys:
            return
        from scraw_fd_open_data_mcp.db import apply_retention

        try:
            stats = apply_retention(self._retention_keys)
            if stats.get("t_deleted") or stats.get("w_deleted") or stats.get("errors"):
                spider.logger.info("retention after crawl: %s", stats)
        except Exception:  # noqa: BLE001 - 见 db.apply_retention
            spider.logger.warning("retention after crawl failed; rows kept",
                                  exc_info=True)

    def _flush(self):
        if not self._buffer:
            return
        from scraw_fd_open_data_mcp.db import report_yield, write_observations

        # swap the buffer first so the writer holds a stable list (no aliasing)
        batch, self._buffer = self._buffer, []
        attempted, inserted = write_observations(batch)
        self._attempted += attempted
        self._new += inserted
        # 记下这批写过哪些 (源, 概念)——收尾在 close_spider 一次做完。
        for r in batch:
            cid = r.get("concept_id")
            if cid is not None:
                self._retention_keys.add((str(r.get("source_used") or ""), cid))
        # report the DELTA each flush, not the cumulative totals: the run row
        # accumulates server-side, so a mid-run pod restart (fresh pipeline,
        # zeroed counters) can never rewind the recorded yield.
        job_ref = self._job_ref
        if job_ref:
            try:
                report_yield(job_ref, attempted, inserted)
            except Exception:  # noqa: BLE001 - never let accounting break the crawl
                import logging
                logging.getLogger(__name__).warning(
                    "yield report failed (job_ref=%s): continuing crawl", job_ref,
                    exc_info=True)


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
