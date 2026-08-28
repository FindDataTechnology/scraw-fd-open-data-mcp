"""Yield-accounting tests for the upsert pipeline (fix-silent-zero-yield-crawls D2).

Proves: the pipeline accumulates (attempted, inserted) across flushes and
reports the DELTA on EVERY flush (not only spider close), keyed by
SCRAW_JOB_REF — so a pod killed mid-run leaves an accurate partial count.
"""
from __future__ import annotations

from unittest.mock import patch

from scraw_fd_open_data_mcp.pipelines import ObservationUpsertPipeline


class _FakeSpider:
    """Minimal spider stub (the pipeline only logs through it)."""
    logger = __import__("logging").getLogger("fake")


def _items(n, start=0):
    return [{"concept_id": 1, "entity_type": "stock", "entity_id": i,
             "date": "2026-08-27", "value": i, "unit": "", "source_used": "s"}
            for i in range(start, start + n)]


def test_counters_reported_per_flush_as_deltas(monkeypatch):
    monkeypatch.setenv("SCRAW_JOB_REF", "aliyun/crawl-policy-1-1")
    reports: list[tuple[int, int]] = []
    monkeypatch.setattr(
        "scraw_fd_open_data_mcp.db.report_yield",
        lambda ref, a, n: reports.append((ref, a, n)))

    pipe = ObservationUpsertPipeline()
    with patch("scraw_fd_open_data_mcp.db.write_observations",
               side_effect=lambda rows: (len(rows), 0)):
        for item in _items(60):  # crosses BATCH=50 -> two flushes
            pipe.process_item(item, _FakeSpider())
        pipe.close_spider(_FakeSpider())

    # one report per flush: 50 then 10 — deltas, never cumulative totals
    assert reports == [("aliyun/crawl-policy-1-1", 50, 0),
                       ("aliyun/crawl-policy-1-1", 10, 0)]
    assert pipe._attempted == 60 and pipe._new == 0


def test_new_rows_accumulate(monkeypatch):
    monkeypatch.setenv("SCRAW_JOB_REF", "tencent/crawl-policy-2-1")
    reports: list[tuple[int, int]] = []
    monkeypatch.setattr(
        "scraw_fd_open_data_mcp.db.report_yield",
        lambda ref, a, n: reports.append((a, n)))

    pipe = ObservationUpsertPipeline()
    with patch("scraw_fd_open_data_mcp.db.write_observations",
               side_effect=lambda rows: (len(rows), len(rows) - 40)):
        for item in _items(50):
            pipe.process_item(item, _FakeSpider())
        pipe.close_spider(_FakeSpider())
    assert reports == [(50, 10)]
    assert (pipe._attempted, pipe._new) == (50, 10)


def test_failed_yield_report_does_not_break_crawl(monkeypatch):
    monkeypatch.setenv("SCRAW_JOB_REF", "x/y")

    def boom(ref, a, n):
        raise RuntimeError("policy_runs unreachable")

    monkeypatch.setattr("scraw_fd_open_data_mcp.db.report_yield", boom)
    pipe = ObservationUpsertPipeline()
    with patch("scraw_fd_open_data_mcp.db.write_observations",
               side_effect=lambda rows: (len(rows), len(rows))):
        for item in _items(5):
            pipe.process_item(item, _FakeSpider())  # must not raise
        pipe.close_spider(_FakeSpider())
    assert pipe._attempted == 5


def test_no_job_ref_still_counts_locally(monkeypatch):
    monkeypatch.delenv("SCRAW_JOB_REF", raising=False)
    called = []
    monkeypatch.setattr(
        "scraw_fd_open_data_mcp.db.report_yield",
        lambda ref, a, n: called.append(ref))
    pipe = ObservationUpsertPipeline()
    with patch("scraw_fd_open_data_mcp.db.write_observations",
               side_effect=lambda rows: (len(rows), 1)):
        for item in _items(5):
            pipe.process_item(item, _FakeSpider())
        pipe.close_spider(_FakeSpider())
    assert called == []  # no ref -> report skipped, crawl unaffected
    assert pipe._attempted == 5 and pipe._new == 1
