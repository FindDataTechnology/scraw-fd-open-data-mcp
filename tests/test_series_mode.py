"""Series-mode tests (add-fund-crawl-control-center, design D6).

Covers the executor-side series contract:
- spider: one fetch:// request per (concept, entity), no date param, start/end in meta
- fetch handler: no-date request -> build_range_params + extract_series -> JSON body
- pipeline: series item explodes into per-date rows, clamped to [start, end]
"""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest


def _plan(mode="series"):
    return {
        "version": "1",
        "mode": mode,
        "wanted_concepts": [{
            "concept_id": 378, "code": "nav.unit", "entity_type": "fund",
            "unit": "CNY", "frequency": "daily",
            "ranked_sources": [{
                "source": "akshare", "score": 0.9, "function_id": 489,
                "function_command": "fund_open_fund_info_em", "column_name": "单位净值",
                "binding_id": 1066, "confidence": 1.0,
            }],
        }],
        "entity_scope": {"entity_type": "fund", "entity_ids": [5369], "filter": None},
        "date_range": {"start": "2024-01-01", "end": "2024-12-31", "frequency": "daily"},
        "unroutable": [], "unmapped": [],
        "persistence": {"table": "semantic_observations"},
    }


def _make_spider(tmp_path, plan=None):
    from scraw_fd_open_data_mcp.spiders.concept_crawl_spider import ConceptCrawlSpider

    p = tmp_path / "plan.json"
    p.write_text(json.dumps(plan or _plan()))
    sp = ConceptCrawlSpider(plan=str(p))
    # spider reads settings only via db_url property; patch it
    from scrapy.settings import Settings
    sp.settings = Settings({"FD_OPEN_DATA_MCP_DATABASE_URL": "sqlite:///"})
    return sp


# ─── spider: request emission ────────────────────────────────────────────────
def test_series_mode_emits_one_request_per_concept_entity(tmp_path):
    sp = _make_spider(tmp_path)
    with patch("scraw_fd_open_data_mcp.spiders.concept_crawl_spider._entities",
               return_value=[(5369, "110011"), (5370, "110022")]):
        reqs = list(sp.start_requests())
    # 1 concept x 2 entities x ONE fetch (no date expansion)
    assert len(reqs) == 2
    for r in reqs:
        assert r.meta["date"] is None
        assert r.meta["start"] == "2024-01-01"
        assert r.meta["end"] == "2024-12-31"
        assert "date=" not in r.url  # no date param in the fetch:// URL


def test_per_date_mode_still_expands_dates(tmp_path):
    sp = _make_spider(tmp_path, plan=_plan(mode="per_date"))
    with patch("scraw_fd_open_data_mcp.spiders.concept_crawl_spider._entities",
               return_value=[(5369, "110011")]):
        reqs = list(sp.start_requests())
    # daily 2024-01-01..2024-12-31 -> 366 (leap year) requests, each with a date
    assert len(reqs) == 366
    assert all(r.meta["date"] is not None for r in reqs)
    assert "start" not in reqs[0].meta


# ─── spider: parse shapes ────────────────────────────────────────────────────
class _FakeResponse:
    def __init__(self, meta, body, status=200):
        self.meta, self.text, self.status = meta, body, status


def test_parse_series_item(tmp_path):
    sp = _make_spider(tmp_path)
    meta = {"concept_id": 378, "entity_type": "fund", "entity_id": 5369,
            "unit": "CNY", "source": "akshare", "date": None,
            "start": "2024-01-01", "end": "2024-12-31"}
    body = json.dumps({"2024-01-02": 1.5, "2024-01-03": 1.6})
    items = list(sp.parse(_FakeResponse(meta, body)))
    assert len(items) == 1
    it = items[0]
    assert it["series"] == {"2024-01-02": 1.5, "2024-01-03": 1.6}
    assert it["start"] == "2024-01-01" and it["end"] == "2024-12-31"
    assert "date" not in it


def test_parse_per_date_item(tmp_path):
    sp = _make_spider(tmp_path)
    meta = {"concept_id": 378, "entity_type": "fund", "entity_id": 5369,
            "unit": "CNY", "source": "akshare", "date": "2024-01-02"}
    items = list(sp.parse(_FakeResponse(meta, "1.5")))
    assert items == [{"concept_id": 378, "entity_type": "fund", "entity_id": 5369,
                      "date": "2024-01-02", "value": "1.5", "unit": "CNY",
                      "source_used": "akshare",
                      "granularity": "day"}]  # default tag when meta carries none


# ─── pipeline: explode + clamp + upsert ──────────────────────────────────────
class _FakeSpider:
    import logging
    logger = logging.getLogger("test")


def test_pipeline_explodes_and_clamps():
    from scraw_fd_open_data_mcp.pipelines import ObservationUpsertPipeline

    pipe = ObservationUpsertPipeline()
    item = {
        "concept_id": 378, "entity_type": "fund", "entity_id": 5369,
        "unit": "CNY", "source_used": "akshare",
        "series": {"2023-12-29": 9.9,            # out of range -> dropped
                   "2024-01-02": 1.5,
                   "2024-12-31": 1.6,
                   "2025-01-02": 9.8},           # out of range -> dropped
        "start": "2024-01-01", "end": "2024-12-31",
    }
    with patch("scraw_fd_open_data_mcp.db.write_observations") as mock_write:
        pipe.process_item(item, _FakeSpider())
        pipe.close_spider(_FakeSpider())
    rows = mock_write.call_args[0][0]
    assert [r["date"] for r in rows] == ["2024-01-02", "2024-12-31"]
    assert all(r["concept_id"] == 378 and r["entity_id"] == 5369 for r in rows)
    assert all(r["source_used"] == "akshare" for r in rows)
    assert all(r["granularity"] == "day" for r in rows)  # default when item carries none


def test_pipeline_per_date_passthrough():
    from scraw_fd_open_data_mcp.pipelines import ObservationUpsertPipeline

    pipe = ObservationUpsertPipeline()
    item = {"concept_id": 378, "entity_type": "fund", "entity_id": 5369,
            "date": "2024-01-02", "value": "1.5", "unit": "CNY", "source_used": "akshare"}
    with patch("scraw_fd_open_data_mcp.db.write_observations") as mock_write:
        pipe.process_item(item, _FakeSpider())
        pipe.close_spider(_FakeSpider())
    rows = mock_write.call_args[0][0]
    assert rows == [item]


# ─── fetch handler: series path ──────────────────────────────────────────────
class _SeriesAdapter:
    def build_range_params(self, fn, identifier, start, end, binding=None):
        return {"symbol": identifier, "start_date": start, "end_date": end}

    def extract_series(self, result, column_name, start, end):
        return {"2024-01-02": 1.5, "2024-01-03": 1.6}


class _FakeScrapyRequest:
    def __init__(self, meta, url="fetch://akshare/fund_open_fund_info_em?symbol=110011"):
        self.meta, self.url = meta, url


def test_handler_series_returns_json_body():
    from scraw_fd_open_data_mcp.fetch_handler import FetchHandler

    meta = {"source": "akshare", "command": "fund_open_fund_info_em",
            "identifier": "110011", "date": None, "column": "单位净值",
            "concept_id": 378, "entity_type": "fund", "entity_id": 5369,
            "start": "2024-01-01", "end": "2024-12-31", "unit": "CNY"}
    req = _FakeScrapyRequest(meta)

    with patch("fd_open_data_mcp.adapters.adapter_for", return_value=_SeriesAdapter()), \
         patch("fd_open_data_mcp.fetch.instrumentation.instrumented_fetch", return_value=object()):
        handler = FetchHandler.__new__(FetchHandler)
        resp = handler.download_request(req, _FakeSpider())
    assert resp.status == 200
    assert json.loads(resp.text) == {"2024-01-02": 1.5, "2024-01-03": 1.6}
