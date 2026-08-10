"""Observation granularity on the crawler write path (fix-observation-time-granularity).

The (…, date, granularity) unique key requires every item to carry a granularity tag.
These tests pin the spider's frequency->granularity mapping and that both per-date and
series items thread the tag into the rows handed to write_observations. The DB-level
coexistence (monthly + daily rows both preserved) is covered by fd-open-data-mcp's
tests/test_observation_granularity.py against the same key.
"""
from __future__ import annotations

from unittest.mock import patch

from scraw_fd_open_data_mcp.pipelines import ObservationUpsertPipeline
from scraw_fd_open_data_mcp.spiders.concept_crawl_spider import _expand_dates, _granularity


def test_granularity_mapping():
    assert _granularity("yearly") == "year"
    assert _granularity("monthly") == "month"
    assert _granularity("daily") == "day"
    assert _granularity("weekly") == "day"
    assert _granularity(None) == "day"


def test_expand_dates_stays_canonical():
    r = {"start": "2024-01-01", "end": "2024-12-31"}
    assert _expand_dates(r, "yearly") == ["2024-12-31"]
    monthly = _expand_dates(r, "monthly")
    assert monthly[:3] == ["2024-01-01", "2024-02-01", "2024-03-01"]
    assert monthly[-1] == "2024-12-01"  # 12 months, first-of-month
    assert len(monthly) == 12
    assert _expand_dates(r, "daily")[:3] == ["2024-01-01", "2024-01-02", "2024-01-03"]
    # canonical full dates only — no bare 'YYYY'/'YYYY-MM' ever emitted
    assert all(len(d) == 10 for d in _expand_dates(r, "monthly"))


class _FakeSpider:
    import logging
    logger = logging.getLogger("test")


def test_monthly_series_explode_tags_rows_month():
    """A monthly series item explodes into rows all tagged granularity='month'."""
    pipe = ObservationUpsertPipeline()
    item = {
        "concept_id": 378, "entity_type": "fund", "entity_id": 5369,
        "unit": "CNY", "source_used": "akshare", "granularity": "month",
        "series": {"2024-06-01": 1.5, "2024-07-01": 1.6},
        "start": "2024-01-01", "end": "2024-12-31",
    }
    with patch("scraw_fd_open_data_mcp.db.write_observations") as mock_write:
        pipe.process_item(item, _FakeSpider())
        pipe.close_spider(_FakeSpider())
    rows = mock_write.call_args[0][0]
    assert len(rows) == 2
    assert all(r["granularity"] == "month" for r in rows)


def test_per_date_item_carries_granularity():
    pipe = ObservationUpsertPipeline()
    item = {"concept_id": 378, "entity_type": "fund", "entity_id": 5369,
            "date": "2024-06-01", "value": "1.5", "unit": "CNY",
            "source_used": "akshare", "granularity": "month"}
    with patch("scraw_fd_open_data_mcp.db.write_observations") as mock_write:
        pipe.process_item(item, _FakeSpider())
        pipe.close_spider(_FakeSpider())
    rows = mock_write.call_args[0][0]
    assert rows == [item]
