"""Multi-source coexistence on the crawler write path (add-multi-source-observations).

The observation unique key gained ``source_used``, so a second source's crawl
lands its OWN rows for a point instead of being swallowed by
first-writer-wins. These tests pin the writer's 6-column conflict target
against a real store: same source twice = one row (idempotent), a second
source = its own row.
"""
from __future__ import annotations

import sqlalchemy as sa

from scraw_fd_open_data_mcp import db as obs_db


def _row(concept_id=378, entity_id=5369, date="2024-06-01", value=1.5,
         source="akshare"):
    return {
        "concept_id": concept_id, "entity_type": "fund", "entity_id": entity_id,
        "date": date, "granularity": "day", "value": value, "unit": "CNY",
        "source_used": source,
    }


def test_writer_second_source_lands_own_row(tmp_path, monkeypatch):
    url = f"sqlite:///{tmp_path}/obs.db"
    monkeypatch.setenv("FD_OPEN_DATA_MCP_DATABASE_URL", url)
    monkeypatch.setattr(obs_db, "_ENGINE", None)  # rebuild the memoized engine

    # create the store with the current (source-aware) model
    from fd_open_data_mcp.models import Base
    eng = sa.create_engine(url)
    Base.metadata.create_all(eng)

    attempted, inserted = obs_db.write_observations([_row()])
    assert (attempted, inserted) == (1, 1)
    # same source re-crawl: idempotent no-op
    assert obs_db.write_observations([_row(value=1.6)]) == (1, 0)
    # a second source for the same point lands its own row
    assert obs_db.write_observations([_row(value=1.55, source="tencent")]) == (1, 1)

    with eng.connect() as conn:
        rows = conn.execute(sa.text(
            "SELECT source_used, value FROM semantic_observations "
            "ORDER BY source_used")).fetchall()
    assert rows == [("akshare", "1.5"), ("tencent", "1.55")]
