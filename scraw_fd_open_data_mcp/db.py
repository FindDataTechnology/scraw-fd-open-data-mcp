"""PG sink for scraw-fd-open-data-mcp: idempotent upsert into semantic_observations
(the mcp's canonical observation store on the remote Postgres)."""
from __future__ import annotations

import os
from typing import Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_ENGINE: Engine | None = None


def _engine() -> Engine:
    global _ENGINE
    if _ENGINE is None:
        url = os.environ.get("FD_OPEN_DATA_MCP_DATABASE_URL")
        if not url:
            raise RuntimeError("FD_OPEN_DATA_MCP_DATABASE_URL must be set to write observations")
        _ENGINE = create_engine(url, connect_args={"connect_timeout": 15}, pool_pre_ping=True)
    return _ENGINE


def write_observations(rows: Iterable[dict]) -> int:
    """Idempotent upsert of observation rows into semantic_observations.

    Each row: {concept_id, entity_type, entity_id, date, value, unit, source_used}.
    ON CONFLICT DO NOTHING keeps the existing (highest-ranked) value (spec D3 conflict policy).
    """
    rows = list(rows)
    if not rows:
        return 0
    from datetime import datetime, timezone
    from psycopg2.extras import execute_values

    now = datetime.now(timezone.utc)
    conn = _engine().raw_connection()
    cur = conn.cursor()
    data = [(r["concept_id"], r["entity_type"], r["entity_id"], r["date"], str(r["value"]),
             r.get("unit") or "", r.get("source_used") or "", now) for r in rows]
    execute_values(cur, """
        INSERT INTO semantic_observations
            (concept_id, entity_type, entity_id, date, value, unit, source_used, fetched_at)
        VALUES %s
        ON CONFLICT (concept_id, entity_type, entity_id, date) DO NOTHING
    """, data)
    conn.commit()
    cur.close()
    conn.close()
    return len(data)
