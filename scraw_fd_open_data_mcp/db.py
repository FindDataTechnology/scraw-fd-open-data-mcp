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


def write_observations(rows: Iterable[dict]) -> tuple[int, int]:
    """Idempotent upsert of observation rows into semantic_observations.

    Each row: {concept_id, entity_type, entity_id, date, granularity, value, unit, source_used}.
    ON CONFLICT DO NOTHING keeps the existing value (first-writer-wins). The
    (…, date, granularity) key makes monthly and daily observations of the same
    period DISTINCT rows, so first-writer-wins no longer silently drops a cadence
    (fix-observation-time-granularity).

    Returns ``(attempted, inserted)``: rows handed to the upsert, and rows that
    actually landed (excluding ON CONFLICT no-ops) — the two counters yield
    accounting needs (design D2). ``rows_attempted == 0`` with a non-empty plan
    is the outage signal; ``rows_new == 0`` with ``rows_attempted > 0`` is the
    frozen-window signal.
    """
    rows = list(rows)
    if not rows:
        return (0, 0)
    from datetime import datetime, timezone
    from psycopg2.extras import execute_values

    now = datetime.now(timezone.utc)
    conn = _engine().raw_connection()
    cur = conn.cursor()
    data = [(r["concept_id"], r["entity_type"], r["entity_id"], r["date"],
             r.get("granularity") or "day", str(r["value"]),
             r.get("unit") or "", r.get("source_used") or "", now) for r in rows]
    # execute_values(fetch=True) returns the RETURNING rows as its RETURN
    # VALUE — they are NOT left on the cursor for a later fetchall(). Reading
    # the cursor (the old code) always yielded 0, so every Scrapy-path run
    # reported rows_new=0 while data landed (found live by expand-crawl-coverage
    # wave 2: 53k observations written, counter stuck at 0).
    inserted = len(execute_values(cur, """
        INSERT INTO semantic_observations
            (concept_id, entity_type, entity_id, date, granularity, value, unit, source_used, fetched_at)
        VALUES %s
        ON CONFLICT (concept_id, entity_type, entity_id, date, granularity) DO NOTHING
        RETURNING 1
    """, data, fetch=True))
    conn.commit()
    cur.close()
    conn.close()
    return (len(data), inserted)


def report_yield(job_ref: str, attempted_delta: int, new_delta: int) -> None:
    """Incrementally add to a run's yield counters (fix-silent-zero-yield-crawls D2).

    Called by the pipeline on EVERY flush (not only spider close) so a pod that
    is SIGKILLed leaves an accurate partial count — a run recorded as zero
    purely because the pod died would be indistinguishable from the outage
    this change exists to detect. Idempotent-safe under pod restarts? No —
    the same batch is never flushed twice (the buffer is swapped first), and
    k8s restarts the whole pod (fresh pipeline), so no double counting.
    """
    if not job_ref or (attempted_delta <= 0 and new_delta <= 0):
        return
    conn = _engine().raw_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE policy_runs
        SET rows_attempted = COALESCE(rows_attempted, 0) + %s,
            rows_new       = COALESCE(rows_new, 0) + %s
        WHERE job_ref = %s
    """, (attempted_delta, new_delta, job_ref))
    conn.commit()
    cur.close()
    conn.close()
