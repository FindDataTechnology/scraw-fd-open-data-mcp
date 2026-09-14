"""Observation sink for scraw-fd-open-data-mcp: idempotent upsert into
semantic_observations (the mcp's canonical observation store).

Targets the remote Postgres in production and an explicit SQLite file for the
local supervised trial (schedule-activation design D3); the SQL is chosen by the
URL's dialect.
"""
from __future__ import annotations

import os
from typing import Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_ENGINE: Engine | None = None

# Column order shared by both dialect paths (write_observations builds tuples).
_COLS = ("concept_id", "entity_type", "entity_id", "date", "granularity",
         "value", "unit", "source_used", "fetched_at")


def _engine() -> Engine:
    global _ENGINE
    if _ENGINE is None:
        url = os.environ.get("FD_OPEN_DATA_MCP_DATABASE_URL")
        if not url:
            raise RuntimeError("FD_OPEN_DATA_MCP_DATABASE_URL must be set to write observations")
        # connect_timeout is a psycopg2 connect arg; sqlite (the trial target)
        # rejects unknown kwargs.
        connect_args = {"connect_timeout": 15} if url.startswith("postgres") else {}
        _ENGINE = create_engine(url, connect_args=connect_args, pool_pre_ping=True)
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

    now = datetime.now(timezone.utc)
    eng = _engine()
    data = [(r["concept_id"], r["entity_type"], r["entity_id"], r["date"],
             r.get("granularity") or "day", str(r["value"]),
             r.get("unit") or "", r.get("source_used") or "", now) for r in rows]

    if eng.dialect.name == "sqlite":
        return _write_observations_sqlite(eng, data)

    from psycopg2.extras import execute_values

    conn = eng.raw_connection()
    cur = conn.cursor()
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


def _write_observations_sqlite(eng: Engine, data: list[tuple]) -> tuple[int, int]:
    """SQLite upsert path for the local supervised trial (design D3).

    Same semantics as the Postgres path — ON CONFLICT DO NOTHING on the 5-column
    key, first-writer-wins — expressed with the sqlite dialect's conflict clause.
    Landed rows are counted as the table size delta, which sidesteps the
    executemany/RETURNING interaction.
    ponytail: the COUNT(*) scans are O(n) per flush; fine for a trial-sized
    store, switch to RETURNING if a sqlite target ever grows.
    """
    from sqlalchemy import func, select
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert
    from fd_open_data_mcp.models import SemanticObservation

    tbl = SemanticObservation.__table__
    stmt = sqlite_insert(tbl).on_conflict_do_nothing(
        index_elements=["concept_id", "entity_type", "entity_id", "date", "granularity"])
    with eng.begin() as conn:
        before = conn.execute(select(func.count()).select_from(tbl)).scalar_one()
        conn.execute(stmt, [dict(zip(_COLS, row)) for row in data])
        after = conn.execute(select(func.count()).select_from(tbl)).scalar_one()
    return (len(data), after - before)


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
    # Named params so the statement compiles for either dialect (the previous
    # pyformat %s only worked against psycopg2).
    with _engine().begin() as conn:
        conn.execute(text("""
            UPDATE policy_runs
            SET rows_attempted = COALESCE(rows_attempted, 0) + :attempted,
                rows_new       = COALESCE(rows_new, 0) + :new
            WHERE job_ref = :job_ref
        """), {"attempted": attempted_delta, "new": new_delta, "job_ref": job_ref})
