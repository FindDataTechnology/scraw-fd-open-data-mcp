"""Record a run's abandonment count on its ledger row (``policy_runs``).

Why this is its own module
--------------------------
The write is three lines of SQL, but it carries two disciplines that are easy to
lose when inlined into the spider's close hook:

1. **Accounting must never fail a crawl.** A missing column (a DB whose image
   predates the migration), an unreachable ledger, a typo in the URL — all of it
   degrades to one warning line. The crawl has already done its work by then.
2. **"Not reported" is not "zero".** The column is nullable on purpose: an
   executor that does not write it leaves NULL, and a reader must say "not
   reported" rather than "0 abandoned". So this function never writes 0 — it
   either adds a positive count or does nothing.

Keeping it here (rather than in ``cell_budget``) because that module is pure
per-run state with no I/O; folding a DB write into it would make the state
machine untestable without a database.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

_SQL = (
    "UPDATE policy_runs SET rows_abandoned = COALESCE(rows_abandoned, 0) + :n "
    "WHERE job_ref = :job_ref"
)


def record_abandoned(
    database_url: Optional[str],
    job_ref: Optional[str],
    count: int,
    *,
    engine=None,
) -> bool:
    """Add ``count`` to ``policy_runs.rows_abandoned`` for ``job_ref``.

    Returns True when the UPDATE ran, False when it was skipped or failed.
    Never raises: the caller is a spider's close hook, where an exception would
    turn a finished crawl into a failed Job.

    ``engine`` is injectable for tests (sqlite in-memory); production passes only
    the URL.
    """
    if not job_ref or count <= 0:
        return False
    try:
        own_engine = engine is None
        if own_engine:
            from sqlalchemy import create_engine

            if not database_url:
                return False
            engine = create_engine(database_url, pool_pre_ping=True)
        try:
            from sqlalchemy import text

            with engine.begin() as conn:
                conn.execute(text(_SQL), {"n": int(count), "job_ref": job_ref})
        finally:
            if own_engine:
                engine.dispose()
        return True
    except Exception as exc:  # noqa: BLE001 - see module docstring, point 1
        logger.warning(
            "rows_abandoned not recorded for %s (%s: %s) — the ABANDONED log line "
            "is then the only trace", job_ref, type(exc).__name__,
            str(exc).splitlines()[0][:160] if str(exc) else "",
        )
        return False
