"""Record a run's abandonment count on its ledger row (``policy_runs``).

Why this is its own module
--------------------------
The write is three lines of SQL, but it carries two disciplines that are easy to
lose when inlined into the spider's close hook:

1. **Accounting must never fail a crawl.** A missing column (a DB whose image
   predates the migration), an unreachable ledger, a typo in the URL — all of it
   degrades to one warning line. The crawl has already done its work by then.
2. **"Not reported" and "zero" are different facts — so both get written.** The
   column stays nullable on purpose: NULL means *this run's executor did not
   report* (an older image, or a write that could not happen), and a reader must
   say "not reported" rather than "0 abandoned". But the close hook always has
   the per-run summary in hand, so "zero" is an available fact and gets written
   as ``0``. Without that, the column would read the same for "old executor" and
   "new executor, nothing abandoned" — and the *common* case (a healthy run that
   abandoned nothing) would be the one nobody could read. Measured 2026-11-16:
   six scheduled rounds, all zero-abandonment, all NULL, which is exactly the
   ambiguity this avoids.

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

    ``count`` may be ``0`` (a run that abandoned nothing): the close hook knows
    the number unconditionally, so writing it is what keeps NULL meaning "did not
    report" only. A negative count is refused — it cannot come from the summary,
    so it means the caller is confused, and inventing a number there is worse
    than leaving the trace to the log line.

    Returns True when the UPDATE ran, False when it was skipped or failed.
    Never raises: the caller is a spider's close hook, where an exception would
    turn a finished crawl into a failed Job.

    ``engine`` is injectable for tests (sqlite in-memory); production passes only
    the URL.
    """
    if not job_ref or count < 0:
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
