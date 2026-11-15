"""Offline tests for the ledger write (no Postgres, no network, no Scrapy).

The point of these arms is the *guards*, not the SQL: an accounting write that
can fail a finished crawl, or that writes 0 where the truth is "not reported",
is worse than no write at all. Each arm pins one of those.
"""
from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from scraw_fd_open_data_mcp.run_ledger import record_abandoned


@pytest.fixture()
def engine():
    eng = create_engine("sqlite://")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE policy_runs (id INTEGER PRIMARY KEY, job_ref TEXT, "
            "rows_attempted INTEGER, rows_new INTEGER, rows_abandoned INTEGER)"
        ))
        conn.execute(text(
            "INSERT INTO policy_runs (job_ref, rows_attempted, rows_new, rows_abandoned)"
            " VALUES ('cheap9/crawl-policy-1-x', 0, 0, NULL)"))
    yield eng
    eng.dispose()


def _value(engine, col="rows_abandoned"):
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT {col} FROM policy_runs")).scalar()


def test_adds_to_the_run_row(engine):
    assert record_abandoned(None, "cheap9/crawl-policy-1-x", 45, engine=engine) is True
    assert _value(engine) == 45


def test_accumulates_rather_than_overwrites(engine):
    record_abandoned(None, "cheap9/crawl-policy-1-x", 45, engine=engine)
    record_abandoned(None, "cheap9/crawl-policy-1-x", 5, engine=engine)
    assert _value(engine) == 50


def test_leaves_attempted_and_new_untouched(engine):
    record_abandoned(None, "cheap9/crawl-policy-1-x", 45, engine=engine)
    assert _value(engine, "rows_attempted") == 0
    assert _value(engine, "rows_new") == 0


def test_never_writes_zero(engine):
    """A zero would erase the "not reported" distinction the column is nullable for."""
    assert record_abandoned(None, "cheap9/crawl-policy-1-x", 0, engine=engine) is False
    assert _value(engine) is None


def test_no_job_ref_means_no_sql(engine):
    assert record_abandoned(None, None, 45, engine=engine) is False
    assert _value(engine) is None


def test_missing_column_degrades_to_false_without_raising():
    """The pre-0025 shape: policy_runs without the column. A crawl must still finish."""
    eng = create_engine("sqlite://")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE policy_runs (id INTEGER PRIMARY KEY, job_ref TEXT)"))
    try:
        assert record_abandoned(None, "cheap9/crawl-policy-1-x", 45, engine=eng) is False
    finally:
        eng.dispose()


def test_unknown_job_ref_is_a_noop_not_an_error(engine):
    assert record_abandoned(None, "no/such-job", 45, engine=engine) is True  # UPDATE ran
    assert _value(engine) is None  # but touched no row
