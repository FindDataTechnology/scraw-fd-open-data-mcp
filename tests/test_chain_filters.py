"""The chain filters apply in every fetch mode (spec concept-fetch).

Two filters run before a cell attempts anything:

  * **suppression** — a `(concept, function)` path whose recent outcomes are all
    permanent is EXCLUDED. A missing callable is missing from every egress, so
    this is cluster-independent and must run even when the cluster is unknown.
  * **demotion** — a `(cluster, function)` known blocked from this cluster's
    egress is REORDERED to the end, still probed (that is how it restores).
    Route health, so it needs the cluster.

Both used to be skipped on some paths: demotion was bypassed entirely for
snapshot-mode cells, and suppression did not exist. Each filter is exercised
here in all three modes — `snapshot`, `per_date`, `series`.
"""
from __future__ import annotations

import pytest
from scrapy.http import Request

import fd_open_data_mcp.db as db_mod
from scraw_fd_open_data_mcp.fetch_handler import FetchHandler

CONCEPT_ID = 234
FUNCTION_ID = 65

MODES = ["snapshot", "per_date", "series"]


class _Spider:
    """Minimal spider stub — the handler only uses ``spider.logger``."""

    class logger:
        @staticmethod
        def warning(*args, **kwargs):
            pass

        @staticmethod
        def info(*args, **kwargs):
            pass


class _Session:
    def close(self):
        pass


class _Database:
    def get_session(self):
        return _Session()


def _request(mode: str, ranked_sources=None):
    meta = {
        "identifier": "600519", "date": "2024-07-26",
        "concept_id": CONCEPT_ID, "entity_type": "stock", "entity_id": 1,
        "source": "akshare", "command": "stock_zh_a_hist", "column": "收盘",
        "function_id": FUNCTION_ID, "unit": "currency",
    }
    if mode == "snapshot":
        meta["snapshot"] = True
    elif mode == "series":
        meta["date"] = None            # series = one call for the whole range
        meta["start"], meta["end"] = "2024-01-01", "2024-12-31"
    if ranked_sources is not None:
        meta["ranked_sources"] = ranked_sources
    return Request("fetch://akshare/stock_zh_a_hist", meta=meta)


@pytest.fixture
def env(monkeypatch):
    """Cluster known, DB stubbed, upstream always unavailable.

    Returns mutable state so a test can seed what the filters report and then
    inspect what the chain walk actually attempted.
    """
    import fd_open_data_mcp.fetch.demote as demote_mod
    import fd_open_data_mcp.fetch.instrumentation as instr
    import fd_open_data_mcp.fetch.suppress as suppress_mod

    monkeypatch.setenv("SCRAW_CLUSTER_ID", "1")
    monkeypatch.setattr(db_mod, "get_database", lambda: _Database())

    state = {"attempted": [], "demotion_consulted": [], "suppression_queries": [],
             "suppressed": set()}

    def _unavailable(source, command, params, **kwargs):
        state["attempted"].append(command)
        raise instr.SourceUnavailable("all proxies OPEN")

    monkeypatch.setattr(instr, "instrumented_fetch", _unavailable)

    def _suppressed(session, pairs, threshold=None):
        state["suppression_queries"].append(set(pairs))
        return set(state["suppressed"])

    monkeypatch.setattr(suppress_mod, "suppressed_paths", _suppressed)

    def _reorder(session, cluster_id, chain):
        state["demotion_consulted"].append(
            (cluster_id, [c.get("function_id") for c in chain]))
        return chain, []

    monkeypatch.setattr(demote_mod, "reorder_chain", _reorder)
    return state


def _fetch(mode, ranked_sources=None):
    FetchHandler(settings=None, crawler=None).download_request(
        _request(mode, ranked_sources), _Spider())


# --- both filters are consulted, per mode ------------------------------------

@pytest.mark.parametrize("mode", MODES)
def test_suppression_is_consulted_in_every_mode(env, mode):
    _fetch(mode)
    assert env["suppression_queries"] == [{(CONCEPT_ID, FUNCTION_ID)}]


@pytest.mark.parametrize("mode", MODES)
def test_demotion_is_consulted_in_every_mode(env, mode):
    _fetch(mode)
    assert env["demotion_consulted"] == [(1, [FUNCTION_ID])]


# --- suppression excludes, per mode ------------------------------------------

@pytest.mark.parametrize("mode", MODES)
def test_a_suppressed_path_is_not_attempted(env, mode):
    env["suppressed"] = {(CONCEPT_ID, FUNCTION_ID)}
    _fetch(mode)
    assert env["attempted"] == []


@pytest.mark.parametrize("mode", MODES)
def test_a_suppressed_path_gets_no_calls_across_cells(env, mode):
    """The point of suppressing across a run: the path is not re-derived and
    re-attempted for every cell."""
    env["suppressed"] = {(CONCEPT_ID, FUNCTION_ID)}
    for _ in range(5):
        _fetch(mode)
    assert env["attempted"] == []
    assert len(env["suppression_queries"]) == 5   # consulted per cell, cheaply


def test_suppression_applies_even_without_a_cluster(env, monkeypatch):
    """Suppression is not route health: a missing callable is missing from every
    egress, so it must not depend on knowing which cluster this is."""
    monkeypatch.delenv("SCRAW_CLUSTER_ID", raising=False)
    env["suppressed"] = {(CONCEPT_ID, FUNCTION_ID)}

    _fetch("snapshot")

    assert env["attempted"] == []                  # still excluded
    assert env["demotion_consulted"] == []         # demotion alone needs the cluster


# --- demotion reorders, and only reorders ------------------------------------

def test_a_demoted_candidate_moves_to_the_end_of_the_chain(env, monkeypatch):
    """Reordering must take effect, not merely be computed: the reachable
    alternative is attempted first."""
    import fd_open_data_mcp.fetch.demote as demote_mod

    def _demote_second(session, cluster_id, chain):
        return ([c for c in chain if c["function_id"] == 2],
                [c for c in chain if c["function_id"] != 2])

    monkeypatch.setattr(demote_mod, "reorder_chain", _demote_second)

    _fetch("snapshot", ranked_sources=[
        {"source": "akshare", "command": "blocked_one", "function_id": 1,
         "column_name": "收盘"},
        {"source": "akshare", "command": "reachable_one", "function_id": 2,
         "column_name": "收盘"},
    ])

    assert env["attempted"][0] == "reachable_one"


def test_a_demoted_path_is_still_attempted(env, monkeypatch):
    """Demotion reorders and never removes — a probe is how it restores."""
    import fd_open_data_mcp.fetch.demote as demote_mod

    monkeypatch.setattr(demote_mod, "reorder_chain",
                        lambda s, c, chain: ([], chain))

    _fetch("per_date", ranked_sources=[
        {"source": "akshare", "command": "blocked_one", "function_id": 1,
         "column_name": "收盘"},
    ])

    assert env["attempted"] == ["blocked_one"]


# --- neither filter may break a fetch ----------------------------------------

def test_a_failing_demotion_lookup_never_breaks_the_fetch(env, monkeypatch):
    import fd_open_data_mcp.fetch.demote as demote_mod

    def _boom(session, cluster_id, chain):
        raise RuntimeError("redis down")

    monkeypatch.setattr(demote_mod, "reorder_chain", _boom)

    _fetch("snapshot")

    assert env["attempted"] == ["stock_zh_a_hist"]


def test_a_failing_suppression_lookup_never_breaks_the_fetch(env, monkeypatch):
    import fd_open_data_mcp.fetch.suppress as suppress_mod

    def _boom(session, pairs, threshold=None):
        raise RuntimeError("db down")

    monkeypatch.setattr(suppress_mod, "suppressed_paths", _boom)

    _fetch("series")

    assert env["attempted"] == ["stock_zh_a_hist"]
