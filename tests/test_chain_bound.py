"""The executor honours the chain bound (spec concept-fetch).

The planner bounds a concept's chain, and the handler bounds it again — a
hand-edited or pre-change plan must not be able to make one cell issue
unbounded upstream calls. Concept `price.close`/`stock` carried 117 candidates,
and each candidate costs up to `max_proxies x per-proxy retries` calls, so the
per-cell ceiling is `bound x 6` at the default retry budget.

Unlike the demotion tests, `instrumented_fetch` is NOT stubbed here: the real
retry loop runs so the call ceiling is measured rather than asserted from
arithmetic. It degrades to the direct sentinel (no FD_PROXY_FORWARDER), and its
`fetch_log` write is skipped when the DB is unusable, which is fine — the
fetch itself never depends on either.
"""
from __future__ import annotations

import pytest
from scrapy.http import Request

from scraw_fd_open_data_mcp.fetch_handler import FetchHandler

# max_proxies (3) x attempts per proxy (2) — the retry budget per candidate.
CALLS_PER_CANDIDATE = 6


class _Spider:
    class logger:
        @staticmethod
        def warning(*args, **kwargs):
            pass

        @staticmethod
        def info(*args, **kwargs):
            pass


def _chain(n: int) -> list[dict]:
    return [
        {"source": "akshare", "command": f"stock_zh_a_hist_{i}", "function_id": 100 + i,
         "column_name": "收盘", "score": 0.9, "binding_id": 200 + i, "confidence": 0.9}
        for i in range(n)
    ]


def _request(chain):
    return Request("fetch://akshare/stock_zh_a_hist", meta={
        "identifier": "600519", "date": "2024-07-26",
        "concept_id": 234, "entity_type": "stock", "entity_id": 1,
        "source": "akshare", "command": "stock_zh_a_hist", "column": "收盘",
        "function_id": 65, "unit": "currency",
        "ranked_sources": chain,
    })


@pytest.fixture
def attempted(monkeypatch, tmp_path):
    """Real instrumented_fetch; a counting run_upstream that always fails."""
    import fd_open_data_mcp.fetch.instrumentation as instr
    from fd_open_data_mcp.fetch.runner import FetchError

    monkeypatch.setenv("FD_OPEN_DATA_MCP_DATABASE_URL", f"sqlite:///{tmp_path}/t.db")
    monkeypatch.delenv("FD_PROXY_FORWARDER", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)

    calls: list[str] = []

    def _failing(source, command, params):
        calls.append(command)
        raise FetchError("RemoteDisconnected('Remote end closed connection')")

    monkeypatch.setattr(instr, "run_upstream", _failing)

    # Suppression is exercised by its own tests; neutralise it here so these
    # cases measure the bound alone.
    import fd_open_data_mcp.fetch.suppress as suppress_mod
    monkeypatch.setattr(suppress_mod, "suppressed_paths",
                        lambda session, pairs, threshold=None: set())
    return calls


def test_a_long_chain_is_bounded_by_the_handler(attempted):
    """117 candidates in, at most the bound attempted."""
    from fd_open_data_mcp.crawl.planner import chain_bound

    bound = chain_bound()
    FetchHandler(settings=None, crawler=None).download_request(
        _request(_chain(117)), _Spider())

    candidates_tried = list(dict.fromkeys(attempted))   # order-preserving unique
    assert len(candidates_tried) == bound
    assert candidates_tried == [f"stock_zh_a_hist_{i}" for i in range(bound)]


def test_the_per_cell_call_count_is_bounded(attempted):
    """bound x retry budget, measured — not derived."""
    from fd_open_data_mcp.crawl.planner import chain_bound

    bound = chain_bound()
    FetchHandler(settings=None, crawler=None).download_request(
        _request(_chain(117)), _Spider())

    assert len(attempted) <= bound * CALLS_PER_CANDIDATE
    # every candidate really was retried, so the ceiling is exercised
    assert len(attempted) == bound * CALLS_PER_CANDIDATE


def test_a_chain_within_the_bound_is_untouched(attempted):
    FetchHandler(settings=None, crawler=None).download_request(
        _request(_chain(3)), _Spider())

    assert list(dict.fromkeys(attempted)) == [
        "stock_zh_a_hist_0", "stock_zh_a_hist_1", "stock_zh_a_hist_2"]


def test_the_bound_is_configurable(attempted, monkeypatch):
    monkeypatch.setenv("FD_CRAWL_CHAIN_MAX", "2")
    FetchHandler(settings=None, crawler=None).download_request(
        _request(_chain(50)), _Spider())

    assert list(dict.fromkeys(attempted)) == [
        "stock_zh_a_hist_0", "stock_zh_a_hist_1"]


def test_a_malformed_bound_falls_back_to_the_default(attempted, monkeypatch):
    """A bad env value must not disable the cap."""
    from fd_open_data_mcp.crawl.planner import chain_bound

    monkeypatch.setenv("FD_CRAWL_CHAIN_MAX", "lots")
    FetchHandler(settings=None, crawler=None).download_request(
        _request(_chain(50)), _Spider())

    assert len(list(dict.fromkeys(attempted))) == chain_bound()
