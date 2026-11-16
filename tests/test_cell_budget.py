"""Offline tests for the per-run cell-failure budget.

No DB, no Scrapy, no network: the budget module is pure state + env, and the
handler wiring is checked structurally (the skip must sit BEFORE the failover
loop — a skip after the loop would still pay for every upstream call, which is
the exact defect this module exists to remove).
"""
from __future__ import annotations

import pathlib

import pytest

from scraw_fd_open_data_mcp import cell_budget


PAIR = (384, "fund", 9831)


@pytest.fixture(autouse=True)
def _clean():
    cell_budget.reset()
    yield
    cell_budget.reset()


def test_budget_default_when_unset(monkeypatch):
    monkeypatch.delenv(cell_budget.BUDGET_ENV, raising=False)
    assert cell_budget.budget() == cell_budget.DEFAULT_BUDGET == 12


def test_budget_zero_disables(monkeypatch):
    monkeypatch.setenv(cell_budget.BUDGET_ENV, "0")
    assert cell_budget.budget() == 0
    for _ in range(100):
        assert cell_budget.note_exhausted(PAIR) is False
        assert cell_budget.is_abandoned(PAIR) is False


def test_unparsable_budget_falls_back_to_default_not_disabled(monkeypatch):
    monkeypatch.setenv(cell_budget.BUDGET_ENV, "twelve")
    assert cell_budget.budget() == cell_budget.DEFAULT_BUDGET != 0


def test_abandons_only_after_the_budget_th_streak(monkeypatch):
    monkeypatch.setenv(cell_budget.BUDGET_ENV, "12")
    for i in range(11):
        cell_budget.note_exhausted(PAIR)
        assert cell_budget.is_abandoned(PAIR) is False, f"abandoned after {i + 1} failures"
    assert cell_budget.note_exhausted(PAIR) is True  # the 12th crosses the line
    assert cell_budget.is_abandoned(PAIR) is True


def test_success_clears_the_streak(monkeypatch):
    monkeypatch.setenv(cell_budget.BUDGET_ENV, "12")
    for _ in range(11):
        cell_budget.note_exhausted(PAIR)
    cell_budget.note_success(PAIR)
    assert cell_budget.is_abandoned(PAIR) is False
    for _ in range(11):
        cell_budget.note_exhausted(PAIR)
    assert cell_budget.is_abandoned(PAIR) is False, "streak survived a success"


def test_pair_identity_is_per_entity(monkeypatch):
    monkeypatch.setenv(cell_budget.BUDGET_ENV, "2")
    other = (384, "fund", 10151)
    for _ in range(2):
        cell_budget.note_exhausted(PAIR)
    assert cell_budget.is_abandoned(PAIR) is True
    assert cell_budget.is_abandoned(other) is False


def test_snapshot_cells_have_no_pair_and_are_never_abandoned(monkeypatch):
    monkeypatch.setenv(cell_budget.BUDGET_ENV, "1")
    no_entity = (384, "fund", None)
    for _ in range(5):
        assert cell_budget.note_exhausted(no_entity) is False
    assert cell_budget.is_abandoned(no_entity) is False


def test_reset_starts_a_new_run(monkeypatch):
    monkeypatch.setenv(cell_budget.BUDGET_ENV, "2")
    for _ in range(2):
        cell_budget.note_exhausted(PAIR)
    cell_budget.note_skipped(PAIR)
    assert cell_budget.is_abandoned(PAIR) is True
    cell_budget.reset()
    assert cell_budget.is_abandoned(PAIR) is False
    assert cell_budget.summary()["cells_abandoned"] == 0


def test_summary_reports_counts_pairs_and_endpoints(monkeypatch):
    monkeypatch.setenv(cell_budget.BUDGET_ENV, "1")
    cell_budget.note_attempt(PAIR, ["fund_etf_hist_sina", "fund_etf_hist_em"])
    cell_budget.note_attempt(PAIR, ["fund_etf_hist_sina"])
    cell_budget.note_exhausted(PAIR)
    for _ in range(3):
        cell_budget.note_skipped(PAIR)
    summary = cell_budget.summary()
    assert summary["cells_abandoned"] == 3
    assert summary["pairs_abandoned"] == 1
    (row,) = summary["pairs"]
    assert row["concept_id"] == 384 and row["entity_id"] == 9831
    assert row["cells_attempted"] == 2
    assert row["endpoints_tried"] == ["fund_etf_hist_em", "fund_etf_hist_sina"]
    assert row["reason"] == "no_candidate_has_data"


def test_threshold_change_flips_the_verdict_on_the_same_sequence(monkeypatch):
    """Red-proof shape: the same 3-failure history must be abandoned at 3, not at 5."""
    monkeypatch.setenv(cell_budget.BUDGET_ENV, "3")
    for _ in range(3):
        cell_budget.note_exhausted(PAIR)
    assert cell_budget.is_abandoned(PAIR) is True
    cell_budget.reset()
    monkeypatch.setenv(cell_budget.BUDGET_ENV, "5")
    for _ in range(3):
        cell_budget.note_exhausted(PAIR)
    assert cell_budget.is_abandoned(PAIR) is False


def test_concurrent_cells_all_count_exactly_once(monkeypatch):
    """Same pair in flight on many downloader threads: no lost or double counts."""
    import threading

    monkeypatch.setenv(cell_budget.BUDGET_ENV, "50")
    reached = []

    def worker():
        if cell_budget.note_exhausted(PAIR):
            reached.append(1)
        cell_budget.note_attempt(PAIR, ["a"])

    threads = [threading.Thread(target=worker) for _ in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # `summary()["pairs"]` lists pairs that actually abandoned cells (a bounded,
    # actionable list — not every pair the run touched). One skip makes the pair
    # appear; the counters it reports must still be exactly the 50 we recorded.
    cell_budget.note_skipped(PAIR)
    (row,) = cell_budget.summary()["pairs"]
    assert row["consecutive_failures"] == 50, row
    assert row["cells_attempted"] == 50, row
    assert len(reached) == 1, "the budget boundary must fire exactly once"


def test_handler_skips_before_building_the_chain():
    """Structural: the skip must precede the failover loop in fetch_handler.

    A skip placed after the loop would still walk every endpoint first — the
    budget would then only save the *logging*, which is worse than useless
    because it looks like a fix.
    """
    src = pathlib.Path(
        "scraw_fd_open_data_mcp/fetch_handler.py"
    ).read_text(encoding="utf-8")
    skip = src.index("cell_budget.is_abandoned")
    loop = src.index("for rs in chain:")
    assert skip < loop, "abandon check must run before the failover loop"
    assert "cell_budget.note_exhausted" in src
    assert "cell_budget.note_success" in src
    # The abandoned response must be distinguishable from an ordinary empty cell.
    assert b"x-fd-abandoned" in src.encode() or "x-fd-abandoned" in src


def test_max_cells_per_pair_reports_the_largest_pair(monkeypatch):
    """The reachability signal: how many cells did the biggest pair plan?

    The budget counts *consecutive* failures of one pair, so this number decides
    whether "nothing abandoned" is a statement about the data or about the plan.
    """
    monkeypatch.setenv(cell_budget.BUDGET_ENV, "12")
    small, big = (384, "fund", 1), (384, "fund", 2)
    cell_budget.note_attempt(small, ["a"])
    for _ in range(23):
        cell_budget.note_attempt(big, ["a"])
    assert cell_budget.summary()["max_cells_per_pair"] == 23


def test_max_cells_per_pair_is_zero_for_an_empty_run():
    """A run that planned nothing must not report a phantom 1."""
    assert cell_budget.summary()["max_cells_per_pair"] == 0


def test_spider_says_when_the_budget_could_not_fire():
    """Structural: the two "nothing abandoned" worlds must not share one line.

    A run whose largest pair is smaller than the budget cannot abandon anything
    however hopeless the upstreams are; printing the plain "nothing abandoned"
    there reads as "every pair had data", which is a different claim (measured
    2026-11-16: series policies plan one cell per pair, so the ceiling is the
    retry count, well under 12).
    """
    src = pathlib.Path(
        "scraw_fd_open_data_mcp/spiders/concept_crawl_spider.py"
    ).read_text(encoding="utf-8")
    assert "could not fire" in src, "unreachable-budget wording missing"
    assert 'max_cells_per_pair' in src, "the verdict must print the measured number"
    # The ledger write must NOT be gated on a positive count any more: zero has
    # to be written, otherwise NULL keeps meaning two different things.
    write = src.index("record_abandoned(")
    gate = src.rindex("if _cells and job", 0, write)
    assert '"cells_abandoned"]' not in src[gate:write], (
        "record_abandoned must be called unconditionally (zero included)"
    )
