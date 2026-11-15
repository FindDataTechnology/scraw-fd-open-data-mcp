"""Per-run stop-loss for cells that no candidate endpoint can serve.

Why this exists
---------------
A crawl expands to ``(concept, entity, date)`` cells and, per cell, walks a
failover chain of endpoints. Two existing mechanisms already prune that walk:
``fetch.suppress`` excludes *permanently* failing paths (a missing callable —
intrinsic to the request) and ``fetch.demote`` reorders endpoints that are
unreachable *from this cluster*. Neither can express the case observed on
2026-11-15: 40 fund entities for which ``fund_etf_hist_sina`` returns a
successful fetch with no row for that entity, so the chain falls through to
``fund_etf_hist_em`` and ``fund_lof_hist_em`` — both 0% successful from that
egress — for every date. Reordering cannot help (sina was already first), and
suppression cannot help (nothing is *permanently* broken; the endpoints do
answer for other entities). The run simply re-walks the full chain for every
date of every such entity until ``activeDeadlineSeconds`` kills it:
measured 3142 + 3132 wasted fetches against 40 entities, 69% of the run.

What this module does
---------------------
Counts consecutive failed cells per ``(concept, entity)`` within one process
(one process == one crawl run) and, once the count reaches the budget, tells
the caller to stop issuing fetches for that pair. Skips are counted so the
run can say "abandoned N cells across M pairs" instead of looking clean.

Deliberately NOT:
- persisted — a new run starts at zero, so a repaired upstream recovers with
  no operator action (same discipline as demotion: make it cheap to retry,
  never a permanent ban);
- keyed on the endpoint — the same endpoint serves other entities fine, which
  is exactly why suppression is the wrong tool here;
- a silent drop — every skipped cell is counted and the reasons are kept for
  the run summary.
"""
from __future__ import annotations

import os
import threading
from typing import Optional

BUDGET_ENV = "SCRAW_CELL_FAILURE_BUDGET"
DEFAULT_BUDGET = 12

_LOCK = threading.Lock()


def budget() -> int:
    """Consecutive failed cells before a ``(concept, entity)`` pair is abandoned.

    ``0`` disables the stop-loss entirely (previous behaviour). An unparsable
    value falls back to the default rather than to "disabled" — a typo must
    not silently turn a stop-loss back into an unbounded retry loop.
    """
    raw = os.environ.get(BUDGET_ENV)
    if raw is None or raw == "":
        return DEFAULT_BUDGET
    try:
        return max(0, int(raw))
    except ValueError:
        return DEFAULT_BUDGET


class _State:
    def __init__(self) -> None:
        self.fail_streak: dict[tuple, int] = {}
        self.abandoned_cells: dict[tuple, int] = {}
        self.endpoints_seen: dict[tuple, set] = {}
        self.cells_seen: dict[tuple, int] = {}

    def reset(self) -> None:
        self.fail_streak.clear()
        self.abandoned_cells.clear()
        self.endpoints_seen.clear()
        self.cells_seen.clear()


_STATE = _State()


def _key(pair) -> Optional[tuple]:
    """Normalise a ``(concept_id, entity_type, entity_id)`` triple.

    Returns ``None`` for a cell with no entity (snapshot mode expands to
    per-entity items after the fetch, so there is no pair to attribute to);
    callers then skip the budget entirely.
    """
    if not pair:
        return None
    concept_id, entity_type, entity_id = (list(pair) + [None, None, None])[:3]
    if concept_id is None or entity_id is None:
        return None
    return (concept_id, entity_type, entity_id)


def is_abandoned(pair) -> bool:
    key = _key(pair)
    if key is None:
        return False
    limit = budget()
    if limit == 0:
        return False
    with _LOCK:
        return _STATE.fail_streak.get(key, 0) >= limit


def note_attempt(pair, sources) -> None:
    """Record which endpoints this cell actually tried (for the run summary)."""
    key = _key(pair)
    if key is None:
        return
    with _LOCK:
        _STATE.cells_seen[key] = _STATE.cells_seen.get(key, 0) + 1
        seen = _STATE.endpoints_seen.setdefault(key, set())
        for source in sources or ():
            if source:
                seen.add(str(source))


def note_success(pair) -> None:
    """A cell of this pair produced a value: clear its failure streak."""
    key = _key(pair)
    if key is None:
        return
    with _LOCK:
        _STATE.fail_streak.pop(key, None)


def note_exhausted(pair) -> bool:
    """A cell of this pair failed with no value from any candidate.

    Returns True when this failure reached the budget — i.e. the NEXT cell of
    the pair will be skipped. The cell that crosses the line is still counted
    as attempted (its fetch_log rows are real), which is why the boundary is
    ``>=`` here and the skip check is also ``>=``.
    """
    key = _key(pair)
    if key is None:
        return False
    limit = budget()
    with _LOCK:
        streak = _STATE.fail_streak.get(key, 0) + 1
        _STATE.fail_streak[key] = streak
        return limit > 0 and streak >= limit


def note_skipped(pair) -> int:
    """Count one abandoned cell; returns the pair's abandoned total."""
    key = _key(pair)
    if key is None:
        return 0
    with _LOCK:
        total = _STATE.abandoned_cells.get(key, 0) + 1
        _STATE.abandoned_cells[key] = total
        return total


def summary() -> dict:
    """Snapshot for the run report. Counts only — no per-cell rows."""
    with _LOCK:
        pairs = [
            {
                "concept_id": key[0],
                "entity_type": key[1],
                "entity_id": key[2],
                "cells_abandoned": cells,
                "cells_attempted": _STATE.cells_seen.get(key, 0),
                "consecutive_failures": _STATE.fail_streak.get(key, 0),
                "endpoints_tried": sorted(_STATE.endpoints_seen.get(key, ())),
                "reason": "no_candidate_has_data",
            }
            for key, cells in sorted(_STATE.abandoned_cells.items())
        ]
        return {
            "budget": budget(),
            "cells_abandoned": sum(_STATE.abandoned_cells.values()),
            "pairs_abandoned": len(_STATE.abandoned_cells),
            "pairs": pairs,
        }


def reset() -> None:
    """Clear all counters (tests; also the natural per-run boundary)."""
    with _LOCK:
        _STATE.reset()
