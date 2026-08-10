"""ConceptCrawlSpider - the crawl mode of the unified crawler.

Consumes a ``CrawlPlan`` (path via the ``plan`` spider arg), lazily expands
``(concept x entity x date)`` and yields one synthetic ``fetch://`` Scrapy Request per
tuple. The ``FetchHandler`` download handler (see ``fetch_handler.py``) calls the shared
``fd_open_data_mcp`` adapter registry + ``run_upstream`` for each, returning the extracted
value; ``parse`` turns it into an ``ObservationItem`` that the pipeline upserts into
``semantic_observations`` (crawl warms the read-cache, design D3).

The ``fetch://`` URL encodes ``(source, command, params)`` so the template-mandated
``scrapy_redis`` RFPDupeFilter dedups by the logical fetch identity (design D7) -
pause/resume and cross-pod distribution do not re-emit items.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import urllib.parse

import scrapy
from sqlalchemy import create_engine, text


class ConceptCrawlSpider(scrapy.Spider):
    name = "concept_crawl"

    def __init__(self, plan: str | None = None, **kwargs):
        super().__init__(**kwargs)
        if not plan:
            raise ValueError("ConceptCrawlSpider requires a CrawlPlan path: -a plan=path.json")
        with open(plan, "r", encoding="utf-8") as f:
            self.plan = json.load(f)

    @property
    def db_url(self) -> str:
        return self.settings["FD_OPEN_DATA_MCP_DATABASE_URL"]

    def _register_egress(self) -> None:
        """Self-register this worker's egress as a per-cluster direct proxy so the
        circuit breaker tracks bans per egress IP across the fleet
        (add-multi-cluster-master-db). No-op when SCRAW_CLUSTER_ID is unset
        (legacy single-cluster mode)."""
        raw = os.environ.get("SCRAW_CLUSTER_ID")
        if not raw:
            return
        try:
            cluster_id = int(raw)
        except ValueError:
            self.logger.warning("SCRAW_CLUSTER_ID=%r not an int; skipping egress registration", raw)
            return
        from sqlalchemy.orm import sessionmaker
        from fd_open_data_mcp.proxy.seed import register_cluster_egress
        eng = create_engine(self.db_url, connect_args={"connect_timeout": 15})
        session = sessionmaker(bind=eng)()
        try:
            action = register_cluster_egress(session, cluster_id)
            session.commit()
            self.logger.info("egress registered for cluster_id=%d (%s)", cluster_id, action)
        except Exception as e:  # noqa: BLE001 - egress registration must not break the crawl
            self.logger.warning("egress registration failed (non-fatal): %s", e)
            session.rollback()
        finally:
            session.close()

    def start_requests(self):
        self._register_egress()
        scope = self.plan["entity_scope"]
        date_range = self.plan["date_range"]
        mode = self.plan.get("mode", "per_date")
        self.logger.info("start_requests: db_url=%s concepts=%d mode=%s",
                         self.db_url, len(self.plan["wanted_concepts"]), mode)
        for pc in self.plan["wanted_concepts"]:
            # ponytail: expand dates per concept — a yearly concept over a year
            # range is ONE fetch, not 365 daily duplicates of the same statement.
            if mode == "series":
                # series mode: one fetch per (concept, entity) with NO date param —
                # the handler calls the bulk_history endpoint for the full range and
                # the pipeline explodes the returned frame (design D6).
                dates = [None]
            else:
                dates = _expand_dates(date_range, pc.get("frequency", "daily"))
            # plan carries the canonical granularity tag; fall back to frequency-derived
            # for hand-edited plans that predate the field
            gran = pc.get("granularity") or _granularity(pc.get("frequency"))
            self.logger.info("  concept=%s freq=%s granularity=%s -> %d dates",
                             pc.get("code"), pc.get("frequency"), gran, len(dates))
            for rs in pc["ranked_sources"]:
                ents = _entities(self.db_url, rs["source"], scope)
                self.logger.info("    source=%s -> %d entities", rs["source"], len(ents))
                for entity_id, identifier in ents:
                    for date in dates:
                        meta = {
                            "source": rs["source"], "command": rs["function_command"],
                            "identifier": identifier, "date": date, "column": rs["column_name"],
                            "concept_id": pc["concept_id"], "entity_type": pc["entity_type"],
                            "entity_id": entity_id, "unit": pc.get("unit") or "",
                            "granularity": gran,
                        }
                        if mode == "series":
                            # the handler/pipeline clamp to the plan's range
                            meta["start"], meta["end"] = date_range["start"], date_range["end"]
                        url = _fetch_url(rs["source"], rs["function_command"], meta)
                        yield scrapy.Request(url, callback=self.parse, meta=meta, dont_filter=False)

    def parse(self, response):
        if response.status != 200:
            return
        value = response.text
        if value == "":
            return
        if response.meta["date"] is None:
            # series mode: body is the full {'YYYY-MM-DD': value} frame as JSON —
            # hand it to the pipeline as ONE item; the pipeline explodes + clamps it.
            yield {
                "concept_id": response.meta["concept_id"],
                "entity_type": response.meta["entity_type"],
                "entity_id": response.meta["entity_id"],
                "unit": response.meta["unit"],
                "source_used": response.meta["source"],
                "granularity": response.meta.get("granularity", "day"),
                "series": json.loads(value),
                "start": response.meta.get("start"),
                "end": response.meta.get("end"),
            }
            return
        yield {
            "concept_id": response.meta["concept_id"],
            "entity_type": response.meta["entity_type"],
            "entity_id": response.meta["entity_id"],
            "date": response.meta["date"],
            "value": value,
            "unit": response.meta["unit"],
            "source_used": response.meta["source"],
            "granularity": response.meta.get("granularity", "day"),
        }


def _fetch_url(source, command, params):
    qs = urllib.parse.urlencode({k: str(v) for k, v in params.items() if v is not None})
    return f"fetch://{source}/{urllib.parse.quote(command)}?{qs}"


def _granularity(frequency: str | None) -> str:
    """Concept frequency -> observation granularity tag (day|month|year).

    The observation unique key is (concept, entity, date, granularity) — the tag
    must travel with every item so a monthly 2024-06-01 and a daily 2024-06-01 land
    in distinct rows instead of silently colliding (fix-observation-time-granularity).
    """
    if frequency == "yearly":
        return "year"
    if frequency == "monthly":
        return "month"
    return "day"  # daily/weekly/None


def _expand_dates(date_range: dict, frequency: str = "daily") -> list[str]:
    """Expand a date range to the fetch dates for a concept's cadence.

    yearly  -> one Dec-31 per year (annual statements are fetched once per year)
    monthly -> first day of each month
    daily   -> every day (default; price/volume concepts)
    """
    start = dt.date.fromisoformat(date_range["start"])
    end = dt.date.fromisoformat(date_range["end"])
    if end < start:
        start, end = end, start

    if frequency == "yearly":
        return [f"{y}-12-31" for y in range(start.year, end.year + 1)]
    if frequency == "monthly":
        out, cur = [], start.replace(day=1)
        while cur <= end:
            out.append(cur.isoformat())
            cur = (cur.replace(day=1) + dt.timedelta(days=32)).replace(day=1)
        return out
    # daily
    out, cur = [], start
    while cur <= end:
        out.append(cur.isoformat())
        cur += dt.timedelta(days=1)
    return out


def _entities(db_url: str, source: str, scope: dict):
    """Return (entity_id, identifier) pairs for entities with an identifier for `source`.

    If the scope carries explicit ``entity_ids``, filter to those (a lazy plan's explicit
    scope); otherwise expand to all entities of the type (a filter/all scope)."""
    if not db_url:
        return []
    eng = create_engine(db_url, connect_args={"connect_timeout": 15})
    ids = scope.get("entity_ids")
    with eng.connect() as c:
        if ids:
            rows = c.execute(text("""
                SELECT entity_id, identifier FROM entity_source_identifiers
                WHERE source=:s AND entity_type=:et AND entity_id = ANY(:ids)
            """), {"s": source, "et": scope["entity_type"], "ids": ids}).fetchall()
        else:
            rows = c.execute(text("""
                SELECT entity_id, identifier FROM entity_source_identifiers
                WHERE source=:s AND entity_type=:et
            """), {"s": source, "et": scope["entity_type"]}).fetchall()
    return [(r[0], r[1]) for r in rows]
