"""Custom Scrapy download handler for ``fetch://`` URLs.

The concept crawl fetches via Python libs (akshare/wbgapi/...), not HTTP. A
``fetch://`` Request encodes ``(source, command, params)``; this handler calls
the shared ``fd_open_data_mcp`` adapter registry + ``instrumented_fetch`` (the
shared fetch-instrumentation chokepoint) and returns the extracted value as a
``TextResponse`` body.

Routing through ``instrumented_fetch`` gives the scraw crawler - for free -
per-fetch proxy selection, ban classification, circuit-breaker recording, and
``fetch_log`` writes (with ``proxy_id`` + ``classification``). This closes the
gap where the executor bypassed ``concept-fetch`` dispatch and never fed
reliability tracking (spec concept-crawl-executor: "Fetch via the shared
adapter registry" + add-source-proxy-health).

Source-level failover: if the request meta carries a ``ranked_sources`` chain
(list of ``{source, command, column_name}``), the handler iterates it on
``SourceUnavailable`` (every proxy for a source is OPEN) and tries the next
source. Otherwise it uses the single ``(source, command, column)`` from meta.

Request meta carries: source, command, identifier, date, column, concept_id,
entity_type, entity_id, unit, [ranked_sources]. Returns 200 + value as body
(204 if no value, 500 on fetch error, 503 if every source is unavailable).

Series mode (design D6): when meta ``date`` is None, meta also carries
``start``/``end`` (the plan's range) and the handler calls the adapter's
``build_range_params`` + ``extract_series`` — one bulk_history fetch for the
whole range — returning the ``{'YYYY-MM-DD': value}`` dict as a JSON body.
The spider's parse + pipeline explode it into per-date observations.
"""
from __future__ import annotations

import json
import os

from scrapy.http import TextResponse


class _FnStub:
    def __init__(self, command: str):
        self.command = command
        self.parameters = []


class _BindingStub:
    """Mimics a ConceptBinding: `.column` is a str-like with `.name`.

    Adapters access the column two ways: `binding.column.name` (akshare) and
    `binding.column.lower()` (cn-report PyPI 0.4.0). A plain str has .lower() but
    not .name; a Column-like needs both. Use a str subclass that exposes .name.
    """
    class _Column(str):
        @property
        def name(self):
            return str(self)

    def __init__(self, column: str):
        self.column = self._Column(column)


def _legacy_extract(result, column_name: str, date: str):
    try:
        import pandas as pd
    except ImportError:
        return None
    if not isinstance(result, pd.DataFrame) or column_name not in result.columns:
        return None
    df = result
    date_col = next((c for c in df.columns if str(c).lower() in ("date", "日期", "datetime", "时间")), None)
    if date_col is not None:
        df = df.set_index(date_col)
    idx_str = [str(i) for i in df.index]
    if date in idx_str:
        return df.iloc[idx_str.index(date)][column_name]
    return None


def _build_params(adapter, command, identifier, date, column):
    if adapter:
        return adapter.build_params(_FnStub(command), identifier, date, _BindingStub(column))
    return {"symbol": identifier, "date": date}


def _extract(adapter, result, column, date, source, command, identifier=None):
    if adapter:
        return adapter.extract_value(result, column, date, identifier=identifier)
    return _legacy_extract(result, column, date)


def _build_series_params(adapter, command, identifier, start, end, column):
    """Range form of ``_build_params`` for series mode (bulk_history endpoints)."""
    if adapter and hasattr(adapter, "build_range_params"):
        return adapter.build_range_params(_FnStub(command), identifier, start, end, _BindingStub(column))
    if adapter:
        return adapter.build_params(_FnStub(command), identifier, start, _BindingStub(column))
    return {"symbol": identifier}


def _extract_series(adapter, result, column, start, end):
    """``{'YYYY-MM-DD': value}`` for the whole frame, or None if unsupported."""
    if adapter and hasattr(adapter, "extract_series"):
        return adapter.extract_series(result, column, start, end)
    return None


def _build_snapshot_params(adapter, command, date, column):
    """Params for a bulk_snapshot call: no identifier — one call covers every
    entity (fix-silent-zero-yield-crawls D6). Rank-frame adapters' build_params
    ignores the identifier anyway; passing None keeps them on their {} shape."""
    if adapter:
        return adapter.build_params(_FnStub(command), None, date, _BindingStub(column))
    return {"date": date} if date else {}


def _extract_snapshot(adapter, result, column, date):
    """``{identifier: value}`` for the full entity cross-section, or None when
    the adapter cannot serve snapshots (no key column / no support)."""
    if adapter and hasattr(adapter, "extract_snapshot"):
        return adapter.extract_snapshot(result, column, date)
    return None


class FetchHandler:
    """Download handler for ``fetch://`` requests -> instrumented adapter fetch."""

    lazy = False

    def __init__(self, settings, crawler):
        self.settings = settings
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings, crawler)

    def close(self):
        pass

    def download_request(self, request, spider):
        from fd_open_data_mcp.adapters import adapter_for
        from fd_open_data_mcp.fetch.instrumentation import (
            FetchError, SourceUnavailable, instrumented_fetch,
        )

        m = request.meta
        identifier, date = m["identifier"], m["date"]
        concept_id = m.get("concept_id")
        entity_type = m.get("entity_type")
        entity_id = m.get("entity_id")
        series_mode = date is None
        snapshot_mode = bool(m.get("snapshot"))
        start, end = m.get("start"), m.get("end")

        # The source chain to try: explicit ranked_sources in meta, else the single source.
        chain = m.get("ranked_sources") or [
            {"source": m["source"], "command": m["command"], "column_name": m["column"],
             "function_id": m.get("function_id")}
        ]

        # Per-(cluster, function) demotion (fix-silent-zero-yield-crawls D5):
        # entries whose endpoint is known-blocked from THIS cluster's egress
        # move to the end of the chain — still probed (that's how they
        # restore), never preferred over a reachable alternative. Keyed on
        # function, never on source: one source spans reachable and blocked
        # hosts simultaneously.
        cluster_id = None
        raw_cluster = os.environ.get("SCRAW_CLUSTER_ID")
        if raw_cluster:
            try:
                cluster_id = int(raw_cluster)
            except ValueError:
                cluster_id = None
        if cluster_id is not None and not snapshot_mode:
            from fd_open_data_mcp.db import get_database
            from fd_open_data_mcp.fetch.demote import reorder_chain
            s = get_database().get_session()
            try:
                healthy, demoted = reorder_chain(s, cluster_id, chain)
                chain[:] = healthy + demoted
            except Exception:  # noqa: BLE001 - demotion must never break a fetch
                spider.logger.warning("demotion lookup failed; using configured rank",
                                      exc_info=True)
            finally:
                s.close()

        last_err = None
        for rs in chain:
            source, command, column = rs["source"], rs["command"], rs.get("column_name") or m["column"]
            function_id = rs.get("function_id") or m.get("function_id")
            adapter = adapter_for(source, command)
            if snapshot_mode:
                # D6: one call returns the full entity cross-section for this
                # date — no identifier param; the returned frame is filtered to
                # the requested entity scope by the caller (spider parse).
                params = _build_snapshot_params(adapter, command, date, column)
            else:
                params = (_build_series_params(adapter, command, identifier, start, end, column)
                          if series_mode else
                          _build_params(adapter, command, identifier, date, column))
            try:
                result = instrumented_fetch(
                    source, command, params,
                    concept_id=concept_id, entity_type=entity_type, entity_id=entity_id,
                    function_id=function_id,
                )
            except SourceUnavailable:
                spider.logger.info("source %s unavailable (all proxies OPEN) - failover", source)
                last_err = f"{source} unavailable"
                continue  # next source in the chain
            except FetchError as e:
                spider.logger.warning("fetch failed %s/%s: %s", source, command, e)
                last_err = str(e)
                continue  # next source
            if snapshot_mode:
                frame = _extract_snapshot(adapter, result, column, date)
                if frame:
                    return TextResponse(
                        request.url, status=200,
                        body=json.dumps(frame, ensure_ascii=False, default=str).encode(),
                        request=request,
                    )
                if frame is None:
                    # adapter cannot serve snapshots - try next source
                    last_err = f"{source}/{command} has no snapshot extraction"
                    continue
                continue  # empty frame from this source - try the next source
            if series_mode:
                series = _extract_series(adapter, result, column, start, end)
                if series:
                    return TextResponse(
                        request.url, status=200,
                        body=json.dumps(series, ensure_ascii=False, default=str).encode(),
                        request=request,
                    )
                if series is None:
                    # adapter cannot serve series (no extract_series) - try next source
                    last_err = f"{source}/{command} has no extract_series"
                    continue
                continue  # empty frame from this source - try the next source
            value = _extract(adapter, result, column, date, source, command, identifier=identifier)
            if value is None:
                # no data for this date from this source - try the next source
                continue
            return TextResponse(request.url, status=200, body=str(value).encode(), request=request)

        # all sources either had no value or failed
        if last_err:
            return TextResponse(request.url, status=503, body=b"", request=request)
        return TextResponse(request.url, status=204, body=b"", request=request)
