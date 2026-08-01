"""Custom Scrapy download handler for ``fetch://`` URLs.

The concept crawl fetches via Python libs (akshare/wbgapi/...), not HTTP. A ``fetch://``
Request encodes ``(source, command, params)``; this handler calls the shared
``fd_open_data_mcp`` adapter registry + ``run_upstream`` and returns the extracted value
as a ``TextResponse`` body. This lets the crawl use Scrapy's request/scheduler machinery
(including scrapy-redis distribution across pods) for lib-based fetches.

Request meta carries: source, command, identifier, date, column, concept_id, entity_type,
entity_id, unit. The handler returns status 200 + the value as body (204 if no value,
500 on fetch error).
"""
from __future__ import annotations

from scrapy.http import TextResponse


class _FnStub:
    def __init__(self, command: str):
        self.command = command
        self.parameters = []


class _BindingStub:
    def __init__(self, column: str):
        class _C:
            name = column
        self.column = _C()


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


class FetchHandler:
    """Download handler for ``fetch://`` requests -> adapter registry fetch."""

    lazy = False

    def __init__(self, settings, crawler):
        self.settings = settings
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings, crawler)

    def download_request(self, request, spider):
        from fd_open_data_mcp.adapters import adapter_for
        from fd_open_data_mcp.fetch.runner import FetchError, run_upstream

        m = request.meta
        source, command = m["source"], m["command"]
        identifier, date, column = m["identifier"], m["date"], m["column"]

        adapter = adapter_for(source, command)
        params = (adapter.build_params(_FnStub(command), identifier, date, _BindingStub(column))
                  if adapter else {"symbol": identifier, "date": date})
        try:
            result = run_upstream(source, command, params)
        except FetchError as e:
            spider.logger.warning("fetch failed %s/%s: %s", source, command, e)
            return TextResponse(request.url, status=500, body=b"", request=request)
        value = adapter.extract_value(result, column, date) if adapter else _legacy_extract(result, column, date)
        body = ("" if value is None else str(value)).encode()
        status = 200 if value is not None else 204
        return TextResponse(request.url, status=status, body=body, request=request)
