"""DataProvider + runner for the Google Data Commons /v2/observation API.

``DCProvider.run("get_observation", params)`` issues a single REST call to
``https://api.datacommons.org/v2/observation`` and returns the parsed JSON
``byVariable`` payload. Facet disambiguation (picking one observation among
several per (entity, variable, date)) lives in the adapter — this module is
just the HTTP wrapper.
"""
from __future__ import annotations

import os
from typing import Any

import requests

from fd_open_data_protocol.provider import BaseDataProvider

from .catalog import CATALOG

_API_URL = "https://api.datacommons.org/v2/observation"


class DCProvider(BaseDataProvider):
    """DataProvider interface for Google Data Commons."""

    name = "datacommons"

    def registry(self) -> dict:
        return CATALOG

    def run(self, command: str, params: dict) -> Any:
        if command == "get_observation":
            return _fetch_observation(params)
        raise NotImplementedError(f"Unknown command: {command}")


def _fetch_observation(params: dict) -> dict:
    """Call the DC /v2/observation endpoint.

    Returns the raw JSON (``byVariable`` + ``facets``). Raises
    ``fd_open_data_mcp.errors.FetchError`` on HTTP/transport failure.
    """
    from fd_open_data_mcp.errors import FetchError

    key = os.environ.get("DC_API_KEY")
    if not key:
        raise FetchError("DC_API_KEY env var is not set", source="datacommons", command="get_observation")

    variable_dcids = params.get("variable_dcids")
    entity_dcids = params.get("entity_dcids")
    date = params.get("date")
    if date is None:
        date = "LATEST"
    # date == "" (all observations) is preserved — DC's native "full history" form,
    # used by the adapter's range/series path. Do NOT collapse "" → "LATEST".
    if not variable_dcids or not entity_dcids:
        raise FetchError("get_observation needs variable_dcids + entity_dcids",
                         source="datacommons", command="get_observation")

    # ``select`` must include variable + entity; value + date are the payload.
    query = {
        "key": key,
        "variable.dcids": variable_dcids,
        "entity.dcids": entity_dcids,
        "date": date,
        "select": ["entity", "variable", "value", "date"],
    }
    try:
        resp = requests.get(_API_URL, params=query, timeout=30)
    except requests.RequestException as e:
        raise FetchError(f"datacommons transport error: {e}",
                         source="datacommons", command="get_observation") from e
    if resp.status_code != 200:
        raise FetchError(
            f"datacommons HTTP {resp.status_code}: {resp.text[:200]}",
            source="datacommons", command="get_observation",
        )
    try:
        return resp.json()
    except ValueError as e:
        raise FetchError(f"datacommons returned non-JSON: {e}",
                         source="datacommons", command="get_observation") from e


def run_dc(command: str, params: dict) -> Any:
    """High-level runner used by ``fetch/runner.py``'s ``run_upstream``."""
    return DCProvider().run(command, params)
