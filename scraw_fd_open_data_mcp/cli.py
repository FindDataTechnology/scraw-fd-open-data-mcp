"""CLI for scraw-fd-open-data-mcp: `crawl <plan>`, `smoke`, `migrate <source>`."""
from __future__ import annotations

import sys


# ponytail: minimal smoke table — one known-good (source, command, params) each.
# Ground-truth fetch check before wiring a source into a crawl plan.
_SMOKE_TARGETS = [
    ("akshare", "stock_zh_a_spot_em", {}),
    ("yfinance", "ticker_info", {"symbol": "AAPL"}),
    ("wbgapi", "list_economies", {}),
    ("cn-report", "list_indicators", {}),
    ("nbs-gdp", "get_gdp_quarterly", {"start_year": 2024}),
    ("cisa-industry", "get_steel_production", {}),
    ("amac-fund", "get_fund_stats", {}),
    ("shfe-metal-futures", "get_metal_pricing", {}),
    ("agriculture", "get_agri_pricing", {}),
    ("cme-agricultural-futures", "get_grain_pricing", {}),
    ("chemicals", "get_chemical_prices", {}),
    ("electronics", "get_semiconductor_stats", {}),
    ("nonferrous", "get_aluminum_prices", {}),
    ("flowers-kifc", "get_daily_prices", {}),
    ("fin_platforms", "get_market_benchmark", {}),
    ("sac-securities", "get_trading_stats", {}),
]


def main() -> None:
    import click

    @click.group()
    def cli():
        """scraw-fd-open-data-mcp: unified concept-driven crawler."""

    @cli.command()
    @click.argument("plan", type=click.Path(exists=True))
    def crawl(plan):
        """Run the ConceptCrawlSpider with a CrawlPlan JSON file."""
        from scrapy.crawler import CrawlerProcess
        from scrapy.utils.project import get_project_settings

        settings = get_project_settings()
        process = CrawlerProcess(settings)
        process.crawl("concept_crawl", plan=plan)
        process.start()

    @cli.command("smoke")
    def smoke():
        """Fetch-test every provider via run_upstream; report which return data."""
        import os
        import time
        import sys as _sys
        from pathlib import Path

        # fd-cn-report must be importable for the cn-report runner.
        _root = Path(__file__).resolve().parents[1].parent
        for p in (str(_root / "fd-cn-report"), str(_root / "fd-open-data-mcp")):
            if p not in _sys.path:
                _sys.path.insert(0, p)

        from fd_open_data_mcp.fetch.runner import run_upstream, FetchError

        def _fmt(v):
            try:
                import pandas as pd
                if isinstance(v, pd.DataFrame):
                    return f"DataFrame({len(v)}r×{len(v.columns)}c)"
                if isinstance(v, dict):
                    return f"dict keys={list(v.keys())[:5]}"
                if isinstance(v, list):
                    return f"list[{len(v)}]"
            except Exception:
                pass
            return str(v)[:60]

        if not os.environ.get("EDGAR_IDENTITY"):
            click.echo("[info] EDGAR_IDENTITY unset — edgar will fail")

        ok = bad = 0
        click.echo(f"\n{'SOURCE':<28} {'COMMAND':<26} {'STATUS':<8} {'MS':>6}  RESULT")
        click.echo("-" * 90)
        for source, command, params in _SMOKE_TARGETS:
            t0 = time.time()
            try:
                r = run_upstream(source, command, params)
                ms = int((time.time() - t0) * 1000)
                click.echo(f"{source:<28} {command:<26} {'OK':<8} {ms:>6}  {_fmt(r)}")
                ok += 1
            except Exception as e:
                ms = int((time.time() - t0) * 1000)
                try:
                    detail = str(e)[:50]
                except Exception:
                    detail = repr(e)[:50]
                click.echo(f"{source:<28} {command:<26} {'FAIL':<8} {ms:>6}  {type(e).__name__}: {detail}")
                bad += 1
        click.echo("-" * 90)
        click.echo(f"\n{ok} ok, {bad} failed")


    @cli.command()
    @click.argument("source", type=click.Choice([
        "astock", "astock-hk", "astock-us",
        "astock-balance", "astock-profit", "astock-cashflow",
    ]))
    @click.option("--symbol", default=None)
    @click.option("--limit", type=int, default=None)
    def migrate(source, symbol, limit):
        """Migrate legacy data into semantic_observations (delegates to fd-open-data-mcp)."""
        from fd_open_data_mcp.crawl.migrate import (
            migrate_astock_daily, migrate_financials, migrate_stock_daily,
        )
        from fd_open_data_mcp.db import get_database

        s = get_database().get_session()
        try:
            if source == "astock":
                click.echo(migrate_astock_daily(s, symbol=symbol, limit=limit))
            elif source == "astock-hk":
                click.echo(migrate_stock_daily(s, "astock_hk_daily", symbol=symbol, limit=limit))
            elif source == "astock-us":
                click.echo(migrate_stock_daily(s, "astock_us_daily", symbol=symbol, limit=limit))
            elif source in ("astock-balance", "astock-profit", "astock-cashflow"):
                table = {"astock-balance": "astock_balance_sheet",
                         "astock-profit": "astock_profit_sheet",
                         "astock-cashflow": "astock_cash_flow"}[source]
                click.echo(migrate_financials(s, table, symbol=symbol, limit=limit))
        finally:
            s.close()

    cli()


if __name__ == "__main__":
    main()
