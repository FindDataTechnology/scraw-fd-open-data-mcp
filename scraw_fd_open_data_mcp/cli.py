"""CLI for scraw-fd-open-data-mcp: `crawl <plan>` and `migrate <source>`."""
from __future__ import annotations

import sys


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
