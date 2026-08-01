"""scraw-fd-open-data-mcp: unified concept-driven crawler for fd-open-data-mcp.

Two modes (per the `unified-data-store` spec):
  crawl  - a ConceptCrawlSpider consumes a CrawlPlan, fetches fresh data via the
           shared adapter registry, and writes semantic_observations.
  migrate - reshape legacy per-source tables into semantic_observations (see
           fd_open_data_mcp.crawl.migrate; exposed via the `migrate` CLI for parity).
"""
__version__ = "0.1.0"
