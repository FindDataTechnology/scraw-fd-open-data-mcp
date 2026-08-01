"""Smoke test: package imports, settings load, spider discovered (scraw-project-template)."""
import importlib

from scrapy.utils.project import get_project_settings


def test_package_imports():
    importlib.import_module("scraw_fd_open_data_mcp")
    importlib.import_module("scraw_fd_open_data_mcp.settings")
    importlib.import_module("scraw_fd_open_data_mcp.spiders.concept_crawl_spider")


def test_settings_load():
    settings = get_project_settings()
    assert settings.get("BOT_NAME") == "scraw_fd_open_data_mcp"
    assert "scraw_fd_open_data_mcp.pipelines.ObservationUpsertPipeline" in settings.get("ITEM_PIPELINES", {})
    assert settings.get("REDIS_KEY") == "scraw_fd_open_data_mcp:start_urls"


def test_scrapy_list_discovers_spider():
    from scrapy.utils.misc import walk_modules
    from scrapy.spiderloader import SpiderLoader

    settings = get_project_settings()
    loader = SpiderLoader.from_settings(settings)
    assert "concept_crawl" in loader.list()
