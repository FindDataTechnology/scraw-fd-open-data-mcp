"""Scrapy middlewares (default)."""
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware


class ScrawFdOpenDataMcpSpiderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_spider_input(self, response, spider):
        return None

    def process_spider_output(self, response, result, spider):
        for r in result:
            yield r


class ScrawFdOpenDataMcpDownloaderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        return cls()
