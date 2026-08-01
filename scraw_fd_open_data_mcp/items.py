"""Scrapy items for the concept crawl."""
import scrapy


class ObservationItem(scrapy.Item):
    """One concept-keyed observation to upsert into semantic_observations."""

    concept_id = scrapy.Field()
    entity_type = scrapy.Field()
    entity_id = scrapy.Field()
    date = scrapy.Field()
    value = scrapy.Field()
    unit = scrapy.Field()
    source_used = scrapy.Field()
