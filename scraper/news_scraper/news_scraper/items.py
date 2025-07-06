import scrapy


class WebsiteItem(scrapy.Item):
    website_id = scrapy.Field()
    website_name = scrapy.Field()
    website_url = scrapy.Field()


class ArticleItem(scrapy.Item):
    article_id = scrapy.Field()
    website_id = scrapy.Field()
    article_title = scrapy.Field()
    article_url = scrapy.Field()


class WordItem(scrapy.Item):
    word_id = scrapy.Field()
    word_text = scrapy.Field()


class OccurrenceItem(scrapy.Item):
    occurrence_id = scrapy.Field()
    word_id = scrapy.Field()
    website_id = scrapy.Field()
    article_id = scrapy.Field()
    timestamp = scrapy.Field()
