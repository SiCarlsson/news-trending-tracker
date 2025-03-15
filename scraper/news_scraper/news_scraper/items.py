# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NewsScraperItem(scrapy.Item):
    word = scrapy.Field()
    url = scrapy.Field()
    site_name = scrapy.Field()
