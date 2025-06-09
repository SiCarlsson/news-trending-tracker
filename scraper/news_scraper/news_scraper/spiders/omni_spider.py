import uuid
from datetime import datetime

from news_scraper.items import WebsiteItem, ArticleItem, WordItem, OccurrenceItem
from .base_spider import BaseSpider


class OmniSpider(BaseSpider):
    name = "omni"
    allowed_domains = ["omni.se"]
    start_urls = ["https://www.omni.se/senaste"]

    def __init__(self, *args, **kwargs):
        super(OmniSpider).__init__(*args, **kwargs)
        self.website_id = str(uuid.uuid4())
        self.website_name = "Omni"
        self.website_url = "https://omni.se"

        self.ad_words = ["annons"]
        self.promotion_words = ["erbjudande", "omni mer"]

    def spider_opened(self, spider):
        """
        Called when the spider is opened. Yields a WebsiteItem with website details.
        Args:
            spider (scrapy.Spider): The spider instance.
        Yields:
            WebsiteItem: An item containing the website ID, name, and URL."""
        website_item = WebsiteItem()
        website_item["website_id"] = self.website_id
        website_item["website_name"] = self.website_name
        website_item["website_url"] = self.website_url
        yield website_item

    def parse(self, response):
        articles = response.xpath(
            "/html/body/main/div/div[2]/div/div[1]/div[1]/div/div"
        )

        for article in articles:
            ad = article.xpath(".//div/div[1]/div/a/span[3]/text()").get()
            if ad and  ad.lower() in self.ad_words:
                continue

            omni_promotion = article.xpath(".//div/div[1]/div[1]/text()").get()
            if omni_promotion and omni_promotion.lower() in self.promotion_words:
                continue

            article_id = str(uuid.uuid4())
            article_title = article.xpath(
                ".//div/div[2]/article/div/a/div/div[1]/h2/text()"
            ).get()
            article_url = article.xpath(".//div/div[2]/article/div/a/@href").get()

            # Create article item
            article_item = ArticleItem()
            article_item["article_id"] = article_id
            article_item["website_id"] = self.website_id
            article_item["article_title"] = article_title
            article_item["article_url"] = article_url
            yield article_item

            words = self.tokenize_title(article_title)
            for word in words:
                word_lower = word.lower()

                # Create word item
                word_id = str(uuid.uuid4())
                word_item = WordItem()
                word_item["word_id"] = word_id
                word_item["word_text"] = word_lower
                yield word_item

                # Create occurrence item
                occurrence_id = str(uuid.uuid4())
                occurrence_timestamp = datetime.now().isoformat()

                occurrence_item = OccurrenceItem()
                occurrence_item["occurrence_id"] = occurrence_id
                occurrence_item["word_id"] = word_id
                occurrence_item["website_id"] = self.website_id
                occurrence_item["article_id"] = article_id
                occurrence_item["timestamp"] = occurrence_timestamp
                yield occurrence_item
