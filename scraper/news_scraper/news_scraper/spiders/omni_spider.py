import scrapy
from .base_spider import BaseSpider


class OmniSpider(BaseSpider):
    name = "omni"
    allowed_domains = ["omni.se"]
    start_urls = ["https://www.omni.se/senaste"]

    def __init__(self, *args, **kwargs):
        super(OmniSpider, self).__init__(*args, **kwargs)
        self.website_name = "Omni"
        self.website_url = "https://omni.se"

        self.ad_words = ["annons"]
        self.promotion_words = ["erbjudande", "omni mer"]

    def parse(self, response):
        """
        Extracts article titles and URLs from the response.
        Args:
            response (scrapy.http.Response): The page response to parse.
        Yields:
            ArticleItem: An item containing article details.
            WordItem: An item for each word in the article title.
            OccurrenceItem: An item linking words to articles and websites.
        """
        # Fetch all articles
        articles = response.xpath(
            "/html/body/main/div/div[2]/div/div[1]/div[1]/div/div"
        )

        for article in articles:
            ad = article.xpath(".//div/div[1]/div/a/span[3]/text()").get()
            if ad and ad.lower() in self.ad_words:
                continue

            omni_promotion = article.xpath(".//div/div[1]/div[1]/text()").get()
            if omni_promotion and omni_promotion.lower() in self.promotion_words:
                continue

            article_title = article.xpath(
                ".//div/div[2]/article/div/a/div/div[1]/h2/text()"
            ).get()
            article_url = article.xpath(".//div/div[2]/article/div/a/@href").get()

            article_id = self.generate_uuid()
            article_item = self.create_article_item(
                article_title, article_url, article_id
            )
            yield article_item

            yield from self.process_article_words(article_title, article_id)
