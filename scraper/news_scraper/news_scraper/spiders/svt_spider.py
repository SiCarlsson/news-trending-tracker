import scrapy
from .base_spider import BaseSpider


class SVTSpider(BaseSpider):
    name = "svt"
    allowed_domains = ["svt.se"]
    start_urls = [
        "https://www.svt.se/nyheter/ekonomi/",
        "https://www.svt.se/nyheter/svtforum/",
        "https://www.svt.se/nyheter/granskning/",
        "https://www.svt.se/nyheter/inrikes/",
        "https://www.svt.se/kultur/",
        "https://www.svt.se/nyheter/utrikes/",
    ]

    def __init__(self, *args, **kwargs):
        super(SVTSpider, self).__init__(*args, **kwargs)
        self.website_name = "SVT"
        self.website_url = "https://www.svt.se"

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
        articles = response.xpath('//*[@id="innehall"]/div/section/ul//li')

        for article in articles:
            article_title = article.xpath(".//article/a/@title").get()
            article_url = article.xpath(".//article/a/@href").get()

            if not article_title or not article_url:
                self.logger.warning(
                    "Missing article title or URL on page: %s", response.url
                )
                continue

            article_id = self.generate_uuid()
            article_item = self.create_article_item(
                article_title, article_url, article_id
            )
            yield article_item

            yield from self.process_article_words(article_title, article_id)
