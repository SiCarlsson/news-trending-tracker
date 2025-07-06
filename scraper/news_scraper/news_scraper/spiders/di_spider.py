import scrapy
from .base_spider import BaseSpider


class DISpider(BaseSpider):
    name = "di"
    allowed_domains = ["di.se"]
    start_urls = ["https://www.di.se/nyheter/"]

    def __init__(self, *args, **kwargs):
        super(DISpider, self).__init__(*args, **kwargs)
        self.website_name = "Dagens Industri"
        self.website_url = "https://di.se"

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
        articles = response.xpath("//article")

        for article in articles:
            article_title = article.xpath(".//div/div/a/div[1]/h2/text()").get()
            article_url = article.xpath(".//div/div/a/@href").get()

            article_id = self.generate_uuid()
            article_item = self.create_article_item(
                article_title, article_url, article_id
            )
            yield article_item

            yield from self.process_article_words(article_title, article_id)
