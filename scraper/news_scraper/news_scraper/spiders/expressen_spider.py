import scrapy
from .base_spider import BaseSpider


class ExpressenSpider(BaseSpider):
    name = "expressen"
    allowed_domains = ["expressen.se"]
    start_urls = ["https://www.expressen.se/nyhetsdygnet/"]

    def __init__(self, *args, **kwargs):
        super(ExpressenSpider, self).__init__(*args, **kwargs)
        self.website_name = "Expressen"
        self.website_url = "https://www.expressen.se"

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
        articles = response.xpath('//li[@class="list-page__item"]')

        for article in articles:
            article_title = article.xpath(".//a/div[1]/h2/text()").get()
            article_url = article.xpath("./a/@href").get()

            article_id = self.generate_uuid()
            article_item = self.create_article_item(
                article_title, article_url, article_id
            )
            yield article_item

            yield from self.process_article_words(article_title, article_id)
