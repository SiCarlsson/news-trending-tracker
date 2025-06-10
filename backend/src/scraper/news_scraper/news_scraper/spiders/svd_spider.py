import scrapy
from .base_spider import BaseSpider


class SVDSpider(BaseSpider):
    name = "svd"
    allowed_domains = ["svd.se"]
    start_urls = ["https://www.svd.se/i/senaste"]

    def __init__(self, *args, **kwargs):
        super(SVDSpider, self).__init__(*args, **kwargs)
        self.website_name = "Svenska Dagbladet"
        self.website_url = "https://www.svd.se"

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
        articles = response.xpath('//*[@id="Page"]/div[4]/div[3]/div/div[1]/div/div')

        for article in articles:
            article_title = article.xpath(".//a/h2/text()").get()
            article_url = article.xpath(".//a/@href").get()

            article_item = self.create_article_item(article_title, article_url)
            yield article_item

            yield from self.process_article_words(article_title, article_url)
