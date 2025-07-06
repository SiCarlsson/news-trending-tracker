import scrapy
from .base_spider import BaseSpider
from news_scraper.utils import generate_article_uuid


class AftonbladetSpider(BaseSpider):
    name = "aftonbladet"
    allowed_domains = ["aftonbladet.se"]
    start_urls = ["https://www.aftonbladet.se/senastenytt"]

    def __init__(self, *args, **kwargs):
        super(AftonbladetSpider, self).__init__(*args, **kwargs)
        self.website_name = "Aftonbladet"
        self.website_url = "https://www.aftonbladet.se"

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
        articles = response.xpath('//*[@id="main"]/div[1]/main/section/div')

        for article in articles:
            article_title = article.xpath(".//a/div/div[2]/h2/text()").get()
            if article_title is None:
                article_title = article.xpath(".//a/div/div[1]/h2/text()").get()

            article_url = article.xpath(".//a/@href").get()

            # Ads give None values
            if article_title is None or article_url is None:
                continue

            article_id = generate_article_uuid(article_url, self.website_url)
            article_item = self.create_article_item(
                article_title, article_url, article_id
            )
            yield article_item

            yield from self.process_article_words(article_title, article_id)
