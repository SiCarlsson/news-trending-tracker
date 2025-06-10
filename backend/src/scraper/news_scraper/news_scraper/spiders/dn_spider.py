import scrapy
from .base_spider import BaseSpider


class DNSpider(BaseSpider):
    name = "dn"
    allowed_domains = ["dn.se"]
    start_urls = ["https://www.dn.se/nyhetsdygnet/"]

    def __init__(self, *args, **kwargs):
        super(DNSpider, self).__init__(*args, **kwargs)
        self.website_name = "Dagens Nyheter"
        self.website_url = "https://www.dn.se"

        self.prefix_words = ["ledare:", "dn debatt."]
        self.unwanted_articles = ["till minne"]
        self.unwanted_urls = ["/dagens-namn/"]

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
        articles = response.xpath('//*[@id="site-body"]/main/div[2]/div/section/a')

        for article in articles:
            prefix = article.xpath(".//div/div[1]/h2/span/text()").get()
            if prefix and prefix.lower() not in self.prefix_words:
                continue

            if not prefix:
                article_title = article.xpath(".//div/div/h2/text()").get()
            else:
                article_title = article.xpath(".//div/div/h2//text()").getall()[1]

            if article_title.lower() in self.unwanted_articles:
                continue

            article_url = article.xpath(".//@href").get()
            if article_url in self.unwanted_urls:
                continue

            article_item = self.create_article_item(article_title, article_url)
            yield article_item

            yield from self.process_article_words(article_title, article_url)
