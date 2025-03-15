import scrapy
import re

from news_scraper.items import NewsScraperItem


class SVTSpider(scrapy.Spider):
    name = "svt"
    allowed_domains = ["svt.se"]
    start_urls = [
        "https://www.svt.se/nyheter/ekonomi/",
        'https://www.svt.se/nyheter/svtforum/',
        'https://www.svt.se/nyheter/granskning/',
        'https://www.svt.se/nyheter/inrikes/',
        'https://www.svt.se/kultur/',
        'https://www.svt.se/nyheter/utrikes/'
    ]

    def parse(self, response):
        """
        Extracts article titles and URLs from the response.

        Args:
            response (scrapy.http.Response): The page response to parse.

        Yields:
            dict: A dictionary containing the title and URL of each article.
        """
        site_name = "SVT"

        articles = response.xpath('//*[@id="innehall"]/div/section/ul//li/article/a')

        for article in articles:
            title = article.xpath("@title").get()
            url = article.xpath("@href").get()

            # Tokenize the title into words
            tokens = self.tokenize_title(title)

            for token in tokens:
                yield NewsScraperItem(word=token, url=url, site_name=site_name)

    def tokenize_title(self, title):
        """
        Tokenizes a title by cleaning and splitting it into words.
        
        Args:
            title (str): The title to tokenize.
        
        Returns:
            list: A list of words (tokens).
        """
        title = title.strip().lower()
        title = re.sub(r"[^\w\s]", "", title)
        tokens = title.split()
        return tokens
