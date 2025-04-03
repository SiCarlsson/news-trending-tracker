import scrapy
import re
import uuid
from datetime import datetime

from news_scraper.items import WebsiteItem, ArticleItem, WordItem, OccurrenceItem


class SVTSpider(scrapy.Spider):
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

    word_cache = {}  # Store known words to avoid duplicates

    def __init__(self, *args, **kwargs):
        super(SVTSpider, self).__init__(*args, **kwargs)

        self.website_id = str(uuid.uuid4())
        self.website_name = "SVT"
        self.website_url = "https://www.svt.se"

    def parse(self, response):
        """
        Extracts article titles and URLs from the response.

        Args:
            response (scrapy.http.Response): The page response to parse.

        Yields:
            dict: A dictionary containing the title and URL of each article.
        """

        # Yield website item only once per spider
        if response.url == self.start_urls[0]:
            website_item = WebsiteItem()
            website_item["website_id"] = self.website_id
            website_item["website_name"] = self.website_name
            website_item["website_url"] = self.website_url

            yield website_item

        # Fetch all articles on current page
        articles = response.xpath('//*[@id="innehall"]/div/section/ul//li')

        print("Total articles: " + str(len(articles)))

        for article in articles:
            article_id = str(uuid.uuid4())
            article_title = article.xpath(".//article/a/@title").get()
            article_url = article.xpath(".//article/a/@href").get()

            article_item = ArticleItem()
            article_item["article_id"] = article_id
            article_item["website_id"] = self.website_id
            article_item["article_title"] = article_title
            article_item["article_url"] = article_url
            yield article_item

            words = self.tokenize_title(article_title)
            for word in words:
                word_lower = word.lower()

                if word_lower not in self.word_cache:
                    word_id = str(uuid.uuid4())
                    self.word_cache[word_lower] = word_id

                    word_item = WordItem()
                    word_item["word_id"] = word_id
                    word_item["word_text"] = word_lower
                    yield word_item
                else:
                    word_id = self.word_cache[word_lower]

                occurrence_id = str(uuid.uuid4())
                occurrence_timestamp = datetime.now().isoformat()

                occurrence_item = OccurrenceItem()
                occurrence_item["occurrence_id"] = occurrence_id
                occurrence_item["word_id"] = word_id
                occurrence_item["website_id"] = self.website_id
                occurrence_item["article_id"] = article_id
                occurrence_item["timestamp"] = occurrence_timestamp
                yield occurrence_item

    def tokenize_title(self, title):
        """
        Tokenizes a title by cleaning and splitting it into words. Filters out special characters and blank words.

        Args:
            title (str): The title to tokenize.

        Returns:
            list: A list of words (tokens).
        """
        if not title:
            return []

        title = title.strip().lower()
        title = re.sub(r"[^\w\s]", "", title)
        tokens = title.split()
        return tokens
