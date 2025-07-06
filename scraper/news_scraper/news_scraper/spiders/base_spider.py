import scrapy
import re
import uuid

from datetime import datetime
from news_scraper.items import WebsiteItem, ArticleItem, WordItem, OccurrenceItem
from news_scraper.utils import (
    generate_website_uuid,
    generate_word_uuid,
    generate_occurrence_uuid,
)


class BaseSpider(scrapy.Spider):
    """
    Base class for all spiders. It provides common functionality that can be shared across different spiders.
    """

    def __init__(self, *args, **kwargs):
        super(BaseSpider, self).__init__(*args, **kwargs)
        self.website_name = None
        self.website_url = None

    @property
    def website_id(self) -> str:
        """Generate deterministic website ID based on URL."""
        return generate_website_uuid(self.website_url)

    def start_requests(self):
        """
        Initializes the spider by yielding requests for each start URL.
        """
        yield self.create_website_item()

        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def create_website_item(self):
        """
        Creates a WebsiteItem.

        Returns:
            WebsiteItem: The created item.
        """
        website_item = WebsiteItem()
        website_item["website_id"] = self.website_id
        website_item["website_name"] = self.website_name
        website_item["website_url"] = self.website_url
        return website_item

    def create_article_item(
        self, article_title: str, article_url: str, article_id: str
    ):
        """
        Creates an ArticleItem.

        Args:
            article_title (str): The title of the article.
            article_url (str): The URL of the article.
            article_id (str): The deterministic article ID.
        Yields:
            ArticleItem: An item containing article details.
        """
        article_item = ArticleItem()
        article_item["article_id"] = article_id
        article_item["website_id"] = self.website_id
        article_item["article_title"] = article_title
        article_item["article_url"] = article_url
        return article_item

    def process_article_words(self, article_title: str, article_id: str):
        """
        Processes words in an article title and creates WordItem and OccurrenceItem for each word.

        Args:
            article_title (str): The title of the article.
            article_id (str): The deterministic article ID.
        Yields:
            WordItem: An item for each word in the article title.
            OccurrenceItem: An item linking words to articles and websites.
        """
        words = self.tokenize_title(article_title)
        for word in words:
            word_lower = word.lower()

            # Create word item without ID
            word_item = WordItem()
            word_item["word_id"] = generate_word_uuid(word_lower)
            word_item["word_text"] = word_lower
            yield word_item

            occurrence_item = OccurrenceItem()
            occurrence_item["occurrence_id"] = generate_occurrence_uuid(
                word_lower, article_id, self.website_url
            )
            occurrence_item["word_id"] = word_item["word_id"]
            occurrence_item["website_id"] = self.website_id
            occurrence_item["article_id"] = article_id
            occurrence_item["timestamp"] = self.generate_timestamp()
            yield occurrence_item

    def generate_uuid(self) -> str:
        """
        Generates a UUID string.

        Returns:
            str: A UUID string.
        """
        return str(uuid.uuid4())

    def generate_timestamp(self) -> str:
        """
        Generates a timestamp in ISO format.

        Returns:
            str: The current timestamp in ISO format.
        """
        return datetime.now().isoformat()

    def tokenize_title(self, title: str) -> list:
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
        title = re.sub(r"[^\w\s\-]", "", title)
        tokens = title.split()
        return tokens
