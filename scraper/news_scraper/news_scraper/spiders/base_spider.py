import scrapy
import re
from datetime import datetime
from news_scraper.items import WebsiteItem, ArticleItem, WordItem, OccurrenceItem


class BaseSpider(scrapy.Spider):
    """
    Base class for all spiders. It provides common functionality that can be shared across different spiders.
    """

    def __init__(self, *args, **kwargs):
        super(BaseSpider, self).__init__(*args, **kwargs)
        self.website_name = None
        self.website_url = None

    def start_requests(self):
        """
        Generate initial requests for the spider and yield website metadata.

        Yields:
            WebsiteItem: Contains website metadata
            Request: HTTP requests to the start_urls for parsing
        """
        website_item = WebsiteItem()
        website_item["website_name"] = self.website_name
        website_item["website_url"] = self.website_url
        yield website_item

        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def create_article_item(self, article_title, article_url):
        """
        Creates an ArticleItem without ID

        Args:
            article_title (str): The title of the article
            article_url (str): The URL of the article

        Returns:
            ArticleItem: The created item without ID
        """
        article_item = ArticleItem()
        article_item["article_title"] = article_title
        article_item["article_url"] = article_url
        return article_item

    def process_article_words(self, article_title, article_url):
        """
        Processes words in an article title and creates WordItem and OccurrenceItem for each word.

        Args:
            article_title (str): The title of the article
            article_url (str): The URL of the article to link occurrences to

        Yields:
            WordItem: An item for each word in the article title (without ID)
            OccurrenceItem: An item linking words to articles and websites (without IDs)
        """
        words = self.tokenize_title(article_title)
        for word in words:
            word_lower = word.lower()

            # Create word item without ID
            word_item = WordItem()
            word_item["word_text"] = word_lower
            yield word_item

            occurrence_item = OccurrenceItem()
            occurrence_item["word_text"] = word_lower
            occurrence_item["article_url"] = article_url
            occurrence_item["website_url"] = self.website_url
            occurrence_item["timestamp"] = datetime.now().isoformat()
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
        title = re.sub(r"[^\w\s\-]", "", title)
        tokens = title.split()
        return tokens
