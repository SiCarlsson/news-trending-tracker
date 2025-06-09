import scrapy
import re
import uuid
from datetime import datetime
from news_scraper.items import WebsiteItem, ArticleItem, WordItem, OccurrenceItem

class BaseSpider(scrapy.Spider):
    """
    Base class for all spiders. It provides common functionality that can be shared across different spiders.
    """

    def __init__(self, *args, **kwargs):
        super(BaseSpider, self).__init__(*args, **kwargs)
        self.website_id = self._generate_id()
        self.website_name = None
        self.website_url = None
    
    def start_requests(self):
        """
        Generate initial requests for the spider and yield website metadata.
        
        Yields:
            WebsiteItem: Contains website metadata (id, name, url)
            Request: HTTP requests to the start_urls for parsing
        """
        website_item = WebsiteItem()
        website_item["website_id"] = self.website_id
        website_item["website_name"] = self.website_name
        website_item["website_url"] = self.website_url
        yield website_item

        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def create_article_item(self, article_title, article_url):
        """
        Creates an ArticleItem with a unique ID.
        
        Args:
            article_title (str): The title of the article
            article_url (str): The URL of the article
            
        Returns:
            tuple: (article_id, ArticleItem) - The article ID and the created item
        """
        article_id = self._generate_id()
        article_item = ArticleItem()
        article_item["article_id"] = article_id
        article_item["website_id"] = self.website_id
        article_item["article_title"] = article_title
        article_item["article_url"] = article_url
        return article_id, article_item

    def process_article_words(self, article_title, article_id):
        """
        Processes words in an article title and creates WordItem and OccurrenceItem for each word.
        
        Args:
            article_title (str): The title of the article
            article_id (str): The ID of the article
            
        Yields:
            WordItem: An item for each word in the article title
            OccurrenceItem: An item linking words to articles and websites
        """
        words = self.tokenize_title(article_title)
        for word in words:
            word_lower = word.lower()

            # Create word item
            word_id = self._generate_id()
            word_item = WordItem()
            word_item["word_id"] = word_id
            word_item["word_text"] = word_lower
            yield word_item

            # Create occurrence item
            occurrence_id = self._generate_id()
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
    
    def _generate_id(self):
        """
        Generates a unique identifier for the spider instance.
        
        Returns:
            str: A unique identifier string.
        """
        return str(uuid.uuid4())