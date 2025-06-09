import scrapy
import re

class BaseSpider(scrapy.Spider):
    """
    Base class for all spiders. It provides common functionality that can be shared across different spiders.
    """

    def __init__(self, *args, **kwargs):
        super(BaseSpider, self).__init__(*args, **kwargs)
        self.website_id = None
        self.website_name = None
        self.website_url = None
    
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