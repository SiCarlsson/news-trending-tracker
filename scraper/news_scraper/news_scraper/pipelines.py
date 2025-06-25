import logging
import uuid

import json
from kafka import KafkaProducer

from google.cloud import bigquery
from google.oauth2 import service_account
from scrapy.utils.project import get_project_settings
from itemadapter import ItemAdapter
from news_scraper.items import WebsiteItem, ArticleItem, WordItem, OccurrenceItem

logger = logging.getLogger(__name__)


class KafkaPipeline:
    def __init__(self):
        self.producer = None

    def open_spider(self, spider):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=["localhost:9092"],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                    "utf-8"
                ),
                key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                compression_type="gzip",
                retries=3,
            )
            logger.info("Kafka producer initialized with UTF-8 support.")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")

    def close_spider(self, spider):
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer flushed and closed.")

    def process_item(self, item, spider):
        # Determine topic based on item type
        item_dict = ItemAdapter(item).asdict()
        topic = f"news-{type(item).__name__.lower()}"

        self.producer.send(topic, item_dict)
        return item


class BigQueryPipeline:
    def __init__(self):
        settings = get_project_settings()

        credentials_path = settings.get("BIGQUERY_CREDENTIALS_PATH")
        if not credentials_path:
            raise ValueError("BIGQUERY_CREDENTIALS_PATH not found in settings")

        # Load credentials and create the BigQuery client
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        project_id = credentials.project_id

        self.client = bigquery.Client(credentials=credentials, project=project_id)
        self.dataset_id = settings.get("BIGQUERY_DATASET_ID")

        # Cache for IDs during processing
        self.website_cache = {}
        self.article_cache = {}
        self.word_cache = {}

    def _generate_id(self):
        """
        Generates a unique identifier.

        Returns:
            str: A unique identifier string.
        """
        return str(uuid.uuid4())

    def process_item(self, item, spider):
        """
        Processes items by generating IDs and inserting data into the appropriate BigQuery table.

        Args:
            item (dict): Scraped data.
            spider (scrapy.Spider): The active Scrapy spider.
        """

        spider.logger.debug(
            f"BigQueryPipeline processing item of type: {type(item).__name__}"
        )

        if isinstance(item, WebsiteItem):
            self._process_website_item(item, spider)

        elif isinstance(item, ArticleItem):
            self._process_article_item(item, spider)

        elif isinstance(item, WordItem):
            self._process_word_item(item, spider)

        elif isinstance(item, OccurrenceItem):
            self._process_occurrence_item(item, spider)

        return item

    def _process_website_item(self, item, spider):
        """Process WebsiteItem by generating ID and inserting if not exists."""
        item_dict = ItemAdapter(item).asdict()
        website_url = item_dict["website_url"]

        if not self.website_exists_in_bigquery(website_url):
            website_id = self._generate_id()
            item_dict["website_id"] = website_id
            self.website_cache[website_url] = website_id

            item["website_id"] = website_id

            self.insert_to_bigquery("websites", item, spider)
        else:
            website_id = self._get_existing_website_id(website_url)
            if website_id:
                self.website_cache[website_url] = website_id
                item["website_id"] = website_id
            else:
                spider.logger.error(
                    f"Failed to get existing website ID for: {website_url}"
                )

    def _process_article_item(self, item, spider):
        """Process ArticleItem by generating ID and inserting if not exists."""
        item_dict = ItemAdapter(item).asdict()
        article_url = item_dict["article_url"]

        if not self.article_exists_in_bigquery(article_url):
            article_id = self._generate_id()
            item_dict["article_id"] = article_id
            self.article_cache[article_url] = article_id

            website_url = spider.website_url
            website_id = self.website_cache.get(website_url)
            if not website_id:
                website_id = self._get_existing_website_id(website_url)
                if website_id:
                    self.website_cache[website_url] = website_id
                else:
                    spider.logger.error(f"Failed to get website ID for: {website_url}")
                    return

            item_dict["website_id"] = website_id

            item["article_id"] = article_id
            item["website_id"] = website_id

            self.insert_to_bigquery("articles", item, spider)
        else:
            article_id = self._get_existing_article_id(article_url)
            if article_id:
                self.article_cache[article_url] = article_id
                item["article_id"] = article_id
            else:
                spider.logger.error(
                    f"Failed to get existing article ID for: {article_url}"
                )

    def _process_word_item(self, item, spider):
        """Process WordItem by generating ID and inserting if not exists."""
        item_dict = ItemAdapter(item).asdict()
        word_text = item_dict["word_text"]

        if not self.word_exists_in_bigquery(word_text):
            word_id = self._generate_id()
            item_dict["word_id"] = word_id
            self.word_cache[word_text] = word_id

            item["word_id"] = word_id

            self.insert_to_bigquery("words", item, spider)
        else:
            word_id = self._get_existing_word_id(word_text)
            if word_id:
                self.word_cache[word_text] = word_id
                item["word_id"] = word_id
            else:
                spider.logger.error(f"Failed to get existing word ID for: {word_text}")

    def _process_occurrence_item(self, item, spider):
        """Process OccurrenceItem by linking to existing entities and inserting."""
        item_dict = ItemAdapter(item).asdict()

        word_text = item_dict["word_text"]
        article_url = item_dict["article_url"]
        website_url = item_dict["website_url"]

        # Get IDs from cache or BigQuery
        word_id = self.word_cache.get(word_text)
        if not word_id:
            word_id = self._get_existing_word_id(word_text)
            if word_id:
                self.word_cache[word_text] = word_id

        article_id = self.article_cache.get(article_url)
        if not article_id:
            article_id = self._get_existing_article_id(article_url)
            if article_id:
                self.article_cache[article_url] = article_id

        website_id = self.website_cache.get(website_url)
        if not website_id:
            website_id = self._get_existing_website_id(website_url)
            if website_id:
                self.website_cache[website_url] = website_id

        # Only process if all required IDs are available
        if not all([word_id, article_id, website_id]):
            spider.logger.warning(
                f"Missing required IDs for occurrence: word_id={word_id}, "
                f"article_id={article_id}, website_id={website_id}. "
                f"Skipping occurrence for word '{word_text}'"
            )
            return

        occurrence_id = self._generate_id()

        # Update item with proper IDs
        item_dict["occurrence_id"] = occurrence_id
        item_dict["word_id"] = word_id
        item_dict["article_id"] = article_id
        item_dict["website_id"] = website_id

        # Remove the text fields used for linking
        item_dict.pop("word_text", None)
        item_dict.pop("article_url", None)
        item_dict.pop("website_url", None)

        if not self.occurrence_exists_in_bigquery(item_dict):
            self.insert_to_bigquery("occurrences", item_dict, spider)
        else:
            spider.logger.debug(f"Occurrence already exists, skipping: {item_dict}")

    def website_exists_in_bigquery(self, website_url):
        """
        Checks if a website already exists in the BigQuery 'websites' table.

        Args:
            website_url (str): The URL of the website to check.

        Returns:
            bool: True if the website exists, False otherwise.
        """
        query = f"""
            SELECT website_url
            FROM `{self.client.project}.{self.dataset_id}.websites`
            WHERE website_url = @website_url
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("website_url", "STRING", website_url)
            ]
        )

        query_job = self.client.query(query, job_config)
        result = query_job.result()

        if result.total_rows > 0:
            return True
        else:
            return False

    def article_exists_in_bigquery(self, article_url):
        """
        Checks if an article already exists in the BigQuery 'articles' table.

        Args:
            article_url (str): The URL of the article to check.

        Returns:
            bool: True if the article exists, False otherwise.
        """
        query = f"""
            SELECT article_url
            FROM `{self.client.project}.{self.dataset_id}.articles`
            WHERE article_url = @article_url
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("article_url", "STRING", article_url)
            ]
        )

        query_job = self.client.query(query, job_config)
        result = query_job.result()

        if result.total_rows > 0:
            return True
        else:
            return False

    def word_exists_in_bigquery(self, word_text):
        """
        Checks if a word already exists in the BigQuery 'words' table.

        Args:
            word_text (str): The word to check.

        Returns:
            bool: True if the word exists, False otherwise.
        """
        query = f"""
            SELECT word_text
            FROM `{self.client.project}.{self.dataset_id}.words`
            WHERE word_text = @word_text
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("word_text", "STRING", word_text)
            ]
        )

        query_job = self.client.query(query, job_config)
        result = query_job.result()

        if result.total_rows > 0:
            return True
        else:
            return False

    def occurrence_exists_in_bigquery(self, occurrence_item):
        """
        Checks if an occurrence already exists in the BigQuery 'occurrences' table.

        Args:
            occurrence_item (dict): The occurrence to check.

        Returns:
            bool: True if the occurrence exists, False otherwise.
        """
        query = f"""
            SELECT occurrence_id
            FROM `{self.client.project}.{self.dataset_id}.occurrences`
            WHERE word_id = @word_id
            AND website_id = @website_id
            AND article_id = @article_id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "word_id", "STRING", occurrence_item["word_id"]
                ),
                bigquery.ScalarQueryParameter(
                    "website_id", "STRING", occurrence_item["website_id"]
                ),
                bigquery.ScalarQueryParameter(
                    "article_id", "STRING", occurrence_item["article_id"]
                ),
            ]
        )

        query_job = self.client.query(query, job_config)
        result = query_job.result()

        if result.total_rows > 0:
            return True
        else:
            return False

    def _get_existing_website_id(self, website_url):
        """Get existing website ID from BigQuery."""
        query = f"""
            SELECT website_id
            FROM `{self.client.project}.{self.dataset_id}.websites`
            WHERE website_url = @website_url
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("website_url", "STRING", website_url)
            ]
        )
        query_job = self.client.query(query, job_config)
        result = query_job.result()

        for row in result:
            return row.website_id
        return None

    def _get_existing_article_id(self, article_url):
        """Get existing article ID from BigQuery."""
        query = f"""
            SELECT article_id
            FROM `{self.client.project}.{self.dataset_id}.articles`
            WHERE article_url = @article_url
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("article_url", "STRING", article_url)
            ]
        )
        query_job = self.client.query(query, job_config)
        result = query_job.result()

        for row in result:
            return row.article_id
        return None

    def _get_existing_word_id(self, word_text):
        """Get existing word ID from BigQuery."""
        query = f"""
            SELECT word_id
            FROM `{self.client.project}.{self.dataset_id}.words`
            WHERE word_text = @word_text
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("word_text", "STRING", word_text)
            ]
        )
        query_job = self.client.query(query, job_config)
        result = query_job.result()

        for row in result:
            return row.word_id
        return None

    def insert_to_bigquery(self, table_id, item, spider):
        """
        Inserts the item into the appropriate BigQuery table.

        Args:
            table_id (str): The BigQuery table name to insert into.
            item (dict): The Scrapy item to insert.
            spider (scrapy.Spider): The active Scrapy spider.
        """
        item_dict = ItemAdapter(item).asdict()

        table_ref = self.client.dataset(self.dataset_id).table(table_id)

        # Insert data into BigQuery table
        try:
            errors = self.client.insert_rows_json(table_ref, [item_dict])
            if errors:
                spider.logger.error(f"Error inserting row into {table_id}: {errors}")
            else:
                spider.logger.info(f"Inserted item into {table_id}: {item_dict}")
        except Exception as e:
            spider.logger.error(f"Error inserting into BigQuery: {e}")
