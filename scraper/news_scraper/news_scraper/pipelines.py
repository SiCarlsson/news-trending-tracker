import os
import logging

from google.cloud import bigquery
from google.oauth2 import service_account
from scrapy.utils.project import get_project_settings
from itemadapter import ItemAdapter
from news_scraper.items import WebsiteItem, ArticleItem, WordItem, OccurrenceItem

logger = logging.getLogger(__name__)
class BigQueryPipeline:
    def __init__(self):
        settings = get_project_settings()

        credentials_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            settings.get("BIGQUERY_CREDENTIALS_PATH"),
        )

        # Load credentials and create the BigQuery client
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        project_id = credentials.project_id

        self.client = bigquery.Client(credentials=credentials, project=project_id)
        self.dataset_id = settings.get("BIGQUERY_DATASET_ID")

    def process_item(self, item, spider):
        """
        Inserts item data into the appropriate BigQuery table.

        Args:
            item (dict): Scraped data.
            spider (scrapy.Spider): The active Scrapy spider.
        """

        spider.logger.debug(
            f"BigQueryPipeline processing item of type: {type(item).__name__}"
        )

        if isinstance(item, WebsiteItem):
            website_url = ItemAdapter(item).asdict()["website_url"]
            if not self.website_exists_in_bigquery(website_url):
                self.insert_to_bigquery("websites", item, spider)

        elif isinstance(item, ArticleItem):
            article_url = ItemAdapter(item).asdict()["article_url"]
            if not self.article_exists_in_bigquery(article_url):
                self.insert_to_bigquery("articles", item, spider)

        elif isinstance(item, WordItem):
            word_text = ItemAdapter(item).asdict()["word_text"]
            if not self.word_exists_in_bigquery(word_text):
                self.insert_to_bigquery("words", item, spider)

        elif isinstance(item, OccurrenceItem):
            occurrence_item = ItemAdapter(item).asdict()
            if not self.occurrence_exists_in_bigquery(occurrence_item):
                self.insert_to_bigquery("occurrences", item, spider)

        return item

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
