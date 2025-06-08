import os
import logging
import sqlite3

from google.cloud import bigquery
from google.oauth2 import service_account
from scrapy.utils.project import get_project_settings
from itemadapter import ItemAdapter
from news_scraper.items import WebsiteItem, ArticleItem, WordItem, OccurrenceItem

logger = logging.getLogger(__name__)


class SQLitePipeline:
    def __init__(self):
        settings = get_project_settings()
        self.db_path = settings.get("SQLITE_DATABASE_PATH")

        self.conn = None
        self.cursor = None

    def open_spider(self, spider):
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        logger.info(f"Connected to SQLite DB at: {self.db_path}")

    def process_item(self, item, spider):
        """
        Process items by storing them in SQLite if they do not already exist.

        Args:
            item (dict): Scraped data.
            spider (scrapy.Spider): The active Scrapy spider
        """

        spider.logger.debug(
            f"SQLitePipeline processing item of type: {type(item).__name__}"
        )

        item_dict = ItemAdapter(item).asdict()

        if isinstance(item, WebsiteItem):
            table = "websites"  # Fixed from "website" to "websites"
            exists = self._exists(table, "website_url", item_dict["website_url"])
        elif isinstance(item, ArticleItem):
            table = "articles"
            exists = self._exists("articles", "article_url", item_dict["article_url"])
        elif isinstance(item, WordItem):
            table = "words"
            exists = self._exists("words", "word_text", item_dict["word_text"])
        elif isinstance(item, OccurrenceItem):
            table = "occurrences"
            exists = self._exists_occurrence(item_dict)

        if not exists:
            self._insert(table, item_dict, spider)

        return item

    def _exists(self, table, field, value):
        """
        Check if a value exists in a specified table field.

        Args:
            table (str): The name of the table to check.
            field (str): The column name to check against.
            value (str): The value to look for.

        Returns:
            bool: True if the value exists, False otherwise.
        """
        query = f"SELECT 1 FROM {table} WHERE {field} = ?"
        self.cursor.execute(query, (value,))
        return self.cursor.fetchone() is not None

    def _exists_occurrence(self, item_dict):
        """
        Special case for checkin if an occurrence exists.

        Args:
            item_dict (dict): The occurrence item dictionary with keys to check.

        Returns:
            bool: True is the occurrence exists, False otherwise.
        """
        query = f"""
            SELECT 1 FROM occurrences
            WHERE word_id = ? AND website_id = ? AND article_id = ?
        """
        self.cursor.execute(
            query,
            (item_dict["word_id"], item_dict["website_id"], item_dict["article_id"]),
        )
        return self.cursor.fetchone() is not None

    def _insert(self, table, item_dict, spider):
        """
        Insert an item into a specified table.

        Args:
            table (str): The name of the table to insert into.
            item_dict (dict): The item data to insert.
            spider (scrapy.Spider): The active Scrapy spider.
        """
        column_names = list(item_dict.keys())
        formatted_column_names = ", ".join(column_names)

        sql_placeholders = ", ".join(
            ["?"] * len(column_names)
        )  # Needs ? for each value
        row_values = list(item_dict.values())

        sql_query = f"INSERT INTO {table} ({formatted_column_names}) VALUES ({sql_placeholders})"

        try:
            self.cursor.execute(sql_query, tuple(row_values))
            self.conn.commit()
            logger.info(f"Inserted into {table}: {item_dict}")
        except sqlite3.Error as e:
            logger.error(f"Error inserting into {table}: {e}")
            self.conn.rollback()

    def close_spider(self, spider):
        """
        Clean up resources when spider is closed.
        """
        if self.conn:
            self.conn.close()
            logger.info("SQLitePipeline connection closed.")


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
