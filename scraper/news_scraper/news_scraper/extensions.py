import os
import logging
import sqlite3

from scrapy import signals
from google.cloud import bigquery
from google.oauth2 import service_account
from scrapy.utils.project import get_project_settings

logger = logging.getLogger(__name__)


class SQLiteSetupExtension:
    def __init__(self):
        self._setup_completed = False
        self.conn = None
        self.cursor = None

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()
        crawler.signals.connect(ext.engine_start, signal=signals.engine_started)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def engine_start(self):
        if self._setup_completed:
            logger.info("SQLite setup has already been completed. Skipping setup.")
            return

        settings = get_project_settings()
        db_path = settings.get("SQLITE_DATABASE_PATH")

        os.makedirs(os.path.dirname(db_path), exist_ok=True) # create data folder
        db_exists = os.path.isfile(db_path)

        if db_exists:
            self._setup_completed = True
            logger.info("SQLite instance is already present. Skipping setup.")
            return

        logger.info(f"Starting SQLite setup with database at: {db_path}")
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

        self._ensure_tables_exist()

        self._setup_completed = True


    def _ensure_tables_exist(self):
        logger.info("Creating SQLite tables...")

        self.cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS websites (
            website_id TEXT PRIMARY KEY,
            website_name TEXT NOT NULL,
            website_url TEXT NOT NULL
        )
        """
        )

        self.cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            article_id TEXT PRIMARY KEY,
            website_id TEXT NOT NULL,
            article_title TEXT NOT NULL,
            article_url TEXT NOT NULL,
            FOREIGN KEY (website_id) REFERENCES websites (website_id)
        )
        """
        )

        self.cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS words (
            word_id TEXT PRIMARY KEY,
            word_text TEXT NOT NULL
        )
        """
        )

        self.cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS occurrences (
            occurrence_id TEXT PRIMARY KEY,
            word_id TEXT NOT NULL,
            website_id TEXT NOT NULL,
            article_id TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            FOREIGN KEY (word_id) REFERENCES words (word_id),
            FOREIGN KEY (website_id) REFERENCES websites (website_id),
            FOREIGN KEY (article_id) REFERENCES articles (article_id)
        )
        """
        )

        # Commit the changes
        self.conn.commit()

        logger.info("SQLite tables created successfully.")

    def spider_closed(self, spider):
        if self.conn:
            self.conn.close()


class BigQuerySetupExtension:
    """
    Extension to initialize BigQuery dataset and tables before all spiders start.
    """

    def __init__(self):
        self._setup_completed = False

    @classmethod
    def from_crawler(cls, crawler):
        """Registers the extension with Scrapy."""
        ext = cls()
        crawler.signals.connect(ext.engine_start, signal=signals.engine_started)
        return ext

    def engine_start(self):
        """
        When the Scrapy engine starts, set up BigQuery infrastructure. Only actuates once per startup even though more spiders are called upon.
        """

        if self._setup_completed:
            logger.info("BigQuery setup has already been completed. Skipping setup.")
            return

        settings = get_project_settings()

        credentials_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            settings.get("BIGQUERY_CREDENTIALS_PATH"),
        )
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        project_id = credentials.project_id
        dataset_id = settings.get("BIGQUERY_DATASET_ID")
        client = bigquery.Client(credentials=credentials, project=project_id)

        logger.info("Starting BigQuery setup...")

        self._ensure_dataset_exists(client, dataset_id)
        self._ensure_tables_exist(client, dataset_id)

        self._setup_completed = True
        logger.info("BigQuery setup successfully completed.")

    def _ensure_dataset_exists(self, client, dataset_id):
        dataset_ref = client.dataset(dataset_id)
        try:
            client.get_dataset(dataset_ref)
            logger.info(f"Dataset '{dataset_id}' already exists. Skipping creation.")

        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "europe"  # Multi-region across the EU
            client.create_dataset(dataset)
            logger.info(f"Dataset '{dataset_id}' created successfully.")

    def _ensure_tables_exist(self, client, dataset_id):
        # Define table schemas
        tables = {
            "websites": [
                bigquery.SchemaField("website_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("website_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("website_url", "STRING", mode="REQUIRED"),
            ],
            "articles": [
                bigquery.SchemaField("article_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("website_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("article_title", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("article_url", "STRING", mode="REQUIRED"),
            ],
            "words": [
                bigquery.SchemaField("word_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("word_text", "STRING", mode="REQUIRED"),
            ],
            "occurrences": [
                bigquery.SchemaField("occurrence_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("word_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("website_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("article_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            ],
        }

        # Create each table
        for table_id, schema in tables.items():
            table_ref = client.dataset(dataset_id).table(table_id)
            try:
                client.get_table(table_ref)
                logger.info(f"Table '{table_id}' already exists. Skipping creation.")

            except Exception:
                table = bigquery.Table(table_ref, schema=schema)
                client.create_table(table)
                logger.info(f"Table '{table_id}' created successfully.")
