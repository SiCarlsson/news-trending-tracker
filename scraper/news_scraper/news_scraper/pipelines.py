import os

from google.cloud import bigquery
from google.oauth2 import service_account

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from news_scraper.items import WebsiteItem, ArticleItem, WordItem, OccurrenceItem


class BigQueryPipeline:
    def __init__(self):
        credentials_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "credentials",
            "bigquery_scraper_service-account.json",
        )
        project_id = "news-trending-tracker"
        dataset_id = "scraper_data"

        # Load credentials and create the BigQuery client
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        self.client = bigquery.Client(credentials=credentials, project=project_id)
        self.dataset_id = dataset_id

    def process_item(self, item, spider):
        """
        Inserts item data into the appropriate BigQuery table.

        Args:
            item (dict): Scraped data.
            spider (scrapy.Spider): The active Scrapy spider.
        """

        if isinstance(item, WebsiteItem):
            self.insert_to_bigquery("websites", item, spider)
        elif isinstance(item, ArticleItem):
            self.insert_to_bigquery("articles", item, spider)
        elif isinstance(item, WordItem):
            self.insert_to_bigquery("words", item, spider)
        elif isinstance(item, OccurrenceItem):
            self.insert_to_bigquery("occurrences", item, spider)

        return item

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
