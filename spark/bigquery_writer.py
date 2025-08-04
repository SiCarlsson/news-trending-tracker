"""
BigQuery operations for the Spark application.

Module to handle all BigQuery write operations including staging table
management and MERGE operations for data deduplication.
"""

import logging
from google.cloud import bigquery
from config import Config


class BigQueryWriter:
    """
    Handles BigQuery write operations with deduplication support.

    Manages writing streaming data to BigQuery using staging tables and
    MERGE operations to prevent duplicate records.
    """

    def __init__(self, credentials_path=None):
        """
        Initialize BigQuery writer.

        Args:
            credentials_path (str, optional): Path to service account JSON.
                If None, uses default from the configuration file.
        """
        self.logger = logging.getLogger(__name__)
        self.credentials_path = credentials_path or Config.BIGQUERY_CREDENTIALS_PATH
        self.client = bigquery.Client.from_service_account_json(self.credentials_path)
        self.project_id = Config.BIGQUERY_PROJECT_ID
        self.dataset = Config.BIGQUERY_DATASET
        self.staging_dataset = Config.BIGQUERY_STAGING_DATASET

    def write_batch_to_bigquery(self, batch_df, batch_id, table_name, key_field):
        """
        Write each batch to staging table in BigQuery and MERGE to final destination.

        Args:
            batch_df: Spark DataFrame containing the batch data.
            batch_id: Unique identifier for this batch.
            table_name (str): Target BigQuery table name.
            key_field (str): Primary key field for deduplication.
        """

        if batch_df.count() == 0:
            self.logger.info(f"Batch {batch_id} is empty, skipping...")
            return

        self.logger.info(
            f"Writing batch {batch_id} with {batch_df.count()} records to {table_name} table..."
        )

        deduplicated_df = batch_df.dropDuplicates([key_field])
        staging_table = f"{self.project_id}.{self.staging_dataset}.staging_{table_name}"

        self._write_to_staging_table(deduplicated_df, staging_table)
        self._merge_staging_to_main(
            staging_table, table_name, key_field, deduplicated_df.columns
        )
        self.logger.info(f"Batch {batch_id} merged successfully into {table_name}!")

    def _write_to_staging_table(self, df, staging_table):
        """
        Write DataFrame to staging table.

        Args:
            df: Spark DataFrame to write.
            staging_table (str): Full staging table name.
        """

        df.write.format("bigquery")\
            .option("table", staging_table)\
            .option("writeMethod", "direct")\
            .option("createDisposition", "CREATE_IF_NEEDED")\
            .option("parentProject", self.project_id)\
            .option("credentialsFile", self.credentials_path)\
            .mode("overwrite")\
            .save()

    def _merge_staging_to_main(self, staging_table, table_name, key_field, columns):
        """
        Merge staging table data into main table.

        Args:
            staging_table (str): Full staging table name.
            table_name (str): Main table name.
            key_field (str): Primary key field for the merge condition.
            columns (list): List of column names for the insert operation.
        """

        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"S.{col}" for col in columns])

        merge_sql = f"""
        MERGE `{self.project_id}.{self.dataset}.{table_name}` T
        USING `{staging_table}` S
        ON T.{key_field} = S.{key_field}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns}) VALUES ({insert_values})
        """

        job = self.client.query(merge_sql)
        job.result()
