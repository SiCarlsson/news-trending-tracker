"""
Kafka streaming service for processing Kafka topics.

Module provides the KafkaStreamingService class that handles reading
data from Kafka topics, parsing JSON messages using predefined schemas,
and writing the processed data to BigQuery.
"""

import logging
from pyspark.sql.functions import from_json, col
from core.bigquery_writer import BigQueryWriter
from config import Config


class KafkaStreamingService:
    """
    Class handling Kafka streaming operations.

    Manages the complete streaming pipeline including reading from Kafka,
    parsing JSON data according to schemas, and writing to BigQuery tables.
    """

    def __init__(self, spark_session):
        """
        Initialize the streaming service.

        Args:
            spark_session: Active Spark session for streaming operations.
        """
        self.logger = logging.getLogger(__name__)
        self.spark = spark_session
        self.config = Config()
        self.bigquery_writer = BigQueryWriter()

    def read_from_kafka(self, topic, schema):
        """
        Read data from Kafka topic and return DataFrame.

        Args:
            topic (str): Kafka topic name to subscribe to.
            schema (StructType): PySpark schema for parsing JSON data.

        Returns:
            DataFrame: Parsed streaming DataFrame with topic data.
        """

        df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.config.KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", topic)
            .option("failOnDataLoss", "false")
            .option("startingOffsets", self.config.KAFKA_STARTING_OFFSETS)
            .load()
        )

        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        return parsed_df

    def create_streaming_query(self, df, table_name, key_field):
        """
        Create a streaming query that writes to BigQuery.

        Args:
            df (DataFrame): Source streaming DataFrame.
            table_name (str): Target BigQuery table name.
            key_field (str): Primary key field for deduplication.

        Returns:
            StreamingQuery: Active streaming query writing to BigQuery.
        """

        def write_batch(batch_df, batch_id):
            self.bigquery_writer.write_batch_to_bigquery(
                batch_df, batch_id, table_name, key_field
            )

        query = (
            df.writeStream.foreachBatch(write_batch)
            .option(
                "checkpointLocation",
                f"{self.config.SPARK_CHECKPOINT_LOCATION}/{table_name}",
            )
            .outputMode("append")
            .trigger(processingTime=self.config.SPARK_PROCESSING_INTERVAL)
            .start()
        )

        return query

    def process_topic(self, topic, config):
        """
        Process a single Kafka topic: read from Kafka and write to BigQuery.

        Args:
            topic (str): Kafka topic name to process.
            config (dict): Topic configuration containing schema, table, and key info.

        Returns:
            StreamingQuery: Active streaming query for this topic.
        """

        self.logger.info(f"Setting up streaming for topic: {topic}")

        df = self.read_from_kafka(topic, config["schema"])

        query = self.create_streaming_query(df, config["table"], config["key"])

        self.logger.info(
            f"Streaming query started for {topic} -> {config['table']} table"
        )
        self.logger.debug(f"Query ID: {query.id}")

        return query
