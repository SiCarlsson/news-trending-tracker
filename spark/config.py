"""
Configuration settings for the Spark application.

Module to provide configuration management for the Spark application, loading
settings from environment variables and .env files with sensible defaults.
"""

import os
from pathlib import Path

try:
    from dotenv import load_dotenv

    # Load from root .env file
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    # python-dotenv not installed, continue with os.getenv defaults
    pass


class Config:
    """
    Configuration class for Spark application settings.

    Loads configuration from environment variables with fallback defaults.
    """

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")

    # BigQuery Configuration
    BIGQUERY_PROJECT_ID = os.getenv(
        "TF_VAR_bigquery_project_id", "news-trending-tracker"
    )
    BIGQUERY_DATASET = os.getenv("TF_VAR_bigquery_dataset_id", "scraper_data")
    BIGQUERY_STAGING_DATASET = os.getenv(
        "TF_VAR_bigquery_staging_dataset_id", "staging_scraper_data"
    )
    BIGQUERY_CREDENTIALS_PATH = os.getenv(
        "BIGQUERY_CREDENTIALS_PATH",
        "../credentials/backend-bigquery-service-account.json",
    )

    # Spark Configuration
    SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "NewsStreamProcessor")
    SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "local[*]")
    SPARK_CHECKPOINT_LOCATION = os.getenv(
        "SPARK_CHECKPOINT_LOCATION", "/tmp/spark_checkpoint"
    )
    SPARK_PROCESSING_INTERVAL = os.getenv("SPARK_PROCESSING_INTERVAL", "60 seconds")

    # Spark Packages
    SPARK_PACKAGES = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
    ]

    @property
    def spark_packages_string(self):
        """
        Get Spark packages as a comma-separated string.

        Returns:
            str: Comma-separated string of Spark package coordinates.
        """
        return ",".join(self.SPARK_PACKAGES)
