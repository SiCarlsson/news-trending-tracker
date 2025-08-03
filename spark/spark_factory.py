"""
Spark session factory for the news trending tracker.

This module provides a factory class for creating properly configured
Spark sessions with all necessary dependencies and optimizations.
"""

from pyspark.sql import SparkSession
from config import Config


class SparkSessionFactory:
    """Factory class for creating configured Spark sessions."""

    @staticmethod
    def create_session():
        """
        Create and configure Spark session.

        Returns:
            SparkSession: Configured Spark session ready for streaming.
        """
        config = Config()

        return (
            SparkSession.builder.appName(config.SPARK_APP_NAME)
            .config("spark.jars.packages", config.spark_packages_string)
            .config(
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                config.BIGQUERY_CREDENTIALS_PATH,
            )
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config(
                "spark.hadoop.google.cloud.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE"
            )
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )
