"""
Schema definitions and topic configuration for the Kafka topics and
BigQuery table structures.

This module contains PySpark schema definitions for all data types in the
news trending tracker system and configuration mappings for Kafka topics
to BigQuery tables.
"""

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

website_schema = StructType(
    [
        StructField("website_id", StringType(), False),
        StructField("website_name", StringType(), False),
        StructField("website_url", StringType(), False),
    ]
)

article_schema = StructType(
    [
        StructField("article_id", StringType(), False),
        StructField("website_id", StringType(), False),
        StructField("article_title", StringType(), False),
        StructField("article_url", StringType(), False),
    ]
)

word_schema = StructType(
    [
        StructField("word_id", StringType(), False),
        StructField("word_text", StringType(), False),
    ]
)

occurrence_schema = StructType(
    [
        StructField("occurrence_id", StringType(), False),
        StructField("word_id", StringType(), False),
        StructField("website_id", StringType(), False),
        StructField("article_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
    ]
)

topic_config = {
    "news-websites": {
        "schema": website_schema,
        "table": "websites",
        "key": "website_id",
    },
    "news-articles": {
        "schema": article_schema,
        "table": "articles",
        "key": "article_id",
    },
    "news-words": {
        "schema": word_schema,
        "table": "words",
        "key": "word_id",
    },
    "news-occurrences": {
        "schema": occurrence_schema,
        "table": "occurrences",
        "key": "occurrence_id",
    },
}
