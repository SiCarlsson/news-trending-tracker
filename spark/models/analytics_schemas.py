"""
Analytics schemas for windowed analytics results.

This module contains PySpark schema definitions for analytics tables
that correspond to the BigQuery schemas defined in Terraform.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    FloatType,
)

activity_metrics_schema = StructType(
    [
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("website_id", StringType(), False),
        StructField("articles_delta", IntegerType(), False),
        StructField("unique_words_delta", IntegerType(), False),
        StructField("total_word_occurrences_delta", IntegerType(), False),
        StructField("articles_cumulative", IntegerType(), False),
        StructField("unique_words_cumulative", IntegerType(), False),
        StructField("total_word_occurrences_cumulative", IntegerType(), False),
        StructField("active_websites_cumulative", IntegerType(), False),
        StructField("avg_words_per_article", FloatType(), False),
        StructField("processing_timestamp", TimestampType(), False),
    ]
)

analytics_table_config = {
    "activity_metrics": {
        "schema": activity_metrics_schema,
        "table": "activity_metrics",
        "dataset": "metrics",
        "key": [
            "window_start",
            "window_end",
            "website_id",
        ],
        "partition_field": "window_start",
        "clustering_fields": ["website_id", "window_start"],
    }
}
