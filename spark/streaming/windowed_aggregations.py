"""
Windowed aggregation functions for streaming data.

This module provides functions to aggregate streaming news data into
10-minute windows for capturing trends.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    window,
    col,
    count,
)


logger = logging.getLogger(__name__)


def create_10min_word_aggregates(df: DataFrame) -> DataFrame:
    """
    Aggregate word occurrences across all websites into 10-minute windows.
    Groups all occurrences by word and time window to calculate total occurrences.

    Args:
        df: Streaming DataFrame with occurrence data including timestamp

    Returns:
        DataFrame: Total occurrence count per word per 10-minute window
    """
    logger.info("Creating 10-minute word aggregations across all sources")

    aggregated_df = (
        df.groupBy(
            window(col("timestamp"), "10 minutes"),
            col("word_id"),
        )
        .agg(
            count("*").alias("total_occurrences"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("word_id"),
            col("total_occurrences"),
        )
    )

    logger.info("Finished creating 10-minute word aggregations.")

    return aggregated_df
