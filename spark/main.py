from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

topics = ["news-articles", "news-occurrences", "news-words", "news-websites"]

word_schema = StructType(
    [
        StructField("word_id", StringType(), False),
        StructField("word_text", StringType(), False),
    ]
)

spark = (
    SparkSession.builder.appName("NewsStreamProcessor")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2",
    )
    .config(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        "../credentials/backend-bigquery-service-account.json",
    )
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
    .getOrCreate()
)


def create_sample_word_data():
    """Create sample word data that matches the BigQuery schema"""
    word_data = [
        ("word_001", "politik"),
        ("word_002", "ekonomi"),
        ("word_003", "sverige"),
        ("word_004", "köper"),
        ("word_005", "säljer"),
    ]

    word_df = spark.createDataFrame(word_data, word_schema)
    return word_df


def write_to_bigquery(df, table_name):
    """Write DataFrame to BigQuery table"""
    df.write.format("bigquery").option(
        "table", f"news-trending-tracker.scraper_data.{table_name}"
    ).option("writeMethod", "direct").mode("append").save()


if __name__ == "__main__":
    word_df = create_sample_word_data()
    print("Sample word data created:")
    word_df.show()
    print("Writing sample word data to BigQuery...")
    write_to_bigquery(word_df, "words")
    print("Sample word data written to BigQuery successfully.")
