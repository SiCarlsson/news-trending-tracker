from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from google.cloud import bigquery

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
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
    )
    .config(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        "/Users/scarlsson/Programmering/Current projects/news-trending-tracker/credentials/backend-bigquery-service-account.json",
    )
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
    .getOrCreate()
)


def read_from_kafka(topic):
    """Read data from Kafka topic and return DataFrame"""
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .load()
    )

    parsed_df = df.select(
        from_json(col("value").cast("string"), word_schema).alias("data")
    ).select("data.*")

    return parsed_df


def write_to_bigquery(df, table_name):
    """Write streaming DataFrame to BigQuery"""

    bq_client = bigquery.Client.from_service_account_json(
        "../credentials/backend-bigquery-service-account.json"
    )

    def write_batch(batch_df, batch_id):
        """Write each batch to BigQuery using MERGE"""
        if batch_df.count() > 0:
            print(
                f"Writing batch {batch_id} with {batch_df.count()} records to BigQuery..."
            )

            deduplicated_df = batch_df.dropDuplicates(["word_id"])

            staging_table = f"news-trending-tracker.scraper_data.staging_{table_name}"

            deduplicated_df.write.format("bigquery").option(
                "table", staging_table
            ).option("writeMethod", "direct").option(
                "createDisposition", "CREATE_IF_NEEDED"
            ).mode("overwrite").save()

            merge_sql = f"""
            MERGE `news-trending-tracker.scraper_data.{table_name}` T
            USING `{staging_table}` S
            ON T.word_id = S.word_id
            WHEN NOT MATCHED THEN
              INSERT (word_id, word_text) VALUES (S.word_id, S.word_text)
            """

            job = bq_client.query(merge_sql)
            job.result()

            print(f"Batch {batch_id} merged successfully!")

    query = (
        df.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", f"/tmp/checkpoint/{table_name}")
        .outputMode("append")
        .trigger(processingTime='60 seconds')
        .start()
    )
    return query


if __name__ == "__main__":
    print("\nStarting Kafka -> Spark -> BigQuery streaming...")

    word_df = read_from_kafka("news-words")
    print("Setting up streaming query to write word data to BigQuery...")

    query = write_to_bigquery(word_df, "words")

    print("\nStreaming query started. Writing word data to BigQuery...")
    print(f"Query ID: {query.id}\n")

    query.awaitTermination()
