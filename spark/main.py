from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from google.cloud import bigquery

topics = ["news-articles", "news-occurrences", "news-words", "news-websites"]

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
    "news-words": {"schema": word_schema, "table": "words", "key": "word_id"},
    "news-occurrences": {
        "schema": occurrence_schema,
        "table": "occurrences",
        "key": "occurrence_id",
    },
}

spark = (
    SparkSession.builder.appName("NewsStreamProcessor")
    # .master("spark://localhost:7077")
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


def read_from_kafka(topic, schema):
    """Read data from Kafka topic and return DataFrame"""
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("failOnDataLoss", "false")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    return parsed_df


def write_to_bigquery(df, table_name, key_field):
    """Write streaming DataFrame to BigQuery"""

    bq_client = bigquery.Client.from_service_account_json(
        "../credentials/backend-bigquery-service-account.json"
    )

    def write_batch(batch_df, batch_id):
        """Write each batch to BigQuery using MERGE"""
        if batch_df.count() > 0:
            print(
                f"Writing batch {batch_id} with {batch_df.count()} records to {table_name} table..."
            )

            deduplicated_df = batch_df.dropDuplicates([key_field])

            staging_table = f"news-trending-tracker.scraper_data.staging_{table_name}"

            deduplicated_df.write.format("bigquery").option(
                "table", staging_table
            ).option("writeMethod", "direct").option(
                "createDisposition", "CREATE_IF_NEEDED"
            ).mode(
                "overwrite"
            ).save()

            # Build dynamic MERGE SQL based on table columns
            columns = deduplicated_df.columns
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"S.{col}" for col in columns])

            merge_sql = f"""
            MERGE `news-trending-tracker.scraper_data.{table_name}` T
            USING `{staging_table}` S
            ON T.{key_field} = S.{key_field}
            WHEN NOT MATCHED THEN
              INSERT ({insert_columns}) VALUES ({insert_values})
            """

            job = bq_client.query(merge_sql)
            job.result()

            print(f"Batch {batch_id} merged successfully into {table_name}!")

    query = (
        df.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", f"/tmp/spark_checkpoint/{table_name}")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )
    return query


if __name__ == "__main__":
    print("\nStarting Kafka -> Spark -> BigQuery streaming for all topics...")

    queries = []

    for topic, config in topic_config.items():
        print(f"Setting up streaming for topic: {topic}")

        df = read_from_kafka(topic, config["schema"])

        query = write_to_bigquery(df, config["table"], config["key"])
        queries.append(query)

        print(f"Streaming query started for {topic} -> {config['table']} table")
        print(f"Query ID: {query.id}")

    print(f"\nAll {len(queries)} streaming queries started successfully!")
    print("Topics being processed:", list(topic_config.keys()))

    for query in queries:
        query.awaitTermination()
