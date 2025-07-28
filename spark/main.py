from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from schemas import NewsSchemas

topics = ["news-articles", "news-occurrences", "news-words", "news-websites"]

spark = SparkSession.builder \
    .appName("NewsStreamProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "news-articles") \
    .load()

# Display the streamed data
df.selectExpr("CAST(value AS STRING)").writeStream \
.outputMode("append") \
.format("console") \
.start() \
.awaitTermination()