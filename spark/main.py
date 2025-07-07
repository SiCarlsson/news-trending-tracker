from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaToBigQuery") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

topics = ["news-articles", "news-occurrences", "news-websites", "news-words"]

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", ",".join(topics)) \
    .load()

query = df.writeStream \
    .format("console") \
    .start()

query.awaitTermination()
