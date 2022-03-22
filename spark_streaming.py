from pyspark.sql import SparkSession
from pyspark.sql import functions as func
# from pyspark.
TOPIC = "tweets_loader"

# create spark session
spark = SparkSession.builder.master("local[*]").appName("Stream-twitter-data").getOrCreate()

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", TOPIC) \
    .option("startingOffsets", "latest").load()

df.printSchema()

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.2.1 spark_streaming.py
