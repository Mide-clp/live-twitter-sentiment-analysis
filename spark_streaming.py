from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import FloatType
from textblob import TextBlob


# from pyspark.sql.functions import r
TOPIC = "tweets_loader"


# clean tweet texts by removing hashtags, line, @ and RT and...
def clean_tweet(lines):
    text_tweet = df.select(func.col("value").cast("string"))
    words = text_tweet.select(func.explode(func.split(func.col("value"), "t_end")).alias("word"))
    words = words.na.replace("", None)
    words = words.na.drop()
    words = words.withColumn("word", func.regexp_replace("word", r"http\S+", ""))
    words = words.withColumn("word", func.regexp_replace("word", "@\w+", ""))
    words = words.withColumn("word", func.regexp_replace("word", "#", ""))
    words = words.withColumn("word", func.regexp_replace("word", "RT", ""))
    words = words.withColumn("word", func.regexp_replace("word", ":", ""))
    return words


# apply sentiment analysis

# get polarity score
# Polarity lies between [-1,1], -1 defines a negative sentiment and 1 defines a positive sentiment.
def getpolarity_score(text):
    return TextBlob(text).sentiment.polarity


# get subjectivity score
# Subjectivity lies between [0,1] Subjectivity quantifies the amount of personal opinion and factual information
# contained in the text. The higher subjectivity means that the text contains personal opinion rather than factual information.
def getsubjectivity_score(text):
    return TextBlob(text).sentiment.subjectivity


def text_sentiment(word):
    # creating user defined function (udf) for use in spark dataframe
    getpolarity_score_udf = func.udf(getpolarity_score, FloatType())
    getsubjectivity_score_udf = func.udf(getsubjectivity_score, FloatType())

    word_sentiment_score = word.withColumn("polarity_score", getpolarity_score_udf(func.col("word")))
    word_sentiment_subjectivity = word_sentiment_score.withColumn("subjectivity_score",
                                                                  getsubjectivity_score_udf(func.col("word")))

    return word_sentiment_subjectivity


# write to mongo database
def write_to_mongo(text, epoch_id):
    text.write \
        .format("mongo") \
        .option("uri", "mongodb://127.0.0.1") \
        .option("database", "twitter") \
        .option("collection", "web3") \
        .mode("append") \
        .save()


# create spark session
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Stream-twitter-data").getOrCreate()

    # read tweet from kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest").load()

    # clean and read the data
    words_df = clean_tweet(df)

    # analyze text to define polarity and subjectivity
    word_sentiment = text_sentiment(words_df)

    word_json = word_sentiment.select(func.to_json(func.struct("word", "polarity_score", "subjectivity_score")).alias("value"))

    # write output to mongodb
    query = word_json.writeStream \
        .format("Kafka") \
        .option("topic", TOPIC) \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("checkpointLocation", "checkpoint") \
        .option("startingOffsets", "latest") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

    spark.stop()

# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 spark_streaming.py
