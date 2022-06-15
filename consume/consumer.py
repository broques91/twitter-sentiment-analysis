import findspark

findspark.init("")
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, udf
from pyspark.sql.types import *
from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer
import pymongo
import re

print("modules imported")

KAFKA_TOPIC_NAME_CONS = "twitter"
KAFKA_BOOTSTRAP_SERVERS_CONS = "kafka:9092"

print("PySpark Structured Streaming with Kafka Application Started ...")

spark = (
    SparkSession.builder.master("spark://spark:7077")
    .appName("Real-Time Twitter Sentiment Analysis")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")
    .getOrCreate()
)

print("Now time to connect to Kafka broker to read Invoice Data")

spark.sparkContext.setLogLevel("ERROR")
print(" kafka Started ...")
# Construct a streaming DataFrame that reads from twitter
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
    .option("subscribe", KAFKA_TOPIC_NAME_CONS)
    .load()
)


def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r"http\S+", "", str(tweet))
    tweet = re.sub(r"bit.ly/\S+", "", str(tweet))
    tweet = tweet.strip("[link]")

    # enlever utilisateur
    tweet = re.sub("(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)", "", str(tweet))
    tweet = re.sub("(@[A-Za-z]+[A-Za-z0-9-_]+)", "", str(tweet))

    # enlever ponctuation
    my_punctuation = "!\"$%&'()*+,-./:;<=>?[\\]^_`{|}~•@â"
    tweet = re.sub("[" + my_punctuation + "]+", " ", str(tweet))

    # enlever les nombres
    tweet = re.sub("([0-9]+)", "", str(tweet))

    # enlever les hashtag
    tweet = re.sub("(#[A-Za-z]+[A-Za-z0-9-_]+)", "", str(tweet))

    return tweet


def getSubjectivity(tweet: str) -> float:
    return TextBlob(
        tweet, pos_tagger=PatternTagger(), analyzer=PatternAnalyzer()
    ).sentiment[1]


def getPolarity(tweet: str) -> float:
    return TextBlob(
        tweet, pos_tagger=PatternTagger(), analyzer=PatternAnalyzer()
    ).sentiment[0]


def getSentiment(polarityValue: float) -> str:
    if polarityValue < 0:
        return "Negative"
    elif polarityValue == 0:
        return "Neutral"
    else:
        return "Positive"


class WriteRowMongo:
    def open(self, partition_id, epoch_id):
        self.myclient = pymongo.MongoClient("db", username="root", password="secret")
        self.mydb = self.myclient["tweets"]
        self.mycol = self.mydb["elections"]
        return True

    def process(self, row):
        self.mycol.insert_one(row.asDict())

    def close(self, error):
        self.myclient.close()
        return True


mySchema = StructType([StructField("text", StringType(), True)])
mySchema2 = StructType([StructField("created_at", StringType(), True)])

values = df.select(
    from_json(df.value.cast("string"), mySchema).alias("tweet"),
    from_json(df.value.cast("string"), mySchema2).alias("date"),
)

df1 = values.select("tweet.*", "date.*")

# TODO
# mySchema = StructType(
#     [StructField("text", StringType(), True),
#     StructField("date", StringType(), True)]
# )

clean_tweets = udf(cleanTweet, StringType())
raw_tweets = df1.withColumn("processed_text", clean_tweets(col("text")))

subjectivity = udf(getSubjectivity, FloatType())
polarity = udf(getPolarity, FloatType())
sentiment = udf(getSentiment, StringType())

subjectivity_tweets = raw_tweets.withColumn(
    "subjectivity", subjectivity(col("processed_text"))
)
polarity_tweets = subjectivity_tweets.withColumn(
    "polarity", polarity(col("processed_text"))
)
sentiment_tweets = polarity_tweets.withColumn("sentiment", sentiment(col("polarity")))

# print("Output :")
# sentiment_tweets.writeStream.outputMode("update").format(
#     "console"
# ).start().awaitTermination()

# Write to MongoDB
sentiment_tweets.writeStream.foreach(WriteRowMongo()).start().awaitTermination()
