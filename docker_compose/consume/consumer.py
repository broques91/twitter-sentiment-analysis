
import re
from datetime import datetime

from pyspark.sql.types import StringType, StructType, StructField, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer

import pymongo

import findspark
findspark.init()


def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # enlever utilisateur
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # enlever ponctuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # enlever les nombres
    tweet = re.sub('([0-9]+)', '', str(tweet))

    # enlever les hashtag
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    return tweet

def getSubjectivity(tweet: str) -> float:
    return TextBlob(tweet, pos_tagger=PatternTagger(), analyzer=PatternAnalyzer()).sentiment[1]

def getPolarity(tweet: str) -> float:
    return TextBlob(tweet, pos_tagger=PatternTagger(), analyzer=PatternAnalyzer()).sentiment[0]

def getSentiment(polarityValue: float) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'


class write_row_in_mongo:
    def open(self, partition_id, epoch_id):
        self.myclient = pymongo.MongoClient('db', username='root', password='secret')
        self.mydb = self.myclient["tweets"]
        self.mycol = self.mydb["elections"]
        return True

    def process(self, row):
        self.mycol.insert_one(row.asDict())
        
        
        
    def close(self, error):
        self.myclient.close()
        return True



def main():
    spark = SparkSession\
            .builder\
            .master("spark://spark:7077")\
            .appName("twitter_sentiment")\
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .getOrCreate()

    tweets = spark\
          .readStream\
          .format("kafka")\
          .option("kafka.bootstrap.servers", "kafka:9092")\
          .option("subscribe", "twitter")\
          .load()
              

    mySchema = StructType([StructField("content", StringType(), True)])
    mySchema2 = StructType([StructField("date", StringType(), True)])
    values = tweets.select(from_json(tweets.value.cast("string"), mySchema).alias("tweet"),from_json(tweets.value.cast("string"),mySchema2).alias("date"))    
    
    df1 = values.select("tweet.*","date.*")
    
    clean_tweets = udf(cleanTweet, StringType())
    raw_tweets = df1.withColumn('processed_text', clean_tweets(col("content")))
    
    
    subjectivity = udf(getSubjectivity, FloatType())
    polarity = udf(getPolarity, FloatType())
    sentiment = udf(getSentiment, StringType())

    subjectivity_tweets = raw_tweets.withColumn('subjectivity', subjectivity(col("processed_text")))
    polarity_tweets = subjectivity_tweets.withColumn("polarity", polarity(col("processed_text")))
    sentiment_tweets = polarity_tweets.withColumn("sentiment", sentiment(col("polarity")))
    
    sentiment_tweets.writeStream.foreach(write_row_in_mongo()).start().awaitTermination()

    # raw_tweets.writeStream\
    #        .outputMode('update')\
    #        .format('console')\
    #        .start()\
    #         .awaitTermination()

if __name__ == "__main__":
    main()







    


