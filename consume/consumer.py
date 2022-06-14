import json
import re
from datetime import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient
from textblob import TextBlob
from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer


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


# Connect to MongoDB and tweets database
try:
   client = MongoClient('db', username='root', password='secret')
   db = client.tweets
   print("Connected successfully!")
except:  
   print("Could not connect to MongoDB")

collection = db.elections
topic_name = 'twitter'

# connect kafka consumer to desired kafka topic	
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['kafka:9092'],
    api_version=(0,11,5),
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Parse received data from Kafka
for message in consumer:
    record = json.loads(json.dumps(message.value))
    print(record)
    print("========")

    content = record["content"]
    created_at = record["date"]
    clean_tweet = cleanTweet(content)

    # TODO
    # Format Date

    # Predict sentiment
    polarity_value = getPolarity(content)
    sentiment = getSentiment(polarity_value)
    print("Sentiment: ", sentiment)
    print("")

    # Create dictionary and ingest data into MongoDB
    try:
        tweet_rec = {
            'date': created_at.encode('utf-8','strict'),
            'content': content.encode('utf-8','strict'),
            'clean_tweet': clean_tweet.encode('utf-8','strict'),
            'sentiment': sentiment.encode('utf-8','strict'),
        }
        rec_id1 = collection.insert_one(tweet_rec)
        print("Data inserted with record ids", rec_id1)

    except Exception as e:
        print(e)
        print("Could not insert into MongoDB")

    


