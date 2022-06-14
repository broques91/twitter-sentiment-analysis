import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from textblob import TextBlob
from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer


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
    print(message.value)
    record = json.loads(json.dumps(message.value))
    #print(record)
    content = record["text"]
    print(content)
    #created_at = record["created_at"]
    #location = record["location"]

    # output sentiment
    polarity_value = getPolarity(content)
    sentiment = getSentiment(polarity_value)
    print("Sentiment: ", sentiment)
    print("")

    # Create dictionary and ingest data into MongoDB
    try:
        tweet_rec = {
        'content': content.encode('utf-8','strict'),
        'sentiment': sentiment.encode('utf-8','strict'),
        #'created_at': created_at.encode('utf-8','strict'),
        #'location': content.encode('utf-8','strict'),
        }
        rec_id1 = collection.insert_one(tweet_rec)
        print("Data inserted with record ids", rec_id1)

    except Exception as e:
        print(e)
        print("Could not insert into MongoDB")


