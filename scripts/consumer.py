import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from textblob import TextBlob
from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer


def getSubjectivity(tweet: str) -> float:
    """
    It takes a tweet as a string, and returns a float between 0 and 1, where 0 is completely objective
    and 1 is completely subjective

    :param tweet: str - The tweet to be analyzed
    :type tweet: str
    :return: The subjectivity of the tweet.
    """
    return TextBlob(
        tweet, pos_tagger=PatternTagger(), analyzer=PatternAnalyzer()
    ).sentiment[1]


def getPolarity(tweet: str) -> float:
    """
    It takes a tweet as a string, and returns a float between -1 and 1, where -1 is the most negative
    and 1 is the most positive

    :param tweet: str - The tweet to be analyzed
    :type tweet: str
    :return: The polarity of the tweet.
    """
    return TextBlob(
        tweet, pos_tagger=PatternTagger(), analyzer=PatternAnalyzer()
    ).sentiment[0]


def getSentiment(polarityValue: float) -> str:
    """
    If the polarity value is less than 0, return "Negative". If the polarity value is equal to 0, return
    "Neutral". Otherwise, return "Positive"

    :param polarityValue: The polarity value of the tweet
    :type polarityValue: float
    :return: A string
    """
    if polarityValue < 0:
        return "Negative"
    elif polarityValue == 0:
        return "Neutral"
    else:
        return "Positive"


# Connect to MongoDB and tweets database
# Trying to connect to the MongoDB database.
try:
    client = MongoClient("db", username="root", password="secret")
    db = client.tweets
    print("Connected successfully!")
except:
    print("Could not connect to MongoDB")

collection = db.elections
topic_name = "twitter"

# connect kafka consumer to desired kafka topic
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=["kafka:9092"],
    api_version=(0, 11, 5),
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

# Parse received data from Kafka
for message in consumer:
    print(message.value)
    record = json.loads(json.dumps(message.value))
    content = record["text"]
    print(content)

    # output sentiment
    polarity_value = getPolarity(content)
    sentiment = getSentiment(polarity_value)
    print("Sentiment: ", sentiment)
    print("")

    # Create dictionary and ingest data into MongoDB
    try:
        tweet_rec = {
            "content": content.encode("utf-8", "strict"),
            "sentiment": sentiment.encode("utf-8", "strict"),
        }
        rec_id1 = collection.insert_one(tweet_rec)
        print("Data inserted with record ids", rec_id1)

    except Exception as e:
        print(e)
        print("Could not insert into MongoDB")
