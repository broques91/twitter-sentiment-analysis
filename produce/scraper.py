import json
import logging
import snscrape.modules.twitter as sntwitter

from kafka import KafkaProducer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
)


# Kafka
producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    api_version=(0, 11, 5),
    value_serializer=lambda t: json.dumps(t, default=str).encode("utf-8"),
)

topic_name = "twitter"

query = "elections lang:fr -is:retweet"
# query = 'elections lang:fr -is:retweet until-2022-06-01 since:2020-01-01'

tweets = []
limit = 100

logging.info("Starting publishing...")
for tweet in sntwitter.TwitterSearchScraper(query).get_items():
    data = vars(tweet)
    if len(tweets) == limit:
        break
    else:
        # send to producer
        producer.send(topic_name, data)
