import json
import snscrape.modules.twitter as sntwitter

from kafka import KafkaProducer


# Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
    api_version=(0,11,5),
    value_serializer=lambda t: json.dumps(t, default=str).encode('utf-8')
)

topic_name = 'twitter'

query = 'elections lang:fr -is:retweet'
#query = 'elections lang:fr -is:retweet until-2022-06-01 since:2020-01-01'
tweets = []
limit = 100

for tweet in sntwitter.TwitterSearchScraper(query).get_items():
    #print(vars(tweet))
    data = vars(tweet)
    #break
    if len(tweets) == limit:
        break
    else:
        # send to producer
        producer.send(topic_name, data)