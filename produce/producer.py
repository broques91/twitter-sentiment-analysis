import tweepy
from tweepy import StreamingClient, StreamRule
import os
import re
import json
from kafka import KafkaProducer
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

bearer_token = os.getenv("bearer_token")


# Kafka
producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    api_version=(0, 11, 5),
    value_serializer=lambda t: json.dumps(t, default=str).encode("utf-8"),
)

topic_name = "twitter"


class TweetPrinterV2(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        #print(f"{tweet.id} {tweet.created_at}: {tweet.text}")
        data = tweet.data
        data["id"] = (tweet.id)
        data["created_at"] = str(tweet.created_at)
        producer.send(topic_name, data)
        #print("-" * 50)


printer = TweetPrinterV2(bearer_token)

# clean-up pre-existing rules
rule_ids = []
result = printer.get_rules()
for rule in result.data:
    print(f"rule marked to delete: {rule.id} - {rule.value}")
    rule_ids.append(rule.id)

if len(rule_ids) > 0:
    printer.delete_rules(rule_ids)
    printer = TweetPrinterV2(bearer_token)
else:
    print("no rules to delete")

# add new rules
rule = StreamRule(value="elections lang:fr")
printer.add_rules(rule)

printer.filter(tweet_fields="created_at")

printer.filter()
