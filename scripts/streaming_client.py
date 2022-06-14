import os
import tweepy
import json
import time
import logging
import configparser

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

#bearer_token = os.environ["BEARER_TOKEN"]
bearer_token = "AAAAAAAAAAAAAAAAAAAAAKWTdAEAAAAA6OTAY4ENATjL8IJPRMorUbOVNZg%3Di8jbokZlzHtlAeSaHuBR3AaCcy0A1F4A2zCBy4NZ8apn2l5K7x"

class MyStreamer(tweepy.StreamingClient):

    def on_connect(self):
        print("Connected")

    def on_tweet(self, tweet):
        if tweet.referenced_tweets == None:
            #print(tweet)
            print(tweet.id)
            print(tweet.text)
            print(tweet.created_at)
            print("======")
            time.sleep(0.2)
        #producer.send("twitter", tweet.text)


if __name__ == "__main__":
    config = configparser.ConfigParser()

    # Kafka
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        api_version=(0,11,5),
        #value_serializer=lambda t: json.dumps(t).encode('utf-8')
    )

    topic_name = 'twitter'

    # Setup logging
    logging.basicConfig(
        level = logging.INFO,
        format = "[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")
    
    # stream
    logging.info("Starting publishing...")
    streamer = MyStreamer(bearer_token)
    streamer.add_rules(tweepy.StreamRule("elections lang:fr"))
    streamer.filter(tweet_fields=["referenced_tweets","created_at"])