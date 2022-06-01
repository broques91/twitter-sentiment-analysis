from sys import api_version
import tweepy
import logging

from kafka import KafkaProducer
import configparser
import json
import os


def stream(data):
    i = 0
    for tweet in api.search_tweets(q=data, count=100, lang='fr'):
        #print(tweet.text)
        print(tweet._json)
        producer.send(topic_name, tweet._json)
        i+=1
        if i == 100:
            break
        else:
            pass


if __name__ == "__main__":
    config = configparser.ConfigParser()

    # kafka
    producer = KafkaProducer(
        bootstrap_servers=['twitter_kafka:9096', 'twitter_kafka:9097', 'twitter_kafka:9098'],
        value_serializer=lambda t: json.dumps(t).encode('utf-8')
        )
    topic_name = 'twitter'

    # Setup logging
    logging.basicConfig(
        level = logging.INFO,
        format = "[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")

    try:
        config.read('config.ini')
        consumer_key = config['twitter']['api_key']
        consumer_secret = config['twitter']['api_key_secret']
        access_token = config['twitter']['access_token']
        access_token_secret = config['twitter']['access_token_secret']
        logging.info("Twitter API credentials parsed.")
    except KeyError as e:
        logging.error("Config file not found. Make sure it is available in the directory.")
        exit()
    except AttributeError as e:
        logging.error("Cannot read Twitter API credentials. Make sure that API key and secret are in the config.ini file (also check spelling).")
        exit()

    # authenticate
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    # stream
    logging.info("Starting publishing...")
    stream(data = ['elections'])

    producer.flush()
