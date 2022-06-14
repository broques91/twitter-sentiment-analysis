import time
import tweepy
import logging

from kafka import KafkaProducer
import configparser
import json


def stream(data):
    i = 0
    for tweet in api.search_tweets(q=data, count=10, lang='fr'):
        producer.send(topic_name, tweet._json)
        time.sleep(5)
        i+=1
        if i == 1000:
            break
        else:
            pass


if __name__ == "__main__":
    config = configparser.ConfigParser()

    # Kafka
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        api_version=(0,11,5),
        value_serializer=lambda t: json.dumps(t).encode('utf-8')
        )

    topic_name = 'twitter'

    # Setup logging
    logging.basicConfig(
        level = logging.INFO,
        format = "[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")
    
    # Read credentials
    try:
        config.read('secret.ini')
        CONSUMER_KEY = config['twitter']['consumer_key']
        CONSUMER_SECRET = config['twitter']['consumer_secret']
        ACCESS_TOKEN = config['twitter']['access_token']
        ACCESS_TOKEN_SECRET = config['twitter']['access_token_secret']
        logging.info("Twitter API credentials parsed.")
    except KeyError as e:
        logging.error("Config file not found. Make sure it is available in the directory.")
        exit()
    except AttributeError as e:
        logging.error("Cannot read Twitter API credentials. Make sure that API key and secret are in the config.ini file (also check spelling).")
        exit()

    # authenticate
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)

    # stream
    logging.info("Starting publishing...")
    stream(data = ['elections'])

