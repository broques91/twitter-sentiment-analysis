import tweepy
from kafka import KafkaProducer
import json

def stream(api,data):
    i = 0
    for tweet in api.search_tweets(q=data, count=20, lang='fr'):
        print(tweet.text)
        producer.send(topic_name, tweet._json)
        i+=1
        if i == 200:
            break
        else:
            pass


consumerKey = "t"
consumerSecret = "t"
accessToken = "t"
accessTokenSecret = "t"

producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
                            value_serializer=lambda m: json.dumps(m).encode('utf-8'))
topic_name = 'twitter'

auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
auth.set_access_token(accessToken, accessTokenSecret)
api = tweepy.API(auth)
# for tweet in api.search_tweets('elections', lang='fr'):
#     producer.send(topic_name, tweet.text.encode('utf-8'))


stream(api, data = ['élections'])