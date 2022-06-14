import tweepy
import json


def getClient():
    client = tweepy.Client(bearer_token="AAAAAAAAAAAAAAAAAAAAAKWTdAEAAAAA6OTAY4ENATjL8IJPRMorUbOVNZg%3Di8jbokZlzHtlAeSaHuBR3AaCcy0A1F4A2zCBy4NZ8apn2l5K7x",
                           consumer_key="WXpCxS68Re9YRWVeNjUxeZ9e3",
                           consumer_secret="dChcmqmrnnmJ7S0W06GSc4jg7vz9nl6YkXuCOihUPV6svpMyGh",
                           access_token="257547724-KpHxkQ2CoibOwZZciWUSCoESOxTOD20t6yZmuf4g",
                           access_token_secret="plIyhNgRskYx566c4eXM9Pia3CZRxSu4QOVWUxBQCBE91")
    return client

  
def searchTweets(client, query, max_results, fields):

    tweets = client.search_recent_tweets(query=query, max_results=max_results, tweet_fields=fields)

    tweet_data =  tweets.data
    results = []

    print(tweet_data)

    if not tweet_data is None and len(tweet_data) > 0:
        for tweet in tweet_data:
            obj = {}
            obj['id'] = tweet.id
            obj['text'] = tweet.text
            obj['created_at'] = tweet.created_at
            obj['geo'] = tweet.created_at
            results.append(obj)

    return results


def return50Tweets(query):
    query = '{} lang:fr -is:retweet'.format(query)
    fields = ['author_id','created_at','text','geo']

    client = getClient()
    tweets = searchTweets(client, query, 50, fields)

    objs = []

    if len(tweets) > 0:
        for tweet in tweets:
            obj = {}
            obj['text'] = tweet['text']
            objs.append(obj)

            print(tweet)

return50Tweets("ukraine")