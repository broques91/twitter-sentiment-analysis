import tweepy


def getClient():
    client = tweepy.Client(bearer_token=bearer_token,
                           consumer_key=consumer_key,
                           consumer_secret=consumer_secret,
                           access_token=access_token,
                           access_token_secret=access_token_secret)
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