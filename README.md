# Twitter Sentiment Analysis

## Overview

This application will collect some tweets, push to Kafka via tweepy, use PySpark to monitor the topic and push to a mongoDB database. Finally Streamlit is used to show a real time view of the data being generated.

## Requirements

- Get an access to the Twitter API via [How to get access to the Twitter API](https://developer.twitter.com/en/docs/twitter-api/getting-started/getting-access-to-the-twitter-api)
  
## Setup

- Clone the repo
- After obtaining your set of Twitter API keys and tokens, you have to set those in the secret.ini file :

```
[twitter]
consumer_key=xxxx
consumer_secret=xxxx
access_token=xxxx
access_token_secret=xxxx
```

### Starting the Services

Services need to be started in a specific order with the following commands:
```
# Start Kafka and MongoDB
docker-compose up -d kafka db

# Start Streamlit
docker-compose up streamlit

# Start the producer and the consumer
docker-compose up -d producer consumer
```