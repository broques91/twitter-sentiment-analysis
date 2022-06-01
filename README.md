# Twitter Sentiment Analysis

## Overview
This application will collect some tweets, push to Kafka via tweepy, use PySpark to monitor the topic and push to a mongoDB database. Finally Streamlit is used to show a real time view of the data being generated.


### Starting the Services
Services need to be started in a specific order with the following commands:
```
# Start Kafka and MongoDB
docker-compose up -d kafka db

# Start Streamlit
docker-compose up -streamlit

# Start the producer and the consumer
docker-compose up -d producer consumer
```