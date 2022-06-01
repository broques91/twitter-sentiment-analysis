from kafka import KafkaConsumer
from pymongo import MongoClient
import json


# Connect to MongoDB and tweets database
try:
   client = MongoClient('localhost',27017)
   db = client.twitter
   print("Connected successfully!")
except:  
   print("Could not connect to MongoDB")

topic_name = 'twitter'

# connect kafka consumer to desired kafka topic	
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['twitter_kafka:9096', 'twitter_kafka:9097', 'twitter_kafka:9098'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Parse received data from Kafka
for message in consumer:
    record = json.loads(json.dumps(message.value))
    print(record)

    content = record['text']

    # Create dictionary and ingest data into MongoDB
    try:
       tweet_rec = {'content':content}
       rec_id1 = db.tweets.insert_one(tweet_rec)
       print("Data inserted with record ids", rec_id1)
    except:
       print("Could not insert into MongoDB")


