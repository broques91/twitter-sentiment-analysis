import streamlit as st
import pymongo


# Add title on the page
st.title("Twitter Sentiment Analysis")

# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    return pymongo.MongoClient(
        host='mongo:27017',
        username='root',
        password='secret'
    )

client = init_connection()


# Pull data from the collection.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def get_data():
    db = client.twitter

    items = db.tweets.find()
    items = list(items)  # make hashable for st.experimental_memo
    return items

items = get_data()

# Print results.
for item in items:
    st.write(f"{item['content']}")

