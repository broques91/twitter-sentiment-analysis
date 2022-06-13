import streamlit as st
import pandas as pd
import plotly.express as px

from pymongo import MongoClient
from datetime import datetime
#from streamlit_autorefresh import st_autorefresh

# update every 5 mins
#st_autorefresh(interval=5 * 60 * 1000, key="dataframerefresh")


st.set_page_config(
    page_title="Real-Time Twitter Sentiment Analysis Dashboard",
    page_icon="✅",
    layout="wide",
)

st.title("Twitter Sentiment Analysis")
st.sidebar.title("Tweets about Elections FR")


# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    return MongoClient(
        host='db',
        username='root',
        password='secret'
    )


# Pull data from the collection.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def get_data():
    db = client.tweets
    items = db.elections.find()
    items = list(items)  # make hashable for st.experimental_memo
    return items


def create_df_from_list(items):
    tweets = []
    feelings = []

    # Remove _id
    for d in items:
        d.pop('_id')
    
    # Print results.
    for item in items:
        tweet = item['content'].decode('utf-8')
        sentiment = item['sentiment'].decode('utf-8')
        tweets.append(tweet)
        feelings.append(sentiment)
        #st.write(f"{tweet}")

    data = {'Tweets':tweets,'Sentiment':feelings}
    # Create the pandas DataFrame
    df = pd.DataFrame(data)
    return df


st.markdown("### Tweets")

client = init_connection()

items = get_data()

df = create_df_from_list(items)
st.write(df.head())


# Charts
st.sidebar.markdown("### Number of tweets by sentiment")
select = st.sidebar.selectbox('Visualization type', ['Histogram', 'Pie Chart'], key='1')

sentiment_count = df['Sentiment'].value_counts()
# st.write(sentiment_count)
sentiment_count = pd.DataFrame({'Sentiment': sentiment_count.index, 'Tweets': sentiment_count.values})

if not st.sidebar.checkbox("Hide", True):
    st.markdown("### Number of tweets by sentiment")
    if select == "Histogram":
        fig = px.bar(sentiment_count, x='Sentiment', y='Tweets', color='Tweets', height=500)
        st.plotly_chart(fig)
    else:
        fig = px.pie(sentiment_count, values='Tweets', names='Sentiment')
        st.plotly_chart(fig)
