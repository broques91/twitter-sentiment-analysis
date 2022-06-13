import streamlit as st
import time
import numpy as np
import pandas as pd
import plotly.express as px

from pymongo import MongoClient
from datetime import datetime

st.set_page_config(
    page_title="Real-Time Twitter Sentiment Analysis Dashboard",
    page_icon="✅",
    layout="wide",
)

# dashboard title
st.title("Real-Time Twitter Sentiment Analysis Dashboard")

# creating a single-element container.
placeholder = st.empty()

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

    data = {'tweet':tweets,'sentiment':feelings}
    # Create the pandas DataFrame
    df = pd.DataFrame(data)
    return df


client = init_connection()

items = get_data()

df = create_df_from_list(items)


while True:
    # Create random col for test
    df['random'] = np.random.randint(1, 6, df.shape[0])

    # creating KPIs 
    count_tweets = len(df)
    count_positive = int(df[(df["sentiment"]=='Positive')]['sentiment'].count() + np.random.choice(range(1,30)))
    count_neutral = int(df[(df["sentiment"]=='Neutral')]['sentiment'].count() + np.random.choice(range(1,30)))
    count_negative = int(df[(df["sentiment"]=='Negative')]['sentiment'].count() + np.random.choice(range(1,30)))

    avg_positive = count_positive / count_tweets
    avg_neutral = count_neutral / count_tweets
    avg_negative = count_negative / count_tweets

    with placeholder.container():
        # create three columns
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)

        # fill in those three columns with respective metrics or KPIs 
        kpi1.metric(label="Tweets", value=int(count_tweets), delta= round(count_tweets) - 10)
        kpi2.metric(label="Positive  😊", value=round(avg_positive), delta= round(avg_positive) - 10)
        kpi3.metric(label="Neutral  😐", value=round(avg_positive), delta= round(avg_positive) - 10)
        kpi4.metric(label="Negative  😒", value=round(avg_negative), delta= round(avg_positive) - 10)


        fig_col1, fig_col2 = st.columns(2)
        with fig_col1:
            st.markdown("### First Chart")
            fig = px.density_heatmap(data_frame=df, y='random', x='sentiment')
            st.write(fig)
        with fig_col2:
            st.markdown("### Second Chart")
            fig2 = px.histogram(data_frame=df, x='sentiment')
            st.write(fig2)

        st.markdown("### Detailed Data View")
        st.dataframe(df)
        time.sleep(1)

