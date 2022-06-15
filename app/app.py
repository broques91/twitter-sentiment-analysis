import streamlit as st
import time
import pandas as pd
import plotly.express as px

from pymongo import MongoClient
from datetime import datetime

st.set_page_config(
    page_title="Real-Time Twitter Sentiment Analysis Dashboard",
    page_icon="✅",
    layout="wide",
)

st.write(
    """
    <style>
    [data-testid="stMetricDelta"] svg {
        display: none;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# dashboard title
st.title("Real-Time Twitter Sentiment Analysis Dashboard")

# creating a single-element container.
placeholder = st.empty()

# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    return MongoClient(host="db", username="root", password="secret")


# Pull data from the collection.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
# @st.experimental_memo(ttl=600)
def get_data():
    """
    It connects to the MongoDB database, finds all the tweets in the elections collection, and returns
    them as a list
    :return: A list of dictionaries.
    """
    db = client.tweets
    items = db.elections.find()
    items = list(items)  # make hashable for st.experimental_memo
    return items


def create_df_from_list(items):
    """
    It takes a list of dictionaries, removes the _id key from each dictionary, and then creates a pandas
    DataFrame from the remaining keys
    
    :param items: the list of dictionaries that we want to convert to a DataFrame
    :return: A dataframe with the date, tweet, clean_tweet, and sentiment.
    """
    dates = []
    tweets = []
    clean_tweets = []
    feelings = []

    # Remove _id
    for d in items:
        d.pop("_id")

    # Print results.
    for item in items:
        created_at = item["created_at"]
        tweet = item["text"]
        clean_tweet = item["processed_text"]
        sentiment = item["sentiment"]
        dates.append(created_at)
        tweets.append(tweet)
        clean_tweets.append(clean_tweet)
        feelings.append(sentiment)

    data = {
        "created_at": dates,
        "tweet": tweets,
        "clean_tweet": clean_tweets,
        "sentiment": feelings,
    }
    # Create the pandas DataFrame
    df = pd.DataFrame(data)
    return df


client = init_connection()
#st.write(get_data())

# A while loop that is constantly updating the dataframe and the charts.
while True:

    items = get_data()
    df = create_df_from_list(items)


    # creating KPIs
    count_tweets = len(df)
    count_positive = len(df[df["sentiment"] == "Positive"])
    count_neutral = len(df[df["sentiment"] == "Neutral"])
    count_negative = len(df[df["sentiment"] == "Negative"])

    df_pos = df[df["sentiment"] == "Positive"]

    avg_positive = count_positive / count_tweets
    avg_neutral = count_neutral / count_tweets
    avg_negative = count_negative / count_tweets

    with placeholder.container():
        # create three columns
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)

        # fill in those three columns with respective metrics or KPIs
        kpi1.metric(label="Tweets", value=int(count_tweets), delta=None)
        kpi2.metric(
            label="Positive  😊",
            value=count_positive,
            delta=f"{round(avg_positive*100, 2)} %",
        )
        kpi3.metric(
            label="Neutral  😐",
            value=count_neutral,
            delta=f"{round(avg_neutral*100, 2)} %",
        )
        kpi4.metric(
            label="Negative  😒",
            value=count_negative,
            delta=f"{round(avg_negative*100, 2)} %",
        )

        fig_col1, fig_col2 = st.columns(2)
        with fig_col1:
            st.markdown("### Time Series")
            fig = px.line(data_frame=df, x="date", color="sentiment")
            st.write(fig)
        with fig_col2:
            st.markdown("### Second Chart")
            fig2 = px.histogram(data_frame=df, x="sentiment")
            st.write(fig2)

        st.markdown("### Detailed Data View")
        st.dataframe(df)
        time.sleep(1)
