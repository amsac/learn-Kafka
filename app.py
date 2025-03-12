import streamlit as st
import pandas as pd
import time
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re

# Initialize Sentiment Analyzer
analyzer = SentimentIntensityAnalyzer()

# Function to clean comment text
def clean_text(text):
    text = re.sub(r'http\S+', '', text)  # Remove URLs
    text = re.sub(r'[^A-Za-z0-9 ]+', '', text)  # Remove special characters
    return text.strip()

# Function to analyze sentiment using VADER
def get_sentiment(text):
    sentiment_score = analyzer.polarity_scores(text)["compound"]
    return "Bullish" if sentiment_score > 0 else "Bearish"

# Streamlit UI
st.title("Real-Time WallStreetBets Sentiment Analysis")

# Read the latest comments from CSV
@st.cache_data(ttl=300)  # Cache for 5 minutes (auto-refresh)
def load_data():
    df = pd.read_csv("comments.csv")
    df.columns = ["comment_id", "post_title", "comment", "created_utc"]  # Make sure Kafka saves data here
    df["comment"] = df["comment"].apply(clean_text)
    df["sentiment"] = df["comment"].apply(get_sentiment)
    return df

df = load_data()

# Display real-time sentiment updates
st.subheader("Live Sentiment Updates")
st.dataframe(df.tail(10))

# Show sentiment distribution
sentiment_counts = df["sentiment"].value_counts()
st.subheader("Market Sentiment Overview")
st.bar_chart(sentiment_counts)

# Auto-refresh the app every 5 minutes
time.sleep(300)
st.rerun()
