import json
import pandas as pd
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "wallstreetbets-stream"

# CSV files
POSTS_CSV = "reddit_posts.csv"
COMMENTS_CSV = "reddit_comments.csv"

# Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

posts = []
comments = []

for message in consumer:
    entry = message.value

    if entry["type"] == "post":
        posts.append(entry)
    elif entry["type"] == "comment":
        comments.append(entry)

    # Save posts every 10 messages
    if len(posts) % 10 == 0:
        df_posts = pd.DataFrame(posts)
        df_posts.to_csv(POSTS_CSV, index=False)
        print(f"Saved {len(posts)} posts to {POSTS_CSV}")

    # Save comments every 10 messages
    if len(comments) % 10 == 0:
        df_comments = pd.DataFrame(comments)
        df_comments.to_csv(COMMENTS_CSV, index=False)
        print(f"Saved {len(comments)} comments to {COMMENTS_CSV}")
