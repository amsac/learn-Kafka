import praw
import json
from kafka import KafkaProducer

# Kafka inside Docker (localhost)
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "wallstreetbets-stream"

# Reddit API Credentials
reddit = praw.Reddit(
    client_id="PSssY2fCBwf8SLSykLYjUg",
    client_secret="E9A3tMBxOpLrHC830enE5wfkhTazUQ",
    user_agent="sentibot"
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

subreddit = reddit.subreddit("wallstreetbets")

# Stream posts and comments
def stream_reddit():
    for post in subreddit.stream.submissions(skip_existing=True):
        data = {
            "type": "post",
            "id": post.id,
            "title": post.title,
            "text": post.selftext,
            "created_utc": post.created_utc
        }
        producer.send(TOPIC_NAME, value=data)
        print(f"Sent Post: {data}")

    for comment in subreddit.stream.comments(skip_existing=True):
        data = {
            "type": "comment",
            "id": comment.id,
            "text": comment.body,
            "created_utc": comment.created_utc
        }
        producer.send(TOPIC_NAME, value=data)
        print(f"Sent Comment: {data}")

stream_reddit()