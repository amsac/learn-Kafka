import praw
import time
from kafka import KafkaProducer
import json

# Initialize Reddit API
reddit = praw.Reddit(
    client_id="PSssY2fCBwf8SLSykLYjUg",
    client_secret="E9A3tMBxOpLrHC830enE5wfkhTazUQ",
    user_agent="sentibot"
)

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = "wallstreetbets-stream"
processed_comments = set()  # Store processed comment IDs

while True:
    print("Fetching new Reddit posts and comments...")

    for submission in reddit.subreddit("wallstreetbets").new(limit=5):
        post_data = {
            "type": "post",
            "id": submission.id,
            "title": submission.title,
            "selftext": submission.selftext,
            "url": submission.url,
            "created_utc": submission.created_utc
        }

        print(f"Fetched Post: {submission.title}")
        producer.send(TOPIC_NAME, value=post_data)

        # Fetch top-level comments
        submission.comments.replace_more(limit=0)
        for comment in submission.comments.list():
            if comment.id not in processed_comments:  # Only process new comments
                comment_data = {
                    "type": "comment",
                    "id": comment.id,
                    "parent_id": comment.parent_id,
                    "body": comment.body,
                    "created_utc": comment.created_utc
                }

                print(f"Fetched New Comment: {comment.body[:50]}")  # Print first 50 chars
                producer.send(TOPIC_NAME, value=comment_data)
                processed_comments.add(comment.id)  # Mark comment as processed

        producer.flush()  # Ensure data is sent

    print("Waiting for new posts and comments...")
    time.sleep(60)  # Wait 1 minute before checking again
