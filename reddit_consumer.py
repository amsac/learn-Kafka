from kafka import KafkaConsumer
import json
import pandas as pd

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'wallstreetbets-stream',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Store posts and comments
posts = {}
posts_data = []
comments_data = []

# File paths
posts_file = 'posts.csv'
comments_file = 'comments.csv'

batch_size = 10  # Number of records to process before saving

while True:
    for message in consumer:
        data = message.value
        
        if data["type"] == "post":
            posts[data['id']] = data['title']  # Store post title
            posts_data.append([data['id'], data['title'], data['selftext'], data['created_utc']])
            print(f"Saved Post: {data['title']}")
        elif data["type"] == "comment":
            parent_id = data['parent_id'].replace('t3_', '')  # Extract post ID
            post_title = posts.get(parent_id, "Unknown Post")
            comments_data.append([data['id'], post_title, data['body'], data['created_utc']])
            print(f"Saved Comment: {data['body'][:50]}")  # Print first 50 chars

        # Save in batches
        if len(posts_data) >= batch_size:
            df_posts = pd.DataFrame(posts_data, columns=["post_id", "title", "selftext", "created_utc"])
            df_posts.to_csv(posts_file, mode='a', header=False, index=False)
            posts_data = []

        if len(comments_data) >= batch_size:
            df_comments = pd.DataFrame(comments_data, columns=["comment_id", "post_title", "comment", "created_utc"])
            df_comments.to_csv(comments_file, mode='a', header=False, index=False)
            comments_data = []