"""
ViralScope — Kafka Producer
Fetches real posts from Hacker News API and streams to Kafka
"""

import requests
import json
import time
from datetime import datetime
from kafka import KafkaProducer
import random

KAFKA_TOPIC = "viralscope_posts"
KAFKA_BROKER = "localhost:9092"

HN_TOP_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
HN_ITEM_URL = "https://hacker-news.firebaseio.com/v0/item/{}.json"

SUBREDDITS = [
    "technology", "programming", "MachineLearning",
    "datascience", "artificial", "Python"
]

def get_hn_posts(limit=50):
    """Fetch top posts from Hacker News."""
    try:
        response = requests.get(HN_TOP_URL, timeout=10)
        story_ids = response.json()[:limit]
        posts = []
        for story_id in story_ids:
            try:
                item_res = requests.get(HN_ITEM_URL.format(story_id), timeout=5)
                item = item_res.json()
                if item and item.get("type") == "story" and item.get("title"):
                    posts.append({
                        "id": str(item.get("id")),
                        "title": item.get("title", ""),
                        "score": item.get("score", 0),
                        "comments": item.get("descendants", 0),
                        "author": item.get("by", "unknown"),
                        "url": item.get("url", ""),
                        "source": "hackernews",
                        "subreddit": random.choice(SUBREDDITS),
                        "timestamp": datetime.now().isoformat(),
                        "unix_time": item.get("time", int(time.time()))
                    })
            except:
                continue
        return posts
    except Exception as e:
        print(f"Error fetching HN posts: {e}")
        return []

def calculate_viral_score(post: dict) -> float:
    """Calculate viral potential score."""
    score = post.get("score", 0)
    comments = post.get("comments", 0)
    age_hours = max(1, (time.time() - post.get("unix_time", time.time())) / 3600)
    viral_score = (score + comments * 2) / (age_hours + 2) ** 1.5
    return round(viral_score, 2)

def run_producer():
    print("🚀 ViralScope Kafka Producer Starting...")
    print(f"   Topic: {KAFKA_TOPIC}")
    print(f"   Broker: {KAFKA_BROKER}")

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None
    )

    batch = 0
    while True:
        batch += 1
        print(f"\n📡 Batch {batch} — Fetching from Hacker News...")
        posts = get_hn_posts(limit=30)

        if not posts:
            print("  No posts fetched, retrying in 30s...")
            time.sleep(30)
            continue

        sent = 0
        for post in posts:
            post["viral_score"] = calculate_viral_score(post)
            post["is_viral"] = post["viral_score"] > 50
            post["batch"] = batch

            producer.send(
                KAFKA_TOPIC,
                key=post["id"],
                value=post
            )
            sent += 1

        producer.flush()
        print(f"  ✅ Sent {sent} posts to Kafka")
        print(f"  🔥 Viral posts: {sum(1 for p in posts if p['is_viral'])}")
        print(f"  📊 Top post: {posts[0]['title'][:60]}... (score: {posts[0]['score']})")
        print(f"  ⏳ Next batch in 60 seconds...")
        time.sleep(60)

if __name__ == "__main__":
    # Wait for Kafka to be ready
    print("Waiting for Kafka to be ready...")
    time.sleep(10)
    run_producer()
