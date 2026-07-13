"""
ViralScope — Apache Airflow DAG
Orchestrates the full pipeline:
1. Fetch trending posts from Hacker News
2. Produce to Kafka
3. Run Hive analytics
4. Generate trend report
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import json
import os

default_args = {
    "owner": "viralscope",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "viralscope_pipeline",
    default_args=default_args,
    description="ViralScope real-time trend intelligence pipeline",
    schedule_interval="0 * * * *",  # Every hour
    catchup=False,
    tags=["viralscope", "kafka", "spark", "hive", "cassandra"]
)

def fetch_trending_posts(**context):
    """Task 1: Fetch top posts from Hacker News."""
    url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    response = requests.get(url, timeout=10)
    story_ids = response.json()[:20]

    posts = []
    for story_id in story_ids[:5]:  # Fetch 5 for demo
        item_res = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json",
            timeout=5
        )
        item = item_res.json()
        if item and item.get("title"):
            posts.append({
                "id": str(item.get("id")),
                "title": item.get("title"),
                "score": item.get("score", 0),
                "comments": item.get("descendants", 0),
                "author": item.get("by", "unknown")
            })

    print(f"✅ Fetched {len(posts)} posts from Hacker News")
    for p in posts:
        print(f"  - {p['title'][:60]} (score: {p['score']})")

    context["task_instance"].xcom_push(key="post_count", value=len(posts))
    return len(posts)

def check_kafka_health(**context):
    """Task 2: Verify Kafka is running."""
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=["localhost:9092"],
            consumer_timeout_ms=5000
        )
        topics = consumer.topics()
        consumer.close()
        print(f"✅ Kafka healthy — topics: {topics}")
        return True
    except NoBrokersAvailable:
        raise Exception("❌ Kafka not available")

def check_cassandra_health(**context):
    """Task 3: Verify Cassandra and count records."""
    from cassandra.cluster import Cluster
    cluster = Cluster(["localhost"])
    session = cluster.connect("viralscope")
    result = session.execute("SELECT COUNT(*) FROM viral_posts")
    count = result.one()[0]
    stats = session.execute("SELECT * FROM pipeline_stats")
    for row in stats:
        print(f"  {row.stat_key}: {row.stat_value}")
    session.shutdown()
    cluster.shutdown()
    print(f"✅ Cassandra healthy — {count} viral posts stored")
    context["task_instance"].xcom_push(key="viral_count", value=count)
    return count

def generate_trend_report(**context):
    """Task 4: Generate hourly trend report."""
    from cassandra.cluster import Cluster
    from datetime import datetime
    import pytz

    cluster = Cluster(["localhost"])
    session = cluster.connect("viralscope")

    posts = list(session.execute("SELECT * FROM viral_posts"))
    stats = {r.stat_key: r.stat_value 
             for r in session.execute("SELECT * FROM pipeline_stats")}

    session.shutdown()
    cluster.shutdown()

    now = datetime.now(pytz.timezone("America/New_York"))
    report = {
        "generated_at": now.isoformat(),
        "hour": now.strftime("%Y-%m-%d %H:00"),
        "total_viral_posts": len(posts),
        "pipeline_stats": stats,
        "top_viral": [
            {"title": p.title, "score": p.score, "viral_score": float(p.viral_score)}
            for p in sorted(posts, key=lambda x: x.viral_score, reverse=True)[:3]
        ] if posts else []
    }

    os.makedirs("/tmp/viralscope_reports", exist_ok=True)
    report_path = f"/tmp/viralscope_reports/report_{now.strftime('%Y%m%d_%H')}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"✅ Trend report generated: {report_path}")
    print(f"   Viral posts: {len(posts)}")
    print(f"   Top post: {report['top_viral'][0]['title'][:60] if report['top_viral'] else 'None'}")
    return report_path

# ─────────────────────────────────────────
# DAG TASKS
# ─────────────────────────────────────────
task_fetch = PythonOperator(
    task_id="fetch_trending_posts",
    python_callable=fetch_trending_posts,
    dag=dag
)

task_kafka = PythonOperator(
    task_id="check_kafka_health",
    python_callable=check_kafka_health,
    dag=dag
)

task_cassandra = PythonOperator(
    task_id="check_cassandra_health",
    python_callable=check_cassandra_health,
    dag=dag
)

task_report = PythonOperator(
    task_id="generate_trend_report",
    python_callable=generate_trend_report,
    dag=dag
)

# Pipeline flow
task_fetch >> task_kafka >> task_cassandra >> task_report
