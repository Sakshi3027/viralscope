"""
ViralScope — Hive Analytics Layer
Simulates Hive-style analytical queries using PySpark SQL
In production: runs on actual Apache Hive with HDFS storage
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from cassandra.cluster import Cluster
import os
import json
from datetime import datetime

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

def load_from_cassandra():
    """Load viral posts from Cassandra into Spark DataFrame."""
    cluster = Cluster(["localhost"])
    session = cluster.connect("viralscope")

    rows = session.execute("SELECT * FROM viral_posts")
    stats = session.execute("SELECT * FROM pipeline_stats")

    posts = [dict(r._asdict()) for r in rows]
    pipeline_stats = {r.stat_key: r.stat_value for r in stats}

    session.shutdown()
    cluster.shutdown()
    return posts, pipeline_stats

def run_hive_analytics():
    print("\n🐝 ViralScope — Hive Analytics Layer")
    print("=" * 55)

    spark = SparkSession.builder \
        .appName("ViralScope-Hive") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.warehouse.dir", "/tmp/viralscope_hive_warehouse") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # Load data
    posts, stats = load_from_cassandra()
    
    if not posts:
        print("  No data in Cassandra yet. Run producer + stream processor first.")
        spark.stop()
        return

    print(f"  Loaded {len(posts)} viral posts from Cassandra")

    # Create Spark DataFrame
    df = spark.createDataFrame(posts)
    
    # Register as Hive table
    df.createOrReplaceTempView("viral_posts")

    print("\n📊 HIVE ANALYTICAL QUERIES")
    print("-" * 55)

    # Query 1 — Viral score distribution
    print("\n[Query 1] Viral Score Distribution:")
    spark.sql("""
        SELECT 
            CASE 
                WHEN viral_score >= 100 THEN 'MEGA VIRAL (100+)'
                WHEN viral_score >= 75 THEN 'HIGH VIRAL (75-100)'
                WHEN viral_score >= 50 THEN 'VIRAL (50-75)'
                ELSE 'TRENDING (<50)'
            END as category,
            COUNT(*) as post_count,
            ROUND(AVG(viral_score), 2) as avg_score
        FROM viral_posts
        GROUP BY 1
        ORDER BY avg_score DESC
    """).show(truncate=False)

    # Query 2 — Top viral posts
    print("\n[Query 2] Top Viral Posts:")
    spark.sql("""
        SELECT 
            id,
            SUBSTRING(title, 1, 50) as title_preview,
            score,
            viral_score
        FROM viral_posts
        ORDER BY viral_score DESC
        LIMIT 5
    """).show(truncate=False)

    # Query 3 — Author analysis
    print("\n[Query 3] Top Authors by Viral Score:")
    spark.sql("""
        SELECT 
            author,
            COUNT(*) as viral_posts,
            ROUND(AVG(viral_score), 2) as avg_viral_score,
            MAX(score) as best_score
        FROM viral_posts
        GROUP BY author
        ORDER BY avg_viral_score DESC
        LIMIT 5
    """).show(truncate=False)

    # Query 4 — Pipeline stats
    print("\n[Query 4] Pipeline Statistics:")
    print(f"  Total posts processed: {stats.get('total_processed', 'N/A')}")
    print(f"  Viral posts detected:  {stats.get('viral_detected', 'N/A')}")
    print(f"  Last batch ID:         {stats.get('last_batch_time', 'N/A')}")

    # Save results
    os.makedirs("data", exist_ok=True)
    results = {
        "generated_at": datetime.now().isoformat(),
        "total_viral_posts": len(posts),
        "pipeline_stats": stats,
        "top_posts": [
            {"id": r["id"], "title": r["title"], "viral_score": float(r["viral_score"])}
            for r in sorted(posts, key=lambda x: x["viral_score"], reverse=True)[:5]
        ]
    }
    with open("data/hive_analytics.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n✅ Hive analytics complete! Results saved to data/hive_analytics.json")
    spark.stop()

if __name__ == "__main__":
    run_hive_analytics()
