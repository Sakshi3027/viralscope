"""
ViralScope — Spark Streaming Processor
Reads from Kafka, computes trending topics, detects viral content
Writes results to Cassandra
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import builtins
builtins_sum = builtins.sum
from pyspark.sql.types import *
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import os

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
os.environ["PYSPARK_PYTHON"] = "python3"

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "viralscope_posts"
CASSANDRA_HOST = "localhost"
CASSANDRA_KEYSPACE = "viralscope"

# ─────────────────────────────────────────
# CASSANDRA SETUP
# ─────────────────────────────────────────
def setup_cassandra():
    """Create Cassandra keyspace and tables."""
    print("Setting up Cassandra...")
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS viralscope
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace(CASSANDRA_KEYSPACE)

    session.execute("""
        CREATE TABLE IF NOT EXISTS trending_topics (
            topic TEXT,
            window_start TIMESTAMP,
            post_count INT,
            avg_score DOUBLE,
            avg_viral_score DOUBLE,
            total_comments INT,
            PRIMARY KEY (topic, window_start)
        ) WITH CLUSTERING ORDER BY (window_start DESC)
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS viral_posts (
            id TEXT PRIMARY KEY,
            title TEXT,
            score INT,
            comments INT,
            viral_score DOUBLE,
            source TEXT,
            author TEXT,
            timestamp TIMESTAMP,
            batch_num INT
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_stats (
            stat_key TEXT PRIMARY KEY,
            stat_value TEXT,
            updated_at TIMESTAMP
        )
    """)

    print("✅ Cassandra tables created")
    session.shutdown()
    cluster.shutdown()

# ─────────────────────────────────────────
# SPARK STREAMING
# ─────────────────────────────────────────
post_schema = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("score", IntegerType()),
    StructField("comments", IntegerType()),
    StructField("author", StringType()),
    StructField("source", StringType()),
    StructField("subreddit", StringType()),
    StructField("viral_score", DoubleType()),
    StructField("is_viral", BooleanType()),
    StructField("timestamp", StringType()),
    StructField("unix_time", LongType()),
    StructField("batch", IntegerType())
])

def write_to_cassandra(df, epoch_id):
    """Write each micro-batch to Cassandra."""
    if df.rdd.isEmpty():
        return

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(CASSANDRA_KEYSPACE)

    rows = df.collect()
    total = len(rows)
    viral = builtins_sum(1 for r in rows if r["is_viral"])

    # Write viral posts
    for row in rows:
        if row["is_viral"]:
            session.execute("""
                INSERT INTO viral_posts (id, title, score, comments, viral_score, source, author, timestamp, batch_num)
                VALUES (%s, %s, %s, %s, %s, %s, %s, toTimestamp(now()), %s)
            """, (
                row["id"], row["title"], row["score"],
                row["comments"], row["viral_score"],
                row["source"], row["author"], row["batch"] if "batch" in row else 0
            ))

    # Write pipeline stats
    session.execute("""
        INSERT INTO pipeline_stats (stat_key, stat_value, updated_at)
        VALUES ('total_processed', %s, toTimestamp(now()))
    """, (str(total),))

    session.execute("""
        INSERT INTO pipeline_stats (stat_key, stat_value, updated_at)
        VALUES ('viral_detected', %s, toTimestamp(now()))
    """, (str(viral),))

    session.execute("""
        INSERT INTO pipeline_stats (stat_key, stat_value, updated_at)
        VALUES ('last_batch_time', %s, toTimestamp(now()))
    """, (str(epoch_id),))

    print(f"  [Epoch {epoch_id}] Processed: {total} posts | Viral: {viral}")
    session.shutdown()
    cluster.shutdown()

def run_spark_streaming():
    print("⚡ ViralScope Spark Streaming Starting...")

    spark = SparkSession.builder \
        .appName("ViralScope-Stream") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/viralscope_checkpoint") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print(f"  Spark version: {spark.version}")

    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON
    parsed = raw_stream.select(
        from_json(col("value").cast("string"), post_schema).alias("data")
    ).select("data.*")

    # Write to Cassandra via foreachBatch
    query = parsed.writeStream \
        .foreachBatch(write_to_cassandra) \
        .outputMode("append") \
        .trigger(processingTime="15 seconds") \
        .start()

    print("✅ Spark Streaming started — processing every 15 seconds")
    query.awaitTermination()

if __name__ == "__main__":
    setup_cassandra()
    run_spark_streaming()
