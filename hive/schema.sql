-- ViralScope — Apache Hive Schema
-- Creates analytical tables on top of processed data
-- Run with: hive -f schema.sql

-- Create database
CREATE DATABASE IF NOT EXISTS viralscope;
USE viralscope;

-- Raw posts table (external table on HDFS/local storage)
CREATE EXTERNAL TABLE IF NOT EXISTS raw_posts (
    id STRING,
    title STRING,
    score INT,
    comments INT,
    author STRING,
    source STRING,
    subreddit STRING,
    viral_score DOUBLE,
    is_viral BOOLEAN,
    timestamp STRING,
    unix_time BIGINT,
    batch_num INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/viralscope/raw/posts';

-- Trending topics view
CREATE VIEW IF NOT EXISTS trending_topics AS
SELECT
    subreddit,
    COUNT(*) as post_count,
    AVG(score) as avg_score,
    AVG(viral_score) as avg_viral_score,
    SUM(comments) as total_comments,
    MAX(viral_score) as peak_viral_score
FROM raw_posts
WHERE is_viral = true
GROUP BY subreddit
ORDER BY avg_viral_score DESC;

-- Viral posts view
CREATE VIEW IF NOT EXISTS viral_posts_view AS
SELECT
    id,
    title,
    score,
    viral_score,
    author,
    timestamp
FROM raw_posts
WHERE viral_score > 50
ORDER BY viral_score DESC;

-- Hourly aggregations
CREATE TABLE IF NOT EXISTS hourly_stats (
    hour_bucket STRING,
    total_posts INT,
    viral_posts INT,
    avg_viral_score DOUBLE,
    top_title STRING
)
STORED AS ORC;
