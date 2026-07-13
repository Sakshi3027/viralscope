from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from cassandra.cluster import Cluster
from datetime import datetime
import pytz

app = FastAPI(title="ViralScope API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_data():
    cluster = Cluster(["localhost"])
    session = cluster.connect("viralscope")
    posts = list(session.execute("SELECT * FROM viral_posts"))
    stats = {r.stat_key: r.stat_value for r in session.execute("SELECT * FROM pipeline_stats")}
    cluster.shutdown()
    return posts, stats

@app.get("/")
def root():
    return {"name": "ViralScope", "tagline": "Real-time social trend intelligence"}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/trending")
def trending():
    posts, stats = get_data()
    now = datetime.now(pytz.timezone("America/New_York"))
    sorted_posts = sorted(posts, key=lambda x: x.viral_score, reverse=True)
    return {
        "generated_at": now.isoformat(),
        "total_viral_posts": len(posts),
        "pipeline_stats": stats,
        "trending": [
            {
                "id": p.id,
                "title": p.title,
                "score": p.score,
                "comments": p.comments,
                "viral_score": float(p.viral_score),
                "author": p.author,
                "source": p.source
            }
            for p in sorted_posts[:20]
        ]
    }

@app.get("/stats")
def stats():
    posts, pipeline = get_data()
    viral_scores = [float(p.viral_score) for p in posts]
    avg_score = sum(viral_scores) / len(viral_scores) if viral_scores else 0
    max_score = max(viral_scores) if viral_scores else 0
    return {
        "total_viral_posts": len(posts),
        "total_processed": pipeline.get("total_processed", "0"),
        "viral_detected": pipeline.get("viral_detected", "0"),
        "avg_viral_score": round(avg_score, 2),
        "peak_viral_score": round(max_score, 2),
        "pipeline_health": "ACTIVE"
    }
