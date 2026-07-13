"use client";
import { useState, useEffect } from "react";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8083";

interface Post {
  id: string;
  title: string;
  score: number;
  comments: number;
  viral_score: number;
  author: string;
  source: string;
}

interface Stats {
  total_viral_posts: number;
  total_processed: string;
  viral_detected: string;
  avg_viral_score: number;
  peak_viral_score: number;
  pipeline_health: string;
}

interface TrendingData {
  generated_at: string;
  total_viral_posts: number;
  trending: Post[];
}

export default function Home() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [trending, setTrending] = useState<TrendingData | null>(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState("");

  const fetchData = async () => {
    try {
      const [statsRes, trendingRes] = await Promise.all([
        fetch(`${API_URL}/stats`),
        fetch(`${API_URL}/trending`)
      ]);
      setStats(await statsRes.json());
      setTrending(await trendingRes.json());
      setLastUpdated(new Date().toLocaleTimeString());
    } catch {
      console.error("API error");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, []);

  const getViralColor = (score: number) => {
    if (score >= 100) return "text-red-400 bg-red-400/10 border-red-400/30";
    if (score >= 75) return "text-orange-400 bg-orange-400/10 border-orange-400/30";
    if (score >= 50) return "text-yellow-400 bg-yellow-400/10 border-yellow-400/30";
    return "text-green-400 bg-green-400/10 border-green-400/30";
  };

  const getViralLabel = (score: number) => {
    if (score >= 100) return "🔥 MEGA VIRAL";
    if (score >= 75) return "⚡ HIGH VIRAL";
    if (score >= 50) return "📈 VIRAL";
    return "🌱 TRENDING";
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-[#0d1117] flex items-center justify-center">
        <div className="text-center">
          <div className="text-4xl mb-4">📡</div>
          <div className="text-white text-xl">Loading ViralScope...</div>
          <div className="text-gray-400 text-sm mt-2">Connecting to real-time pipeline</div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[#0d1117] text-white">
      {/* Header */}
      <header className="border-b border-gray-800 px-6 py-4">
        <div className="max-w-7xl mx-auto flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold">📡 ViralScope</h1>
            <p className="text-gray-400 text-sm">Real-time social trend intelligence</p>
          </div>
          <div className="text-right">
            <div className="text-green-400 font-semibold">● LIVE</div>
            <div className="text-gray-400 text-xs">Updated: {lastUpdated}</div>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-6 py-6">
        {/* Pipeline Banner */}
        <div className="bg-[#161b22] border border-purple-500/20 rounded-lg p-4 mb-6">
          <div className="flex items-center gap-2 mb-2">
            <span className="text-purple-400 font-semibold text-sm">⚡ Apache Stack Pipeline</span>
            <span className="text-xs bg-green-500/20 text-green-400 px-2 py-0.5 rounded border border-green-500/30">{stats?.pipeline_health}</span>
          </div>
          <div className="flex flex-wrap gap-2 text-xs text-gray-400">
            <span className="bg-gray-800 px-2 py-1 rounded">Hacker News API</span>
            <span className="text-gray-600">→</span>
            <span className="bg-gray-800 px-2 py-1 rounded">Apache Kafka</span>
            <span className="text-gray-600">→</span>
            <span className="bg-gray-800 px-2 py-1 rounded">Apache Spark Streaming</span>
            <span className="text-gray-600">→</span>
            <span className="bg-gray-800 px-2 py-1 rounded">Apache Cassandra</span>
            <span className="text-gray-600">→</span>
            <span className="bg-gray-800 px-2 py-1 rounded">Apache Hive Analytics</span>
            <span className="text-gray-600">→</span>
            <span className="bg-gray-800 px-2 py-1 rounded">Apache Airflow</span>
          </div>
        </div>

        {/* Stats Cards */}
        {stats && (
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
            <div className="bg-[#161b22] border border-gray-800 rounded-lg p-4">
              <div className="text-gray-400 text-xs mb-1">Total Processed</div>
              <div className="text-2xl font-bold text-blue-400">{stats.total_processed}</div>
            </div>
            <div className="bg-[#161b22] border border-gray-800 rounded-lg p-4">
              <div className="text-gray-400 text-xs mb-1">Viral Detected</div>
              <div className="text-2xl font-bold text-red-400">{stats.viral_detected}</div>
            </div>
            <div className="bg-[#161b22] border border-gray-800 rounded-lg p-4">
              <div className="text-gray-400 text-xs mb-1">Viral Posts</div>
              <div className="text-2xl font-bold text-orange-400">{stats.total_viral_posts}</div>
            </div>
            <div className="bg-[#161b22] border border-gray-800 rounded-lg p-4">
              <div className="text-gray-400 text-xs mb-1">Avg Viral Score</div>
              <div className="text-2xl font-bold text-yellow-400">{stats.avg_viral_score}</div>
            </div>
            <div className="bg-[#161b22] border border-gray-800 rounded-lg p-4">
              <div className="text-gray-400 text-xs mb-1">Peak Score</div>
              <div className="text-2xl font-bold text-green-400">{stats.peak_viral_score}</div>
            </div>
          </div>
        )}

        {/* Trending Posts */}
        <div className="bg-[#161b22] border border-gray-800 rounded-lg overflow-hidden">
          <div className="px-4 py-3 border-b border-gray-800 flex items-center justify-between">
            <h2 className="font-semibold text-gray-200">🔥 Trending Now — {trending?.total_viral_posts} Viral Posts Detected</h2>
            <span className="text-xs text-gray-500">Refreshes every 30s</span>
          </div>
          <div className="divide-y divide-gray-800">
            {trending?.trending.map((post, i) => (
              <div key={post.id} className="px-4 py-4 hover:bg-gray-800/30 transition">
                <div className="flex items-start gap-3">
                  <div className="text-2xl font-bold text-gray-700 w-8 shrink-0">
                    {i + 1}
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1 flex-wrap">
                      <span className={`text-xs px-2 py-0.5 rounded border font-medium ${getViralColor(post.viral_score)}`}>
                        {getViralLabel(post.viral_score)}
                      </span>
                      <span className="text-xs text-gray-500">Score: {post.viral_score.toFixed(1)}</span>
                    </div>
                    <p className="text-white font-medium text-sm mb-2">{post.title}</p>
                    <div className="flex items-center gap-4 text-xs text-gray-400">
                      <span>👤 {post.author}</span>
                      <span>⬆️ {post.score} points</span>
                      <span>💬 {post.comments} comments</span>
                      <span>📰 {post.source}</span>
                    </div>
                  </div>
                  <div className="shrink-0">
                    <div className="w-16 h-2 bg-gray-700 rounded-full overflow-hidden">
                      <div
                        className="h-full bg-gradient-to-r from-yellow-400 to-red-400 rounded-full"
                        style={{ width: `${Math.min(100, post.viral_score)}%` }}
                      />
                    </div>
                    <div className="text-xs text-gray-500 text-center mt-1">{post.viral_score.toFixed(0)}</div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="mt-6 text-center text-xs text-gray-600">
          <p>ViralScope — Powered by Apache Kafka · Spark · Cassandra · Hive · Airflow</p>
          <p className="mt-1">Real data from Hacker News. Updated in real-time.</p>
        </div>
      </main>
    </div>
  );
}
