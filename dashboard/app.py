"""
Real-Time Fraud Detection Dashboard
=====================================
Streamlit app that reads live data from Redis and the Kafka
fraud-alerts topic to render a real-time monitoring dashboard.

Panels:
  • KPI cards  – throughput, alert rate, avg score, avg latency
  • Live alerts feed with colour-coded risk levels
  • Fraud score distribution histogram
  • Alerts over time (rolling 60s window)
  • Top flagged accounts / categories / countries
  • Rule trigger frequency heatmap
"""
from __future__ import annotations

import json
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import redis
import streamlit as st
from confluent_kafka import Consumer, KafkaError

# ── Config ────────────────────────────────────────────────────────────────────
REDIS_HOST    = os.getenv("REDIS_HOST",  "localhost")
REDIS_PORT    = int(os.getenv("REDIS_PORT", "6379"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ALERT_TOPIC   = os.getenv("KAFKA_ALERT_TOPIC", "fraud-alerts")
REFRESH_SECS  = 2

st.set_page_config(
    page_title="Fraud Detection · Live",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  .main { background: #0e1117; }
  .kpi-card {
    background: #1a1d27;
    border: 1px solid #2d3147;
    border-radius: 10px;
    padding: 20px;
    text-align: center;
  }
  .kpi-value { font-size: 2.4rem; font-weight: 700; color: #e0e0e0; }
  .kpi-label { font-size: 0.85rem; color: #888; margin-top: 4px; }
  .alert-row-fraud  { background: rgba(239,68,68,0.12);  border-left: 3px solid #ef4444; padding: 8px; border-radius: 4px; margin: 4px 0; }
  .alert-row-review { background: rgba(245,158,11,0.12); border-left: 3px solid #f59e0b; padding: 8px; border-radius: 4px; margin: 4px 0; }
  div[data-testid="metric-container"] { background: #1a1d27; border: 1px solid #2d3147; border-radius:8px; padding:10px; }
</style>
""", unsafe_allow_html=True)


# ── Redis client ──────────────────────────────────────────────────────────────
@st.cache_resource
def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT,
        decode_responses=True, socket_timeout=2,
    )


def fetch_live_alerts(r: redis.Redis, limit: int = 200) -> list[dict]:
    raw = r.lrange("fraud:live_alerts", 0, limit - 1)
    alerts = []
    for item in raw:
        try:
            alerts.append(json.loads(item))
        except Exception:
            pass
    return alerts


def fetch_global_stats(r: redis.Redis) -> dict:
    return {
        "total_alerts":       int(r.get("fraud:stats:total_alerts") or 0),
        "total_amount_flagged": float(r.get("fraud:stats:total_amount_flagged") or 0),
    }


# ── Dashboard ─────────────────────────────────────────────────────────────────

def render_header():
    col1, col2 = st.columns([5, 1])
    with col1:
        st.markdown("## 🛡️ Real-Time Fraud Detection")
        st.caption(f"Live · refreshing every {REFRESH_SECS}s · {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    with col2:
        st.markdown(
            '<span style="background:#22c55e;color:#000;padding:4px 12px;border-radius:20px;font-size:12px;font-weight:700;">● LIVE</span>',
            unsafe_allow_html=True,
        )


def render_kpis(alerts: list[dict], stats: dict):
    if not alerts:
        st.info("Waiting for alerts… Is the producer and Spark engine running?")
        return

    df = pd.DataFrame(alerts)
    fraud_count  = (df["label"] == "FRAUD").sum()
    review_count = (df["label"] == "REVIEW").sum()
    avg_score    = df["fraud_score"].mean()
    avg_latency  = df["detection_latency_ms"].mean() if "detection_latency_ms" in df else 0

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("🔴 Fraud Alerts",   f"{fraud_count}",           f"{fraud_count/len(alerts)*100:.1f}% of window")
    c2.metric("🟡 Review Alerts",  f"{review_count}",          f"{review_count/len(alerts)*100:.1f}% of window")
    c3.metric("📊 Avg Fraud Score", f"{avg_score:.3f}",        "↑ higher = riskier")
    c4.metric("⚡ Avg Latency",     f"{avg_latency:.0f} ms",   "end-to-end")
    c5.metric("🔔 Total Alerts",    f"{stats['total_alerts']:,}")
    c6.metric("💰 Amount Flagged",  f"${stats['total_amount_flagged']:,.0f}")


def render_live_feed(alerts: list[dict]):
    st.markdown("### 🔴 Live Alert Feed")
    if not alerts:
        st.write("No alerts yet.")
        return

    for a in alerts[:25]:
        label      = a.get("label", "REVIEW")
        css_class  = "alert-row-fraud" if label == "FRAUD" else "alert-row-review"
        icon       = "🔴" if label == "FRAUD" else "🟡"
        score      = a.get("fraud_score", 0)
        amount     = a.get("amount", 0)
        account    = a.get("account_id", "")
        cat        = a.get("merchant_category", "")
        country    = a.get("country", "")
        latency    = a.get("detection_latency_ms", 0)
        rules      = ", ".join(a.get("rules_triggered", [])[:3])
        detected   = a.get("detected_at", "")[:19]

        st.markdown(f"""
<div class="{css_class}">
  {icon} <b>{label}</b> &nbsp;·&nbsp; Score: <b>{score:.3f}</b>
  &nbsp;·&nbsp; Amount: <b>${amount:,.2f}</b>
  &nbsp;·&nbsp; Account: <code>{account}</code>
  &nbsp;·&nbsp; {cat} / {country}
  &nbsp;·&nbsp; Latency: <b>{latency:.0f}ms</b><br>
  <span style="color:#888;font-size:11px">Rules: {rules} &nbsp;|&nbsp; {detected}</span>
</div>
""", unsafe_allow_html=True)


def render_charts(alerts: list[dict]):
    if not alerts:
        return

    df = pd.DataFrame(alerts)
    df["detected_at"] = pd.to_datetime(df["detected_at"], utc=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Fraud Score Distribution")
        fig = px.histogram(
            df, x="fraud_score", nbins=20,
            color_discrete_sequence=["#ef4444"],
            template="plotly_dark",
        )
        fig.update_layout(
            plot_bgcolor="#1a1d27", paper_bgcolor="#1a1d27",
            xaxis_title="Fraud Score", yaxis_title="Count",
            margin=dict(t=10, b=10, l=10, r=10),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### Alerts Over Time (rolling window)")
        df_time = (
            df.set_index("detected_at")
            .resample("5s")["alert_id"]
            .count()
            .reset_index()
            .rename(columns={"alert_id": "count"})
        )
        fig = px.area(
            df_time, x="detected_at", y="count",
            color_discrete_sequence=["#f97316"],
            template="plotly_dark",
        )
        fig.update_layout(
            plot_bgcolor="#1a1d27", paper_bgcolor="#1a1d27",
            xaxis_title="Time", yaxis_title="Alerts / 5s",
            margin=dict(t=10, b=10, l=10, r=10),
        )
        st.plotly_chart(fig, use_container_width=True)

    col3, col4, col5 = st.columns(3)

    with col3:
        st.markdown("### Top Flagged Categories")
        cat_counts = df["merchant_category"].value_counts().head(8).reset_index()
        cat_counts.columns = ["category", "count"]
        fig = px.bar(cat_counts, x="count", y="category", orientation="h",
                     color="count", color_continuous_scale="Reds",
                     template="plotly_dark")
        fig.update_layout(plot_bgcolor="#1a1d27", paper_bgcolor="#1a1d27",
                          margin=dict(t=5, b=5, l=5, r=5), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        st.markdown("### Top Flagged Countries")
        country_counts = df["country"].value_counts().head(8).reset_index()
        country_counts.columns = ["country", "count"]
        fig = px.bar(country_counts, x="count", y="country", orientation="h",
                     color="count", color_continuous_scale="Oranges",
                     template="plotly_dark")
        fig.update_layout(plot_bgcolor="#1a1d27", paper_bgcolor="#1a1d27",
                          margin=dict(t=5, b=5, l=5, r=5), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col5:
        st.markdown("### Rule Trigger Frequency")
        rule_counter: Counter = Counter()
        for row in alerts:
            for rule in row.get("rules_triggered", []):
                short_rule = rule.split(":")[0]
                rule_counter[short_rule] += 1
        rule_df = pd.DataFrame(
            rule_counter.most_common(10), columns=["rule", "count"]
        )
        fig = px.bar(rule_df, x="count", y="rule", orientation="h",
                     color="count", color_continuous_scale="Purples",
                     template="plotly_dark")
        fig.update_layout(plot_bgcolor="#1a1d27", paper_bgcolor="#1a1d27",
                          margin=dict(t=5, b=5, l=5, r=5), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # Latency percentile table
    st.markdown("### ⚡ Latency Percentiles (ms)")
    if "detection_latency_ms" in df.columns:
        lat = df["detection_latency_ms"]
        lat_data = pd.DataFrame({
            "Metric": ["p50", "p75", "p90", "p95", "p99", "mean"],
            "Latency (ms)": [
                lat.quantile(0.50), lat.quantile(0.75), lat.quantile(0.90),
                lat.quantile(0.95), lat.quantile(0.99), lat.mean(),
            ],
        }).round(1)
        st.dataframe(lat_data, hide_index=True, use_container_width=True)


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    render_header()
    st.divider()

    try:
        r = get_redis()
        alerts = fetch_live_alerts(r)
        stats  = fetch_global_stats(r)
    except Exception as e:
        st.error(f"Redis connection failed: {e}")
        st.info("Make sure Redis is running: `docker compose up -d redis`")
        st.stop()

    render_kpis(alerts, stats)
    st.divider()

    tab1, tab2 = st.tabs(["📡 Live Feed", "📊 Analytics"])
    with tab1:
        render_live_feed(alerts)
    with tab2:
        render_charts(alerts)

    st.divider()
    with st.expander("🔧 System Links"):
        st.markdown("""
| Service | URL |
|---------|-----|
| Airflow / Spark UI | http://localhost:4040 |
| Kafka UI | http://localhost:8082 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |
| Schema Registry | http://localhost:8081 |
""")

    time.sleep(REFRESH_SECS)
    st.rerun()


if __name__ == "__main__":
    main()
