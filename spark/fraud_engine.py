"""
Spark Streaming Fraud Detection Engine
=======================================
Consumes raw transactions from Kafka, applies a multi-layer
rule-based fraud scoring engine using window aggregations,
and publishes FraudAlert events back to Kafka.

Detection layers
────────────────
1. Rule engine      – instant per-event checks (amount, MCC, country)
2. Window engine    – sliding/tumbling window aggregations (velocity,
                      spend anomaly, device switching)
3. Scoring          – weighted combination → fraud_score ∈ [0, 1]
4. Alert publisher  – scored events above threshold → fraud-alerts topic

Metrics
───────
Prometheus counters/histograms exposed on :8000/metrics
"""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Iterator

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, LongType, TimestampType,
)
from pyspark.sql.streaming import DataStreamWriter
from confluent_kafka import Producer as KafkaProducer
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import redis

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("fraud_engine")

# ── Environment ───────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC      = os.getenv("KAFKA_INPUT_TOPIC",  "transactions")
ALERT_TOPIC      = os.getenv("KAFKA_ALERT_TOPIC",  "fraud-alerts")
REDIS_HOST       = os.getenv("REDIS_HOST",  "localhost")
REDIS_PORT       = int(os.getenv("REDIS_PORT", "6379"))
METRICS_PORT     = int(os.getenv("METRICS_PORT", "8000"))
FRAUD_THRESHOLD  = float(os.getenv("FRAUD_THRESHOLD", "0.65"))
CHECKPOINT_DIR   = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints")

# ── Prometheus Metrics ────────────────────────────────────────────────────────
TXNS_PROCESSED  = Counter("fraud_txns_processed_total",  "Transactions processed")
ALERTS_EMITTED  = Counter("fraud_alerts_emitted_total",  "Fraud alerts emitted")
DETECTION_LAT   = Histogram(
    "fraud_detection_latency_ms",
    "End-to-end detection latency (ms)",
    buckets=[5, 10, 25, 50, 100, 250, 500, 1000, 2500],
)
FRAUD_SCORE_HIST = Histogram(
    "fraud_score_distribution",
    "Distribution of fraud scores",
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)
THROUGHPUT_GAUGE = Gauge("fraud_throughput_tps", "Approximate transactions/sec processed")

# ── Transaction Schema ────────────────────────────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",    StringType(),  False),
    StructField("account_id",        StringType(),  False),
    StructField("merchant_id",       StringType(),  False),
    StructField("amount",            DoubleType(),  False),
    StructField("currency",          StringType(),  True),
    StructField("transaction_type",  StringType(),  True),
    StructField("merchant_category", StringType(),  True),
    StructField("country",           StringType(),  True),
    StructField("city",              StringType(),  True),
    StructField("device_type",       StringType(),  True),
    StructField("ip_address",        StringType(),  True),
    StructField("timestamp",         StringType(),  False),
    StructField("is_fraud",          BooleanType(), True),
])

# ── High-risk sets ─────────────────────────────────────────────────────────────
HIGH_RISK_COUNTRIES  = {"NG", "RO", "CN", "PK", "VN"}
HIGH_RISK_CATEGORIES = {"GAMBLING", "CRYPTO_EXCHANGE", "MONEY_TRANSFER"}


# ══════════════════════════════════════════════════════════════════════════════
#  Rule Engine
# ══════════════════════════════════════════════════════════════════════════════

class RuleEngine:
    """
    Instant per-event rules returning (score_contribution, rule_name).
    All rules are independent; scores are summed and capped at 1.0.
    """

    RULES = [
        ("r01_high_amount",       0.30),
        ("r02_very_high_amount",  0.50),
        ("r03_high_risk_mcc",     0.25),
        ("r04_high_risk_country", 0.20),
        ("r05_unknown_device",    0.10),
        ("r06_micro_amount",      0.15),   # card-testing proxy
        ("r07_round_amount",      0.08),
    ]

    @staticmethod
    def evaluate(txn: dict) -> tuple[float, list[str]]:
        score    = 0.0
        triggered: list[str] = []
        amount   = txn.get("amount", 0)
        category = txn.get("merchant_category", "")
        country  = txn.get("country", "")
        device   = txn.get("device_type", "")

        if amount >= 3_000:
            score += 0.30; triggered.append("HIGH_AMOUNT_3K")
        if amount >= 8_000:
            score += 0.20; triggered.append("VERY_HIGH_AMOUNT_8K")
        if category in HIGH_RISK_CATEGORIES:
            score += 0.25; triggered.append(f"HIGH_RISK_MCC:{category}")
        if country in HIGH_RISK_COUNTRIES:
            score += 0.20; triggered.append(f"HIGH_RISK_COUNTRY:{country}")
        if device == "unknown":
            score += 0.10; triggered.append("UNKNOWN_DEVICE")
        if 0 < amount < 2.0:
            score += 0.15; triggered.append("MICRO_AMOUNT_CARD_TEST")
        if amount % 100 == 0 and amount >= 500:
            score += 0.08; triggered.append("SUSPICIOUSLY_ROUND_AMOUNT")

        return min(score, 1.0), triggered


# ══════════════════════════════════════════════════════════════════════════════
#  Redis Feature Store (window counters via foreachBatch)
# ══════════════════════════════════════════════════════════════════════════════

class FeatureStore:
    """
    Maintains per-account velocity and spend features in Redis
    with TTL-based sliding windows.
    """
    WINDOW_SECONDS = 300   # 5-minute window

    def __init__(self):
        self._client: redis.Redis | None = None

    @property
    def client(self) -> redis.Redis:
        if self._client is None:
            self._client = redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT,
                decode_responses=True, socket_timeout=2,
            )
        return self._client

    def _key(self, account_id: str, feature: str) -> str:
        return f"fraud:{account_id}:{feature}"

    def increment_count(self, account_id: str) -> int:
        key = self._key(account_id, "txn_count")
        pipe = self.client.pipeline()
        pipe.incr(key)
        pipe.expire(key, self.WINDOW_SECONDS)
        count, _ = pipe.execute()
        return int(count)

    def add_spend(self, account_id: str, amount: float) -> float:
        key = self._key(account_id, "spend")
        pipe = self.client.pipeline()
        pipe.incrbyfloat(key, amount)
        pipe.expire(key, self.WINDOW_SECONDS)
        total, _ = pipe.execute()
        return float(total)

    def get_window_features(self, account_id: str) -> dict:
        count = self.client.get(self._key(account_id, "txn_count")) or 0
        spend = self.client.get(self._key(account_id, "spend"))     or 0
        return {"window_count": int(count), "window_spend": float(spend)}

    def record_alert(self, alert: dict) -> None:
        key = f"fraud:alerts:{alert['account_id']}"
        pipe = self.client.pipeline()
        pipe.lpush(key, json.dumps(alert))
        pipe.ltrim(key, 0, 99)             # keep last 100 alerts per account
        pipe.expire(key, 86_400)
        pipe.execute()

        # Global stats counters
        self.client.incr("fraud:stats:total_alerts")
        self.client.incrbyfloat("fraud:stats:total_amount_flagged", alert["amount"])

        # Live dashboard feed
        self.client.lpush("fraud:live_alerts", json.dumps(alert))
        self.client.ltrim("fraud:live_alerts", 0, 499)


# ══════════════════════════════════════════════════════════════════════════════
#  Kafka Alert Publisher
# ══════════════════════════════════════════════════════════════════════════════

_kafka_producer: KafkaProducer | None = None

def _get_kafka_producer() -> KafkaProducer:
    global _kafka_producer
    if _kafka_producer is None:
        _kafka_producer = KafkaProducer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "1",
            "queue.buffering.max.ms": 10,
        })
    return _kafka_producer


def _publish_alert(alert: dict) -> None:
    prod = _get_kafka_producer()
    prod.produce(
        ALERT_TOPIC,
        key=alert["account_id"].encode(),
        value=json.dumps(alert).encode(),
    )
    prod.poll(0)


# ══════════════════════════════════════════════════════════════════════════════
#  Spark Session & Streaming Logic
# ══════════════════════════════════════════════════════════════════════════════

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FraudDetectionStreaming")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.kafka.consumer.cache.enabled", "false")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.kafka:kafka-clients:3.6.0",
        )
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 5_000)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_transactions(raw: DataFrame) -> DataFrame:
    """Deserialise JSON from Kafka value bytes."""
    return (
        raw
        .select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
        )
        .select(
            "kafka_timestamp",
            "data.*",
            F.to_timestamp("data.timestamp").alias("event_time"),
        )
        .withWatermark("event_time", "30 seconds")
    )


def build_velocity_features(parsed: DataFrame) -> DataFrame:
    """
    Sliding window (5-min window, 1-min slide) aggregations per account:
      - transaction count
      - total spend
      - distinct merchants
      - distinct countries
    """
    return (
        parsed
        .groupBy(
            F.window("event_time", "5 minutes", "1 minute"),
            "account_id",
        )
        .agg(
            F.count("*").alias("window_txn_count"),
            F.sum("amount").alias("window_total_spend"),
            F.countDistinct("merchant_id").alias("window_distinct_merchants"),
            F.countDistinct("country").alias("window_distinct_countries"),
            F.max("amount").alias("window_max_amount"),
            F.avg("amount").alias("window_avg_amount"),
        )
    )


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch handler:
      1. Apply rule engine per-row
      2. Enrich with Redis window features
      3. Compute composite fraud score
      4. Publish alerts for high-score transactions
      5. Update Prometheus metrics
    """
    if batch_df.rdd.isEmpty():
        return

    rows      = batch_df.collect()
    rule_eng  = RuleEngine()
    feat_store = FeatureStore()
    batch_start = time.monotonic()
    batch_alerts = 0

    for row in rows:
        ingest_ts = time.monotonic()
        txn = row.asDict()

        # ── Rule engine ──────────────────────────────────────────────────────
        rule_score, rules = rule_eng.evaluate(txn)

        # ── Window features from Redis ────────────────────────────────────────
        try:
            count       = feat_store.increment_count(txn["account_id"])
            window_spend = feat_store.add_spend(txn["account_id"], txn["amount"])
        except Exception:
            count, window_spend = 1, txn["amount"]

        # ── Velocity score ────────────────────────────────────────────────────
        velocity_score = 0.0
        if count > 20:
            velocity_score = min((count - 20) / 30.0, 0.40)
            rules.append(f"VELOCITY_COUNT:{count}")
        if window_spend > 5_000:
            velocity_score += min((window_spend - 5_000) / 10_000, 0.30)
            rules.append(f"VELOCITY_SPEND:{window_spend:.0f}")

        # ── Composite score ───────────────────────────────────────────────────
        fraud_score = min(rule_score + velocity_score, 1.0)
        FRAUD_SCORE_HIST.observe(fraud_score)

        # ── Latency ───────────────────────────────────────────────────────────
        latency_ms = (time.monotonic() - ingest_ts) * 1000
        DETECTION_LAT.observe(latency_ms)
        TXNS_PROCESSED.inc()

        # ── Emit alert ────────────────────────────────────────────────────────
        if fraud_score >= FRAUD_THRESHOLD:
            label = "FRAUD" if fraud_score >= 0.80 else "REVIEW"
            alert = {
                "alert_id":             str(uuid.uuid4()),
                "transaction_id":       txn["transaction_id"],
                "account_id":           txn["account_id"],
                "merchant_category":    txn.get("merchant_category", ""),
                "amount":               txn["amount"],
                "country":              txn.get("country", ""),
                "fraud_score":          round(fraud_score, 4),
                "label":                label,
                "rules_triggered":      rules,
                "detection_latency_ms": round(latency_ms, 2),
                "detected_at":          datetime.now(timezone.utc).isoformat(),
                "is_fraud_ground_truth": txn.get("is_fraud", False),
            }
            try:
                _publish_alert(alert)
                feat_store.record_alert(alert)
            except Exception as e:
                log.error("Alert publish failed: %s", e)

            ALERTS_EMITTED.inc()
            batch_alerts += 1

    batch_elapsed = time.monotonic() - batch_start
    if len(rows) > 0:
        THROUGHPUT_GAUGE.set(len(rows) / max(batch_elapsed, 0.001))
        log.info(
            "Batch %d processed | rows=%d alerts=%d elapsed_ms=%.1f",
            batch_id, len(rows), batch_alerts, batch_elapsed * 1000,
        )


def write_velocity_to_console(velocity_df: DataFrame) -> DataStreamWriter:
    """Debug: stream window aggregations to stdout."""
    return (
        velocity_df.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 5)
        .trigger(processingTime="30 seconds")
        .queryName("velocity_monitor")
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Entry Point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("Starting Prometheus metrics server on :%d", METRICS_PORT)
    start_http_server(METRICS_PORT)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session started | version=%s", spark.version)

    # ── Stream 1: Transaction scoring ─────────────────────────────────────────
    raw_stream   = read_kafka_stream(spark)
    parsed       = parse_transactions(raw_stream)

    txn_query = (
        parsed.writeStream
        .outputMode("append")
        .foreachBatch(process_batch)
        .trigger(processingTime="2 seconds")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/scoring")
        .queryName("fraud_scoring")
        .start()
    )

    # ── Stream 2: Window velocity aggregations ────────────────────────────────
    velocity_df  = build_velocity_features(parsed)
    vel_query    = write_velocity_to_console(velocity_df).start()

    log.info("Streaming queries active. Spark UI → http://localhost:4040")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        log.info("Shutting down…")
        txn_query.stop()
        vel_query.stop()


if __name__ == "__main__":
    main()
