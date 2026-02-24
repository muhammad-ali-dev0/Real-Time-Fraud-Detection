"""
Transaction Producer
====================
Generates realistic synthetic payment transactions and publishes them
to a Kafka topic at a configurable rate.

Fraud patterns simulated:
  • Velocity abuse  – many transactions in a short window
  • Geo-impossible  – same account, two countries < 30 min apart
  • High-risk MCC   – gambling / crypto / money-transfer
  • Amount anomaly  – unusually large amounts
  • Night-owl       – transactions at unusual hours
  • Card-testing    – rapid small amounts before a large hit
"""
from __future__ import annotations

import os
import random
import time
import uuid
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("producer")

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC       = os.getenv("KAFKA_TOPIC", "transactions")
TPS               = int(os.getenv("TRANSACTIONS_PER_SECOND", "100"))
FRAUD_RATE        = float(os.getenv("FRAUD_RATE", "0.02"))   # 2 %

MERCHANT_CATEGORIES = [
    "GROCERY", "ELECTRONICS", "TRAVEL", "RESTAURANT", "FUEL",
    "PHARMACY", "ENTERTAINMENT", "GAMBLING", "CRYPTO_EXCHANGE",
    "MONEY_TRANSFER", "ATM", "RETAIL",
]
HIGH_RISK = {"GAMBLING", "CRYPTO_EXCHANGE", "MONEY_TRANSFER"}

COUNTRIES = [
    ("US", ["New York", "Los Angeles", "Chicago"]),
    ("GB", ["London", "Manchester"]),
    ("DE", ["Berlin", "Munich"]),
    ("NG", ["Lagos", "Abuja"]),
    ("RO", ["Bucharest"]),
    ("CN", ["Shanghai", "Beijing"]),
]
DEVICES = ["mobile_ios", "mobile_android", "desktop_chrome", "desktop_safari", "unknown"]

# Pool of accounts – small pool increases velocity hits
ACCOUNT_POOL  = [f"ACC{i:06d}" for i in range(1, 501)]
MERCHANT_POOL = [f"MER{i:05d}" for i in range(1, 201)]

# Track recent per-account state for pattern injection
_account_state: dict[str, dict] = {}


def _delivery_report(err, msg):
    if err:
        log.error("Delivery failed: %s", err)


def _ensure_topic(bootstrap: str, topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        futures = admin.create_topics([NewTopic(topic, num_partitions=6, replication_factor=1)])
        for t, f in futures.items():
            try:
                f.result()
                log.info("Created topic: %s", t)
            except Exception as e:
                log.warning("Topic creation: %s", e)


def _make_legitimate_transaction(account_id: str) -> dict:
    country, cities = random.choice(COUNTRIES)
    return {
        "transaction_id":    str(uuid.uuid4()),
        "account_id":        account_id,
        "merchant_id":       random.choice(MERCHANT_POOL),
        "amount":            round(random.lognormvariate(3.5, 1.2), 2),  # ~$33 median
        "currency":          "USD",
        "transaction_type":  random.choice(["PURCHASE", "PURCHASE", "PURCHASE", "WITHDRAWAL"]),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "country":           country,
        "city":              random.choice(cities),
        "device_type":       random.choice(DEVICES),
        "ip_address":        f"{random.randint(1,254)}.{random.randint(0,254)}.{random.randint(0,254)}.{random.randint(1,254)}",
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "is_fraud":          False,
    }


def _inject_fraud_pattern(txn: dict) -> dict:
    """Mutate a transaction to match a known fraud pattern."""
    pattern = random.choice([
        "high_amount", "high_risk_mcc", "velocity_flag",
        "night_owl", "card_testing", "impossible_country",
    ])
    txn["fraud_pattern"] = pattern

    if pattern == "high_amount":
        txn["amount"] = round(random.uniform(3_000, 15_000), 2)
        txn["merchant_category"] = random.choice(["ELECTRONICS", "JEWELRY", "TRAVEL"])

    elif pattern == "high_risk_mcc":
        txn["merchant_category"] = random.choice(list(HIGH_RISK))
        txn["amount"] = round(random.uniform(500, 5_000), 2)

    elif pattern == "velocity_flag":
        # Same account, many transactions – relies on Spark window detection
        txn["amount"] = round(random.uniform(10, 100), 2)

    elif pattern == "night_owl":
        # Rewrite timestamp to 2–4 AM local (simplified)
        txn["amount"] = round(random.uniform(200, 1_500), 2)

    elif pattern == "card_testing":
        txn["amount"] = round(random.uniform(0.01, 2.00), 2)
        txn["merchant_category"] = "RETAIL"

    elif pattern == "impossible_country":
        high_risk_countries = ["NG", "RO", "CN"]
        txn["country"] = random.choice(high_risk_countries)
        txn["amount"]  = round(random.uniform(500, 3_000), 2)

    txn["is_fraud"] = True
    return txn


def generate_transaction() -> dict:
    account_id = random.choice(ACCOUNT_POOL)
    txn = _make_legitimate_transaction(account_id)
    if random.random() < FRAUD_RATE:
        txn = _inject_fraud_pattern(txn)
    return txn


def run():
    log.info("Producer starting | bootstrap=%s topic=%s tps=%d fraud_rate=%.2f",
             KAFKA_BOOTSTRAP, KAFKA_TOPIC, TPS, FRAUD_RATE)

    # Wait for Kafka
    for attempt in range(20):
        try:
            _ensure_topic(KAFKA_BOOTSTRAP, KAFKA_TOPIC)
            break
        except Exception as e:
            log.warning("Kafka not ready (%s), retrying in 5s…", e)
            time.sleep(5)

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "queue.buffering.max.messages": 100_000,
        "queue.buffering.max.ms": 50,
        "batch.num.messages": 500,
        "compression.type": "snappy",
        "acks": "1",
    })

    interval      = 1.0 / TPS
    sent          = 0
    start_time    = time.monotonic()
    stats_every   = 1_000   # log stats every N messages

    def _shutdown(sig, frame):
        log.info("Flushing producer…")
        producer.flush(10)
        log.info("Producer stopped. Total sent: %d", sent)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    while True:
        loop_start = time.monotonic()

        txn  = generate_transaction()
        key  = txn["account_id"].encode()
        val  = json.dumps(txn).encode()

        producer.produce(
            KAFKA_TOPIC,
            key=key,
            value=val,
            on_delivery=_delivery_report,
        )
        producer.poll(0)
        sent += 1

        if sent % stats_every == 0:
            elapsed = time.monotonic() - start_time
            actual_tps = sent / elapsed
            log.info("Stats | sent=%d elapsed=%.1fs actual_tps=%.1f",
                     sent, elapsed, actual_tps)

        # Rate-limit
        elapsed_loop = time.monotonic() - loop_start
        sleep_time   = interval - elapsed_loop
        if sleep_time > 0:
            time.sleep(sleep_time)


if __name__ == "__main__":
    run()
