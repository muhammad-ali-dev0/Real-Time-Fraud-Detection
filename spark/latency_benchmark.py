"""
Latency Benchmark
==================
Sends tagged probe transactions into Kafka and measures the round-trip
latency until the corresponding alert appears in the fraud-alerts topic.

Reports:
  p50 / p90 / p99 / p999 latency
  Throughput (alerts/sec)
  False-positive / false-negative rates (vs ground-truth is_fraud label)
"""
from __future__ import annotations

import json
import os
import statistics
import time
import uuid
from datetime import datetime, timezone
from threading import Thread

from confluent_kafka import Producer, Consumer, KafkaError

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC      = os.getenv("KAFKA_INPUT_TOPIC", "transactions")
ALERT_TOPIC      = os.getenv("KAFKA_ALERT_TOPIC", "fraud-alerts")
NUM_PROBES       = int(os.getenv("BENCHMARK_PROBES", "200"))
TIMEOUT_SECS     = int(os.getenv("BENCHMARK_TIMEOUT", "30"))


def _make_probe(fraud: bool) -> dict:
    return {
        "transaction_id":    f"PROBE-{uuid.uuid4()}",
        "account_id":        "BENCH_ACCOUNT",
        "merchant_id":       "BENCH_MERCHANT",
        "amount":            9_999.99 if fraud else 12.50,
        "currency":          "USD",
        "transaction_type":  "PURCHASE",
        "merchant_category": "GAMBLING" if fraud else "GROCERY",
        "country":           "NG" if fraud else "US",
        "city":              "Lagos" if fraud else "New York",
        "device_type":       "unknown" if fraud else "mobile_ios",
        "ip_address":        "1.2.3.4",
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "is_fraud":          fraud,
        "_probe_sent_at":    time.monotonic(),
    }


def run_benchmark() -> dict:
    print(f"\n{'='*60}")
    print(" Fraud Detection Latency Benchmark")
    print(f"{'='*60}")
    print(f" Probes       : {NUM_PROBES}")
    print(f" Timeout      : {TIMEOUT_SECS}s")
    print(f" Kafka        : {KAFKA_BOOTSTRAP}")
    print(f"{'='*60}\n")

    sent_probes: dict[str, dict] = {}
    latencies:   list[float]     = []
    tp = fp = tn = fn = 0

    # ── Consumer thread ────────────────────────────────────────────────────────
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id":          f"benchmark-{uuid.uuid4()}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([ALERT_TOPIC])
    stop_consumer = [False]

    def consume_alerts():
        nonlocal tp, fp, tn, fn
        deadline = time.monotonic() + TIMEOUT_SECS + 60
        while not stop_consumer[0] and time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue
            alert = json.loads(msg.value().decode())
            txn_id = alert.get("transaction_id", "")
            if txn_id in sent_probes:
                probe      = sent_probes[txn_id]
                sent_at    = probe["_probe_sent_at"]
                latency_ms = (time.monotonic() - sent_at) * 1000
                latencies.append(latency_ms)

                is_fraud_gt    = probe["is_fraud"]
                detected_fraud = alert.get("label") in ("FRAUD", "REVIEW")

                if is_fraud_gt and detected_fraud:  tp += 1
                elif not is_fraud_gt and detected_fraud: fp += 1
                elif is_fraud_gt and not detected_fraud: fn += 1
                else:                                    tn += 1

        consumer.close()

    t = Thread(target=consume_alerts, daemon=True)
    t.start()

    # ── Producer ───────────────────────────────────────────────────────────────
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    fraud_count = NUM_PROBES // 5   # 20% fraud probes

    probes = (
        [_make_probe(fraud=True) for _ in range(fraud_count)] +
        [_make_probe(fraud=False) for _ in range(NUM_PROBES - fraud_count)]
    )
    import random; random.shuffle(probes)

    print(f"Sending {NUM_PROBES} probe transactions…")
    for probe in probes:
        txn_id = probe["transaction_id"]
        # Store send time separately (not serialised)
        send_time = time.monotonic()
        payload = {k: v for k, v in probe.items() if k != "_probe_sent_at"}
        probe["_probe_sent_at"] = send_time
        sent_probes[txn_id] = probe

        producer.produce(
            INPUT_TOPIC,
            key=probe["account_id"].encode(),
            value=json.dumps(payload).encode(),
        )
        producer.poll(0)
        time.sleep(0.01)

    producer.flush(10)
    print("Probes sent. Waiting for alerts…")
    time.sleep(TIMEOUT_SECS)
    stop_consumer[0] = True
    t.join(timeout=5)

    # ── Results ────────────────────────────────────────────────────────────────
    if not latencies:
        print("No alerts received – is the fraud engine running?")
        return {}

    latencies.sort()
    n = len(latencies)

    def percentile(pct: float) -> float:
        idx = min(int(n * pct / 100), n - 1)
        return latencies[idx]

    results = {
        "probes_sent":      NUM_PROBES,
        "alerts_received":  n,
        "coverage_pct":     round(n / NUM_PROBES * 100, 1),
        "latency_p50_ms":   round(percentile(50),  2),
        "latency_p90_ms":   round(percentile(90),  2),
        "latency_p99_ms":   round(percentile(99),  2),
        "latency_p999_ms":  round(percentile(99.9),2),
        "latency_mean_ms":  round(statistics.mean(latencies),   2),
        "latency_stdev_ms": round(statistics.stdev(latencies),  2) if n > 1 else 0,
        "true_positives":   tp,
        "false_positives":  fp,
        "true_negatives":   tn,
        "false_negatives":  fn,
        "precision":        round(tp / max(tp + fp, 1), 4),
        "recall":           round(tp / max(tp + fn, 1), 4),
    }
    results["f1_score"] = round(
        2 * results["precision"] * results["recall"] /
        max(results["precision"] + results["recall"], 0.0001), 4
    )

    print(f"\n{'─'*50}")
    print(" BENCHMARK RESULTS")
    print(f"{'─'*50}")
    print(f" Probes sent       : {results['probes_sent']}")
    print(f" Alerts received   : {results['alerts_received']} ({results['coverage_pct']}%)")
    print(f"")
    print(f" Latency p50       : {results['latency_p50_ms']:>8.1f} ms")
    print(f" Latency p90       : {results['latency_p90_ms']:>8.1f} ms")
    print(f" Latency p99       : {results['latency_p99_ms']:>8.1f} ms")
    print(f" Latency p99.9     : {results['latency_p999_ms']:>8.1f} ms")
    print(f" Latency mean      : {results['latency_mean_ms']:>8.1f} ms  ± {results['latency_stdev_ms']:.1f}")
    print(f"")
    print(f" Precision         : {results['precision']:.4f}")
    print(f" Recall            : {results['recall']:.4f}")
    print(f" F1 Score          : {results['f1_score']:.4f}")
    print(f" True  Positives   : {results['true_positives']}")
    print(f" False Positives   : {results['false_positives']}")
    print(f" True  Negatives   : {results['true_negatives']}")
    print(f" False Negatives   : {results['false_negatives']}")
    print(f"{'─'*50}\n")

    with open("benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print("Results saved to benchmark_results.json")
    return results


if __name__ == "__main__":
    run_benchmark()
