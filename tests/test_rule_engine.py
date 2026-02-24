"""
Unit tests for the RuleEngine and scoring logic.
Run: pytest tests/ -v
"""
from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


# ── Minimal stub so we can test without PySpark installed ────────────────────
# Import only the pure-Python parts of fraud_engine
import importlib, types

# Patch pyspark before importing fraud_engine
for mod in ["pyspark", "pyspark.sql", "pyspark.sql.functions",
            "pyspark.sql.types", "pyspark.sql.streaming",
            "prometheus_client", "confluent_kafka", "redis"]:
    sys.modules.setdefault(mod, types.ModuleType(mod))

# Provide fake symbols used at module level
sys.modules["prometheus_client"].Counter   = lambda *a, **k: type("C", (), {"inc": lambda s: None, "observe": lambda s, v: None})()
sys.modules["prometheus_client"].Histogram = lambda *a, **k: type("H", (), {"observe": lambda s, v: None})()
sys.modules["prometheus_client"].Gauge     = lambda *a, **k: type("G", (), {"set": lambda s, v: None})()
sys.modules["prometheus_client"].start_http_server = lambda p: None

from spark.fraud_engine import RuleEngine


# ─────────────────────────────────────────────────────────────────────────────

def make_txn(**overrides) -> dict:
    base = {
        "transaction_id":    "TXN-001",
        "account_id":        "ACC-001",
        "merchant_id":       "MER-001",
        "amount":            25.00,
        "currency":          "USD",
        "transaction_type":  "PURCHASE",
        "merchant_category": "GROCERY",
        "country":           "US",
        "device_type":       "mobile_ios",
        "is_fraud":          False,
    }
    base.update(overrides)
    return base


class TestRuleEngine:

    def test_legitimate_transaction_scores_zero(self):
        score, rules = RuleEngine.evaluate(make_txn())
        assert score == 0.0
        assert rules == []

    def test_high_amount_triggers_rule(self):
        score, rules = RuleEngine.evaluate(make_txn(amount=4_000))
        assert score > 0
        assert any("HIGH_AMOUNT" in r for r in rules)

    def test_very_high_amount_scores_higher_than_high(self):
        score_high, _  = RuleEngine.evaluate(make_txn(amount=4_000))
        score_vhigh, _ = RuleEngine.evaluate(make_txn(amount=9_000))
        assert score_vhigh > score_high

    def test_gambling_mcc_triggers_rule(self):
        score, rules = RuleEngine.evaluate(make_txn(merchant_category="GAMBLING"))
        assert score > 0
        assert any("GAMBLING" in r for r in rules)

    def test_crypto_exchange_triggers_rule(self):
        score, rules = RuleEngine.evaluate(make_txn(merchant_category="CRYPTO_EXCHANGE"))
        assert any("CRYPTO_EXCHANGE" in r for r in rules)

    def test_high_risk_country_triggers_rule(self):
        score, rules = RuleEngine.evaluate(make_txn(country="NG"))
        assert score > 0
        assert any("HIGH_RISK_COUNTRY" in r for r in rules)

    def test_unknown_device_triggers_rule(self):
        score, rules = RuleEngine.evaluate(make_txn(device_type="unknown"))
        assert any("UNKNOWN_DEVICE" in r for r in rules)

    def test_micro_amount_card_testing(self):
        score, rules = RuleEngine.evaluate(make_txn(amount=0.99))
        assert any("MICRO_AMOUNT" in r for r in rules)

    def test_round_amount_triggers_rule(self):
        score, rules = RuleEngine.evaluate(make_txn(amount=1_000.00))
        assert any("ROUND" in r for r in rules)

    def test_multiple_rules_accumulate(self):
        # gambling + high risk country + unknown device
        score, rules = RuleEngine.evaluate(make_txn(
            merchant_category="GAMBLING",
            country="NG",
            device_type="unknown",
            amount=5_000,
        ))
        assert score > 0.5
        assert len(rules) >= 3

    def test_score_capped_at_one(self):
        score, _ = RuleEngine.evaluate(make_txn(
            amount=50_000,
            merchant_category="GAMBLING",
            country="NG",
            device_type="unknown",
        ))
        assert score <= 1.0

    def test_score_is_float(self):
        score, _ = RuleEngine.evaluate(make_txn())
        assert isinstance(score, float)

    def test_rules_is_list_of_strings(self):
        _, rules = RuleEngine.evaluate(make_txn(merchant_category="GAMBLING"))
        assert isinstance(rules, list)
        assert all(isinstance(r, str) for r in rules)

    def test_legitimate_grocery_purchase_no_rules(self):
        score, rules = RuleEngine.evaluate(make_txn(
            amount=12.50,
            merchant_category="GROCERY",
            country="US",
            device_type="mobile_ios",
        ))
        assert score == 0.0
        assert rules == []

    def test_amount_just_below_threshold_not_triggered(self):
        score, rules = RuleEngine.evaluate(make_txn(amount=2_999.99))
        assert not any("HIGH_AMOUNT" in r for r in rules)

    def test_amount_exactly_at_threshold_triggered(self):
        score, rules = RuleEngine.evaluate(make_txn(amount=3_000))
        assert any("HIGH_AMOUNT" in r for r in rules)
