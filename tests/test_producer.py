"""
Unit tests for the transaction producer.
"""
from __future__ import annotations

import sys, os, types

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Stub confluent_kafka so we don't need a real broker
ck = types.ModuleType("confluent_kafka")
ck.Producer = object
ck.admin    = types.ModuleType("confluent_kafka.admin")

class _AdminClient:
    def list_topics(self, timeout=None):
        class T:
            topics = {}
        return T()
    def create_topics(self, topics):
        return {t.topic: type("F", (), {"result": lambda self: None})() for t in topics}

class _NewTopic:
    def __init__(self, topic, **kw): self.topic = topic

ck.admin.AdminClient = _AdminClient
ck.admin.NewTopic    = _NewTopic
sys.modules["confluent_kafka"]       = ck
sys.modules["confluent_kafka.admin"] = ck.admin

from producer.producer import generate_transaction, _inject_fraud_pattern, _make_legitimate_transaction
from datetime import date


class TestTransactionGenerator:

    def test_generate_returns_dict(self):
        txn = generate_transaction()
        assert isinstance(txn, dict)

    def test_required_fields_present(self):
        txn = generate_transaction()
        required = [
            "transaction_id", "account_id", "merchant_id", "amount",
            "currency", "transaction_type", "merchant_category",
            "country", "city", "device_type", "timestamp", "is_fraud",
        ]
        for field in required:
            assert field in txn, f"Missing field: {field}"

    def test_amount_is_positive(self):
        for _ in range(50):
            txn = generate_transaction()
            assert txn["amount"] > 0

    def test_transaction_id_is_unique(self):
        ids = {generate_transaction()["transaction_id"] for _ in range(100)}
        assert len(ids) == 100

    def test_currency_is_usd(self):
        txn = generate_transaction()
        assert txn["currency"] == "USD"

    def test_is_fraud_is_boolean(self):
        txn = generate_transaction()
        assert isinstance(txn["is_fraud"], bool)

    def test_fraud_pattern_injected(self):
        txn = _make_legitimate_transaction("ACC-001")
        fraud_txn = _inject_fraud_pattern(txn.copy())
        assert fraud_txn["is_fraud"] is True
        assert "fraud_pattern" in fraud_txn

    def test_high_amount_pattern(self):
        txn = _make_legitimate_transaction("ACC-001")
        txn["fraud_pattern"] = "high_amount"
        for _ in range(20):
            result = _inject_fraud_pattern(dict(txn, fraud_pattern=""))
            if result.get("fraud_pattern") == "high_amount":
                assert result["amount"] >= 3_000
                break

    def test_card_testing_pattern_has_micro_amount(self):
        txn = _make_legitimate_transaction("ACC-001")
        # Force card_testing by setting the pattern directly
        import unittest.mock as mock
        import random as rnd
        with mock.patch.object(rnd, "choice", return_value="card_testing"):
            result = _inject_fraud_pattern(txn.copy())
        assert result["amount"] <= 2.0
        assert result["is_fraud"] is True
