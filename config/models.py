"""
Shared data models used by producer, Spark engine, and dashboard.
"""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
import json


class TransactionType(str, Enum):
    PURCHASE     = "PURCHASE"
    TRANSFER     = "TRANSFER"
    WITHDRAWAL   = "WITHDRAWAL"
    REFUND       = "REFUND"


class FraudLabel(str, Enum):
    LEGITIMATE = "LEGITIMATE"
    FRAUD      = "FRAUD"
    REVIEW     = "REVIEW"           # flagged, human review needed


@dataclass
class Transaction:
    transaction_id:   str
    account_id:       str
    merchant_id:      str
    amount:           float
    currency:         str
    transaction_type: str
    merchant_category: str
    country:          str
    city:             str
    device_type:      str
    ip_address:       str
    timestamp:        str           # ISO-8601 UTC
    is_fraud:         bool = False  # ground-truth label (for simulation)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Transaction":
        return cls(**d)

    @classmethod
    def from_json(cls, s: str) -> "Transaction":
        return cls.from_dict(json.loads(s))


@dataclass
class FraudAlert:
    alert_id:         str
    transaction_id:   str
    account_id:       str
    amount:           float
    fraud_score:      float          # 0.0 – 1.0
    label:            str            # FraudLabel value
    rules_triggered:  list[str]
    detection_latency_ms: float      # end-to-end latency
    detected_at:      str            # ISO-8601

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_dict(cls, d: dict) -> "FraudAlert":
        return cls(**d)


# ── MCC categories ────────────────────────────────────────────────────────────
MERCHANT_CATEGORIES = [
    "GROCERY", "ELECTRONICS", "TRAVEL", "RESTAURANT", "FUEL",
    "PHARMACY", "ENTERTAINMENT", "GAMBLING", "CRYPTO_EXCHANGE",
    "MONEY_TRANSFER", "ATM", "RETAIL",
]

HIGH_RISK_CATEGORIES = {"GAMBLING", "CRYPTO_EXCHANGE", "MONEY_TRANSFER", "ATM"}

COUNTRIES = [
    ("US", ["New York", "Los Angeles", "Chicago", "Houston"]),
    ("GB", ["London", "Manchester", "Birmingham"]),
    ("DE", ["Berlin", "Munich", "Hamburg"]),
    ("NG", ["Lagos", "Abuja"]),
    ("RO", ["Bucharest", "Cluj"]),
    ("BR", ["São Paulo", "Rio de Janeiro"]),
]

DEVICE_TYPES = ["mobile_ios", "mobile_android", "desktop_chrome",
                "desktop_safari", "api_key", "unknown"]
