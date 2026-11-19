from datetime import datetime, timezone
from decimal import Decimal

from retail_etl.hashing import sha256_attributes


def test_hash_is_deterministic_and_ignores_untracked_fields():
    first = {
        "customer_name": "Asha Rao",
        "email": "asha@example.test",
        "updated_at": datetime(2026, 11, 18, tzinfo=timezone.utc),
    }
    second = {**first, "updated_at": datetime(2026, 11, 19, tzinfo=timezone.utc)}
    assert sha256_attributes(first, ["customer_name", "email"]) == sha256_attributes(
        second, ["customer_name", "email"]
    )


def test_hash_changes_when_tracked_value_changes():
    base = {"customer_name": "Asha Rao", "city": "Hyderabad", "balance": Decimal("10.00")}
    changed = {**base, "city": "Bengaluru"}
    assert sha256_attributes(base, base.keys()) != sha256_attributes(changed, changed.keys())


def test_hash_distinguishes_null_from_literal_null_text():
    null_value = {"phone": None}
    text_value = {"phone": "null"}
    assert sha256_attributes(null_value, ["phone"]) != sha256_attributes(text_value, ["phone"])
