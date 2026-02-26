"""Schema contract tests — validate that cleaned Parquet output conforms to JSON Schema contracts."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import jsonschema

_CONTRACTS_DIR = Path(__file__).resolve().parents[2] / "data" / "contracts"


def _load_contract(name: str) -> dict:
    schema_path = _CONTRACTS_DIR / f"{name}.schema.json"
    return json.loads(schema_path.read_text(encoding="utf-8"))


class TestUserContract:
    schema = _load_contract("users")

    def test_valid_user(self) -> None:
        record = {
            "user_id": "abc123",
            "full_name": "João Silva",
            "email": "joao.silva@gmail.com",
            "phone_number": "+55 11 98765-4321",
            "customer_city": "São Paulo",
            "customer_state": "SP",
            "loyalty_tier": "Gold",
            "created_at": "2018-01-15T10:30:00",
        }
        jsonschema.validate(record, self.schema)

    def test_nullable_fields(self) -> None:
        record = {
            "user_id": "abc123",
            "full_name": "João Silva",
            "email": None,
            "phone_number": None,
            "customer_city": "São Paulo",
            "customer_state": "SP",
            "loyalty_tier": None,
            "created_at": "2018-01-15T10:30:00",
        }
        jsonschema.validate(record, self.schema)

    def test_missing_required_field_fails(self) -> None:
        record = {"user_id": "abc123", "email": "a@b.com"}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(record, self.schema)

    def test_invalid_loyalty_tier_fails(self) -> None:
        record = {
            "user_id": "abc123",
            "full_name": "João Silva",
            "email": None,
            "phone_number": None,
            "customer_city": "São Paulo",
            "customer_state": "SP",
            "loyalty_tier": "Diamond",
            "created_at": "2018-01-15T10:30:00",
        }
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(record, self.schema)


class TestProductContract:
    schema = _load_contract("products")

    def test_valid_product(self) -> None:
        record = {
            "product_id": "prod001",
            "product_name": "Health Beauty Product #1",
            "category": "health_beauty",
            "cost_price": 49.90,
        }
        jsonschema.validate(record, self.schema)

    def test_negative_cost_fails(self) -> None:
        record = {
            "product_id": "prod001",
            "product_name": "Test",
            "category": None,
            "cost_price": -10.0,
        }
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(record, self.schema)


class TestOrderContract:
    schema = _load_contract("orders")

    def test_valid_order(self) -> None:
        record = {
            "order_id": "ord001",
            "user_id": "user001",
            "total_amount": 199.99,
            "order_status": "Completed",
            "payment_method": "Credit Card",
            "created_at": "2018-06-01T14:00:00",
        }
        jsonschema.validate(record, self.schema)

    def test_invalid_status_fails(self) -> None:
        record = {
            "order_id": "ord001",
            "user_id": None,
            "total_amount": 10,
            "order_status": "shipped",
            "payment_method": None,
            "created_at": "2018-06-01T14:00:00",
        }
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(record, self.schema)


class TestEventContract:
    schema = _load_contract("events")

    def test_valid_event(self) -> None:
        record = {
            "event_id": "evt_001",
            "timestamp": "2018-07-01T10:00:00.123",
            "user_id": "user001",
            "session_id": "sess_abc",
            "event_type": "view_item",
            "product_id": "prod001",
            "device_os": "Android",
            "time_spent_seconds": 45,
        }
        jsonschema.validate(record, self.schema)

    def test_invalid_event_type_fails(self) -> None:
        record = {
            "event_id": "evt_001",
            "timestamp": "2018-07-01T10:00:00",
            "user_id": None,
            "session_id": "sess_abc",
            "event_type": "purchase",
            "product_id": None,
            "device_os": None,
            "time_spent_seconds": None,
        }
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(record, self.schema)
