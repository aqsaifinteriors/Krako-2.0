from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from krako2.billing.consumer import BillingConsumer
from krako2.billing.money import dec, quant6, serialize_decimal
from krako2.domain.models import Event, EventType


def _make_event(event_id: str, cpu_seconds: str = "10", llm_tokens: int = 2000) -> Event:
    return Event(
        id=event_id,
        type=EventType.WORKUNIT_COMPLETED,
        idempotency_key=f"idem:{event_id}",
        work_unit_id=f"wu:{event_id}",
        payload={
            "tenant_id": "tenant-a",
            "correlation_id": f"corr:{event_id}",
            "cpu_seconds": cpu_seconds,
            "llm_tokens": llm_tokens,
            "amount": "999.123",  # ignored in v0.1
        },
        created_at=datetime.now(timezone.utc),
    )


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def test_duplicate_event_id_is_noop(tmp_path: Path) -> None:
    ledger = tmp_path / "billing_ledger.jsonl"
    dedupe = tmp_path / "billing_dedupe.json"
    consumer = BillingConsumer(ledger, dedupe)

    event = _make_event("e-1")
    assert consumer.consume(event) is True
    assert consumer.consume(event) is False

    rows = _read_jsonl(ledger)
    assert len(rows) == 1


def test_replay_twice_no_extra_rows(tmp_path: Path) -> None:
    ledger = tmp_path / "billing_ledger.jsonl"
    dedupe = tmp_path / "billing_dedupe.json"
    consumer = BillingConsumer(ledger, dedupe)

    events = [_make_event("e-1"), _make_event("e-2", cpu_seconds="3.5", llm_tokens=0)]
    for e in events:
        assert consumer.consume(e) is True

    for e in events:
        assert consumer.consume(e) is False

    rows = _read_jsonl(ledger)
    assert len(rows) == 2


def test_decimal_serialization_exact_6dp() -> None:
    assert serialize_decimal(dec("1")) == "1.000000"
    assert serialize_decimal(dec("1.2")) == "1.200000"
    assert serialize_decimal(dec("0.000001")) == "0.000001"


def test_round_half_even_boundary() -> None:
    assert serialize_decimal(quant6(dec("1.2345645"))) == "1.234564"
    assert serialize_decimal(quant6(dec("1.2345655"))) == "1.234566"


def test_schema_fields_present(tmp_path: Path) -> None:
    ledger = tmp_path / "billing_ledger.jsonl"
    dedupe = tmp_path / "billing_dedupe.json"
    consumer = BillingConsumer(ledger, dedupe)

    assert consumer.consume(_make_event("e-3", cpu_seconds="2", llm_tokens=1500)) is True

    row = _read_jsonl(ledger)[0]
    expected = {
        "event_id",
        "event_type",
        "work_unit_id",
        "tenant_id",
        "correlation_id",
        "execution_session_id",
        "line_item_type",
        "cpu_seconds",
        "llm_tokens",
        "llm_tokens_event_total",
        "cpu_unit_price_usd",
        "llm_unit_price_usd_per_1k",
        "subtotal_cpu_usd",
        "subtotal_llm_usd",
        "total_usd",
        "currency",
        "rounded_scale",
        "rounding_mode",
        "pricing_version",
    }
    assert expected.issubset(set(row.keys()))
    assert row["currency"] == "USD"
    assert row["rounded_scale"] == 6
    assert row["rounding_mode"] == "ROUND_HALF_EVEN"
    assert row["pricing_version"] == "0.1"
