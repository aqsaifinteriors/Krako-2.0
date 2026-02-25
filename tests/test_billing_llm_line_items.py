from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from krako2.billing.anomaly import check_billing_anomalies
from krako2.billing.consumer import BillingConsumer
from krako2.domain.models import Event, EventType


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def _workunit_completed(event_id: str, *, with_cpu: bool = True, llm_tokens: int = 120) -> Event:
    payload = {
        "tenant_id": "tenant-a",
        "correlation_id": "sess:s1",
        "execution_session_id": "s1",
        "llm_tokens": llm_tokens,
    }
    if with_cpu:
        payload["cpu_seconds"] = "1.000000"
    return Event(
        id=event_id,
        type=EventType.WORKUNIT_COMPLETED,
        idempotency_key=f"idem:{event_id}",
        work_unit_id="wu-1",
        payload=payload,
        created_at=datetime.now(timezone.utc),
    )


def _llm_invocation_completed(event_id: str, total_tokens: int = 120) -> Event:
    return Event(
        id=event_id,
        type=EventType.LLM_INVOCATION_COMPLETED,
        idempotency_key=f"idem:{event_id}",
        work_unit_id="wu-1",
        payload={
            "tenant_id": "tenant-a",
            "correlation_id": "sess:s1",
            "execution_session_id": "s1",
            "total_tokens": total_tokens,
        },
        created_at=datetime.now(timezone.utc),
    )


def test_llm_invocation_creates_llm_line_item_only(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("KRAKO_BILL_LLM_FROM_WORKUNIT_COMPLETED", raising=False)
    monkeypatch.delenv("KRAKO_BILL_LLM_FROM_INVOCATION", raising=False)

    ledger = tmp_path / "billing_ledger.jsonl"
    dedupe = tmp_path / "billing_dedupe.json"
    consumer = BillingConsumer(ledger, dedupe)

    assert consumer.consume(_workunit_completed("wu-evt", with_cpu=True, llm_tokens=100)) is True
    assert consumer.consume(_llm_invocation_completed("llm-evt", total_tokens=100)) is True

    rows = _read_jsonl(ledger)
    assert len(rows) == 2
    cpu_rows = [r for r in rows if r["line_item_type"] == "workunit_cpu"]
    llm_rows = [r for r in rows if r["line_item_type"] == "llm_tokens"]
    assert len(cpu_rows) == 1
    assert len(llm_rows) == 1
    # No token charge should be included in workunit.completed by default.
    assert cpu_rows[0]["subtotal_llm_usd"] == "0.000000"
    assert llm_rows[0]["llm_tokens_event_total"] == 100


def test_workunit_llm_flag_enables_double_charge(tmp_path: Path, monkeypatch) -> None:
    workunit_evt = _workunit_completed("wu-evt", with_cpu=False, llm_tokens=250)
    invocation_evt = _llm_invocation_completed("llm-evt", total_tokens=250)

    monkeypatch.setenv("KRAKO_BILL_LLM_FROM_WORKUNIT_COMPLETED", "0")
    monkeypatch.setenv("KRAKO_BILL_LLM_FROM_INVOCATION", "1")
    consumer_default = BillingConsumer(tmp_path / "ledger_default.jsonl", tmp_path / "dedupe_default.json")
    assert consumer_default.consume(workunit_evt) is False
    assert consumer_default.consume(invocation_evt) is True
    default_rows = _read_jsonl(tmp_path / "ledger_default.jsonl")

    monkeypatch.setenv("KRAKO_BILL_LLM_FROM_WORKUNIT_COMPLETED", "1")
    monkeypatch.setenv("KRAKO_BILL_LLM_FROM_INVOCATION", "1")
    consumer_double = BillingConsumer(tmp_path / "ledger_double.jsonl", tmp_path / "dedupe_double.json")
    assert consumer_double.consume(workunit_evt) is True
    assert consumer_double.consume(invocation_evt) is True
    double_rows = _read_jsonl(tmp_path / "ledger_double.jsonl")

    assert len(double_rows) > len(default_rows)


def test_line_item_type_field_present(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("KRAKO_BILL_LLM_FROM_WORKUNIT_COMPLETED", raising=False)
    monkeypatch.delenv("KRAKO_BILL_LLM_FROM_INVOCATION", raising=False)

    consumer = BillingConsumer(tmp_path / "billing_ledger.jsonl", tmp_path / "billing_dedupe.json")
    assert consumer.consume(_workunit_completed("evt-1", with_cpu=True, llm_tokens=12)) is True

    row = _read_jsonl(tmp_path / "billing_ledger.jsonl")[0]
    assert row["line_item_type"] == "workunit_cpu"
    assert "llm_tokens_event_total" in row


def test_anomaly_flags_token_mismatch(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    ledger = tmp_path / "billing_ledger.jsonl"

    _write_jsonl(
        events,
        [
            {
                "id": "llm-evt-1",
                "type": "llm.invocation.completed",
                "work_unit_id": "wu-1",
                "payload": {
                    "execution_session_id": "s1",
                    "total_tokens": 100,
                },
            }
        ],
    )

    _write_jsonl(
        ledger,
        [
            {
                "event_id": "llm-evt-1",
                "event_type": "llm.invocation.completed",
                "work_unit_id": "wu-1",
                "tenant_id": "tenant-a",
                "correlation_id": "sess:s1",
                "execution_session_id": "s1",
                "line_item_type": "llm_tokens",
                "cpu_seconds": "0.000000",
                "llm_tokens": 90,
                "llm_tokens_event_total": 90,
                "cpu_unit_price_usd": "0.000500",
                "llm_unit_price_usd_per_1k": "0.002000",
                "subtotal_cpu_usd": "0.000000",
                "subtotal_llm_usd": "0.000180",
                "total_usd": "0.000180",
                "currency": "USD",
                "rounded_scale": 6,
                "rounding_mode": "ROUND_HALF_EVEN",
                "pricing_version": "0.1",
            }
        ],
    )

    report = check_billing_anomalies(events, ledger)

    assert report["summary"]["sessions_flagged"] == 1
    assert "llm_token_mismatch" in (report["checks"]["sessions"][0]["flagged_reason"] or "")
