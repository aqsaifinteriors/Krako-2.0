from __future__ import annotations

import json
from pathlib import Path

from krako2.billing.anomaly import check_billing_anomalies


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def _ledger_row(**kwargs) -> dict:
    row = {
        "event_id": "e-1",
        "event_type": "workunit.submitted",
        "work_unit_id": "sess:s1:wu-1",
        "tenant_id": "tenant-a",
        "correlation_id": "sess:s1",
        "cpu_seconds": "1.000000",
        "llm_tokens": 0,
        "cpu_unit_price_usd": "1.000000",
        "llm_unit_price_usd_per_1k": "0.000000",
        "subtotal_cpu_usd": "1.000000",
        "subtotal_llm_usd": "0.000000",
        "total_usd": "1.000000",
        "currency": "USD",
        "rounded_scale": 6,
        "rounding_mode": "ROUND_HALF_EVEN",
        "pricing_version": "0.1",
    }
    row.update(kwargs)
    return row


def test_global_schema_validation_flags_missing_fields(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    ledger = tmp_path / "billing_ledger.jsonl"
    _write_jsonl(events, [])
    _write_jsonl(ledger, [{"event_id": "x", "total_usd": "1.000000"}])

    report = check_billing_anomalies(events, ledger)

    assert report["summary"]["global_flagged"] is True
    assert "missing_required_schema_fields" in report["checks"]["global"]["flagged_reasons"]


def test_global_total_non_negative(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    ledger = tmp_path / "billing_ledger.jsonl"
    _write_jsonl(events, [])
    _write_jsonl(ledger, [_ledger_row(total_usd="-0.100000")])

    report = check_billing_anomalies(events, ledger)

    assert report["summary"]["global_flagged"] is True
    assert "ledger_total_negative" in report["checks"]["global"]["flagged_reasons"]


def test_session_diff_flags_when_mapping_present(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    ledger = tmp_path / "billing_ledger.jsonl"

    _write_jsonl(
        events,
        [
            {
                "type": "ExecutionSessionCompleted",
                "payload": {"execution_session_id": "s1", "total_usd": "10.000000"},
            }
        ],
    )
    _write_jsonl(ledger, [_ledger_row(correlation_id="sess:s1", total_usd="8.500000")])

    report = check_billing_anomalies(events, ledger)

    assert report["summary"]["sessions_checked"] == 1
    assert report["summary"]["sessions_flagged"] == 1
    session = report["checks"]["sessions"][0]
    assert session["execution_session_id"] == "s1"
    assert session["flagged_reason"] is not None


def test_no_flag_when_expected_equals_actual(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    ledger = tmp_path / "billing_ledger.jsonl"

    _write_jsonl(
        events,
        [
            {
                "type": "ExecutionSessionCompleted",
                "payload": {
                    "execution_session_id": "s1",
                    "total_usage": {"total_usd": "1.000000"},
                },
            }
        ],
    )
    _write_jsonl(
        ledger,
        [
            _ledger_row(event_id="e-1", correlation_id="sess:s1", total_usd="0.400000"),
            _ledger_row(event_id="e-2", correlation_id="sess:s1", total_usd="0.600000"),
        ],
    )

    report = check_billing_anomalies(events, ledger)

    assert report["summary"]["global_flagged"] is False
    assert report["summary"]["sessions_checked"] == 1
    assert report["summary"]["sessions_flagged"] == 0
    assert report["checks"]["sessions"][0]["flagged_reason"] is None
