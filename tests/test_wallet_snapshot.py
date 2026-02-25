from __future__ import annotations

import json
from pathlib import Path

from krako2.billing.wallet import compute_wallet_snapshot


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True) + "\n")


def test_snapshot_empty_ledger_produces_zero_totals(tmp_path: Path) -> None:
    ledger = tmp_path / "billing_ledger.jsonl"
    snapshot_path = tmp_path / "wallet_snapshot.json"
    ledger.touch()

    snapshot = compute_wallet_snapshot(ledger, snapshot_path)

    assert snapshot["version"] == "0.1"
    assert snapshot["tenants"] == {}
    assert snapshot["grand_total_debit_usd"] == "0.000000"
    assert snapshot["grand_record_count"] == 0


def test_snapshot_sums_per_tenant_deterministically(tmp_path: Path) -> None:
    ledger = tmp_path / "billing_ledger.jsonl"
    snapshot_path = tmp_path / "wallet_snapshot.json"
    _write_jsonl(
        ledger,
        [
            {"tenant_id": "tenant-b", "total_usd": "1.000000"},
            {"tenant_id": "tenant-a", "total_usd": "2.500000"},
            {"tenant_id": "tenant-a", "total_usd": "0.500000"},
        ],
    )

    snapshot = compute_wallet_snapshot(ledger, snapshot_path)

    assert list(snapshot["tenants"].keys()) == ["tenant-a", "tenant-b"]
    assert snapshot["tenants"]["tenant-a"]["total_debit_usd"] == "3.000000"
    assert snapshot["tenants"]["tenant-a"]["record_count"] == 2
    assert snapshot["tenants"]["tenant-b"]["total_debit_usd"] == "1.000000"
    assert snapshot["tenants"]["tenant-b"]["record_count"] == 1
    assert snapshot["grand_total_debit_usd"] == "4.000000"
    assert snapshot["grand_record_count"] == 3


def test_snapshot_is_stable_over_repeated_compute(tmp_path: Path) -> None:
    ledger = tmp_path / "billing_ledger.jsonl"
    snapshot_path = tmp_path / "wallet_snapshot.json"
    _write_jsonl(
        ledger,
        [
            {"tenant_id": "tenant-z", "total_usd": "0.333333"},
            {"tenant_id": "tenant-z", "total_usd": "0.666667"},
            {"tenant_id": "tenant-y", "total_usd": "5.000000"},
        ],
    )

    first = compute_wallet_snapshot(ledger, snapshot_path)
    second = compute_wallet_snapshot(ledger, snapshot_path)

    for key in ["grand_total_debit_usd", "grand_record_count", "tenants", "ledger_path", "version"]:
        assert first[key] == second[key]
