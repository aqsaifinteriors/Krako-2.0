from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from krako2.billing.money import dec, serialize_decimal


def compute_wallet_snapshot(ledger_path: Path, snapshot_path: Path) -> dict[str, Any]:
    tenants: dict[str, dict[str, Any]] = {}
    grand_total = dec("0")
    grand_count = 0

    if ledger_path.exists():
        with ledger_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)

                tenant_id = str(row.get("tenant_id", "default"))
                total_usd = dec(str(row.get("total_usd", "0")))

                if tenant_id not in tenants:
                    tenants[tenant_id] = {
                        "total_debit_usd": dec("0"),
                        "record_count": 0,
                    }

                tenants[tenant_id]["total_debit_usd"] += total_usd
                tenants[tenant_id]["record_count"] += 1
                grand_total += total_usd
                grand_count += 1

    sorted_tenants: dict[str, dict[str, Any]] = {}
    for tenant_id in sorted(tenants.keys()):
        sorted_tenants[tenant_id] = {
            "total_debit_usd": serialize_decimal(tenants[tenant_id]["total_debit_usd"]),
            "record_count": int(tenants[tenant_id]["record_count"]),
        }

    snapshot = {
        "version": "0.1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ledger_path": str(ledger_path),
        "tenants": sorted_tenants,
        "grand_total_debit_usd": serialize_decimal(grand_total),
        "grand_record_count": grand_count,
    }

    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = snapshot_path.with_suffix(snapshot_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, sort_keys=True, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, snapshot_path)

    return snapshot
