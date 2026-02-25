from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _run_demo(script: Path, data_dir: Path, reset: bool) -> dict:
    cmd = [
        sys.executable,
        str(script),
        "--data-dir",
        str(data_dir),
        "--polls",
        "2",
    ]
    if reset:
        cmd.append("--reset")
    out = subprocess.check_output(cmd, text=True)
    return json.loads(out)


def test_e2e_demo_runs_and_produces_wallet_snapshot(tmp_path: Path) -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "e2e_demo.py"

    summary = _run_demo(script, tmp_path, reset=True)

    assert summary["scheduled"]["status"] in {"scheduled", "already_completed"}
    assert (tmp_path / "billing_ledger.jsonl").exists()
    assert (tmp_path / "wallet_snapshot.json").exists()
    assert (tmp_path / "billing_anomalies.json").exists()


def test_e2e_demo_is_idempotent_on_second_run_without_reset(tmp_path: Path) -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "e2e_demo.py"

    first = _run_demo(script, tmp_path, reset=True)
    second = _run_demo(script, tmp_path, reset=False)

    assert first["wallet"]["grand_record_count"] == second["wallet"]["grand_record_count"]
    assert first["wallet"]["grand_total_debit_usd"] == second["wallet"]["grand_total_debit_usd"]
