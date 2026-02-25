from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from krako2.billing.wallet import compute_wallet_snapshot


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute wallet snapshot from billing ledger")
    parser.add_argument("--ledger", default="data/billing_ledger.jsonl")
    parser.add_argument("--out", default="data/wallet_snapshot.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ledger_path = Path(args.ledger)
    out_path = Path(args.out)
    snapshot = compute_wallet_snapshot(ledger_path=ledger_path, snapshot_path=out_path)
    print(json.dumps(snapshot, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
