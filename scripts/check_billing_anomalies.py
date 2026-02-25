from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from krako2.billing.anomaly import check_billing_anomalies, write_anomaly_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check billing ledger anomalies")
    parser.add_argument("--events", default="data/events.jsonl")
    parser.add_argument("--ledger", default="data/billing_ledger.jsonl")
    parser.add_argument("--out", default="data/billing_anomalies.json")
    parser.add_argument("--fail-on-flag", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = check_billing_anomalies(Path(args.events), Path(args.ledger))
    write_anomaly_report(report, Path(args.out))
    print(json.dumps(report, sort_keys=True))

    if args.fail_on_flag and (
        report["summary"]["global_flagged"] or report["summary"]["sessions_flagged"] > 0
    ):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
