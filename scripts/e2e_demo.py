from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from krako2.agent.agent import NodeAgent
from krako2.billing.anomaly import check_billing_anomalies, write_anomaly_report
from krako2.billing.consumer import BillingConsumer
from krako2.billing.wallet import compute_wallet_snapshot
from krako2.domain.models import WorkUnit
from krako2.scheduler.node_registry import NodeRegistry
from krako2.scheduler.service import SchedulerService
from krako2.storage.event_log import EventLog
from krako2.telemetry.publisher import EventPublisher
from krako2.trust.consumer import TrustConsumer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic Krako2 E2E demo")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--node-id", default="node-1")
    parser.add_argument("--region", default="eu-west")
    parser.add_argument("--kind", default="cpu")
    parser.add_argument("--simulated-ms", type=int, default=10)
    parser.add_argument("--tenant-id", default="tenant-a")
    parser.add_argument("--llm-tokens", type=int, default=0)
    parser.add_argument("--polls", type=int, default=3)
    parser.add_argument("--reset", action="store_true")
    return parser.parse_args()


def _reset_data(data_dir: Path, node_id: str) -> None:
    targets = [
        data_dir / "events.jsonl",
        data_dir / "billing_ledger.jsonl",
        data_dir / "billing_dedupe.json",
        data_dir / "wallet_snapshot.json",
        data_dir / "billing_anomalies.json",
        data_dir / "scheduler_state.json",
        data_dir / "congestion_state.json",
        data_dir / "retry_budget_state.json",
        data_dir / f"agent_state_{node_id}.json",
        data_dir / "node_registry.json",
    ]
    for p in targets:
        p.unlink(missing_ok=True)


def _seed_node_registry(data_dir: Path, node_id: str, region: str) -> None:
    registry_path = data_dir / "node_registry.json"
    registry = NodeRegistry(registry_path=registry_path)
    nodes = registry.list_nodes()

    node_doc = {
        "node_id": node_id,
        "enabled": True,
        "health_status": "healthy",
        "supported_kinds": ["cpu"],
        "available_concurrency": 4,
        "active_queue_depth": 0,
        "utilization": 0.2,
        "trust_score": 0.8,
        "region": region,
        "version": "0.1.0",
        "last_heartbeat_ts": datetime.now(timezone.utc).isoformat(),
    }

    if not nodes:
        registry_path.write_text(json.dumps({"nodes": [node_doc]}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return

    existing = {n.node_id for n in nodes}
    if node_id in existing:
        registry.update_node(node_id, node_doc)
    else:
        raw = json.loads(registry_path.read_text(encoding="utf-8"))
        raw.setdefault("nodes", []).append(node_doc)
        registry_path.write_text(json.dumps(raw, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _has_demo_completion(event_log: EventLog, work_unit_id: str, node_id: str) -> bool:
    for event in event_log.read_events():
        if event.type.value != "workunit.completed":
            continue
        if event.work_unit_id != work_unit_id:
            continue
        payload = event.payload or {}
        if payload.get("selected_node_id") == node_id:
            return True
    return False


def run_demo(args: argparse.Namespace) -> dict[str, Any]:
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    if args.reset:
        _reset_data(data_dir, args.node_id)

    _seed_node_registry(data_dir, args.node_id, args.region)

    event_log = EventLog(data_dir / "events.jsonl")
    publisher = EventPublisher(event_log)
    scheduler = SchedulerService(
        state_path=data_dir / "scheduler_state.json",
        retry_budget_state_path=data_dir / "retry_budget_state.json",
        congestion_state_path=data_dir / "congestion_state.json",
        publisher=publisher,
    )

    work_unit = WorkUnit(
        id="demo-1-workunit",
        kind=args.kind,
        region=args.region,
        required_concurrency=1,
        payload={
            "tenant_id": args.tenant_id,
            "correlation_id": "sess:demo-1",
            "simulated_ms": args.simulated_ms,
            "llm_tokens": args.llm_tokens,
            "attempt_index": 1,
        },
    )

    nodes = NodeRegistry(registry_path=data_dir / "node_registry.json").list_nodes()
    if _has_demo_completion(event_log, work_unit.id, args.node_id):
        scheduled = {
            "selected_node_id": args.node_id,
            "dispatch_event_id": None,
            "status": "already_completed",
        }
    else:
        selected_node_id, debug = scheduler.schedule_and_emit(work_unit, nodes, publisher)
        scheduled = {
            "selected_node_id": selected_node_id,
            "dispatch_event_id": debug.get("dispatch_event_id"),
            "status": "scheduled" if selected_node_id else "not_scheduled",
        }

    agent = NodeAgent(node_id=args.node_id, data_dir=data_dir)
    agent_stats = {"processed": 0, "skipped": 0}
    for _ in range(max(1, int(args.polls))):
        out = agent.poll_once()
        agent_stats["processed"] += out["processed"]
        agent_stats["skipped"] += out["skipped"]

    # Replay billing + trust over append-only events.
    billing = BillingConsumer(
        ledger_path=data_dir / "billing_ledger.jsonl",
        dedupe_path=data_dir / "billing_dedupe.json",
    )
    trust = TrustConsumer(data_dir / "trust_state.json")

    billed = 0
    trust_updates = 0
    for event in event_log.read_events():
        if billing.consume(event):
            billed += 1
        if trust.consume(event):
            trust_updates += 1

    wallet = compute_wallet_snapshot(
        ledger_path=data_dir / "billing_ledger.jsonl",
        snapshot_path=data_dir / "wallet_snapshot.json",
    )

    anomalies = check_billing_anomalies(
        event_log_path=data_dir / "events.jsonl",
        ledger_path=data_dir / "billing_ledger.jsonl",
    )
    write_anomaly_report(anomalies, data_dir / "billing_anomalies.json")

    summary = {
        "scheduled": scheduled,
        "agent": agent_stats,
        "billing": {"records_written": billed, "trust_updates": trust_updates},
        "wallet": {
            "grand_total_debit_usd": wallet["grand_total_debit_usd"],
            "grand_record_count": wallet["grand_record_count"],
            "tenants": wallet["tenants"],
        },
        "anomalies": anomalies["summary"],
    }
    return summary


def main() -> int:
    args = parse_args()
    summary = run_demo(args)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
