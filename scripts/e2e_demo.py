from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from krako2.agent.agent import NodeAgent
from krako2.agent.claim_index import load_index
from krako2.autoscaling.controller import AutoscalingController, Metrics
from krako2.autoscaling.metrics import compute_metrics_from_registry
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
    parser.add_argument("--llm-tokens", type=int, default=None)
    parser.add_argument("--priority", default="p2")
    parser.add_argument("--burst", type=int, default=1)
    parser.add_argument("--polls", type=int, default=3)
    parser.add_argument("--simulate-pressure", choices=["none", "low", "high", "critical", "auto"], default="none")
    parser.add_argument("--multi-agent", action="store_true")
    parser.add_argument("--reset", action="store_true")
    return parser.parse_args()


def _reset_data(data_dir: Path, node_id: str) -> None:
    targets = [
        data_dir / "events.jsonl",
        data_dir / "billing_ledger.jsonl",
        data_dir / "billing_dedupe.json",
        data_dir / "wallet_snapshot.json",
        data_dir / "billing_anomalies.json",
        data_dir / "claim_index.json",
        data_dir / "scheduler_state.json",
        data_dir / "congestion_state.json",
        data_dir / "retry_budget_state.json",
        data_dir / "capacity_state.json",
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


def _upsert_node(registry_path: Path, node_doc: dict[str, Any]) -> None:
    if not registry_path.exists():
        registry_path.write_text(json.dumps({"nodes": [node_doc]}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return
    raw = json.loads(registry_path.read_text(encoding="utf-8"))
    nodes = raw.setdefault("nodes", [])
    replaced = False
    for idx, existing in enumerate(nodes):
        if existing.get("node_id") == node_doc["node_id"]:
            nodes[idx] = {**existing, **node_doc}
            replaced = True
            break
    if not replaced:
        nodes.append(node_doc)
    registry_path.write_text(json.dumps(raw, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _has_demo_completion(event_log: EventLog, work_unit_id: str, node_id: str, execution_session_id: str) -> bool:
    for event in event_log.read_events():
        if event.type.value != "workunit.completed":
            continue
        if event.work_unit_id != work_unit_id:
            continue
        payload = event.payload or {}
        if payload.get("execution_session_id") not in {execution_session_id, None}:
            continue
        if payload.get("selected_node_id") == node_id:
            return True
    return False


def run_demo(args: argparse.Namespace) -> dict[str, Any]:
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    effective_node_id = args.node_id
    if args.kind == "llm_pod" and args.node_id == "node-1":
        effective_node_id = "pod-1"
    llm_tokens = args.llm_tokens if args.llm_tokens is not None else (1200 if args.kind == "llm_pod" else 0)
    burst = max(1, int(args.burst))

    if args.reset:
        _reset_data(data_dir, effective_node_id)
        if args.multi_agent:
            (data_dir / f"agent_state_{effective_node_id}_b.json").unlink(missing_ok=True)

    _seed_node_registry(data_dir, "node-1", args.region)
    if args.kind == "llm_pod":
        _upsert_node(
            data_dir / "node_registry.json",
            {
                "node_id": "pod-1",
                "enabled": True,
                "health_status": "healthy",
                "supported_kinds": ["llm_pod"],
                "available_concurrency": 8,
                "active_queue_depth": 0,
                "utilization": 0.1,
                "trust_score": 0.7,
                "region": args.region,
                "version": "0.1.0",
                "last_heartbeat_ts": datetime.now(timezone.utc).isoformat(),
            },
        )

    event_log = EventLog(data_dir / "events.jsonl")
    publisher = EventPublisher(event_log)
    scheduler = SchedulerService(
        state_path=data_dir / "scheduler_state.json",
        retry_budget_state_path=data_dir / "retry_budget_state.json",
        congestion_state_path=data_dir / "congestion_state.json",
        trust_state_path=data_dir / "trust_state.json",
        capacity_state_path=data_dir / "capacity_state.json",
        publisher=publisher,
    )
    autoscaling = AutoscalingController(state_path=data_dir / "capacity_state.json", publisher=publisher)
    trust = TrustConsumer(state_path=data_dir / "trust_state.json", registry_path=data_dir / "node_registry.json")
    seen_event_ids: set[str] = set()

    def consume_new_events_for_trust() -> int:
        updates = 0
        for event in event_log.read_events():
            if event.id in seen_event_ids:
                continue
            seen_event_ids.add(event.id)
            if trust.consume(event):
                updates += 1
        return updates

    agent = NodeAgent(node_id=effective_node_id, data_dir=data_dir)
    second_agent: NodeAgent | None = None
    if args.multi_agent:
        second_agent = NodeAgent(
            node_id=effective_node_id,
            data_dir=data_dir,
            state_path=data_dir / f"agent_state_{effective_node_id}_b.json",
        )
    trust_updates = 0
    autoscaling_events_emitted = 0
    autoscaling_last_metrics = {"queue_depth": 0, "w95_wait_s": 0.0, "utilization": 0.0}
    # Ensure heartbeat exists before scheduling so trust can influence scheduling score.
    agent.emit_heartbeat()
    trust_updates += consume_new_events_for_trust()

    if args.simulate_pressure in {"low", "high", "critical"}:
        pressure_map = {
            "low": Metrics(queue_depth=10, w95_wait_s=0.2, utilization=0.2),
            "high": Metrics(queue_depth=300, w95_wait_s=2.5, utilization=0.85),
            "critical": Metrics(queue_depth=1000, w95_wait_s=5.0, utilization=0.95),
        }
        windows = 3 if args.simulate_pressure in {"high", "critical"} else 1
        m = pressure_map[args.simulate_pressure]
        for _ in range(windows):
            out = autoscaling.evaluate(m)
            autoscaling_events_emitted += int(out["events_emitted"])
            autoscaling_last_metrics = {
                "queue_depth": m.queue_depth,
                "w95_wait_s": m.w95_wait_s,
                "utilization": m.utilization,
            }

    scheduled_items: list[dict[str, Any]] = []
    admission_items: list[dict[str, Any]] = []
    for i in range(1, burst + 1):
        execution_session_id = f"demo-{i}"
        work_unit = WorkUnit(
            id=f"demo-{i}-workunit",
            execution_session_id=execution_session_id,
            kind=args.kind,
            region=args.region,
            required_concurrency=1,
            payload={
                "tenant_id": args.tenant_id,
                "correlation_id": f"sess:{execution_session_id}",
                "execution_session_id": execution_session_id,
                "simulated_ms": args.simulated_ms,
                "llm_tokens": llm_tokens,
                "attempt_index": 1,
                "priority": args.priority,
                "kind": args.kind,
            },
        )
        if args.kind == "llm_pod":
            work_unit.payload["prompt"] = "Tell me a joke about kraken."
            work_unit.payload["model"] = "stub-1"

        nodes = NodeRegistry(registry_path=data_dir / "node_registry.json").list_nodes()
        if _has_demo_completion(event_log, work_unit.id, effective_node_id, execution_session_id):
            scheduled = {
                "work_unit_id": work_unit.id,
                "selected_node_id": effective_node_id,
                "dispatch_event_id": None,
                "status": "already_completed",
            }
            admission = {"status": "already_completed", "reason_code": None}
        else:
            selected_node_id, debug = scheduler.schedule_and_emit(work_unit, nodes, publisher)
            scheduled = {
                "work_unit_id": work_unit.id,
                "selected_node_id": selected_node_id,
                "dispatch_event_id": debug.get("dispatch_event_id"),
                "status": "scheduled" if selected_node_id else "not_scheduled",
            }
            admission = {"status": scheduled["status"], "reason_code": debug.get("reason_code")}

        scheduled_items.append(scheduled)
        admission_items.append(admission)

    pending_windows_by_node: dict[str, int] = {}
    for s in scheduled_items:
        node = s.get("selected_node_id")
        if s.get("status") == "scheduled" and isinstance(node, str):
            pending_windows_by_node[node] = pending_windows_by_node.get(node, 0) + 1

    agent_stats = {"processed": 0, "skipped": 0}
    for _ in range(max(1, int(args.polls))):
        if args.simulate_pressure == "auto":
            registry = NodeRegistry(registry_path=data_dir / "node_registry.json")
            for node_id, pending_windows in pending_windows_by_node.items():
                util = 0.0
                if pending_windows > 0:
                    util = min(1.0, 0.81 + 0.02 * max(0, pending_windows - 1))
                registry.update_node(
                    node_id,
                    {
                        "active_queue_depth": max(0, pending_windows),
                        "utilization": util,
                    },
                )

        out = agent.poll_once()
        agent_stats["processed"] += out["processed"]
        agent_stats["skipped"] += out["skipped"]
        if second_agent is not None:
            out2 = second_agent.poll_once()
            agent_stats["processed"] += out2["processed"]
            agent_stats["skipped"] += out2["skipped"]

        if args.simulate_pressure == "auto":
            nodes_live = NodeRegistry(registry_path=data_dir / "node_registry.json").list_nodes()
            metrics = compute_metrics_from_registry(nodes_live)
            auto_out = autoscaling.evaluate(metrics)
            autoscaling_events_emitted += int(auto_out["events_emitted"])
            autoscaling_last_metrics = {
                "queue_depth": metrics.queue_depth,
                "w95_wait_s": metrics.w95_wait_s,
                "utilization": metrics.utilization,
            }
            for node_id in list(pending_windows_by_node.keys()):
                pending_windows_by_node[node_id] = max(0, pending_windows_by_node[node_id] - 1)

        trust_updates += consume_new_events_for_trust()

    # Replay billing + trust over append-only events.
    if args.kind == "llm_pod":
        os.environ["KRAKO_BILL_LLM_FROM_WORKUNIT_COMPLETED"] = "0"
        os.environ["KRAKO_BILL_LLM_FROM_INVOCATION"] = "1"
    billing = BillingConsumer(
        ledger_path=data_dir / "billing_ledger.jsonl",
        dedupe_path=data_dir / "billing_dedupe.json",
    )
    billed = 0
    for event in event_log.read_events():
        if billing.consume(event):
            billed += 1

    billing_rows: list[dict[str, Any]] = []
    ledger_path = data_dir / "billing_ledger.jsonl"
    if ledger_path.exists():
        for line in ledger_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            billing_rows.append(json.loads(line))
    cpu_rows = sum(1 for row in billing_rows if row.get("line_item_type") == "workunit_cpu")
    llm_rows = sum(1 for row in billing_rows if row.get("line_item_type") == "llm_tokens")
    ledger_llm_total = "0.000000"

    wallet = compute_wallet_snapshot(
        ledger_path=data_dir / "billing_ledger.jsonl",
        snapshot_path=data_dir / "wallet_snapshot.json",
    )

    anomalies = check_billing_anomalies(
        event_log_path=data_dir / "events.jsonl",
        ledger_path=data_dir / "billing_ledger.jsonl",
    )
    write_anomaly_report(anomalies, data_dir / "billing_anomalies.json")
    ledger_llm_total = anomalies["checks"]["global"]["ledger_llm_total_usd"]

    claim_count = 0
    completed_count = 0
    llm_invocations_completed_count = 0
    total_llm_tokens_reported = 0
    for event in event_log.read_events():
        if event.type.value == "workunit.claimed":
            claim_count += 1
        if event.type.value == "workunit.completed":
            completed_count += 1
        if event.type.value == "llm.invocation.completed":
            llm_invocations_completed_count += 1
            payload = event.payload if isinstance(event.payload, dict) else {}
            total_llm_tokens_reported += int(payload.get("total_tokens", 0))
    claim_index_size = 0
    try:
        claim_index_size = len(load_index(data_dir / "claim_index.json").get("claims", {}))
    except ValueError:
        claim_index_size = 0

    summary = {
        "effective_node_id": effective_node_id,
        "capacity_state": autoscaling.load_state(),
        "autoscaling_events_emitted": autoscaling_events_emitted,
        "autoscaling": {
            "capacity_state": autoscaling.load_state(),
            "events_emitted_total": autoscaling_events_emitted,
            "last_metrics": autoscaling_last_metrics,
        },
        "node_registry_snapshot": {
            n.node_id: {
                "active_queue_depth": n.active_queue_depth,
                "utilization": n.utilization,
                "last_heartbeat_ts": n.last_heartbeat_ts,
            }
            for n in NodeRegistry(registry_path=data_dir / "node_registry.json").list_nodes()
        },
        "scheduled": scheduled_items[0],
        "scheduled_batch": scheduled_items,
        "admission_result": admission_items[0],
        "admission_results": admission_items,
        "agent": agent_stats,
        "agent_metrics_final": {
            "active_queue_depth": agent.active_queue_depth,
            "utilization": agent.utilization,
            "available_concurrency": agent.available_concurrency,
        },
        "claim_count": claim_count,
        "claimed_count": claim_count,
        "claim_index_size": claim_index_size,
        "completed_count": completed_count,
        "llm_invocations_completed_count": llm_invocations_completed_count,
        "total_llm_tokens_reported": total_llm_tokens_reported,
        "billing": {"records_written": billed, "trust_updates": trust_updates},
        "billing_breakdown": {"cpu_rows": cpu_rows, "llm_rows": llm_rows},
        "ledger_llm_total_usd": ledger_llm_total,
        "wallet": {
            "grand_total_debit_usd": wallet["grand_total_debit_usd"],
            "grand_record_count": wallet["grand_record_count"],
            "tenants": wallet["tenants"],
        },
        "anomalies": anomalies["summary"],
    }
    summary["registry_snapshot_after"] = summary["node_registry_snapshot"]
    return summary


def main() -> int:
    args = parse_args()
    summary = run_demo(args)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
