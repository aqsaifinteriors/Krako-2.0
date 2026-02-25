from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from krako2.domain.models import WorkUnit
from krako2.scheduler.node_registry import Node
from krako2.scheduler.service import SchedulerService


def _service(tmp_path: Path) -> SchedulerService:
    return SchedulerService(
        state_path=tmp_path / "scheduler_state.json",
        retry_budget_state_path=tmp_path / "retry_budget_state.json",
        congestion_state_path=tmp_path / "congestion_state.json",
        trust_state_path=tmp_path / "trust_state.json",
    )


def test_llm_workunit_never_schedules_to_cpu_only_node(tmp_path: Path) -> None:
    service = _service(tmp_path)
    work_unit = WorkUnit(kind="llm_pod", region="eu-west")
    nodes = [
        Node(
            node_id="cpu-1",
            enabled=True,
            health_status="healthy",
            supported_kinds=["cpu"],
            available_concurrency=8,
            active_queue_depth=0,
            utilization=0.1,
            trust_score=0.9,
            region="eu-west",
            version="0.1.0",
        ),
        Node(
            node_id="pod-1",
            enabled=True,
            health_status="healthy",
            supported_kinds=["llm_pod"],
            available_concurrency=8,
            active_queue_depth=0,
            utilization=0.1,
            trust_score=0.7,
            region="eu-west",
            version="0.1.0",
        ),
    ]

    selected, _ = service.select_node_for_workunit(work_unit, nodes)
    assert selected == "pod-1"


def test_cpu_workunit_never_schedules_to_llm_only_node(tmp_path: Path) -> None:
    service = _service(tmp_path)
    work_unit = WorkUnit(kind="cpu", region="eu-west")
    nodes = [
        Node(
            node_id="pod-1",
            enabled=True,
            health_status="healthy",
            supported_kinds=["llm_pod"],
            available_concurrency=8,
            active_queue_depth=0,
            utilization=0.1,
            trust_score=0.7,
            region="eu-west",
            version="0.1.0",
        ),
        Node(
            node_id="cpu-1",
            enabled=True,
            health_status="healthy",
            supported_kinds=["cpu"],
            available_concurrency=4,
            active_queue_depth=0,
            utilization=0.2,
            trust_score=0.8,
            region="eu-west",
            version="0.1.0",
        ),
    ]

    selected, _ = service.select_node_for_workunit(work_unit, nodes)
    assert selected == "cpu-1"


def test_e2e_llm_lane_produces_nonzero_wallet_debit(tmp_path: Path) -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "e2e_demo.py"
    cmd = [
        sys.executable,
        str(script),
        "--data-dir",
        str(tmp_path),
        "--kind",
        "llm_pod",
        "--llm-tokens",
        "1200",
        "--polls",
        "2",
        "--reset",
    ]
    out = subprocess.check_output(cmd, text=True)
    summary = json.loads(out)

    assert summary["wallet"]["grand_total_debit_usd"] != "0.000000"
    assert summary["wallet"]["grand_record_count"] > 0
