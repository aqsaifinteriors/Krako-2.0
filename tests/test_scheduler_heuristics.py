from __future__ import annotations

import json
from pathlib import Path

from krako2.domain.models import WorkUnit
from krako2.scheduler.node_registry import Node
from krako2.scheduler.service import SchedulerService


def _service(tmp_path: Path) -> SchedulerService:
    return SchedulerService(state_path=tmp_path / "scheduler_state.json")


def test_hard_filter_excludes_unsupported_kind(tmp_path: Path) -> None:
    service = _service(tmp_path)
    work_unit = WorkUnit(kind="cpu", required_concurrency=1)
    nodes = [
        Node(
            node_id="node-1",
            enabled=True,
            health_status="healthy",
            supported_kinds=["llm_pod"],
            available_concurrency=4,
            active_queue_depth=0,
            utilization=0.2,
            trust_score=0.9,
            region="eu-west",
            version="0.1.0",
        )
    ]

    selected, debug = service.select_node_for_workunit(work_unit, nodes)

    assert selected is None
    assert debug["reason_code"] == "no_eligible_nodes"
    assert debug["eligible_node_count"] == 0


def test_deterministic_tie_break_by_queue_then_id(tmp_path: Path) -> None:
    service = _service(tmp_path)
    work_unit = WorkUnit(kind="cpu", required_concurrency=1)
    nodes = [
        Node(
            node_id="node-z",
            enabled=True,
            health_status="healthy",
            supported_kinds=["cpu"],
            available_concurrency=2,
            active_queue_depth=2,
            utilization=0.1,
            trust_score=0.8,
            region="eu-west",
            version="0.1.0",
        ),
        Node(
            node_id="node-a",
            enabled=True,
            health_status="healthy",
            supported_kinds=["cpu"],
            available_concurrency=1,
            active_queue_depth=1,
            utilization=0.1,
            trust_score=0.8,
            region="eu-west",
            version="0.1.0",
        ),
    ]

    selected, debug = service.select_node_for_workunit(work_unit, nodes)

    # Scores are equal, lower queue depth wins.
    assert selected == "node-a"
    assert debug["reason_code"] == "best_score"


def test_soft_anti_affinity_switches_when_within_threshold(tmp_path: Path) -> None:
    state_path = tmp_path / "scheduler_state.json"
    state_path.write_text(
        json.dumps(
            {
                "last_selected_node_id": "node-best",
                "node_assignment_streak": {"node-best": 6, "node-second": 0},
                "scheduling_epoch": 3,
            }
        ),
        encoding="utf-8",
    )

    service = SchedulerService(state_path=state_path)
    work_unit = WorkUnit(kind="cpu", required_concurrency=1)
    nodes = [
        Node(
            node_id="node-best",
            enabled=True,
            health_status="healthy",
            supported_kinds=["cpu"],
            available_concurrency=4,
            active_queue_depth=0,
            utilization=0.2,
            trust_score=0.90,
            region="eu-west",
            version="0.1.0",
        ),
        Node(
            node_id="node-second",
            enabled=True,
            health_status="healthy",
            supported_kinds=["cpu"],
            available_concurrency=4,
            active_queue_depth=0,
            utilization=0.2,
            trust_score=0.88,
            region="eu-west",
            version="0.1.0",
        ),
    ]

    selected, debug = service.select_node_for_workunit(work_unit, nodes)

    assert selected == "node-second"
    assert debug["reason_code"] == "anti_affinity"

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["last_selected_node_id"] == "node-second"
    assert state["node_assignment_streak"]["node-second"] == 1
