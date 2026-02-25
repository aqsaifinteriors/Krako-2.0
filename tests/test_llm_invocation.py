from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from krako2.agent.agent import NodeAgent


def _append_event(path: Path, event: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")


def _read_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _append_llm_scheduled(events: Path, event_id: str, work_unit_id: str) -> None:
    _append_event(
        events,
        {
            "id": event_id,
            "type": "workunit.scheduled",
            "idempotency_key": f"schedule:{work_unit_id}:1",
            "work_unit_id": work_unit_id,
            "payload": {
                "selected_node_id": "node-1",
                "kind": "llm_pod",
                "prompt": "Tell me a joke about kraken.",
                "model": "stub-1",
                "simulated_ms": 0,
                "tenant_id": "tenant-a",
                "correlation_id": f"sess:{work_unit_id}",
                "attempt_index": 1,
            },
        },
    )


def test_llm_pod_emits_llm_invocation_completed(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    _append_llm_scheduled(events, "dispatch-1", "wu-1")

    agent = NodeAgent(
        node_id="node-1",
        data_dir=tmp_path,
        event_log_path=events,
        state_path=tmp_path / "agent_state.json",
    )
    out = agent.poll_once()
    assert out["processed"] == 1

    rows = _read_events(events)
    completed = [e for e in rows if e.get("type") == "llm.invocation.completed" and e.get("work_unit_id") == "wu-1"]
    assert len(completed) == 1
    payload = completed[0]["payload"]
    assert isinstance(payload.get("tokens_in"), int)
    assert isinstance(payload.get("tokens_out"), int)
    assert isinstance(payload.get("total_tokens"), int)
    assert payload["total_tokens"] > 0


def test_llm_tokens_flow_into_workunit_completed_payload(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    _append_llm_scheduled(events, "dispatch-2", "wu-2")

    agent = NodeAgent(
        node_id="node-1",
        data_dir=tmp_path,
        event_log_path=events,
        state_path=tmp_path / "agent_state.json",
    )
    agent.poll_once()

    rows = _read_events(events)
    invocations = [e for e in rows if e.get("type") == "llm.invocation.completed" and e.get("work_unit_id") == "wu-2"]
    completions = [e for e in rows if e.get("type") == "workunit.completed" and e.get("work_unit_id") == "wu-2"]

    assert len(invocations) == 1
    assert len(completions) == 1

    inv_total = int(invocations[0]["payload"]["total_tokens"])
    done_llm_tokens = int(completions[0]["payload"]["llm_tokens"])
    assert done_llm_tokens == inv_total


def test_e2e_llm_pod_reports_llm_invocation_count(tmp_path: Path) -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "e2e_demo.py"
    out = subprocess.check_output(
        [
            sys.executable,
            str(script),
            "--data-dir",
            str(tmp_path),
            "--kind",
            "llm_pod",
            "--polls",
            "2",
            "--reset",
        ],
        text=True,
    )
    summary = json.loads(out)
    assert summary["llm_invocations_completed_count"] > 0
    assert summary["total_llm_tokens_reported"] > 0
