from __future__ import annotations

import json
from pathlib import Path

from krako2.agent.agent import NodeAgent


def _append_event(path: Path, event: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")


def _read_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _scheduled_event(event_id: str, work_unit_id: str) -> dict:
    return {
        "id": event_id,
        "type": "workunit.scheduled",
        "idempotency_key": f"schedule:{work_unit_id}:1",
        "work_unit_id": work_unit_id,
        "payload": {
            "selected_node_id": "node-1",
            "simulated_ms": 0,
            "tenant_id": "tenant-a",
            "correlation_id": f"sess:{work_unit_id}",
            "attempt_index": 1,
        },
    }


def test_first_agent_claims_second_skips(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    dispatch_event = _scheduled_event("dispatch-1", "wu-1")
    _append_event(events, dispatch_event)

    agent_a = NodeAgent(
        node_id="node-1",
        data_dir=tmp_path,
        event_log_path=events,
        state_path=tmp_path / "agent_a_state.json",
    )
    agent_b = NodeAgent(
        node_id="node-1",
        data_dir=tmp_path,
        event_log_path=events,
        state_path=tmp_path / "agent_b_state.json",
    )

    out_a = agent_a.poll_once()
    out_b = agent_b.poll_once()

    assert out_a["processed"] == 1
    assert out_b["processed"] == 0

    all_events = _read_events(events)
    completed = [e for e in all_events if e.get("type") == "workunit.completed"]
    claimed = [e for e in all_events if e.get("type") == "workunit.claimed"]
    assert len(completed) == 1
    assert len(claimed) == 1


def test_claim_prevents_double_execution_even_if_both_poll(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    dispatch_event = _scheduled_event("dispatch-2", "wu-2")
    _append_event(events, dispatch_event)

    agent_a = NodeAgent(
        node_id="node-1",
        data_dir=tmp_path,
        event_log_path=events,
        state_path=tmp_path / "agent_a_state.json",
    )
    agent_b = NodeAgent(
        node_id="node-1",
        data_dir=tmp_path,
        event_log_path=events,
        state_path=tmp_path / "agent_b_state.json",
    )

    assert agent_a.poll_once()["processed"] == 1
    _append_event(
        events,
        {
            "id": "evt-unrelated",
            "type": "billing.consumed",
            "idempotency_key": "billing:unrelated",
            "payload": {},
        },
    )
    assert agent_b.poll_once()["processed"] == 0

    all_events = _read_events(events)
    completed_for_dispatch = [
        e
        for e in all_events
        if e.get("type") == "workunit.completed" and e.get("work_unit_id") == "wu-2"
    ]
    claimed_for_dispatch = [
        e
        for e in all_events
        if e.get("type") == "workunit.claimed"
        and (e.get("payload") or {}).get("dispatch_event_id") == "dispatch-2"
    ]

    assert len(completed_for_dispatch) == 1
    assert len(claimed_for_dispatch) == 1


def test_claim_event_contains_expected_payload_fields(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    dispatch_event = _scheduled_event("dispatch-3", "wu-3")
    _append_event(events, dispatch_event)

    agent = NodeAgent(
        node_id="node-1",
        data_dir=tmp_path,
        event_log_path=events,
        state_path=tmp_path / "agent_state.json",
    )
    agent.poll_once()

    all_events = _read_events(events)
    claims = [
        e
        for e in all_events
        if e.get("type") == "workunit.claimed"
        and (e.get("payload") or {}).get("dispatch_event_id") == "dispatch-3"
    ]
    assert len(claims) == 1
    payload = claims[0]["payload"]
    assert payload["work_unit_id"] == "wu-3"
    assert payload["selected_node_id"] == "node-1"
    assert payload["node_id"] == "node-1"
    assert payload["dispatch_event_id"] == "dispatch-3"
