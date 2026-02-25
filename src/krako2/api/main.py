from __future__ import annotations

import argparse
from typing import Any

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel, Field

from krako2.domain.models import EventType, WorkUnit
from krako2.scheduler.node_registry import NodeRegistry
from krako2.scheduler.service import SchedulerService
from krako2.storage.event_log import EventLog
from krako2.telemetry.publisher import EventPublisher

app = FastAPI(title="Krako 2.0 API")
_event_log = EventLog()
_publisher = EventPublisher(_event_log)
_node_registry = NodeRegistry()
_scheduler = SchedulerService()


class SubmitWorkUnitRequest(BaseModel):
    kind: str
    region: str | None = None
    required_concurrency: int = 1
    min_runtime_version: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/workunits/submit")
def submit_workunit(request: SubmitWorkUnitRequest) -> dict[str, object]:
    work_unit = WorkUnit(
        kind=request.kind,
        region=request.region,
        required_concurrency=request.required_concurrency,
        min_runtime_version=request.min_runtime_version,
        payload=request.payload,
    )

    nodes = _node_registry.list_nodes()
    selected_node_id, debug = _scheduler.schedule_and_emit(work_unit, nodes, _publisher)
    return {
        "work_unit_id": work_unit.id,
        "selected_node_id": selected_node_id,
        "debug": debug,
    }


@app.post("/work-units")
def submit_work_unit(work_unit: WorkUnit) -> dict[str, object]:
    event, created = _publisher.emit(
        EventType.WORKUNIT_SUBMITTED,
        idempotency_key=f"workunit:{work_unit.id}",
        work_unit_id=work_unit.id,
        payload={
            "kind": work_unit.kind,
            "region": work_unit.region,
            "required_concurrency": work_unit.required_concurrency,
            **work_unit.payload,
        },
    )
    return {
        "work_unit_id": work_unit.id,
        "event_id": event.id,
        "event_created": created,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Krako 2.0 API")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--check", action="store_true", help="Validate app import and exit")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.check:
        print("krako2.api.main import OK")
    else:
        uvicorn.run("krako2.api.main:app", host=args.host, port=args.port)
