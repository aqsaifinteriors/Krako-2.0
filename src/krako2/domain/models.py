from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class WorkUnit(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    kind: str = "generic"
    region: str | None = None
    required_concurrency: int = 1
    min_runtime_version: str | None = None
    execution_session_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class EventType(str, Enum):
    NODE_HEALTH_UPDATED = "node.health.updated"
    CAPACITY_SCALE_REQUESTED = "capacity.scale.requested"
    CAPACITY_ADMISSION_MODE_CHANGED = "capacity.admission.mode.changed"
    WORKUNIT_SUBMITTED = "workunit.submitted"
    WORKUNIT_SCHEDULING_DEFERRED = "workunit.scheduling.deferred"
    WORKUNIT_REJECTED = "workunit.rejected"
    WORKUNIT_SCHEDULED = "workunit.scheduled"
    WORKUNIT_CLAIMED = "workunit.claimed"
    WORKUNIT_COMPLETED = "workunit.completed"
    WORKUNIT_FAILED = "workunit.failed"
    LLM_INVOCATION_COMPLETED = "llm.invocation.completed"
    LLM_INVOCATION_FAILED = "llm.invocation.failed"
    WORKUNIT_RETRY_SCHEDULED = "workunit.retry.scheduled"
    WORKUNIT_RETRY_DROPPED = "workunit.retry.dropped"
    CONGESTION_MODE_CHANGED = "congestion.mode.changed"
    BILLING_CONSUMED = "billing.consumed"
    TRUST_CONSUMED = "trust.consumed"


class Event(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    type: EventType
    idempotency_key: str
    work_unit_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
