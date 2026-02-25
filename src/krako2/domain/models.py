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
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class EventType(str, Enum):
    WORKUNIT_SUBMITTED = "workunit.submitted"
    WORKUNIT_SCHEDULED = "workunit.scheduled"
    BILLING_CONSUMED = "billing.consumed"
    TRUST_CONSUMED = "trust.consumed"


class Event(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    type: EventType
    idempotency_key: str
    work_unit_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
