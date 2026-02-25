from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from krako2.billing.money import quant6, serialize_decimal
from krako2.domain.models import EventType
from krako2.storage.event_log import EventLog
from krako2.telemetry.publisher import EventPublisher


class NodeAgent:
    def __init__(
        self,
        *,
        node_id: str,
        data_dir: str | Path = "data",
        poll_interval_ms: int = 500,
        event_log_path: str | Path | None = None,
        state_path: str | Path | None = None,
        publisher: EventPublisher | None = None,
    ) -> None:
        self.node_id = node_id
        self.data_dir = Path(data_dir)
        self.poll_interval_ms = int(poll_interval_ms)
        self.event_log_path = Path(event_log_path) if event_log_path else self.data_dir / "events.jsonl"
        self.state_path = Path(state_path) if state_path else self.data_dir / f"agent_state_{node_id}.json"
        self.region = os.getenv("NODE_REGION")

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.event_log_path.parent.mkdir(parents=True, exist_ok=True)
        self.event_log_path.touch(exist_ok=True)

        self.publisher = publisher or EventPublisher(EventLog(self.event_log_path))

        default_state = {
            "last_offset_bytes": 0,
            "processed_event_ids": [],
            "claimed_dispatch_event_ids": [],
            "active_queue_depth": 0,
            "utilization": 0.2,
            "available_concurrency": 4,
        }
        if not self.state_path.exists():
            self._atomic_write_state(default_state)

        state = self._read_state()
        self.active_queue_depth = int(state.get("active_queue_depth", 0))
        self.utilization = float(state.get("utilization", 0.2))
        self.available_concurrency = int(state.get("available_concurrency", 4))
        self._last_heartbeat_epoch_ms = 0

    @staticmethod
    def _clamp(value: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, value))

    def _read_state(self) -> dict[str, Any]:
        with self.state_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _atomic_write_state(self, state: dict[str, Any]) -> None:
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, sort_keys=True, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.state_path)

    def _persist_metrics(self) -> None:
        state = self._read_state()
        state["active_queue_depth"] = int(self.active_queue_depth)
        state["utilization"] = float(self.utilization)
        state["available_concurrency"] = int(self.available_concurrency)
        self._atomic_write_state(state)

    def _tail_new_events(self) -> list[dict[str, Any]]:
        state = self._read_state()
        offset = int(state.get("last_offset_bytes", 0))
        rows: list[dict[str, Any]] = []

        with self.event_log_path.open("rb") as f:
            f.seek(offset)
            while True:
                line = f.readline()
                if not line:
                    break
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    obj = json.loads(stripped.decode("utf-8"))
                    if isinstance(obj, dict):
                        rows.append(obj)
                except Exception:
                    continue
            new_offset = f.tell()

        state["last_offset_bytes"] = new_offset
        state["active_queue_depth"] = int(self.active_queue_depth)
        state["utilization"] = float(self.utilization)
        state["available_concurrency"] = int(self.available_concurrency)
        self._atomic_write_state(state)
        return rows

    def _already_processed(self, event_id: str) -> bool:
        state = self._read_state()
        return event_id in set(state.get("processed_event_ids", []))

    def _mark_processed(self, event_id: str) -> None:
        state = self._read_state()
        ids = list(state.get("processed_event_ids", []))
        if event_id in ids:
            return
        ids.append(event_id)
        if len(ids) > 2000:
            ids = ids[-2000:]
        state["processed_event_ids"] = ids
        state["active_queue_depth"] = int(self.active_queue_depth)
        state["utilization"] = float(self.utilization)
        state["available_concurrency"] = int(self.available_concurrency)
        self._atomic_write_state(state)

    def _mark_claimed_dispatch(self, dispatch_event_id: str) -> None:
        state = self._read_state()
        ids = list(state.get("claimed_dispatch_event_ids", []))
        if dispatch_event_id in ids:
            return
        ids.append(dispatch_event_id)
        if len(ids) > 2000:
            ids = ids[-2000:]
        state["claimed_dispatch_event_ids"] = ids
        state["active_queue_depth"] = int(self.active_queue_depth)
        state["utilization"] = float(self.utilization)
        state["available_concurrency"] = int(self.available_concurrency)
        self._atomic_write_state(state)

    def _is_already_claimed(self, work_unit_id: str, dispatch_event_id: str) -> bool:
        state = self._read_state()
        if dispatch_event_id in set(state.get("claimed_dispatch_event_ids", [])):
            return True

        for existing in self.publisher.event_log.read_events():
            if existing.type != EventType.WORKUNIT_CLAIMED:
                continue
            payload = existing.payload if isinstance(existing.payload, dict) else {}
            if payload.get("work_unit_id") != work_unit_id:
                continue
            if payload.get("dispatch_event_id") != dispatch_event_id:
                continue
            return True
        return False

    def _try_claim_dispatch(self, dispatch_event: dict[str, Any], payload: dict[str, Any]) -> bool:
        work_unit_id = str(dispatch_event.get("work_unit_id", ""))
        dispatch_event_id = str(dispatch_event.get("id", ""))
        if not work_unit_id or not dispatch_event_id:
            return False
        if self._is_already_claimed(work_unit_id, dispatch_event_id):
            return False

        claim_payload = {
            "work_unit_id": work_unit_id,
            "selected_node_id": payload.get("selected_node_id"),
            "node_id": self.node_id,
            "dispatch_event_id": dispatch_event_id,
        }
        _, created = self.publisher.emit(
            EventType.WORKUNIT_CLAIMED,
            idempotency_key=f"claim:{work_unit_id}:{dispatch_event_id}:{self.node_id}",
            work_unit_id=work_unit_id,
            payload=claim_payload,
        )
        if created:
            self._mark_claimed_dispatch(dispatch_event_id)
        return created

    def emit_heartbeat(self) -> bool:
        epoch_ms = int(time.time() * 1000)
        if epoch_ms <= self._last_heartbeat_epoch_ms:
            epoch_ms = self._last_heartbeat_epoch_ms + 1
        self._last_heartbeat_epoch_ms = epoch_ms

        payload = {
            "node_id": self.node_id,
            "health_status": "healthy",
            "active_queue_depth": int(self.active_queue_depth),
            "utilization": float(self.utilization),
            "available_concurrency": int(self.available_concurrency),
            "region": self.region,
            "trust_score_hint": 0.5,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        _, created = self.publisher.emit(
            EventType.NODE_HEALTH_UPDATED,
            idempotency_key=f"heartbeat:{self.node_id}:{epoch_ms}",
            payload=payload,
        )
        return created

    def _emit_completed(self, dispatch_event: dict[str, Any], payload: dict[str, Any]) -> bool:
        work_unit_id = dispatch_event.get("work_unit_id")
        dispatch_event_id = dispatch_event.get("id")

        simulated_ms = int(payload.get("simulated_ms", 50))
        cpu_seconds = quant6(Decimal(simulated_ms) / Decimal(1000))

        completion_payload = {
            "tenant_id": str(payload.get("tenant_id", "default")),
            "correlation_id": str(payload.get("correlation_id", f"sess:{work_unit_id}")),
            "cpu_seconds": serialize_decimal(cpu_seconds),
            "llm_tokens": int(payload.get("llm_tokens", 0)),
            "selected_node_id": self.node_id,
            "attempt_index": int(payload.get("attempt_index", 1)),
            "execution_session_id": payload.get("execution_session_id"),
        }
        _, created = self.publisher.emit(
            EventType.WORKUNIT_COMPLETED,
            idempotency_key=f"complete:{work_unit_id}:{dispatch_event_id}",
            work_unit_id=work_unit_id,
            payload=completion_payload,
        )
        return created

    def _emit_failed(self, dispatch_event: dict[str, Any], payload: dict[str, Any], reason: str) -> bool:
        work_unit_id = dispatch_event.get("work_unit_id")
        dispatch_event_id = dispatch_event.get("id")
        fail_payload = {
            "tenant_id": str(payload.get("tenant_id", "default")),
            "correlation_id": str(payload.get("correlation_id", f"sess:{work_unit_id}")),
            "selected_node_id": self.node_id,
            "attempt_index": int(payload.get("attempt_index", 1)),
            "execution_session_id": payload.get("execution_session_id"),
            "error_code": "agent_stub_error",
            "error_reason": reason,
        }
        _, created = self.publisher.emit(
            EventType.WORKUNIT_FAILED,
            idempotency_key=f"fail:{work_unit_id}:{dispatch_event_id}",
            work_unit_id=work_unit_id,
            payload=fail_payload,
        )
        return created

    def poll_once(self) -> dict[str, int]:
        self.emit_heartbeat()
        rows = self._tail_new_events()
        processed = 0
        skipped = 0

        for event in rows:
            if event.get("type") != EventType.WORKUNIT_SCHEDULED.value:
                continue

            event_id = str(event.get("id", ""))
            if not event_id or self._already_processed(event_id):
                skipped += 1
                continue

            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            selected_node = payload.get("selected_node_id")
            if selected_node != self.node_id:
                skipped += 1
                continue
            if not self._try_claim_dispatch(event, payload):
                skipped += 1
                continue

            self.active_queue_depth += 1
            self.utilization = self._clamp(self.utilization + 0.15, 0.0, 1.0)
            self._persist_metrics()
            self.emit_heartbeat()

            try:
                simulated_ms = int(payload.get("simulated_ms", 50))
                time.sleep(max(0, simulated_ms) / 1000.0)
                self._emit_completed(event, payload)
            except Exception as exc:  # pragma: no cover - guarded by tests through normal path
                self._emit_failed(event, payload, str(exc))
            finally:
                self.active_queue_depth = max(0, self.active_queue_depth - 1)
                self.utilization = self._clamp(self.utilization - 0.15, 0.0, 1.0)
                self._persist_metrics()
                self.emit_heartbeat()
                self._mark_processed(event_id)
                processed += 1

        return {"processed": processed, "skipped": skipped}

    def run_forever(self) -> None:
        while True:
            self.poll_once()
            time.sleep(max(1, self.poll_interval_ms) / 1000.0)
