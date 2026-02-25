from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from krako2.domain.models import EventType, WorkUnit
from krako2.scheduler.circuit_breaker import CircuitBreakerManager
from krako2.scheduler.node_registry import Node
from krako2.scheduler.retry import compute_backoff_seconds, is_retryable_error, max_attempts
from krako2.scheduler.retry_budget import RetryBudgetStore
from krako2.telemetry.publisher import EventPublisher


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _parse_semver(v: str) -> tuple[int, int, int]:
    parts = [p for p in v.split(".") if p != ""]
    nums = []
    for p in parts[:3]:
        digits = "".join(ch for ch in p if ch.isdigit())
        nums.append(int(digits) if digits else 0)
    while len(nums) < 3:
        nums.append(0)
    return tuple(nums[:3])


def _version_gte(node_version: str, min_version: str) -> bool:
    return _parse_semver(node_version) >= _parse_semver(min_version)


class SchedulerService:
    def __init__(
        self,
        state_path: str | Path = "data/scheduler_state.json",
        retry_budget_state_path: str | Path = "data/retry_budget_state.json",
        congestion_state_path: str | Path = "data/congestion_state.json",
        trust_state_path: str | Path = "data/trust_state.json",
        publisher: EventPublisher | None = None,
        circuit_breaker: CircuitBreakerManager | None = None,
    ) -> None:
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.state_path.exists():
            self._atomic_write(
                {
                    "last_selected_node_id": None,
                    "node_assignment_streak": {},
                    "scheduling_epoch": 0,
                }
            )

        self.retry_budget = RetryBudgetStore(state_path=retry_budget_state_path)
        self.congestion_state_path = Path(congestion_state_path)
        self.congestion_state_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.congestion_state_path.exists():
            self._atomic_write_congestion({"mode": "NORMAL"})

        self.publisher = publisher
        self.circuit_breaker = circuit_breaker or CircuitBreakerManager()
        self.trust_state_path = Path(trust_state_path)

    def _read_state(self) -> dict[str, Any]:
        with self.state_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _atomic_write(self, data: dict[str, Any]) -> None:
        tmp_path = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, sort_keys=True, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, self.state_path)

    def _read_congestion(self) -> dict[str, Any]:
        with self.congestion_state_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _atomic_write_congestion(self, data: dict[str, Any]) -> None:
        tmp_path = self.congestion_state_path.with_suffix(self.congestion_state_path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, sort_keys=True, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, self.congestion_state_path)

    def _next_scheduling_epoch(self) -> int:
        state = self._read_state()
        epoch = int(state.get("scheduling_epoch", 0)) + 1
        state["scheduling_epoch"] = epoch
        self._atomic_write(state)
        return epoch

    def _update_streak(self, selected_node_id: str) -> int:
        state = self._read_state()
        previous = state.get("last_selected_node_id")
        streaks: dict[str, int] = {
            k: int(v) for k, v in dict(state.get("node_assignment_streak", {})).items()
        }

        for k in list(streaks.keys()):
            streaks[k] = 0

        if previous == selected_node_id:
            new_streak = int(dict(state.get("node_assignment_streak", {})).get(selected_node_id, 0)) + 1
        else:
            new_streak = 1

        streaks[selected_node_id] = new_streak
        state["node_assignment_streak"] = streaks
        state["last_selected_node_id"] = selected_node_id
        self._atomic_write(state)
        return new_streak

    def _trust_score_for_node(self, node_id: str) -> tuple[float, bool]:
        # Returns (trust_score, is_fresh).
        if not self.trust_state_path.exists():
            return 0.5, False
        try:
            with self.trust_state_path.open("r", encoding="utf-8") as f:
                state = json.load(f)
            nodes = state.get("nodes", {})
            entry = nodes.get(node_id, {})
            score = _clamp(float(entry.get("score", 0.5)), 0.0, 1.0)

            ts_raw = entry.get("last_seen_ts")
            if not isinstance(ts_raw, str):
                return score, False
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            age_seconds = (now - ts.astimezone(timezone.utc)).total_seconds()
            return score, age_seconds <= 60
        except Exception:
            return 0.5, False

    def _node_score(self, node: Node, work_unit: WorkUnit) -> tuple[float, dict[str, float]]:
        denominator = max(1, node.available_concurrency + node.active_queue_depth)
        c = min(1.0, node.available_concurrency / denominator)
        l = _clamp(1.0 - float(node.utilization), 0.0, 1.0)
        trust_score, is_fresh = self._trust_score_for_node(node.node_id)
        t = trust_score if trust_score is not None else _clamp(float(node.trust_score), 0.0, 1.0)
        if not is_fresh:
            t = t * 0.5

        if work_unit.region is None:
            r = 0.5
        elif node.region == work_unit.region:
            r = 1.0
        else:
            r = 0.0

        score = 0.35 * c + 0.30 * l + 0.25 * t + 0.10 * r
        return score, {"C": c, "L": l, "T": t, "R": r}

    def detect_congestion_mode(self, nodes: list[Node], publisher: EventPublisher | None = None) -> str:
        enabled = [n for n in nodes if n.enabled]
        avg_q = 0.0
        if enabled:
            avg_q = sum(n.active_queue_depth for n in enabled) / len(enabled)
        new_mode = "HIGH" if avg_q > 800 else "NORMAL"

        state = self._read_congestion()
        prev_mode = state.get("mode", "NORMAL")
        if prev_mode != new_mode:
            state["mode"] = new_mode
            self._atomic_write_congestion(state)

            emitter = publisher or self.publisher
            if emitter is not None:
                emitter.emit(
                    EventType.CONGESTION_MODE_CHANGED,
                    idempotency_key=f"congestion:{new_mode}:{self._next_scheduling_epoch()}",
                    payload={"previous_mode": prev_mode, "mode": new_mode, "avg_active_queue_depth": avg_q},
                )
        return new_mode

    def select_node_for_workunit(self, work_unit: WorkUnit, nodes: list[Node]) -> tuple[str | None, dict[str, Any]]:
        required = max(1, int(work_unit.required_concurrency or 1))
        min_version = work_unit.min_runtime_version

        eligible: list[dict[str, Any]] = []
        filtered_out: list[dict[str, str]] = []

        for node in nodes:
            if not node.enabled:
                filtered_out.append({"node_id": node.node_id, "reason": "disabled"})
                continue
            if node.health_status in {"down", "draining"}:
                filtered_out.append({"node_id": node.node_id, "reason": "health_status"})
                continue
            if work_unit.kind not in node.supported_kinds:
                filtered_out.append({"node_id": node.node_id, "reason": "unsupported_kind"})
                continue
            if node.available_concurrency < required:
                filtered_out.append({"node_id": node.node_id, "reason": "insufficient_concurrency"})
                continue
            if min_version and not _version_gte(node.version, min_version):
                filtered_out.append({"node_id": node.node_id, "reason": "runtime_version"})
                continue

            score, components = self._node_score(node, work_unit)
            eligible.append(
                {
                    "node": node,
                    "score": score,
                    "components": components,
                }
            )

        debug: dict[str, Any] = {
            "eligible_node_count": len(eligible),
            "filtered_out": filtered_out,
        }

        if not eligible:
            debug.update({"reason_code": "no_eligible_nodes", "selected_score": None})
            return None, debug

        ranked = sorted(
            eligible,
            key=lambda item: (-item["score"], item["node"].active_queue_depth, item["node"].node_id),
        )

        best_score = ranked[0]["score"]
        near_best = [item for item in ranked if abs(item["score"] - best_score) <= 1e-9]
        if len(near_best) > 1:
            near_best_sorted = sorted(
                near_best,
                key=lambda item: (item["node"].active_queue_depth, item["node"].node_id),
            )
            best = near_best_sorted[0]
            others = [item for item in ranked if item is not best]
            ranked = [best] + others

        state = self._read_state()
        streaks = dict(state.get("node_assignment_streak", {}))
        best = ranked[0]
        selected = best
        reason_code = "best_score"

        if len(ranked) >= 2:
            second = ranked[1]
            best_streak = int(streaks.get(best["node"].node_id, 0))
            if best_streak > 5 and (best["score"] - second["score"]) <= 0.03:
                selected = second
                reason_code = "anti_affinity"

        selected_node: Node = selected["node"]
        new_streak = self._update_streak(selected_node.node_id)

        debug.update(
            {
                "reason_code": reason_code,
                "selected_score": selected["score"],
                "selected_components": selected["components"],
                "selected_node_id": selected_node.node_id,
                "selected_node_streak": new_streak,
                "ranked_candidates": [
                    {
                        "node_id": item["node"].node_id,
                        "score": item["score"],
                        "active_queue_depth": item["node"].active_queue_depth,
                    }
                    for item in ranked
                ],
            }
        )

        return selected_node.node_id, debug

    def schedule_and_emit(
        self,
        work_unit: WorkUnit,
        nodes: list[Node],
        publisher: EventPublisher,
    ) -> tuple[str | None, dict[str, Any]]:
        selected_node_id, debug = self.select_node_for_workunit(work_unit, nodes)
        epoch = self._next_scheduling_epoch()
        debug["scheduling_epoch"] = epoch

        if selected_node_id is None:
            return None, debug

        if work_unit.kind == "llm_pod":
            selected_node = next((n for n in nodes if n.node_id == selected_node_id), None)
            if selected_node is None or "llm_pod" not in selected_node.supported_kinds:
                raise AssertionError("llm_pod work unit selected on non-llm_pod node")

        payload = {
            **(work_unit.payload or {}),
            "work_unit_id": work_unit.id,
            "selected_node_id": selected_node_id,
            "score": debug["selected_score"],
            "eligible_node_count": debug["eligible_node_count"],
            "reason_code": debug["reason_code"],
            "scheduling_epoch": epoch,
        }
        if work_unit.execution_session_id is not None:
            payload["execution_session_id"] = work_unit.execution_session_id

        event, created = publisher.emit(
            EventType.WORKUNIT_SCHEDULED,
            idempotency_key=f"schedule:{work_unit.id}:{epoch}",
            work_unit_id=work_unit.id,
            payload=payload,
        )
        debug["dispatch_event_id"] = event.id
        debug["dispatch_event_created"] = created
        return selected_node_id, debug

    def schedule_retry(
        self,
        work_unit: WorkUnit,
        tenant_id: str,
        error_code: str,
        attempt_index: int,
        congestion_mode: str,
    ) -> dict[str, Any]:
        priority = str((work_unit.payload or {}).get("priority", "p2"))
        cap = max_attempts(congestion_mode, priority)
        reason = "scheduled"

        if not is_retryable_error(error_code):
            reason = "non_retryable_error"
            event_type = EventType.WORKUNIT_RETRY_DROPPED
            delay = 0.0
        elif attempt_index > cap:
            reason = "attempt_cap_exceeded"
            event_type = EventType.WORKUNIT_RETRY_DROPPED
            delay = 0.0
        elif not self.retry_budget.allow_retry(tenant_id):
            reason = "retry_budget_exhausted"
            event_type = EventType.WORKUNIT_RETRY_DROPPED
            delay = 0.0
        else:
            delay = compute_backoff_seconds(work_unit.id, attempt_index)
            event_type = EventType.WORKUNIT_RETRY_SCHEDULED

        payload = {
            "work_unit_id": work_unit.id,
            "attempt_index": attempt_index,
            "delay_seconds": delay,
            "reason": reason,
            "tenant_id": tenant_id,
            "congestion_mode": congestion_mode,
            "error_code": error_code,
        }

        emitted = False
        if self.publisher is not None:
            self.publisher.emit(
                event_type,
                idempotency_key=f"retry:{work_unit.id}:{attempt_index}:{reason}",
                work_unit_id=work_unit.id,
                payload=payload,
            )
            emitted = True

        return {
            "event_type": event_type.value,
            "scheduled": event_type == EventType.WORKUNIT_RETRY_SCHEDULED,
            "delay_seconds": delay,
            "reason": reason,
            "attempt_cap": cap,
            "emitted": emitted,
        }
