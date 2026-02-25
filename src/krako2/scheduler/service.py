from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from krako2.domain.models import EventType, WorkUnit
from krako2.scheduler.node_registry import Node
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
    def __init__(self, state_path: str | Path = "data/scheduler_state.json") -> None:
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

    def _node_score(self, node: Node, work_unit: WorkUnit) -> tuple[float, dict[str, float]]:
        denominator = max(1, node.available_concurrency + node.active_queue_depth)
        c = min(1.0, node.available_concurrency / denominator)
        l = _clamp(1.0 - float(node.utilization), 0.0, 1.0)
        t = _clamp(float(node.trust_score), 0.0, 1.0)

        if work_unit.region is None:
            r = 0.5
        elif node.region == work_unit.region:
            r = 1.0
        else:
            r = 0.0

        score = 0.35 * c + 0.30 * l + 0.25 * t + 0.10 * r
        return score, {"C": c, "L": l, "T": t, "R": r}

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

        # Enforce deterministic tie-break for near-equal scores (<=1e-9).
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

        payload = {
            "work_unit_id": work_unit.id,
            "selected_node_id": selected_node_id,
            "score": debug["selected_score"],
            "eligible_node_count": debug["eligible_node_count"],
            "reason_code": debug["reason_code"],
            "scheduling_epoch": epoch,
        }

        event, created = publisher.emit(
            EventType.WORKUNIT_SCHEDULED,
            idempotency_key=f"schedule:{work_unit.id}:{epoch}",
            work_unit_id=work_unit.id,
            payload=payload,
        )
        debug["dispatch_event_id"] = event.id
        debug["dispatch_event_created"] = created
        return selected_node_id, debug
