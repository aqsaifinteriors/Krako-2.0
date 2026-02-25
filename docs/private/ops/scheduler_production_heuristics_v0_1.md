# Scheduler Production Heuristics Specification v0.1

• Version: `0.1`
• Status: Internal Production Draft
• Owner Module Path: `src/krako2/scheduler/service.py`
• Related Publisher Path: `src/krako2/telemetry/publisher.py`

## Purpose

• Define deterministic node placement for WorkUnit dispatch under heterogeneous capacity.
• Constrain scheduler behavior with hard filters and stable scoring.

## Event Consumption References

• Consumed events:
– `workunit.submitted`
– `trust.score.updated` (future feed)
– `node.health.updated` (future feed)
• Emitted dispatch event:
– `workunit.scheduled`

## Invariants

• Scheduler MUST NOT rewrite `WorkUnit.kind` or semantic payload fields.
• Each scheduling attempt emits at most one `workunit.scheduled` event per `(work_unit_id, scheduling_epoch)`.
• Node eligibility filtering precedes scoring; filtered nodes are never scored.
• Placement decision is deterministic for equal inputs by lexicographic node-id tie-break.

## Hard Filter Rules

• Node is ineligible if any condition is true:
– `node.enabled = false`
– `node.health_status ∈ {"down", "draining"}`
– `node.supported_kinds` does not contain `WorkUnit.kind`
– `node.available_concurrency < WorkUnit.required_concurrency`
– `node.version < WorkUnit.min_runtime_version`

## Placement Scoring Formula

• For each eligible node `n`, compute:
– `S(n) = w_c·C(n) + w_l·L(n) + w_t·T(n) + w_r·R(n)`
• Components normalized to `[0,1]`:
– `C(n)` capacity headroom ratio
– `L(n)` inverse load (`1 - utilization`)
– `T(n)` trust-normalized score
– `R(n)` locality/routing affinity
• Default weights:
– `w_c = 0.35`
– `w_l = 0.30`
– `w_t = 0.25`
– `w_r = 0.10`
• Weight invariant:
– `w_c + w_l + w_t + w_r = 1.0`

## Load Balancing Strategy

• Use weighted-best selection with soft anti-affinity:
– If top node has consecutive assignment streak > `streak_limit`, choose next best if `ΔS < 0.03`.
• Tie-break order:
– Highest score
– Lowest active queue depth
– Lexicographically smallest `node_id`

## Node Health Usage

• Health freshness window: 15 seconds.
• If health telemetry age exceeds window:
– Apply uncertainty penalty `U = 0.15` to `S(n)`.
• If explicit error-rate exceeds threshold `e > 0.1` over 1-minute window:
– Force filter-out until next healthy window.

## Dispatch Event Emission

• On successful placement emit `workunit.scheduled` with payload:
– `work_unit_id`
– `selected_node_id`
– `score`
– `eligible_node_count`
– `reason_code`
• Idempotency key format:
– `schedule:{work_unit_id}:{scheduling_epoch}`

## Failure Modes

• No eligible node:
– Emit `workunit.scheduling.deferred`; enqueue for retry window.
• Stale topology snapshot:
– Reject cycle and force topology refresh before new decision.
• Score component missing:
– Use deterministic default `0` for missing component and add diagnostic flag.

## CI Invariant Checklist

• Hard filter test:
– Unsupported `WorkUnit.kind` never reaches scoring stage.
• Determinism test:
– Same inputs produce same node selection and score.
• Weight sum test:
– Config with weight sum != 1 fails validation.
• Tie-break test:
– Equal scores resolved by queue depth then node id.
• Emission test:
– Exactly one dispatch event per accepted scheduling attempt.
