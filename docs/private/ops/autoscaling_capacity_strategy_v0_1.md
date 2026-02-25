# Autoscaling Capacity Strategy Specification v0.1

• Version: `0.1`
• Status: Internal Production Draft
• Owner Module Path: `src/krako2/scheduler/service.py` (capacity controller integration)
• Data Inputs: scheduler queue metrics + node telemetry stream

## Purpose

• Define deterministic scaling policy for replica count and effective concurrency.
• Prevent oscillation while maintaining queue latency SLO.

## Event Consumption References

• Consumed telemetry events:
– `queue.depth.updated`
– `node.health.updated`
– `workunit.scheduled`
– `workunit.completed`
• Emitted control events:
– `capacity.scale.requested`
– `capacity.admission.mode.changed`

## Scaling Targets

• Controlled variables:
– `R` = pod replicas per pool
– `K` = per-replica concurrency limit
• Capacity target:
– `C_total = R × K`
• Objective:
– Keep p95 queue wait `W95 ≤ 2.0s` under steady-state load.

## Trigger Metrics and Thresholds

• Scale-up trigger (all over 3 consecutive windows):
– `queue_depth > 200`
– OR `W95 > 2.0s`
– OR `utilization > 0.80`
• Scale-down trigger (all over 6 consecutive windows):
– `queue_depth < 40`
– AND `W95 < 1.0s`
– AND `utilization < 0.45`
• Window size: 10 seconds.

## Hysteresis Policy

• Cooldown after scale-up: 60 seconds.
• Cooldown after scale-down: 180 seconds.
• Minimum step: ±1 replica.
• Maximum step per decision: +3 / -2 replicas.

## Queue Pressure Handling

• Pressure score:
– `P = min(1, queue_depth / Q_max)` where `Q_max = 1000`
• If `P ≥ 0.9`:
– force immediate scale-up by +3 replicas (bounded by max replicas).
• If max replicas reached and `P ≥ 0.9` persists for 2 windows:
– enter admission control mode `THROTTLED`.

## Admission Control

• Modes:
– `OPEN`
– `THROTTLED`
– `CRITICAL`
• Transition rules:
– `OPEN -> THROTTLED` when queue_depth >= 0.9·Q_max
– `THROTTLED -> CRITICAL` when queue_depth >= Q_max for 3 windows
– Recovery requires 6 healthy windows below 0.5·Q_max
• THROTTLED behavior:
– reject low-priority new work units deterministically by priority class.

## Graceful Degradation Modes

• Degradation level 1:
– lower non-critical WorkUnit concurrency class by 30%.
• Degradation level 2:
– disable optional LLM fallback path for best-effort workloads.
• Degradation level 3:
– accept only priority tiers `P0`, `P1`.

## Failure Containment

• Split-brain scaler control:
– leadership lock required; non-leader instances are read-only.
• Telemetry loss:
– freeze scaling decisions, preserve current `R`, switch to conservative `THROTTLED`.
• Runaway scaling guard:
– hard bound `R_min <= R <= R_max`, reject out-of-range control events.

## Invariants

• Replica decisions are monotonic within a single evaluation cycle.
• No scale-down is permitted during active incident state.
• Admission control transitions are event-driven and append-only in audit log.

## CI Invariant Checklist

• Hysteresis test:
– rapid metric oscillation does not produce replica flapping.
• Bound test:
– `R` never exceeds configured limits.
• Cooldown test:
– no decision before cooldown expiry.
• Admission mode test:
– deterministic transitions for synthetic queue trajectories.
• Telemetry loss test:
– controller enters safe frozen state.
