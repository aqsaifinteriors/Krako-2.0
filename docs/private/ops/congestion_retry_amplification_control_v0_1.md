# Congestion, Retry, and Amplification Control Specification v0.1

• Version: `0.1`
• Status: Internal Production Draft
• Owner Module Path: `src/krako2/scheduler/service.py` (retry policy integration)
• Related Billing Path: `src/krako2/billing/consumer.py`

## Purpose

• Define deterministic retry behavior that avoids congestion collapse and billing amplification.
• Bound retries per tenant and per work unit under incident conditions.

## Event Consumption References

• Consumed events:
– `workunit.dispatch.failed`
– `workunit.timeout`
– `workunit.completed`
– `queue.depth.updated`
– `node.health.updated`
• Emitted events:
– `workunit.retry.scheduled`
– `workunit.retry.dropped`
– `congestion.mode.changed`

## Invariants

• Retry attempts never mutate original WorkUnit semantic fields.
• A retry must never produce duplicate billable charge for same logical execution attempt.
• Retry schedule is deterministic from `(work_unit_id, attempt_index, congestion_mode)`.
• Retry attempt count is monotonic and capped.

## Retry Eligibility Rules

• Eligible failure classes:
– transient network failure
– node timeout without completion ack
– pre-execution admission reject
• Ineligible failure classes:
– validation error
– authorization failure
– explicit non-retryable business rejection
• Hard cap:
– `attempt_index ≤ 5`

## Backoff Formula (Exponential + Jitter)

• Base delay: `b = 0.5s`
• Exponential component for attempt `k (>=1)`:
– `d_exp(k) = b × 2^(k-1)`
• Deterministic bounded jitter:
– `j(k, id) = frac(hash(id || k) / 2^64) × 0.2 × d_exp(k)`
• Final delay:
– `d(k,id) = min(d_max, d_exp(k) + j(k,id))`, `d_max = 30s`

## Retry Amplification Protection

• Global amplification factor:
– `A = total_retries_5m / total_primary_dispatches_5m`
• Guardrail thresholds:
– warning at `A ≥ 0.25`
– hard protection at `A ≥ 0.40`
• At hard protection:
– block retries for priority tiers below `P1`
– enforce minimum retry delay floor `>= 5s`

## Circuit Breaker Definition

• Breaker states:
– `CLOSED`
– `OPEN`
– `HALF_OPEN`
• Open condition:
– failure rate > 50% over last 60 attempts OR timeout burst >= 20 in 30s.
• OPEN behavior:
– reject new retries immediately with `workunit.retry.dropped`.
• HALF_OPEN probing:
– allow 1 probe per 5 seconds; close only after 5 consecutive successes.

## Tenant Retry Budget

• Budget window: 1 minute sliding.
• Per-tenant retry token bucket:
– capacity `B = 40`
– refill `r = 20 tokens/min`
• Retry requires one token; if empty, retry is dropped deterministically.

## Congestion Detection Metrics

• Congestion mode enters `HIGH` when any condition holds:
– queue depth > 800
– p95 dispatch latency > 3s
– node unhealthy ratio > 0.3
• Mode exits `HIGH` after 6 consecutive healthy windows.
• In `HIGH` mode:
– multiply computed backoff by `1.5`
– reduce max attempts from `5` to `3` for non-critical workloads.

## Interaction with Billing Invariants

• Billing dedupe key remains `event_id`; retries must carry unique dispatch event ids.
• Logical charge key:
– `(work_unit_id, execution_epoch)` maps to at most one successful billable completion.
• Failed retries without completion acknowledgement are non-billable.
• Replay must preserve non-double-billing even when retry events are duplicated.

## Failure Modes

• Clock skew across schedulers:
– use monotonic local timers for delay, not wall-clock absolute schedule.
• Hash jitter mismatch across runtimes:
– mandate stable xxhash64 implementation and test vectors.
• Token bucket state corruption:
– fail closed for non-critical retries; preserve critical path with capped fallback.

## CI Invariant Checklist

• Eligibility test:
– non-retryable errors never schedule retry.
• Backoff test:
– delays follow formula and cap at `d_max`.
• Determinism test:
– identical inputs yield identical retry delay sequence.
• Amplification guard test:
– retries are curtailed when `A` exceeds threshold.
• Circuit breaker test:
– OPEN state blocks retries and HALF_OPEN probing rules hold.
• Billing interaction test:
– duplicated retry event stream cannot create duplicate ledger charges.
