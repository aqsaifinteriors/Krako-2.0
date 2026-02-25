# Trust Scoring Algorithm Specification v0.1

• Version: `0.1`
• Status: Internal Production Draft
• Owner Module Path: `src/krako2/trust/consumer.py`
• State Path: `data/trust_state.json`

## Purpose

• Compute deterministic node trust score from reliability and behavioral telemetry.
• Provide scheduler-consumable normalized score in `[0,1]`.

## Event Consumption References

• Consumed events:
– `workunit.scheduled`
– `workunit.completed`
– `workunit.timeout`
– `workunit.failed`
– `node.health.updated`
• Emitted event:
– `trust.score.updated`

## Invariants

• Trust score remains bounded: `0 ≤ trust_score ≤ 1`.
• State transitions are append-derived; no hidden external inputs.
• Replay of identical event sequence yields identical trust trajectory.

## Beta-Binomial Reliability Model

• For node `n`, maintain success/failure counts with priors:
– `α0 = 2`, `β0 = 2`
• Updates per outcome:
– success: `α_n <- α_n + 1`
– failure/timeout: `β_n <- β_n + 1`
• Reliability posterior mean:
– `R_n = α_n / (α_n + β_n)`

## EWMA Timeout Tracking

• Timeout indicator per interval: `x_t ∈ {0,1}`.
• EWMA update:
– `E_t = λ·x_t + (1-λ)·E_{t-1}`
– with `λ = 0.2`
• Timeout penalty:
– `P_timeout = min(0.4, E_t)`

## Decay Rule

• To prioritize recency, apply periodic decay every `Δ = 1 hour`:
– `α_n <- 1 + (α_n - 1)·d`
– `β_n <- 1 + (β_n - 1)·d`
– `d = 0.98`
• This preserves prior floor while attenuating stale evidence.

## Sybil and Collusion Defense

• Identity hardness requirement:
– trust contributions accepted only for nodes with verified identity attestation.
• Neighborhood concentration penalty:
– if >`k=3` nodes share same operator fingerprint and mutual success-only pattern, apply group penalty `P_group=0.1` each.
• Cross-tenant diversity gate:
– reliability updates are down-weighted by 50% if activity originates from single tenant only.

## Score Normalization

• Raw score:
– `S_raw = R_n - P_timeout - P_group - P_health`
• Health penalty `P_health` from `node.health.updated`:
– `0` for healthy
– `0.2` for degraded
– `0.5` for unhealthy
• Normalized score:
– `S_norm = clamp(S_raw, 0, 1)`

## Scheduler Usage Contract

• Scheduler consumes latest `S_norm` as component `T(n)` in placement scoring.
• Contract guarantees:
– update freshness timestamp included with each trust event
– missing trust score defaults to `0.5` neutral prior
– scores older than 10 minutes are treated stale and penalized by scheduler

## Failure Modes

• Missing identity metadata:
– freeze node trust at max `0.3` until identity validated.
• Corrupted trust state file:
– restore from last valid snapshot and replay events from checkpoint.
• Event time skew:
– reject events with timestamp drift > 5 minutes from monotonic watermark.

## CI Invariant Checklist

• Bounds test:
– score never exits `[0,1]`.
• Replay determinism test:
– same event log reproduces identical final state.
• Timeout penalty test:
– repeated timeouts monotonically reduce trust.
• Decay test:
– stale high trust decays toward prior baseline.
• Sybil defense test:
– clustered fake peers receive group penalty.
