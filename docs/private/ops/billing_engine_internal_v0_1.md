# Billing Engine Internal Specification v0.1

• Version: `0.1`
• Status: Internal Production Draft
• Owner Module Path: `src/krako2/billing/consumer.py`
• Primary Storage Path: `data/billing_ledger.jsonl`

## Purpose

• Define deterministic billing ingestion and ledger writing for metered execution.
• Guarantee replay-safe accounting under retries and duplicated upstream emissions.

## Event Consumption References

• Consumed event stream: `data/events.jsonl` via `src/krako2/storage/event_log.py`.
• Accepted event types:
– `workunit.submitted`
– `workunit.scheduled`
– `billing.consumed` (self-observation, non-billable)
• Required event fields:
– `id` (global event identifier)
– `idempotency_key`
– `work_unit_id`
– `payload.amount` (optional base amount)
– `payload.cpu_seconds`, `payload.llm_tokens` (when metered)

## Invariants

• `event_id` uniqueness in billing ledger is absolute.
• No double billing on retry: if `event_id` already exists, consumer MUST no-op.
• Event consumption is idempotent: repeated replay yields byte-equivalent ledger output ordering for identical input.
• Billing record immutability: appended JSON line MUST never be edited in place.

## Ledger Schema (Fixed JSON Fields)

• Ledger row schema (JSON object):
– `event_id: string`
– `event_type: string`
– `work_unit_id: string | null`
– `tenant_id: string`
– `correlation_id: string`
– `cpu_seconds: string` (decimal serialized)
– `llm_tokens: integer`
– `cpu_unit_price_usd: string` (decimal serialized)
– `llm_unit_price_usd_per_1k: string` (decimal serialized)
– `subtotal_cpu_usd: string` (decimal serialized)
– `subtotal_llm_usd: string` (decimal serialized)
– `total_usd: string` (decimal serialized)
– `currency: "USD"`
– `rounded_scale: integer` (always `6`)
– `rounding_mode: "ROUND_HALF_EVEN"`

## Dedupe Model

• Primary dedupe key: `event_id`.
• Secondary correlation key (diagnostic only): `(tenant_id, correlation_id)`.
• Decision function:
– `consume(event) = false` if `event.id ∈ ProcessedEventIds`
– `consume(event) = true` and append ledger row otherwise

## Correlation Handling

• `correlation_id` derivation order:
– `payload.correlation_id` if present
– else `idempotency_key`
• `tenant_id` derivation order:
– `payload.tenant_id` if present
– else literal `"default"`
• Correlation IDs are not used for dedupe; they are used for cross-module trace joins.

## Pricing Formulas

• Constants:
– `P_cpu` = CPU unit price in USD/second
– `P_llm` = LLM unit price in USD per 1000 tokens
• Inputs:
– `C` = `cpu_seconds`
– `T` = `llm_tokens`
• Formula:
– `subtotal_cpu = C × P_cpu`
– `subtotal_llm = (T / 1000) × P_llm`
– `total = subtotal_cpu + subtotal_llm`

## Decimal Precision and Rounding

• Internal computation precision: 28 decimal digits.
• Persisted monetary scale: 6 fractional digits.
• Rounding mode: Banker’s rounding (`ROUND_HALF_EVEN`).
• Serialized decimals MUST be strings to avoid float drift.

## Replay Algorithm (Deterministic)

• Input: append-only `events.jsonl` in physical line order.
• Steps:
– Initialize empty `ProcessedEventIds` from existing ledger or replay baseline.
– For each event line `e_i` in order `i = 1..N`:
  – Parse and validate required fields.
  – If `e_i.id ∈ ProcessedEventIds`, skip.
  – Compute billing tuple using fixed formulas and constants.
  – Append exactly one ledger JSON object.
  – Add `e_i.id` to `ProcessedEventIds`.
• Determinism condition:
– Given identical input file bytes and pricing constants, output ledger bytes are identical.

## Failure Modes

• Invalid event schema:
– Action: reject row, emit `billing.consume.error` diagnostic event, continue stream.
• Corrupted JSON line:
– Action: hard-fail replay with line index and checksum mismatch reason.
• Partial write (process crash during append):
– Action: detect trailing non-JSON fragment, truncate last invalid line before replay.
• Price constant misconfiguration:
– Action: fail fast on startup if missing or non-decimal.

## CI Invariant Test Checklist

• Idempotency test:
– Replaying same event file twice does not increase ledger row count.
• Duplicate event test:
– Two identical `event_id` lines produce one ledger row.
• Precision test:
– Decimal totals persist with exactly 6 fractional digits.
• Rounding test:
– Half-even behavior validated on boundary values.
• Ordering test:
– Output order matches first-seen order of accepted events.
• Correlation mapping test:
– Fallback mapping to `idempotency_key` is stable.
