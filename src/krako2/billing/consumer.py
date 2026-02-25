from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
from typing import Any

from krako2.billing.money import dec, quant6, serialize_decimal
from krako2.billing.storage import BillingDedupeStore, BillingLedgerWriter
from krako2.domain.models import Event, EventType


def _env_flag(name: str, default: str) -> bool:
    raw = os.getenv(name, default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


class BillingConsumer:
    def __init__(
        self,
        ledger_path: str | Path = "data/billing_ledger.jsonl",
        dedupe_path: str | Path = "data/billing_dedupe.json",
    ) -> None:
        self.ledger = BillingLedgerWriter(ledger_path)
        self.dedupe = BillingDedupeStore(dedupe_path)

        self.cpu_unit_price = dec(os.getenv("KRAKO_CPU_USD_PER_SEC", "0.000500"))
        self.llm_unit_price_per_1k = dec(os.getenv("KRAKO_LLM_USD_PER_1K_TOKENS", "0.002000"))
        self.bill_llm_from_workunit_completed = _env_flag("KRAKO_BILL_LLM_FROM_WORKUNIT_COMPLETED", "0")
        self.bill_llm_from_invocation = _env_flag("KRAKO_BILL_LLM_FROM_INVOCATION", "1")

    def _parse_cpu_seconds(self, payload: dict[str, Any]) -> Decimal | None:
        raw = payload.get("cpu_seconds")
        if raw is None:
            return None
        try:
            return dec(str(raw))
        except Exception:
            return None

    def _parse_llm_tokens(self, payload: dict[str, Any]) -> int | None:
        raw = payload.get("llm_tokens")
        if raw is None:
            return None
        try:
            value = int(raw)
            if value < 0:
                return None
            return value
        except Exception:
            return None

    def _build_record(
        self,
        *,
        event: Event,
        payload: dict[str, Any],
        cpu_seconds: Decimal,
        llm_tokens: int,
        line_item_type: str,
        llm_tokens_event_total: int,
    ) -> dict[str, Any]:
        subtotal_cpu = quant6(cpu_seconds * self.cpu_unit_price)
        subtotal_llm = quant6((dec(llm_tokens) / dec("1000")) * self.llm_unit_price_per_1k)
        total = quant6(subtotal_cpu + subtotal_llm)
        tenant_id = str(payload.get("tenant_id", "default"))
        correlation_id = str(payload.get("correlation_id", event.idempotency_key))

        return {
            "event_id": event.id,
            "event_type": event.type.value,
            "work_unit_id": event.work_unit_id,
            "tenant_id": tenant_id,
            "correlation_id": correlation_id,
            "execution_session_id": payload.get("execution_session_id"),
            "line_item_type": line_item_type,
            "cpu_seconds": serialize_decimal(cpu_seconds),
            "llm_tokens": llm_tokens,
            "llm_tokens_event_total": int(llm_tokens_event_total),
            "cpu_unit_price_usd": serialize_decimal(self.cpu_unit_price),
            "llm_unit_price_usd_per_1k": serialize_decimal(self.llm_unit_price_per_1k),
            "subtotal_cpu_usd": serialize_decimal(subtotal_cpu),
            "subtotal_llm_usd": serialize_decimal(subtotal_llm),
            "total_usd": serialize_decimal(total),
            "currency": "USD",
            "rounded_scale": 6,
            "rounding_mode": "ROUND_HALF_EVEN",
            "pricing_version": "0.1",
        }

    def _build_workunit_completed_record(self, event: Event, payload: dict[str, Any]) -> dict[str, Any] | None:
        cpu_seconds = self._parse_cpu_seconds(payload)
        llm_tokens = self._parse_llm_tokens(payload)

        should_bill_llm = self.bill_llm_from_workunit_completed
        if cpu_seconds is None and not (should_bill_llm and llm_tokens is not None):
            return None

        billed_cpu = cpu_seconds if cpu_seconds is not None else dec("0")
        billed_llm = llm_tokens if should_bill_llm and llm_tokens is not None else 0

        return self._build_record(
            event=event,
            payload=payload,
            cpu_seconds=billed_cpu,
            llm_tokens=billed_llm,
            line_item_type="workunit_cpu",
            llm_tokens_event_total=0,
        )

    def _build_llm_invocation_record(self, event: Event, payload: dict[str, Any]) -> dict[str, Any] | None:
        if not self.bill_llm_from_invocation:
            return None

        tokens_raw = payload.get("total_tokens", payload.get("llm_tokens"))
        if tokens_raw is None:
            return None
        try:
            tokens = int(tokens_raw)
            if tokens < 0:
                return None
        except Exception:
            return None

        return self._build_record(
            event=event,
            payload=payload,
            cpu_seconds=dec("0"),
            llm_tokens=tokens,
            line_item_type="llm_tokens",
            llm_tokens_event_total=tokens,
        )

    def _build_record_for_event(self, event: Event) -> dict[str, Any] | None:
        payload = event.payload if isinstance(event.payload, dict) else {}
        if event.type == EventType.WORKUNIT_COMPLETED:
            return self._build_workunit_completed_record(event, payload)
        if event.type == EventType.LLM_INVOCATION_COMPLETED:
            return self._build_llm_invocation_record(event, payload)
        return None

    def consume(self, event: Event) -> bool:
        if self.dedupe.has(event.id):
            return False

        record = self._build_record_for_event(event)
        if record is None:
            return False

        self.ledger.append(record)
        self.dedupe.mark_processed(event.id)
        return True
