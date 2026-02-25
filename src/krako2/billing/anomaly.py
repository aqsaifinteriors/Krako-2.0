from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from krako2.billing.money import dec, quant6, serialize_decimal

_REQUIRED_LEDGER_FIELDS = {
    "event_id",
    "event_type",
    "work_unit_id",
    "tenant_id",
    "correlation_id",
    "cpu_seconds",
    "llm_tokens",
    "cpu_unit_price_usd",
    "llm_unit_price_usd_per_1k",
    "subtotal_cpu_usd",
    "subtotal_llm_usd",
    "total_usd",
    "currency",
    "rounded_scale",
    "rounding_mode",
    "pricing_version",
}

_6DP_RE = re.compile(r"^-?\d+\.\d{6}$")
_TOL = dec("0.000001")


def _parse_decimal_6(value: Any) -> tuple[bool, Any]:
    try:
        d = quant6(dec(str(value)))
        return True, d
    except Exception:
        return False, f"invalid_decimal:{value}"


def _extract_expected_total(payload: dict[str, Any]) -> Any:
    total_usage = payload.get("total_usage")
    if isinstance(total_usage, dict) and "total_usd" in total_usage:
        return total_usage["total_usd"]
    if "total_usd" in payload:
        return payload["total_usd"]
    if "billing_total_usd" in payload:
        return payload["billing_total_usd"]
    return None


def _read_expected_session_totals(event_log_path: Path) -> dict[str, Any]:
    expected: dict[str, Any] = {}
    if not event_log_path.exists():
        return expected

    with event_log_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            event_type = event.get("type")
            if event_type != "ExecutionSessionCompleted":
                continue

            payload = event.get("payload")
            if not isinstance(payload, dict):
                continue

            session_id = payload.get("execution_session_id")
            if not isinstance(session_id, str) or not session_id:
                continue

            raw_total = _extract_expected_total(payload)
            if raw_total is None:
                continue

            ok, parsed = _parse_decimal_6(raw_total)
            if not ok:
                continue

            expected[session_id] = parsed

    return expected


def check_billing_anomalies(event_log_path: Path, ledger_path: Path) -> dict[str, Any]:
    expected_sessions = _read_expected_session_totals(event_log_path)

    ledger_total = dec("0")
    ledger_count = 0
    missing_fields_rows = 0
    invalid_total_6dp_rows = 0

    session_actuals: dict[str, Any] = {}
    session_mappings: dict[str, bool] = {}

    if ledger_path.exists():
        with ledger_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                ledger_count += 1

                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    missing_fields_rows += 1
                    continue

                if not _REQUIRED_LEDGER_FIELDS.issubset(set(row.keys())):
                    missing_fields_rows += 1

                total_raw = row.get("total_usd")
                if not isinstance(total_raw, str) or not _6DP_RE.match(total_raw):
                    invalid_total_6dp_rows += 1
                    ok, parsed = _parse_decimal_6(total_raw)
                    if not ok:
                        continue
                    total = parsed
                else:
                    total = dec(total_raw)

                ledger_total += total

                corr = row.get("correlation_id")
                wu = row.get("work_unit_id")
                mapped_session: str | None = None
                if isinstance(corr, str) and corr.startswith("sess:"):
                    mapped_session = corr[len("sess:") :]
                elif isinstance(wu, str) and wu.startswith("sess:"):
                    mapped_session = wu[len("sess:") :]

                if mapped_session:
                    session_mappings[mapped_session] = True
                    session_actuals[mapped_session] = session_actuals.get(mapped_session, dec("0")) + total

    global_flags: list[str] = []
    if ledger_total < dec("0"):
        global_flags.append("ledger_total_negative")
    if missing_fields_rows > 0:
        global_flags.append("missing_required_schema_fields")
    if invalid_total_6dp_rows > 0:
        global_flags.append("invalid_total_usd_6dp")

    sessions: list[dict[str, Any]] = []
    sessions_checked = 0
    sessions_flagged = 0

    for session_id in sorted(expected_sessions.keys()):
        if not session_mappings.get(session_id, False):
            continue

        expected = expected_sessions[session_id]
        actual = quant6(session_actuals.get(session_id, dec("0")))
        diff = quant6(abs(expected - actual))

        reason: str | None = None
        if diff > _TOL:
            reason = "absolute_diff_exceeds_tolerance"
        if expected > dec("0"):
            ratio = diff / expected
            if ratio > dec("0.01"):
                reason = "relative_diff_exceeds_1_percent"

        flagged = reason is not None
        if flagged:
            sessions_flagged += 1
        sessions_checked += 1

        sessions.append(
            {
                "execution_session_id": session_id,
                "expected_usd": serialize_decimal(expected),
                "actual_usd": serialize_decimal(actual),
                "diff_usd": serialize_decimal(diff),
                "flagged_reason": reason,
            }
        )

    report = {
        "version": "0.1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "checks": {
            "global": {
                "ledger_total_usd": serialize_decimal(quant6(ledger_total)),
                "ledger_record_count": ledger_count,
                "missing_required_schema_fields_count": missing_fields_rows,
                "invalid_total_usd_6dp_count": invalid_total_6dp_rows,
                "global_flagged": len(global_flags) > 0,
                "flagged_reasons": global_flags,
            },
            "sessions": sessions,
        },
        "summary": {
            "sessions_checked": sessions_checked,
            "sessions_flagged": sessions_flagged,
            "global_flagged": len(global_flags) > 0,
        },
    }
    return report


def write_anomaly_report(report: dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, sort_keys=True, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, out_path)
