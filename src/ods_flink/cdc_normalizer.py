"""Translate native Debezium envelopes into explicit changelog events."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .models import ChangeEvent, ChangeKind, DecodeResult, DecodeStatus, DlqRecord


def to_changelog(result: DecodeResult) -> tuple[list[ChangeEvent], DlqRecord | None]:
    """Return valid Flink-style events or exactly one DLQ record.

    The function is deliberately framework-free. The PyFlink adapter maps
    ``ChangeKind`` to ``RowKind`` at the job boundary.
    """
    if result.status is DecodeStatus.FAILURE:
        return [], DlqRecord(
            reason_code="GLUE_AVRO_DECODE_FAILED",
            reason=result.error_message or "Glue/Avro decode failed",
            metadata=result.metadata,
        )

    envelope = result.envelope
    if not envelope:
        return [], _dlq("MISSING_ENVELOPE", "Decoded record has no Debezium envelope", result)

    operation = envelope.get("op")
    if operation not in {"c", "r", "u", "d"}:
        return [], _dlq("INVALID_DEBEZIUM_OPERATION", f"Unsupported Debezium op: {operation!r}", result)

    before = _as_row(envelope.get("before"))
    after = _as_row(envelope.get("after"))
    if operation in {"c", "r"}:
        if after is None:
            return [], _dlq("MISSING_AFTER", f"Debezium {operation} event has no after image", result)
        return [ChangeEvent(ChangeKind.INSERT, after, result.metadata, operation)], None
    if operation == "u":
        if before is None or after is None:
            return [], _dlq("INVALID_UPDATE_IMAGES", "Debezium update needs before and after images", result)
        return [
            ChangeEvent(ChangeKind.UPDATE_BEFORE, before, result.metadata, operation),
            ChangeEvent(ChangeKind.UPDATE_AFTER, after, result.metadata, operation),
        ], None
    if before is None:
        return [], _dlq("MISSING_BEFORE", "Debezium delete has no before image", result)
    return [ChangeEvent(ChangeKind.DELETE, before, result.metadata, operation)], None


def normalize_many(results: Iterable[DecodeResult]) -> tuple[list[ChangeEvent], list[DlqRecord]]:
    changes: list[ChangeEvent] = []
    dlq: list[DlqRecord] = []
    for result in results:
        events, failure = to_changelog(result)
        changes.extend(events)
        if failure:
            dlq.append(failure)
    return changes, dlq


def _as_row(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _dlq(code: str, reason: str, result: DecodeResult) -> DlqRecord:
    return DlqRecord(code, reason, result.metadata, result.envelope)

