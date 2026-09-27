"""Transport-neutral types shared by source, normalizer, and DLQ modules."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class DecodeStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class ChangeKind(str, Enum):
    INSERT = "INSERT"
    UPDATE_BEFORE = "UPDATE_BEFORE"
    UPDATE_AFTER = "UPDATE_AFTER"
    DELETE = "DELETE"


@dataclass(frozen=True)
class KafkaMetadata:
    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None = None
    key_b64: str | None = None
    value_b64: str | None = None


@dataclass(frozen=True)
class DecodeResult:
    """Result returned by the safe JVM Glue decoder.

    A decoder failure is data, not an exception. This distinction is what
    permits per-record DLQ routing while retaining Kafka coordinates.
    """

    status: DecodeStatus
    metadata: KafkaMetadata
    envelope: dict[str, Any] | None = None
    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class ChangeEvent:
    kind: ChangeKind
    row: dict[str, Any]
    metadata: KafkaMetadata
    operation: str


@dataclass(frozen=True)
class DlqRecord:
    reason_code: str
    reason: str
    metadata: KafkaMetadata
    envelope: dict[str, Any] | None = None

