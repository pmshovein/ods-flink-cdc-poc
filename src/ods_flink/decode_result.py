"""Parse the JSON wire contract emitted by the JVM Kafka source bridge."""

from __future__ import annotations

import json
from typing import Any

from .models import DecodeResult, DecodeStatus, KafkaMetadata


def from_bridge_json(value: str) -> DecodeResult:
    """Convert one JVM bridge payload into the transport-neutral model."""
    document: dict[str, Any] = json.loads(value)
    metadata = document["metadata"]
    envelope_json = document.get("envelope_json")
    return DecodeResult(
        status=DecodeStatus(document["status"]),
        metadata=KafkaMetadata(
            topic=metadata["topic"],
            partition=metadata["partition"],
            offset=metadata["offset"],
            timestamp_ms=metadata.get("timestamp_ms"),
            key_b64=metadata.get("key_b64"),
            value_b64=metadata.get("value_b64"),
        ),
        envelope=json.loads(envelope_json) if envelope_json else None,
        error_type=document.get("error_type"),
        error_message=document.get("error_message"),
    )
