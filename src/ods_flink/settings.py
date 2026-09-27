"""Application settings read from MSF runtime properties or local env vars."""

from __future__ import annotations

import os
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class JobSettings:
    environment: str
    bootstrap_servers: str
    topic_pattern: str
    dlq_topic: str
    glue_database: str
    iceberg_warehouse: str
    checkpoint_interval_ms: int

    @classmethod
    def from_runtime_properties(cls) -> "JobSettings":
        """Read MSF's mounted runtime-property file, with local fallback.

        MSF mounts runtime properties at `/etc/flink/application_properties.json`.
        Local runs set `IS_LOCAL=true` and use a git-ignored
        `application_properties.json` copied from the example file.
        """
        property_path = Path(
            "application_properties.json"
            if os.getenv("IS_LOCAL")
            else "/etc/flink/application_properties.json"
        )
        if property_path.exists():
            groups = json.loads(property_path.read_text(encoding="utf-8"))
            properties = next(
                (group["PropertyMap"] for group in groups if group["PropertyGroupId"] == "ods.cdc"),
                None,
            )
            if properties is None:
                raise ValueError("Missing ods.cdc MSF runtime property group")
            return cls(
                environment=properties["environment"],
                bootstrap_servers=properties["bootstrap_servers"],
                topic_pattern=properties["topic_pattern"],
                dlq_topic=properties["dlq_topic"],
                glue_database=properties["glue_database"],
                iceberg_warehouse=properties["iceberg_warehouse"],
                checkpoint_interval_ms=int(properties["checkpoint_interval_ms"]),
            )
        return cls.from_environment()

    @classmethod
    def from_environment(cls) -> "JobSettings":
        return cls(
            environment=os.getenv("ODS_ENVIRONMENT", "local"),
            bootstrap_servers=os.getenv("ODS_MSK_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic_pattern=os.getenv("ODS_CDC_TOPIC_PATTERN", "local.cdc.*"),
            dlq_topic=os.getenv("ODS_DLQ_TOPIC", "local.cdc.dlq"),
            glue_database=os.getenv("ODS_GLUE_DATABASE", "ods"),
            iceberg_warehouse=os.getenv("ODS_ICEBERG_WAREHOUSE", "file:///tmp/ods-iceberg"),
            checkpoint_interval_ms=int(os.getenv("ODS_CHECKPOINT_INTERVAL_MS", "60000")),
        )
