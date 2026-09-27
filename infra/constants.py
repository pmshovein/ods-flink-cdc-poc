"""One configuration surface for local/CDK/MSF deployment values."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} must be supplied for a CDK deployment")
    return value


@dataclass(frozen=True)
class OdsEnvironment:
    name: str
    account: str
    region: str
    artifact_bucket: str
    artifact_key: str
    execution_role_arn: str
    bootstrap_servers: str
    topic_pattern: str
    dlq_topic: str
    glue_database: str
    iceberg_warehouse: str
    checkpoint_interval_ms: str

    @classmethod
    def from_environment(cls) -> "OdsEnvironment":
        return cls(
            name=os.getenv("ODS_ENVIRONMENT", "dev"),
            account=_required("ODS_AWS_ACCOUNT"),
            region=os.getenv("ODS_AWS_REGION", "us-east-1"),
            artifact_bucket=_required("ODS_ARTIFACT_BUCKET"),
            artifact_key=_required("ODS_ARTIFACT_KEY"),
            execution_role_arn=_required("ODS_MSF_EXECUTION_ROLE_ARN"),
            bootstrap_servers=_required("ODS_MSK_BOOTSTRAP_SERVERS"),
            topic_pattern=_required("ODS_CDC_TOPIC_PATTERN"),
            dlq_topic=_required("ODS_DLQ_TOPIC"),
            glue_database=_required("ODS_GLUE_DATABASE"),
            iceberg_warehouse=_required("ODS_ICEBERG_WAREHOUSE"),
            checkpoint_interval_ms=os.getenv("ODS_CHECKPOINT_INTERVAL_MS", "60000"),
        )

