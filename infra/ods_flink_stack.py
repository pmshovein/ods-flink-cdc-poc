from __future__ import annotations

from aws_cdk import CfnParameter, Stack, aws_kinesisanalyticsv2 as kda
from constructs import Construct

from .constants import OdsEnvironment


class OdsFlinkStack(Stack):
    """Deploys an MSF Python application from a pre-built S3 ZIP artifact."""

    def __init__(self, scope: Construct, construct_id: str, *, config: OdsEnvironment, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Kept as an explicit deployment-time parameter so CI can promote an
        # immutable S3 object version without changing application code.
        artifact_version = CfnParameter(self, "ArtifactVersion", type="String", default="")

        kda.CfnApplication(
            self,
            "CdcApplication",
            application_name=f"ods-cdc-{config.name}",
            runtime_environment="FLINK-2_3",
            service_execution_role=config.execution_role_arn,
            application_configuration=kda.CfnApplication.ApplicationConfigurationProperty(
                application_code_configuration=kda.CfnApplication.ApplicationCodeConfigurationProperty(
                    code_content=kda.CfnApplication.CodeContentProperty(
                        s3_content_location=kda.CfnApplication.S3ContentLocationProperty(
                            bucket_arn=f"arn:{self.partition}:s3:::{config.artifact_bucket}",
                            file_key=config.artifact_key,
                            object_version=artifact_version.value_as_string,
                        )
                    ),
                    code_content_type="ZIPFILE",
                ),
                environment_properties=kda.CfnApplication.EnvironmentPropertiesProperty(
                    property_groups=[
                        kda.CfnApplication.PropertyGroupProperty(
                            property_group_id="kinesis.analytics.flink.run.options",
                            property_map={
                                "python": "ods_flink/main.py",
                                "jarfile": "lib/ods-flink-dependencies.jar",
                            },
                        ),
                        kda.CfnApplication.PropertyGroupProperty(
                            property_group_id="ods.cdc",
                            property_map={
                                "environment": config.name,
                                "bootstrap_servers": config.bootstrap_servers,
                                "topic_pattern": config.topic_pattern,
                                "dlq_topic": config.dlq_topic,
                                "glue_database": config.glue_database,
                                "iceberg_warehouse": config.iceberg_warehouse,
                                "checkpoint_interval_ms": config.checkpoint_interval_ms,
                            },
                        ),
                    ]
                ),
            ),
        )

