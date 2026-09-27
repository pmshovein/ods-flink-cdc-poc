"""MSF entry point.

The runtime-specific source bridge is intentionally isolated behind
``create_safe_glue_debezium_source``. The pure-Python normalizer is testable
without AWS, Kafka, or PyFlink.
"""

from __future__ import annotations

from .settings import JobSettings


def main() -> None:
    settings = JobSettings.from_runtime_properties()
    # The next implementation step registers the JVM bridge JAR and builds the
    # Table/DataStream branches: DecodeResult.SUCCESS -> Iceberg, FAILURE -> DLQ.
    # Keeping that bridge explicit prevents accidental decode exceptions from
    # bypassing the DLQ path.
    print(
        "ODS CDC job configuration loaded: "
        f"environment={settings.environment}, topic_pattern={settings.topic_pattern}"
    )


if __name__ == "__main__":
    main()
