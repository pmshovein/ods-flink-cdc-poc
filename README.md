# MSF PyFlink CDC-to-Iceberg POC

This is a local, credential-free reference implementation for a reusable Amazon
Managed Service for Apache Flink (MSF) ingestion job.

It intentionally keeps the Debezium PostgreSQL connector unmodified. The job
uses the DataStream API, accepts native Debezium envelopes encoded with AWS
Glue Schema Registry Avro, then routes either a valid Flink changelog event or
a structured DLQ event.

```
MSK bytes -> safe Glue/Avro decode -> DecodeResult
                                      |- failure -> DLQ
                                      `- success -> Debezium normalizer -> Iceberg v2 upsert
```

## Repository layout

- `src/ods_flink/`: reusable Python domain logic and the PyFlink entry point.
- `jvm-bridge/`: minimal Java Glue decoder; it catches decode errors instead of
  failing the source task.
- `infra/`: Python CDK application and environment-driven constants.
- `tests/`: no-AWS unit tests for CDC changelog and DLQ decisions.

## Local checks

The domain logic has no PyFlink dependency, so it can be tested without Kafka,
AWS, or Java:

```bash
python3 -m pytest
```

When Java 17 and Maven are available, build the dependency JAR:

```bash
mvn package
```

The MSF deployment archive is assembled by CI/CD from `src/`,
`target/ods-flink-dependencies.jar`, and the generated Python dependency
archive. Do not use `pip install` during MSF startup: all dependencies must be
inside the uploaded application ZIP.

## Environment configuration

`infra/constants.py` reads deployment values from environment variables. Copy
`.env.example` into your deployment system's environment (or CDK context/CI
variables); never commit credentials. MSF passes non-secret runtime values to
the job through the `ods.cdc` property group; use
`application_properties.example.json` as the local equivalent.

The first required production values are `ODS_ARTIFACT_BUCKET`,
`ODS_ARTIFACT_KEY`, `ODS_MSF_EXECUTION_ROLE_ARN`, `ODS_MSK_BOOTSTRAP_SERVERS`,
`ODS_GLUE_DATABASE`, and `ODS_ICEBERG_WAREHOUSE`.

## Important boundary

The Java code is an interoperability component, not upsert logic. The Python
normalizer owns the Debezium `c/r/u/d` to Flink changelog contract. Iceberg
owns the v2 upsert write.

The JVM source uses `KafkaRecordDeserializationSchema` so it sees an entire
Kafka `ConsumerRecord`, rather than only its value. It returns a JSON
`DecodeResult` for every readable Kafka record and *never throws for a
per-record Glue/Avro error*. PyFlink then parses that result and uses a side
output for DLQ routing. Connectivity, authentication, and checkpoint failures
still fail the job as they should.
