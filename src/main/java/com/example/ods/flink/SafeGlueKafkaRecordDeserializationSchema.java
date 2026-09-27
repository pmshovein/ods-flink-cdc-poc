package com.example.ods.flink;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Converts every readable Kafka record to a JSON DecodeResult.
 *
 * This is deliberately a source deserialization schema, not a Flink UDF. It
 * retains topic/partition/offset/key/value information required for a useful
 * DLQ record. Only an individual Glue/Avro decode is caught; Kafka source and
 * checkpoint failures continue to fail the job normally.
 */
public final class SafeGlueKafkaRecordDeserializationSchema
    implements KafkaRecordDeserializationSchema<String> {
  private final Map<String, ?> glueProperties;
  private transient SafeGlueAvroDecoder decoder;

  public SafeGlueKafkaRecordDeserializationSchema(Map<String, ?> glueProperties) {
    this.glueProperties = glueProperties;
  }

  @Override
  public void open(DeserializationSchema.InitializationContext context) {
    decoder = new SafeGlueAvroDecoder(glueProperties);
  }

  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out) throws IOException {
    GlueDecodeResult decoded = decoder.decode(record.topic(), record.value());
    out.collect(asJson(record, decoded));
  }

  @Override
  public TypeInformation<String> getProducedType() {
    return TypeInformation.of(String.class);
  }

  private static String asJson(ConsumerRecord<byte[], byte[]> record, GlueDecodeResult decoded) {
    String metadata = "\"metadata\":{" +
        "\"topic\":" + quote(record.topic()) + "," +
        "\"partition\":" + record.partition() + "," +
        "\"offset\":" + record.offset() + "," +
        "\"timestamp_ms\":" + record.timestamp() + "," +
        "\"key_b64\":" + quote(base64(record.key())) + "," +
        "\"value_b64\":" + quote(base64(record.value())) + "}";
    String payload = decoded.envelopeJson() == null ? "null" : quote(decoded.envelopeJson());
    return "{" +
        "\"status\":" + quote(decoded.status().name()) + "," +
        metadata + "," +
        "\"envelope_json\":" + payload + "," +
        "\"error_type\":" + quote(decoded.errorType()) + "," +
        "\"error_message\":" + quote(decoded.errorMessage()) +
        "}";
  }

  private static String base64(byte[] value) {
    return value == null ? null : Base64.getEncoder().encodeToString(value);
  }

  private static String quote(String value) {
    if (value == null) {
      return "null";
    }
    String escaped = value.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\b", "\\b")
        .replace("\f", "\\f")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
    return "\"" + escaped + "\"";
  }
}
