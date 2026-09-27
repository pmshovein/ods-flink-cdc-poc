package com.example.ods.flink;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

/**
 * Thin AWS Glue interoperability boundary.
 *
 * A calling source/connector supplies Kafka coordinates and raw bytes. Decode
 * errors are returned as data so the Python layer can publish a DLQ record.
 */
public final class SafeGlueAvroDecoder {
  private final GlueSchemaRegistryKafkaDeserializer deserializer;

  public SafeGlueAvroDecoder(Map<String, ?> properties) {
    this.deserializer = new GlueSchemaRegistryKafkaDeserializer();
    this.deserializer.configure(properties, false);
  }

  public GlueDecodeResult decode(String topic, byte[] value) {
    try {
      Object decoded = deserializer.deserialize(topic, value);
      if (!(decoded instanceof GenericRecord record)) {
        throw new IllegalArgumentException("Expected an Avro GenericRecord, got " + decoded.getClass().getName());
      }
      return GlueDecodeResult.success(toJson(record));
    } catch (Exception error) {
      return GlueDecodeResult.failure(error);
    }
  }

  private static String toJson(GenericRecord record) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), output);
    new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
    encoder.flush();
    return output.toString(StandardCharsets.UTF_8);
  }
}
