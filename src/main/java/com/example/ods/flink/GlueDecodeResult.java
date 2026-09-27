package com.example.ods.flink;

/** A non-throwing decoder result consumed by the PyFlink routing layer. */
public record GlueDecodeResult(
    DecodeStatus status,
    String envelopeJson,
    String errorType,
    String errorMessage) {

  public static GlueDecodeResult success(String envelopeJson) {
    return new GlueDecodeResult(DecodeStatus.SUCCESS, envelopeJson, null, null);
  }

  public static GlueDecodeResult failure(Exception error) {
    return new GlueDecodeResult(
        DecodeStatus.FAILURE,
        null,
        error.getClass().getName(),
        error.getMessage());
  }
}

