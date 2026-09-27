from ods_flink.cdc_normalizer import to_changelog
from ods_flink.models import ChangeKind, DecodeResult, DecodeStatus, KafkaMetadata


def record(envelope):
    return DecodeResult(
        status=DecodeStatus.SUCCESS,
        metadata=KafkaMetadata("claims.public.claim", 2, 19),
        envelope=envelope,
    )


def test_update_emits_before_then_after():
    changes, dlq = to_changelog(record({"op": "u", "before": {"id": 7, "status": "NEW"}, "after": {"id": 7, "status": "PAID"}}))
    assert dlq is None
    assert [change.kind for change in changes] == [ChangeKind.UPDATE_BEFORE, ChangeKind.UPDATE_AFTER]
    assert changes[1].row["status"] == "PAID"


def test_delete_emits_delete_using_before_image():
    changes, dlq = to_changelog(record({"op": "d", "before": {"id": 7}, "after": None}))
    assert dlq is None
    assert changes[0].kind is ChangeKind.DELETE


def test_decode_failure_goes_to_dlq_with_kafka_coordinates():
    result = DecodeResult(
        status=DecodeStatus.FAILURE,
        metadata=KafkaMetadata("claims.public.claim", 2, 19, value_b64="bad-frame"),
        error_type="AvroTypeException",
        error_message="invalid frame",
    )
    changes, dlq = to_changelog(result)
    assert changes == []
    assert dlq.reason_code == "GLUE_AVRO_DECODE_FAILED"
    assert dlq.metadata.offset == 19

