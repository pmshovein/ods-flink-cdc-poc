from ods_flink.decode_result import from_bridge_json
from ods_flink.models import DecodeStatus


def test_bridge_result_preserves_metadata_and_envelope():
    result = from_bridge_json(
        '{"status":"SUCCESS","metadata":{"topic":"claims.public.claim","partition":1,"offset":9,'
        '"timestamp_ms":42,"key_b64":"a2V5","value_b64":"dmFsdWU="},'
        '"envelope_json":"{\\"op\\":\\"c\\",\\"after\\":{\\"id\\":1}}",'
        '"error_type":null,"error_message":null}'
    )
    assert result.status is DecodeStatus.SUCCESS
    assert result.metadata.offset == 9
    assert result.envelope == {"op": "c", "after": {"id": 1}}
