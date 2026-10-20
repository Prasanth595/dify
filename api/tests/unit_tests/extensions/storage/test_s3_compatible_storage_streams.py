from collections.abc import Iterator
from io import BytesIO

import boto3
import pytest
from botocore.response import StreamingBody

from extensions.storage.aws_s3_storage import AwsS3Storage
from extensions.storage.oracle_oci_storage import OracleOCIStorage


@pytest.mark.parametrize("storage_type", [AwsS3Storage, OracleOCIStorage])
def test_load_once_closes_response_body(storage_type: type[AwsS3Storage] | type[OracleOCIStorage]) -> None:
    raw = BytesIO(b"payload")
    body = StreamingBody(raw, len(b"payload"))
    storage = storage_type.__new__(storage_type)
    storage.bucket_name = "test-bucket"
    storage.client = _s3_client()
    storage.client.get_object = lambda **_: {"Body": body}

    assert storage.load_once("key") == b"payload"

    assert raw.closed


@pytest.mark.parametrize("storage_type", [AwsS3Storage, OracleOCIStorage])
@pytest.mark.parametrize("close_early", [True, False])
def test_load_stream_closes_response_body(
    storage_type: type[AwsS3Storage] | type[OracleOCIStorage], close_early: bool
) -> None:
    raw = BytesIO(b"x" * 2048)
    body = StreamingBody(raw, 2048)
    storage = storage_type.__new__(storage_type)
    storage.bucket_name = "test-bucket"
    storage.client = _s3_client()
    storage.client.get_object = lambda **_: {"Body": body}

    stream = storage.load_stream("key")
    if close_early:
        assert next(stream) == b"x" * 1024
        stream.close()
    else:
        assert b"".join(stream) == b"x" * 2048

    assert raw.closed


@pytest.mark.parametrize("storage_type", [AwsS3Storage, OracleOCIStorage])
def test_load_stream_closes_response_body_when_iteration_fails(
    storage_type: type[AwsS3Storage] | type[OracleOCIStorage],
) -> None:
    class FailingBody:
        closed = False

        def iter_chunks(self) -> Iterator[bytes]:
            yield b"first chunk"
            raise OSError("stream read failed")

        def close(self) -> None:
            self.closed = True

    body = FailingBody()
    storage = storage_type.__new__(storage_type)
    storage.bucket_name = "test-bucket"
    storage.client = _s3_client()
    storage.client.get_object = lambda **_: {"Body": body}

    stream = storage.load_stream("key")
    assert next(stream) == b"first chunk"

    with pytest.raises(OSError, match="stream read failed"):
        next(stream)

    assert body.closed


def _s3_client():
    return boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="test-access-key",
        aws_secret_access_key="test-secret-key",
    )
