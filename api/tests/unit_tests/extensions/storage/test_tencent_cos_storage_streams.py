from io import BytesIO
from unittest.mock import MagicMock

import requests
from qcloud_cos.cos_client import StreamBody
from urllib3.response import HTTPResponse

from extensions.storage.tencent_cos_storage import TencentCosStorage


def test_load_stream_closes_response_body_when_consumer_stops_early() -> None:
    raw, body = _stream_body(b"x" * 8192)
    storage = TencentCosStorage.__new__(TencentCosStorage)
    storage.bucket_name = "test-bucket"
    storage.client = MagicMock()
    storage.client.get_object.return_value = {"Body": body}

    stream = storage.load_stream("key")
    assert next(stream) == b"x" * 4096
    stream.close()

    assert raw.closed


def test_load_once_closes_response_body() -> None:
    raw, body = _stream_body(b"payload")
    storage = TencentCosStorage.__new__(TencentCosStorage)
    storage.bucket_name = "test-bucket"
    storage.client = MagicMock()
    storage.client.get_object.return_value = {"Body": body}

    assert storage.load_once("key") == b"payload"

    assert raw.closed


def _stream_body(payload: bytes) -> tuple[HTTPResponse, StreamBody]:
    raw = HTTPResponse(body=BytesIO(payload), headers={"Content-Length": str(len(payload))}, preload_content=False)
    http_response = requests.Response()
    http_response.status_code = 200
    http_response.headers["Content-Length"] = str(len(payload))
    http_response.raw = raw
    body = StreamBody(http_response)
    return raw, body
