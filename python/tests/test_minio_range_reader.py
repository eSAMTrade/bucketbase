import io
from dataclasses import dataclass
from typing import Iterable, Iterator, Optional
from unittest import TestCase

import pyarrow as pa
import pyarrow.parquet as pq
from aicodesign import ai_blackbox
from minio import Minio
from minio.datatypes import Object
from minio.deleteobjects import DeleteError
from minio.error import S3Error
from minio.helpers import DictType
from minio.sse import SseCustomerKey
from typing_extensions import override
from urllib3 import HTTPResponse

from bucketbase.minio_bucket import MinioBucket


@ai_blackbox()
@dataclass(frozen=True)
class RangeReadCall:
    offset: int
    length: int
    request_headers: Optional[DictType]
    version_id: Optional[str]


@ai_blackbox()
class TrackingHTTPResponse(HTTPResponse):
    @ai_blackbox()
    def __init__(self, content: bytes, read_error: OSError | None = None, close_error: OSError | None = None) -> None:
        super().__init__(body=io.BytesIO(content), headers={"content-length": str(len(content))}, preload_content=False)
        self.read_error = read_error
        self.close_error = close_error
        self.release_count = 0

    @override
    @ai_blackbox()
    def read(self, amt: int | None = None, decode_content: bool | None = None, cache_content: bool = False) -> bytes:
        if self.read_error is not None:
            raise self.read_error
        return super().read(amt=amt, decode_content=decode_content, cache_content=cache_content)

    @override
    @ai_blackbox()
    def close(self) -> None:
        super().close()
        if self.close_error is not None:
            raise self.close_error

    @override
    @ai_blackbox()
    def release_conn(self) -> None:
        self.release_count += 1
        super().release_conn()


@ai_blackbox()
class FakeRangeMinioClient(Minio):
    @ai_blackbox()
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.etag = "test-etag"
        self.stat_error: S3Error | None = None
        self.get_error: S3Error | None = None
        self.response_read_error: OSError | None = None
        self.response_close_error: OSError | None = None
        self.truncate_response_by = 0
        self.stat_calls: list[tuple[str, str, Optional[str]]] = []
        self.range_read_calls: list[RangeReadCall] = []
        self.responses: list[TrackingHTTPResponse] = []

    @ai_blackbox()
    def stat_object(
        self,
        bucket_name: str,
        object_name: str,
        ssec: Optional[SseCustomerKey] = None,
        version_id: Optional[str] = None,
        extra_headers: Optional[DictType] = None,
        extra_query_params: Optional[DictType] = None,
    ) -> Object:
        del ssec, extra_headers, extra_query_params
        self.stat_calls.append((bucket_name, object_name, version_id))
        if self.stat_error is not None:
            raise self.stat_error
        return Object(bucket_name, object_name, etag=self.etag, size=len(self.content), version_id=version_id)

    @ai_blackbox()
    def get_object(
        self,
        bucket_name: str,
        object_name: str,
        offset: int = 0,
        length: int = 0,
        request_headers: Optional[DictType] = None,
        ssec: Optional[SseCustomerKey] = None,
        version_id: Optional[str] = None,
        extra_query_params: Optional[DictType] = None,
    ) -> HTTPResponse:
        del bucket_name, object_name, ssec, extra_query_params
        self.range_read_calls.append(RangeReadCall(offset, length, request_headers, version_id))
        if self.get_error is not None:
            raise self.get_error
        end = len(self.content) if length == 0 else offset + length
        response_content = self.content[offset:end]
        if self.truncate_response_by:
            response_content = response_content[: -self.truncate_response_by]
        response = TrackingHTTPResponse(response_content, self.response_read_error, self.response_close_error)
        self.responses.append(response)
        return response

    @ai_blackbox()
    def remove_objects(self, bucket_name: str, delete_object_list: Iterable[object], bypass_governance_mode: bool = False) -> Iterator[DeleteError]:
        del bucket_name, delete_object_list, bypass_governance_mode
        return iter(())


@ai_blackbox()
class TestMinioRangeReader(TestCase):
    @ai_blackbox()
    def setUp(self) -> None:
        self.content = bytes(range(256)) * 2048
        self.client = FakeRangeMinioClient(self.content)

    @staticmethod
    @ai_blackbox()
    def _make_s3_error(code: str) -> S3Error:
        return S3Error(HTTPResponse(status=412), code, code, "resource", "request-id", "host-id")

    @ai_blackbox()
    def test_get_object_uses_one_get_without_stat_and_releases_response(self) -> None:
        bucket = MinioBucket("test-bucket", self.client)

        result = bucket.get_object("object.bin")

        self.assertEqual(self.content, result)
        self.assertEqual([], self.client.stat_calls)
        self.assertEqual([RangeReadCall(0, 0, None, None)], self.client.range_read_calls)
        self.assertTrue(self.client.responses[0].closed)
        self.assertEqual(1, self.client.responses[0].release_count)

    @ai_blackbox()
    def test_get_object_releases_response_when_read_fails(self) -> None:
        self.client.response_read_error = OSError("read failed")
        bucket = MinioBucket("test-bucket", self.client)

        with self.assertRaisesRegex(OSError, "read failed"):
            bucket.get_object("object.bin")

        self.assertTrue(self.client.responses[0].closed)
        self.assertEqual(1, self.client.responses[0].release_count)

    @ai_blackbox()
    def test_get_object_releases_connection_when_close_fails(self) -> None:
        self.client.response_close_error = OSError("close failed")
        bucket = MinioBucket("test-bucket", self.client)

        with self.assertRaisesRegex(OSError, "close failed"):
            bucket.get_object("object.bin")

        self.assertTrue(self.client.responses[0].closed)
        self.assertEqual(1, self.client.responses[0].release_count)

    @ai_blackbox()
    def test_get_object_translates_missing_object_and_preserves_other_s3_errors(self) -> None:
        bucket = MinioBucket("test-bucket", self.client)

        self.client.get_error = self._make_s3_error("NoSuchKey")
        with self.assertRaises(FileNotFoundError):
            bucket.get_object("missing.bin")

        self.client.get_error = self._make_s3_error("AccessDenied")
        with self.assertRaises(S3Error):
            bucket.get_object("object.bin")

    @ai_blackbox()
    def test_default_buffer_prefetches_once_and_reuses_cached_bytes(self) -> None:
        bucket = MinioBucket("test-bucket", self.client)

        with bucket.get_object_stream("object.bin") as stream:
            stream.seek(1_000)
            first = stream.read(16)
            stream.seek(1_020)
            second = stream.read(16)

        self.assertEqual(self.content[1_000:1_016], first)
        self.assertEqual(self.content[1_020:1_036], second)
        self.assertEqual([("test-bucket", "object.bin", None)], self.client.stat_calls)
        self.assertEqual([RangeReadCall(1_000, MinioBucket.DEFAULT_READ_BUFFER_SIZE, {"If-Match": f'"{self.client.etag}"'}, None)], self.client.range_read_calls)
        self.assertTrue(self.client.responses[0].closed)
        self.assertEqual(1, self.client.responses[0].release_count)

    @ai_blackbox()
    def test_zero_buffer_reads_exact_ranges_and_supports_all_seek_modes(self) -> None:
        bucket = MinioBucket("test-bucket", self.client, read_buffer_size=0)

        with bucket.get_object_stream("object.bin") as stream:
            self.assertEqual(10, stream.seek(10, io.SEEK_SET))
            self.assertEqual(15, stream.seek(5, io.SEEK_CUR))
            self.assertEqual(len(self.content) - 4, stream.seek(-4, io.SEEK_END))
            destination = bytearray(8)
            self.assertEqual(4, stream.readinto(destination))  # type: ignore[attr-defined]
            self.assertEqual(self.content[-4:], destination[:4])
            self.assertEqual(b"", stream.read(1))
            self.assertEqual(len(self.content) + 10, stream.seek(10, io.SEEK_END))
            self.assertEqual(b"", stream.read())
            with self.assertRaisesRegex(ValueError, "Negative seek position"):
                stream.seek(-1)
            with self.assertRaisesRegex(ValueError, "Invalid whence"):
                stream.seek(0, 99)

        self.assertEqual([RangeReadCall(len(self.content) - 4, 4, {"If-Match": f'"{self.client.etag}"'}, None)], self.client.range_read_calls)

    @ai_blackbox()
    def test_large_read_prefetch_is_limited_to_one_buffer(self) -> None:
        bucket = MinioBucket("test-bucket", self.client)
        requested_size = MinioBucket.DEFAULT_READ_BUFFER_SIZE + 1

        with bucket.get_object_stream("object.bin") as stream:
            stream.seek(100)
            result = stream.read(requested_size)

        self.assertEqual(self.content[100 : 100 + requested_size], result)
        self.assertGreaterEqual(sum(call.length for call in self.client.range_read_calls), requested_size)
        self.assertLess(sum(call.length for call in self.client.range_read_calls), requested_size + MinioBucket.DEFAULT_READ_BUFFER_SIZE)
        self.assertTrue(all(call.length <= requested_size for call in self.client.range_read_calls))

    @ai_blackbox()
    def test_short_range_response_raises_and_releases_connection(self) -> None:
        self.client.truncate_response_by = 1
        bucket = MinioBucket("test-bucket", self.client, read_buffer_size=0)

        with bucket.get_object_stream("object.bin") as stream:
            with self.assertRaisesRegex(OSError, "Expected 10 bytes"):
                stream.read(10)

        self.assertTrue(self.client.responses[0].closed)
        self.assertEqual(1, self.client.responses[0].release_count)

    @ai_blackbox()
    def test_parquet_tail_reads_selected_columns_without_full_object_get(self) -> None:
        parquet_buffer = io.BytesIO()
        schema = pa.schema([("id", pa.int64()), ("name", pa.string()), ("active", pa.bool_())])
        with pq.ParquetWriter(parquet_buffer, schema) as writer:
            for row_group in range(3):
                row_count = 20_000
                start = row_group * row_count
                writer.write_batch(
                    pa.record_batch(
                        {
                            "id": list(range(start, start + row_count)),
                            "name": [f"row-{row_group}-{row:05d}" * 3 for row in range(row_count)],
                            "active": [row % 2 == 0 for row in range(start, start + row_count)],
                        },
                        schema=schema,
                    )
                )
        parquet_content = parquet_buffer.getvalue()
        client = FakeRangeMinioClient(parquet_content)
        bucket = MinioBucket("test-bucket", client)

        with bucket.get_object_stream("table.parquet") as stream:
            parquet_file = pq.ParquetFile(stream)
            selected = parquet_file.read_row_group(parquet_file.num_row_groups - 1, columns=["id", "active"])
            tail = selected.slice(len(selected) - 75)

        self.assertEqual(["id", "active"], tail.column_names)
        self.assertEqual(list(range(60_000 - 75, 60_000)), tail.column("id").to_pylist())
        self.assertTrue(client.range_read_calls)
        self.assertTrue(all(call.length > 0 for call in client.range_read_calls))
        self.assertLess(sum(call.length for call in client.range_read_calls), len(parquet_content))

    @ai_blackbox()
    def test_range_read_releases_response_when_read_fails(self) -> None:
        self.client.response_read_error = OSError("read failed")
        bucket = MinioBucket("test-bucket", self.client, read_buffer_size=0)

        with bucket.get_object_stream("object.bin") as stream:
            with self.assertRaisesRegex(OSError, "read failed"):
                stream.read(10)

        self.assertTrue(self.client.responses[0].closed)
        self.assertEqual(1, self.client.responses[0].release_count)

    @ai_blackbox()
    def test_range_read_releases_connection_when_close_fails(self) -> None:
        self.client.response_close_error = OSError("close failed")
        bucket = MinioBucket("test-bucket", self.client, read_buffer_size=0)

        with bucket.get_object_stream("object.bin") as stream:
            with self.assertRaisesRegex(OSError, "close failed"):
                stream.read(10)

        self.assertTrue(self.client.responses[0].closed)
        self.assertEqual(1, self.client.responses[0].release_count)

    @ai_blackbox()
    def test_missing_object_is_translated_during_stream_creation(self) -> None:
        self.client.stat_error = self._make_s3_error("NoSuchKey")
        bucket = MinioBucket("test-bucket", self.client)

        with self.assertRaises(FileNotFoundError):
            bucket.get_object_stream("missing.bin")

    @ai_blackbox()
    def test_object_replacement_is_translated_to_os_error(self) -> None:
        self.client.get_error = self._make_s3_error("PreconditionFailed")
        bucket = MinioBucket("test-bucket", self.client, read_buffer_size=0)

        with bucket.get_object_stream("object.bin") as stream:
            with self.assertRaisesRegex(OSError, "changed while the stream was open") as raised:
                stream.read(10)

        self.assertIsInstance(raised.exception.__cause__, S3Error)

    @ai_blackbox()
    def test_other_range_errors_remain_s3_errors(self) -> None:
        self.client.get_error = self._make_s3_error("AccessDenied")
        bucket = MinioBucket("test-bucket", self.client, read_buffer_size=0)

        with bucket.get_object_stream("object.bin") as stream:
            with self.assertRaises(S3Error):
                stream.read(10)

    @ai_blackbox()
    def test_closed_stream_rejects_operations(self) -> None:
        bucket = MinioBucket("test-bucket", self.client, read_buffer_size=0)
        object_stream = bucket.get_object_stream("object.bin")

        with object_stream as stream:
            pass

        self.assertTrue(stream.closed)
        with self.assertRaises(ValueError):
            stream.read(1)
        with self.assertRaises(ValueError):
            stream.seek(0)
        with self.assertRaises(ValueError):
            stream.tell()

    @ai_blackbox()
    def test_read_buffer_size_must_be_a_non_negative_integer(self) -> None:
        for invalid_size in (-1, True, 1.5):
            with self.subTest(invalid_size=invalid_size):
                with self.assertRaisesRegex(ValueError, "read_buffer_size must be a non-negative int"):
                    MinioBucket("test-bucket", self.client, read_buffer_size=invalid_size)  # type: ignore[arg-type]
