import io
import uuid
from pathlib import PurePosixPath
from typing import Iterable, Iterator
from unittest import TestCase
from unittest.mock import patch

from minio.commonconfig import ENABLED
from minio.datatypes import Object
from minio.deleteobjects import DeleteError, DeleteObject
from minio.error import S3Error
from minio.helpers import MAX_PART_SIZE, MIN_PART_SIZE
from minio.versioningconfig import VersioningConfig
from urllib3 import HTTPResponse

from bucketbase.ibucket import ObjectVersion
from bucketbase.minio_bucket import MinioBucket, build_minio_client
from tests.bucket_tester import IBucketTester
from tests.config import CONFIG


class FakeVersionedMinioClient:
    def __init__(self) -> None:
        self.list_objects_response: list[Object] = []
        self.get_object_responses_by_version: dict[str | None, HTTPResponse] = {}
        self.get_object_error: S3Error | None = None
        self.remove_errors: list[DeleteError] = []
        self.list_objects_calls: list[dict[str, object]] = []
        self.get_object_calls: list[dict[str, object]] = []
        self.remove_objects_calls: list[tuple[str, list[DeleteObject]]] = []

    def list_objects(self, **kwargs: object) -> Iterator[Object]:
        self.list_objects_calls.append(kwargs)
        return iter(self.list_objects_response)

    def get_object(self, bucket_name: str, object_name: str, version_id: str | None = None) -> HTTPResponse:
        self.get_object_calls.append({"bucket_name": bucket_name, "object_name": object_name, "version_id": version_id})
        if self.get_object_error is not None:
            raise self.get_object_error
        return self.get_object_responses_by_version[version_id]

    def remove_objects(self, bucket_name: str, delete_object_list: Iterable[DeleteObject], bypass_governance_mode: bool = False) -> Iterator[DeleteError]:
        self.remove_objects_calls.append((bucket_name, list(delete_object_list)))
        return iter(self.remove_errors)


class TestMinioBucketVersionMethods(TestCase):
    def setUp(self) -> None:
        self.client = FakeVersionedMinioClient()
        self.bucket = MinioBucket(bucket_name="test-bucket", minio_client=self.client)

    @staticmethod
    def _make_object(name: str, version_id: str | None, is_latest: object = "false", is_delete_marker: bool = False) -> Object:
        return Object(
            bucket_name="test-bucket",
            object_name=name,
            version_id=version_id,
            is_latest=is_latest,
            is_delete_marker=is_delete_marker,
        )

    @staticmethod
    def _make_response(content: bytes) -> HTTPResponse:
        return HTTPResponse(body=io.BytesIO(content), headers={"content-length": str(len(content))}, preload_content=False)

    @staticmethod
    def _make_s3_error(code: str) -> S3Error:
        return S3Error(HTTPResponse(status=404), code, code, "resource", "request-id", "host-id")

    def test_list_object_versions_filters_exact_name(self) -> None:
        self.client.list_objects_response = [
            self._make_object("dir/file.txt", "v2", is_latest="true"),
            self._make_object("dir/file.txt.bak", "other", is_latest="true"),
            self._make_object("dir/file.txt", "v1", is_delete_marker=True),
        ]

        versions = self.bucket.list_object_versions("dir/file.txt")

        self.assertEqual(
            [
                ObjectVersion(name=PurePosixPath("dir/file.txt"), version_id="v2", is_latest=True, is_delete_marker=False),
                ObjectVersion(name=PurePosixPath("dir/file.txt"), version_id="v1", is_latest=False, is_delete_marker=True),
            ],
            list(versions),
        )
        self.assertEqual(
            {"bucket_name": "test-bucket", "prefix": "dir/file.txt", "recursive": True, "include_version": True},
            self.client.list_objects_calls[0],
        )

    def test_list_object_versions_requires_version_id(self) -> None:
        self.client.list_objects_response = [self._make_object("dir/file.txt", None)]

        with self.assertRaisesRegex(ValueError, "has no version id"):
            self.bucket.list_object_versions("dir/file.txt")

    def test_get_object_version_reads_specific_version(self) -> None:
        self.client.get_object_responses_by_version["v1"] = self._make_response(b"old content")

        content = self.bucket.get_object_version("dir/file.txt", "v1")

        self.assertEqual(b"old content", content)
        self.assertEqual([{"bucket_name": "test-bucket", "object_name": "dir/file.txt", "version_id": "v1"}], self.client.get_object_calls)

    def test_get_object_version_missing_version_raises_file_not_found(self) -> None:
        self.client.get_object_error = self._make_s3_error("NoSuchVersion")

        with self.assertRaises(FileNotFoundError):
            self.bucket.get_object_version("dir/file.txt", "missing")

    def test_get_object_version_delete_marker_raises_file_not_found(self) -> None:
        self.client.get_object_error = self._make_s3_error("MethodNotAllowed")

        with self.assertRaises(FileNotFoundError):
            self.bucket.get_object_version("dir/file.txt", "delete-marker-version")

    def test_remove_object_all_versions_deletes_listed_versions(self) -> None:
        self.client.list_objects_response = [
            self._make_object("dir/file.txt", "v2", is_latest=True),
            self._make_object("dir/file.txt", "v1", is_delete_marker=True),
        ]

        errors = self.bucket.remove_object_all_versions("dir/file.txt")

        self.assertEqual([], list(errors))
        self.assertEqual("test-bucket", self.client.remove_objects_calls[0][0])
        self.assertEqual(
            [("dir/file.txt", "v2"), ("dir/file.txt", "v1")],
            [(obj.name, obj.version_id) for obj in self.client.remove_objects_calls[0][1]],
        )

    def test_remove_object_all_versions_without_versions_does_not_delete(self) -> None:
        errors = self.bucket.remove_object_all_versions("dir/file.txt")

        self.assertEqual([], list(errors))
        self.assertEqual([], self.client.remove_objects_calls)


class TestIntegratedMinioBucket(TestCase):
    def setUp(self) -> None:
        self.assertIsNotNone(CONFIG.MINIO_PUBLIC_SERVER, "MINIO_PUBLIC_SERVER not set")
        self.assertIsNotNone(CONFIG.MINIO_ACCESS_KEY, "MINIO_ACCESS_KEY not set")
        self.assertIsNotNone(CONFIG.MINIO_SECRET_KEY, "MINIO_SECRET_KEY not set")
        self.minio_client = build_minio_client(
            endpoints=CONFIG.MINIO_PUBLIC_SERVER, access_key=CONFIG.MINIO_ACCESS_KEY, secret_key=CONFIG.MINIO_SECRET_KEY, timeout=30
        )
        self.bucket = MinioBucket(bucket_name=CONFIG.MINIO_DEV_TESTS_BUCKET, minio_client=self.minio_client)
        if not self.minio_client.bucket_exists(CONFIG.MINIO_DEV_TESTS_BUCKET):
            self.minio_client.make_bucket(bucket_name=CONFIG.MINIO_DEV_TESTS_BUCKET)
        self.tester = IBucketTester(self.bucket, self)

    def tearDown(self) -> None:
        self.tester.cleanup()

    def test_put_and_get_object(self) -> None:
        self.tester.test_put_and_get_object()

    def test_put_and_get_object_stream(self) -> None:
        self.tester.test_put_and_get_object_stream()

    def test_list_objects(self) -> None:
        self.tester.test_list_objects()

    def test_list_objects_with_2025_keys(self) -> None:
        self.tester.test_list_objects_with_over1000keys()

    def test_shallow_list_objects(self) -> None:
        self.tester.test_shallow_list_objects()

    def test_shallow_list_objects_with_2025_keys(self) -> None:
        self.tester.test_shallow_list_objects_with_over1000keys()

    def test_exists(self) -> None:
        self.tester.test_exists()

    def test_remove_objects(self) -> None:
        self.tester.test_remove_objects()

    def test_get_size(self) -> None:
        self.tester.test_get_size()

    def test_open_write(self) -> None:
        self.tester.test_open_write()

    def test_open_write_timeout(self) -> None:
        self.tester.test_open_write_timeout()

    def test_open_write_consumer_throws(self) -> None:
        self.tester.test_open_write_consumer_throws()

    def test_open_write_feeder_throws(self) -> None:
        self.tester.test_open_write_feeder_throws()

    def test_open_write_with_parquet(self) -> None:
        self.tester.test_open_write_with_parquet()

    def test_streaming_failure_atomicity(self) -> None:
        self.tester.test_streaming_failure_atomicity()

    def test_put_object_stream_exception_cleanup(self) -> None:
        self.tester.test_put_object_stream_exception_cleanup()

    def test_open_write_partial_write_exception_cleanup(self) -> None:
        self.tester.test_open_write_partial_write_exception_cleanup()

    def test_open_write_without_proper_close(self) -> None:
        self.tester.test_open_write_without_proper_close()

    def _test_part_size_used(self, bucket: MinioBucket, expected_part_size: int) -> None:
        with patch.object(bucket._minio_client, "put_object") as mock_put:
            stream = io.BytesIO(b"test data")
            bucket.put_object_stream("test.txt", stream)
            call_kwargs = mock_put.call_args.kwargs
            self.assertEqual(call_kwargs["part_size"], expected_part_size)

    def test_default_part_size_used_in_put_object_stream(self) -> None:
        self._test_part_size_used(self.bucket, MinioBucket.DEFAULT_PART_SIZE)

    def test_custom_part_size_used_in_put_object_stream(self) -> None:
        custom_part_size = 10 * 1024 * 1024
        bucket = MinioBucket(CONFIG.MINIO_DEV_TESTS_BUCKET, self.minio_client, part_size=custom_part_size)
        self._test_part_size_used(bucket, custom_part_size)

    def test_part_size_out_of_boundaries(self) -> None:
        regex = f"part_size must be between {MIN_PART_SIZE} and {MAX_PART_SIZE}"

        with self.assertRaisesRegex(ValueError, regex):
            MinioBucket(CONFIG.MINIO_DEV_TESTS_BUCKET, self.minio_client, part_size=MIN_PART_SIZE - 1)

        with self.assertRaisesRegex(ValueError, regex):
            MinioBucket(CONFIG.MINIO_DEV_TESTS_BUCKET, self.minio_client, part_size=MAX_PART_SIZE + 1)

    def test_regression_parquet_exception_thrown_in_prq_writer_by_AMX(self) -> None:
        self.tester.test_regression_exception_thrown_in_parquet_writer_context_doesnt_save_object()

    def test_regression_exception_thrown_in_arrow_sink_by_AMX(self) -> None:
        self.tester.test_regression_exception_thrown_in_arrow_sink_context_doesnt_save_object()

    def test_regression_exception_thrown_in_open_write_context_by_AMX(self) -> None:
        self.tester.test_regression_exception_thrown_in_open_write_context_doesnt_save_object()

    def test_regression_infinite_cycle_on_unentered_open_write_context(self):
        self.tester.test_regression_infinite_cycle_on_unentered_open_write_context()


class TestIntegratedMinioBucketVersioning(TestCase):
    def setUp(self) -> None:
        self.assertIsNotNone(CONFIG.MINIO_PUBLIC_SERVER, "MINIO_PUBLIC_SERVER not set")
        self.assertIsNotNone(CONFIG.MINIO_ACCESS_KEY, "MINIO_ACCESS_KEY not set")
        self.assertIsNotNone(CONFIG.MINIO_SECRET_KEY, "MINIO_SECRET_KEY not set")
        self.minio_client = build_minio_client(
            endpoints=CONFIG.MINIO_PUBLIC_SERVER, access_key=CONFIG.MINIO_ACCESS_KEY, secret_key=CONFIG.MINIO_SECRET_KEY, timeout=30
        )
        self.bucket_name = self._make_locked_bucket_name()
        self.minio_client.make_bucket(bucket_name=self.bucket_name, object_lock=True)
        self.bucket = MinioBucket(bucket_name=self.bucket_name, minio_client=self.minio_client)
        self._ensure_bucket_versioning_enabled()
        self.tester = IBucketTester(self.bucket, self)

    def tearDown(self) -> None:
        self._remove_all_bucket_versions()
        self.minio_client.remove_bucket(self.bucket_name)

    @staticmethod
    def _make_locked_bucket_name() -> str:
        suffix = f"-versioning-{uuid.uuid4().hex[:12]}"
        prefix = CONFIG.MINIO_DEV_TESTS_BUCKET[: 63 - len(suffix)].rstrip(".-")
        return f"{prefix or 'bucketbase'}{suffix}"

    def _remove_all_bucket_versions(self) -> None:
        objects = list(self.minio_client.list_objects(self.bucket_name, recursive=True, include_version=True))
        if not objects:
            return

        delete_objects = [DeleteObject(MinioBucket._get_object_name(obj), obj.version_id) for obj in objects]
        errors = list(self.minio_client.remove_objects(self.bucket_name, delete_objects))
        self.assertEqual([], errors)

    def _ensure_bucket_versioning_enabled(self) -> None:
        versioning_config = self.minio_client.get_bucket_versioning(self.bucket_name)
        if versioning_config.status != ENABLED:
            self.minio_client.set_bucket_versioning(self.bucket_name, VersioningConfig(ENABLED))

    def _put_two_object_versions(self, path: PurePosixPath) -> tuple[str, str]:
        self.bucket.put_object(path, b"old content")
        self.bucket.put_object(path, b"new content")

        versions = list(self.bucket.list_object_versions(path))
        latest_versions = [version for version in versions if version.is_latest and not version.is_delete_marker]
        old_versions = [version for version in versions if not version.is_latest and not version.is_delete_marker]

        self.assertEqual(2, len(versions))
        self.assertEqual(1, len(latest_versions))
        self.assertEqual(1, len(old_versions))
        return old_versions[0].version_id, latest_versions[0].version_id

    def test_list_object_versions(self) -> None:
        path = PurePosixPath(f"dir{self.tester.us}/integrated-version-list.txt")

        old_version_id, latest_version_id = self._put_two_object_versions(path)
        versions = list(self.bucket.list_object_versions(path))

        self.assertEqual(2, len(versions))
        self.assertEqual({old_version_id, latest_version_id}, {version.version_id for version in versions})
        self.assertTrue(all(version.name == path for version in versions))
        self.assertEqual(1, len([version for version in versions if version.is_latest]))
        self.assertFalse(any(version.is_delete_marker for version in versions))

    def test_get_object_version(self) -> None:
        path = PurePosixPath(f"dir{self.tester.us}/integrated-version-read.txt")

        old_version_id, latest_version_id = self._put_two_object_versions(path)

        self.assertEqual(b"new content", self.bucket.get_object(path))
        self.assertEqual(b"old content", self.bucket.get_object_version(path, old_version_id))
        self.assertEqual(b"new content", self.bucket.get_object_version(path, latest_version_id))

    def test_remove_object_all_versions(self) -> None:
        path = PurePosixPath(f"dir{self.tester.us}/integrated-version-delete.txt")

        self._put_two_object_versions(path)
        self.bucket.remove_objects([path])
        versions_before_delete = list(self.bucket.list_object_versions(path))

        errors = self.bucket.remove_object_all_versions(path)

        self.assertEqual([], list(errors))
        self.assertTrue(any(version.is_delete_marker for version in versions_before_delete))
        self.assertFalse(self.bucket.exists(path))
        self.assertEqual([], list(self.bucket.list_object_versions(path)))
