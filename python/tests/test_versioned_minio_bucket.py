import io
import uuid
from pathlib import PurePosixPath
from typing import Iterable, Iterator, Optional
from unittest import TestCase

from minio import Minio
from minio.datatypes import Object
from minio.deleteobjects import DeleteError, DeleteObject
from minio.error import S3Error
from minio.helpers import DictType
from minio.sse import SseCustomerKey
from minio.versioningconfig import ENABLED, VersioningConfig
from streamerate import slist
from typing_extensions import override
from urllib3 import HTTPResponse

from bucketbase.minio_bucket import build_minio_client
from bucketbase.versioned_minio_bucket import ObjectVersion, VersionedMinioBucket
from tests.config import CONFIG


class VersionedIBucketTester:
    def __init__(self, storage: VersionedMinioBucket, test_case: TestCase) -> None:
        self.storage = storage
        self.test_case = test_case
        self.us = uuid.uuid4().hex
        self._tracked_names: list[PurePosixPath] = []

    def cleanup(self) -> None:
        for name in self._tracked_names:
            self.storage.remove_object_with_versions(name)

    def _track(self, name: PurePosixPath) -> PurePosixPath:
        self._tracked_names.append(name)
        return name

    def test_full_cycle_object_versions_after_overwrite(self) -> None:
        path = self._track(PurePosixPath(f"dir{self.us}/versioned.txt"))

        self.storage.put_object(path, b"old content")
        self.storage.put_object(path, b"new content")

        versions = self.storage.list_object_versions(path)
        object_versions = [version for version in versions if not version.is_delete_marker]
        latest_versions = [version for version in object_versions if version.is_latest]
        old_versions = [version for version in object_versions if not version.is_latest]

        self.test_case.assertIsInstance(versions, slist)
        self.test_case.assertEqual(2, len(object_versions))
        self.test_case.assertEqual(1, len(latest_versions))
        self.test_case.assertEqual(1, len(old_versions))
        self.test_case.assertEqual(b"new content", self.storage.get_object(path))
        self.test_case.assertEqual(b"old content", self.storage.get_object_version(path, old_versions[0].version_id))
        with self.storage.get_object_version_stream(path, old_versions[0].version_id) as stream:
            self.test_case.assertEqual(b"old content", stream.read())

        errors = self.storage.remove_objects([path])
        versions_after_delete = self.storage.list_object_versions(path)
        delete_markers = [version for version in versions_after_delete if version.is_delete_marker]

        self.test_case.assertEqual([], list(errors))
        self.test_case.assertFalse(self.storage.exists(path))
        self.test_case.assertEqual(1, len(delete_markers))
        self.test_case.assertTrue(delete_markers[0].is_latest)
        self.test_case.assertEqual(b"old content", self.storage.get_object_version(path, old_versions[0].version_id))
        with self.test_case.assertRaises(FileNotFoundError):
            self.storage.get_object(path)
        with self.test_case.assertRaises(FileNotFoundError):
            self.storage.get_object_version(path, delete_markers[0].version_id)

        errors = self.storage.remove_object_with_versions(path)

        self.test_case.assertEqual([], list(errors))
        self.test_case.assertFalse(self.storage.exists(path))
        self.test_case.assertEqual([], list(self.storage.list_object_versions(path)))
        with self.test_case.assertRaises(FileNotFoundError):
            self.storage.get_object_version(path, old_versions[0].version_id)

    def test_remove_objects_for_missing_name_does_not_create_version_history(self) -> None:
        path = self._track(PurePosixPath(f"dir{self.us}/missing-versioned.txt"))

        errors = self.storage.remove_objects([path])

        self.test_case.assertEqual([], list(errors))
        self.test_case.assertEqual([], list(self.storage.list_object_versions(path)))

    def test_invalid_names_raise_for_version_methods(self) -> None:
        self.test_case.assertRaises(ValueError, self.storage.list_object_versions, "/")
        self.test_case.assertRaises(ValueError, self.storage.get_object_version, "/", "v1")
        self.test_case.assertRaises(ValueError, self.storage.get_object_version_stream, "/", "v1")
        self.test_case.assertRaises(ValueError, self.storage.remove_object_with_versions, "/")


class MockVersionedMinioClient(Minio):
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

    @override
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
        self.get_object_calls.append({"bucket_name": bucket_name, "object_name": object_name, "version_id": version_id})
        if self.get_object_error is not None:
            raise self.get_object_error
        return self.get_object_responses_by_version[version_id]

    @override
    def remove_objects(self, bucket_name: str, delete_object_list: Iterable[DeleteObject], bypass_governance_mode: bool = False) -> Iterator[DeleteError]:
        self.remove_objects_calls.append((bucket_name, list(delete_object_list)))
        return iter(self.remove_errors)


class TestVersionedMinioBucket(TestCase):
    def setUp(self) -> None:
        self.mock_client = MockVersionedMinioClient()
        self.bucket = VersionedMinioBucket(bucket_name="test-bucket", minio_client=self.mock_client)

    @staticmethod
    def _make_object(name: str, version_id: str | None, is_latest: str = "false", is_delete_marker: bool = False) -> Object:
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
        self.mock_client.list_objects_response = [
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
            self.mock_client.list_objects_calls[0],
        )

    def test_list_object_versions_requires_version_id(self) -> None:
        self.mock_client.list_objects_response = [self._make_object("dir/file.txt", None)]

        with self.assertRaisesRegex(ValueError, "has no version id"):
            self.bucket.list_object_versions("dir/file.txt")

    def test_get_object_version_reads_specific_version(self) -> None:
        self.mock_client.get_object_responses_by_version["v1"] = self._make_response(b"old content")

        content = self.bucket.get_object_version("dir/file.txt", "v1")

        self.assertEqual(b"old content", content)
        self.assertEqual([{"bucket_name": "test-bucket", "object_name": "dir/file.txt", "version_id": "v1"}], self.mock_client.get_object_calls)

    def test_get_object_version_requires_string_version_id(self) -> None:
        with self.assertRaisesRegex(ValueError, "version_id must be str"):
            self.bucket.get_object_version("dir/file.txt", None)  # type: ignore[arg-type]

    def test_get_object_version_missing_version_raises_file_not_found(self) -> None:
        self.mock_client.get_object_error = self._make_s3_error("NoSuchVersion")

        with self.assertRaises(FileNotFoundError):
            self.bucket.get_object_version("dir/file.txt", "missing")

    def test_get_object_version_delete_marker_raises_file_not_found(self) -> None:
        self.mock_client.get_object_error = self._make_s3_error("MethodNotAllowed")

        with self.assertRaises(FileNotFoundError):
            self.bucket.get_object_version("dir/file.txt", "delete-marker-version")

    def test_remove_object_with_versions_deletes_listed_versions(self) -> None:
        self.mock_client.list_objects_response = [
            self._make_object("dir/file.txt", "v2", is_latest=True),
            self._make_object("dir/file.txt", "v1", is_delete_marker=True),
        ]

        errors = self.bucket.remove_object_with_versions("dir/file.txt")

        self.assertEqual([], list(errors))
        self.assertEqual("test-bucket", self.mock_client.remove_objects_calls[0][0])
        self.assertEqual(
            [("dir/file.txt", "v2"), ("dir/file.txt", "v1")],
            [(obj.name, obj.version_id) for obj in self.mock_client.remove_objects_calls[0][1]],
        )

    def test_remove_object_with_versions_without_versions_does_not_delete(self) -> None:
        errors = self.bucket.remove_object_with_versions("dir/file.txt")

        self.assertEqual([], list(errors))
        self.assertEqual([], self.mock_client.remove_objects_calls)


class TestIntegratedVersionedMinioBucket(TestCase):
    def setUp(self) -> None:
        self.assertIsNotNone(CONFIG.MINIO_PUBLIC_SERVER, "MINIO_PUBLIC_SERVER not set")
        self.assertIsNotNone(CONFIG.MINIO_ACCESS_KEY, "MINIO_ACCESS_KEY not set")
        self.assertIsNotNone(CONFIG.MINIO_SECRET_KEY, "MINIO_SECRET_KEY not set")
        self.minio_client = build_minio_client(
            endpoints=CONFIG.MINIO_PUBLIC_SERVER,
            access_key=CONFIG.MINIO_ACCESS_KEY,
            secret_key=CONFIG.MINIO_SECRET_KEY,
            timeout=30,
        )
        self.bucket_name = self._make_bucket_name()
        self.minio_client.make_bucket(bucket_name=self.bucket_name)
        self.minio_client.set_bucket_versioning(self.bucket_name, VersioningConfig(ENABLED))
        self.bucket = VersionedMinioBucket(bucket_name=self.bucket_name, minio_client=self.minio_client)
        self.tester = VersionedIBucketTester(self.bucket, self)

    def tearDown(self) -> None:
        if not hasattr(self, "minio_client") or not hasattr(self, "bucket_name"):
            return

        try:
            self._remove_all_bucket_versions()
        finally:
            self.minio_client.remove_bucket(self.bucket_name)

    @staticmethod
    def _make_bucket_name() -> str:
        suffix = f"-versioning-{uuid.uuid4().hex[:12]}"
        prefix = CONFIG.MINIO_DEV_TESTS_BUCKET[: 63 - len(suffix)].rstrip(".-")
        return f"{prefix or 'bucketbase'}{suffix}"

    def _remove_all_bucket_versions(self) -> None:
        objects = list(self.minio_client.list_objects(self.bucket_name, recursive=True, include_version=True))
        if not objects:
            return

        delete_objects = [DeleteObject(VersionedMinioBucket._get_object_name(obj), obj.version_id) for obj in objects]
        errors = list(self.minio_client.remove_objects(self.bucket_name, delete_objects))
        self.assertEqual([], errors)

    def test_bucket_versioning_is_enabled(self) -> None:
        versioning_config = self.minio_client.get_bucket_versioning(self.bucket_name)

        self.assertEqual(ENABLED, versioning_config.status)

    def test_full_cycle_object_versions_after_overwrite(self) -> None:
        self.tester.test_full_cycle_object_versions_after_overwrite()

    def test_remove_objects_for_missing_name_does_not_create_version_history(self) -> None:
        self.tester.test_remove_objects_for_missing_name_does_not_create_version_history()

    def test_invalid_names_raise_for_version_methods(self) -> None:
        self.tester.test_invalid_names_raise_for_version_methods()
