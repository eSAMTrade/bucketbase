import io
import uuid
from pathlib import PurePosixPath
from typing import Iterable, Iterator
from unittest import TestCase

from minio.datatypes import Object
from minio.deleteobjects import DeleteError, DeleteObject
from minio.error import S3Error
from minio.versioningconfig import ENABLED, VersioningConfig
from urllib3 import HTTPResponse

from bucketbase.minio_bucket import build_minio_client
from bucketbase.versioned_minio_bucket import ObjectVersion, VersionedMinioBucket
from tests.bucket_tester import VersionedIBucketTester
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


class TestVersionedMinioBucketMethods(TestCase):
    def setUp(self) -> None:
        self.client = FakeVersionedMinioClient()
        self.bucket = VersionedMinioBucket(bucket_name="test-bucket", minio_client=self.client)

    @staticmethod
    def _make_object(name: str, version_id: str | None, is_latest: object = "false", is_delete_marker: object = False) -> Object:
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

    def test_get_object_version_requires_string_version_id(self) -> None:
        with self.assertRaisesRegex(ValueError, "version_id must be str"):
            self.bucket.get_object_version("dir/file.txt", None)  # type: ignore[arg-type]

    def test_get_object_version_missing_version_raises_file_not_found(self) -> None:
        self.client.get_object_error = self._make_s3_error("NoSuchVersion")

        with self.assertRaises(FileNotFoundError):
            self.bucket.get_object_version("dir/file.txt", "missing")

    def test_get_object_version_delete_marker_raises_file_not_found(self) -> None:
        self.client.get_object_error = self._make_s3_error("MethodNotAllowed")

        with self.assertRaises(FileNotFoundError):
            self.bucket.get_object_version("dir/file.txt", "delete-marker-version")

    def test_remove_object_with_versions_deletes_listed_versions(self) -> None:
        self.client.list_objects_response = [
            self._make_object("dir/file.txt", "v2", is_latest=True),
            self._make_object("dir/file.txt", "v1", is_delete_marker=True),
        ]

        errors = self.bucket.remove_object_with_versions("dir/file.txt")

        self.assertEqual([], list(errors))
        self.assertEqual("test-bucket", self.client.remove_objects_calls[0][0])
        self.assertEqual(
            [("dir/file.txt", "v2"), ("dir/file.txt", "v1")],
            [(obj.name, obj.version_id) for obj in self.client.remove_objects_calls[0][1]],
        )

    def test_remove_object_with_versions_without_versions_does_not_delete(self) -> None:
        errors = self.bucket.remove_object_with_versions("dir/file.txt")

        self.assertEqual([], list(errors))
        self.assertEqual([], self.client.remove_objects_calls)


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
        if hasattr(self, "tester"):
            self.tester.cleanup()
        if hasattr(self, "minio_client") and hasattr(self, "bucket_name"):
            self._remove_all_bucket_versions()
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
