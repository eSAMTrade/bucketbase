from unittest import TestCase

from bucketbase.minio_bucket import MinioBucket, build_minio_client
from tests.bucket_tester import IBucketTester
from tests.config import CONFIG


class TestIntegratedMinioBucket(TestCase):
    def setUp(self) -> None:
        minio_client = build_minio_client(endpoints=CONFIG.MINIO_PUBLIC_SERVER, access_key=CONFIG.MINIO_ACCESS_KEY, secret_key=CONFIG.MINIO_SECRET_KEY)
        self.bucket = MinioBucket(bucket_name=CONFIG.MINIO_DEV_TESTS_BUCKET, minio_client=minio_client)
        self.tester = IBucketTester(self.bucket, self)

    def tearDown(self) -> None:
        self.tester.cleanup()

    def test_put_and_get_object(self):
        self.tester.test_put_and_get_object()

    def test_put_and_get_object_stream(self):
        self.tester.test_put_and_get_object_stream()

    def test_list_objects(self):
        self.tester.test_list_objects()

    def test_list_objects_with_2025_keys(self):
        self.tester.test_list_objects_with_over1000keys()

    def test_shallow_list_objects(self):
        self.tester.test_shallow_list_objects()

    def test_shallow_list_objects_with_2025_keys(self):
        self.tester.test_shallow_list_objects_with_over1000keys()

    def test_exists(self):
        self.tester.test_exists()

    def test_remove_objects(self):
        self.tester.test_remove_objects()

    def test_get_size(self):
        self.tester.test_get_size()

    def test_open_write(self):
        self.tester.test_open_write()

    def test_open_write_timeout(self):
        self.tester.test_open_write_timeout()

    def test_open_write_with_parquet(self):
        self.tester.test_open_write_with_parquet()
