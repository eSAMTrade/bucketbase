from unittest import TestCase, skipUnless

from bucketbase.minio_bucket import MinioBucket, build_minio_client
from tests.bucket_tester import IBucketTester
from tests.config import CONFIG


@skipUnless(
    CONFIG.MINIO_PUBLIC_SERVER and CONFIG.MINIO_ACCESS_KEY and CONFIG.MINIO_SECRET_KEY,
    "Minio configuration not provided",
)
class TestIntegratedMinioBucket(TestCase):
    def setUp(self) -> None:
        minio_client = build_minio_client(endpoints=CONFIG.MINIO_PUBLIC_SERVER, access_key=CONFIG.MINIO_ACCESS_KEY, secret_key=CONFIG.MINIO_SECRET_KEY)
        self.bucket = MinioBucket(bucket_name=CONFIG.MINIO_DEV_TESTS_BUCKET, minio_client=minio_client)
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
