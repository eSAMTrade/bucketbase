import io
from unittest import TestCase
from unittest.mock import patch

from minio.helpers import MAX_PART_SIZE, MIN_PART_SIZE

from bucketbase.minio_bucket import MinioBucket, build_minio_client
from tests.bucket_tester import IBucketTester
from tests.config import CONFIG


class TestIntegratedMinioBucket(TestCase):
    def setUp(self) -> None:
        self.assertIsNotNone(CONFIG.MINIO_PUBLIC_SERVER, "MINIO_PUBLIC_SERVER not set")
        self.assertIsNotNone(CONFIG.MINIO_ACCESS_KEY, "MINIO_ACCESS_KEY not set")
        self.assertIsNotNone(CONFIG.MINIO_SECRET_KEY, "MINIO_SECRET_KEY not set")
        self.minio_client = build_minio_client(endpoints=CONFIG.MINIO_PUBLIC_SERVER, access_key=CONFIG.MINIO_ACCESS_KEY, secret_key=CONFIG.MINIO_SECRET_KEY)
        self.bucket = MinioBucket(bucket_name=CONFIG.MINIO_DEV_TESTS_BUCKET, minio_client=self.minio_client)
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
