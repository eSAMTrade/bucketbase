from unittest import TestCase

from bucketbase import MemoryBucket
from tests.bucket_tester import IBucketTester


class TestMemoryBucket(TestCase):
    def setUp(self):
        self.storage = MemoryBucket()
        self.tester = IBucketTester(self.storage, self)

    def test_put_and_get_object(self):
        self.tester.test_put_and_get_object()

    def test_put_and_get_object_stream(self):
        self.tester.test_put_and_get_object_stream()

    def test_list_objects(self):
        self.tester.test_list_objects()

    def test_shallow_list_objects(self):
        self.tester.test_shallow_list_objects()

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

    def test_open_write_consumer_throws(self):
        self.tester.test_open_write_consumer_throws()

    def test_open_write_feeder_throws(self):
        self.tester.test_open_write_feeder_throws()

    def test_open_write_with_parquet(self):
        self.tester.test_open_write_with_parquet()

    def test_streaming_failure_atomicity(self):
        self.tester.test_streaming_failure_atomicity()

    def test_put_object_stream_exception_cleanup(self):
        self.tester.test_put_object_stream_exception_cleanup()

    def test_open_write_partial_write_exception_cleanup(self):
        self.tester.test_open_write_partial_write_exception_cleanup()

    def test_open_write_without_proper_close(self):
        self.tester.test_open_write_without_proper_close()

    def test_open_write_sync_exception_cleanup(self):
        self.tester.test_open_write_sync_exception_cleanup()

    def test_regression_parquet_exception_thrown_in_prq_writer_by_AMX(self):
        self.tester.test_regression_exception_thrown_in_parquet_writer_context_doesnt_save_object()

    def test_regression_exception_thrown_in_arrow_sink_by_AMX(self):
        self.tester.test_regression_exception_thrown_in_arrow_sink_context_doesnt_save_object()

    def test_regression_exception_thrown_in_open_write_context_by_AMX(self):
        self.tester.test_regression_exception_thrown_in_open_write_context_doesnt_save_object()
