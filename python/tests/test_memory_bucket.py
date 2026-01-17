import multiprocessing
import pickle
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

    def test_regression_infinite_cycle_on_unentered_open_write_context(self):
        self.tester.test_regression_infinite_cycle_on_unentered_open_write_context()


class SharedManagerMixin:
    """Provides a single shared manager for all shared memory tests to speed up execution."""

    _shared_manager = None

    @classmethod
    def get_shared_manager(cls):
        if cls._shared_manager is None:
            ctx = multiprocessing.get_context("spawn")
            cls._shared_manager = ctx.Manager()
        return cls._shared_manager

    @classmethod
    def shutdown_shared_manager(cls):
        if cls._shared_manager is not None:
            cls._shared_manager.shutdown()
            cls._shared_manager = None


class TestSharedMemoryBucket(TestCase, SharedManagerMixin):
    """
    Runs ALL MemoryBucket tests using shared state to ensure full compatibility.
    """

    @classmethod
    def setUpClass(cls):
        cls.manager = cls.get_shared_manager()

    def setUp(self):
        self.storage = MemoryBucket.create_shared(manager=self.manager)
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

    def test_regression_infinite_cycle_on_unentered_open_write_context(self):
        self.tester.test_regression_infinite_cycle_on_unentered_open_write_context()


class TestMemoryBucketPickle(TestCase, SharedManagerMixin):
    def test_normal_memory_bucket_is_not_picklable(self):
        bucket = MemoryBucket()
        with self.assertRaises(TypeError) as cm:
            pickle.dumps(bucket)
        self.assertIn("RLock", str(cm.exception))

    def test_shared_memory_bucket_is_picklable(self):
        bucket = MemoryBucket.create_shared(manager=self.get_shared_manager())

        bucket.put_object("shared.txt", b"shared data")
        pickled_data = pickle.dumps(bucket)
        unpickled_bucket: MemoryBucket = pickle.loads(pickled_data)
        self.assertEqual(unpickled_bucket.get_object("shared.txt"), b"shared data")


class TestSharedMemoryBucketMultiprocessing(TestCase, SharedManagerMixin):
    """Stress tests for MemoryBucket with multiple processes."""

    @classmethod
    def tearDownClass(cls):
        cls.shutdown_shared_manager()

    @staticmethod
    def worker_put_object(bucket, index):
        bucket.put_object(f"obj_{index}.txt", f"content_{index}".encode())

    @staticmethod
    def worker_open_write(bucket, index):
        with bucket.open_write(f"stream_{index}.txt") as writer:
            writer.write(f"streamed_content_{index}".encode())

    def _helper_run_multiprocess_orchestration(self, worker_target, num_processes=3):
        bucket = MemoryBucket.create_shared(manager=self.get_shared_manager())
        ctx = multiprocessing.get_context("spawn")
        processes = [ctx.Process(target=worker_target, args=(bucket, i)) for i in range(num_processes)]
        for p in processes:
            p.start()
        for p in processes:
            p.join()
        return bucket

    def test_shared_memory_bucket_factory_creates_isolated_manager(self):
        """Verify that create_shared() without manager creates its own isolated manager."""
        bucket_factory = MemoryBucket.create_shared()
        bucket_factory.put_object("factory.txt", b"ok")
        self.assertEqual(bucket_factory.get_object("factory.txt"), b"ok")

    def test_multiprocess_put_object(self):
        bucket = self._helper_run_multiprocess_orchestration(self.worker_put_object)

        self.assertEqual(len(bucket.list_objects()), 3)
        for i in range(3):
            self.assertEqual(bucket.get_object(f"obj_{i}.txt"), f"content_{i}".encode())

    def test_multiprocess_open_write(self):
        bucket = self._helper_run_multiprocess_orchestration(self.worker_open_write)

        self.assertEqual(len(bucket.list_objects()), 3)
        for i in range(3):
            self.assertEqual(bucket.get_object(f"stream_{i}.txt"), f"streamed_content_{i}".encode())
