import atexit
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
    def get_multiprocessing_context(cls):
        return multiprocessing.get_context("spawn")

    @classmethod
    def get_shared_manager(cls):
        if cls._shared_manager is None:
            ctx = cls.get_multiprocessing_context()
            cls._shared_manager = ctx.Manager()
            # Ensure the manager is shut down when the process exits
            atexit.register(cls.shutdown_shared_manager)
        return cls._shared_manager

    @classmethod
    def shutdown_shared_manager(cls):
        if cls._shared_manager is not None:
            try:
                cls._shared_manager.shutdown()
            except (AttributeError, RuntimeError):
                pass
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

    def test_unpickle_after_manager_shutdown_raises_error(self):
        bucket = MemoryBucket.create_shared()
        bucket.put_object("abc", b"123")
        pickled_data = pickle.dumps(bucket)

        bucket._manager.shutdown()

        with self.assertRaises((EOFError, ConnectionError, BrokenPipeError, FileNotFoundError, Exception)):
            unpickled_bucket: MemoryBucket = pickle.loads(pickled_data)
            unpickled_bucket.get_object("abc")


class TestSharedMemoryBucketMultiprocessingCorrectness(TestCase, SharedManagerMixin):
    """Functional correctness tests for MemoryBucket with multiple processes."""

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

    def _helper_run_multiprocess_orchestration(self, worker_target, num_processes=3, args=None):
        bucket = MemoryBucket.create_shared(manager=self.get_shared_manager())
        ctx = self.get_multiprocessing_context()
        processes = []
        for i in range(num_processes):
            worker_args = (bucket, i) if args is None else (bucket,) + args
            p = ctx.Process(target=worker_target, args=worker_args)
            processes.append(p)

        for p in processes:
            p.start()
        for p in processes:
            p.join()
        return bucket

    def test_create_shared_without_manager_parameter(self):
        """Verify that create_shared() without manager parameter works."""
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

    @staticmethod
    def worker_increment(bucket, key, iters=50):
        for _ in range(iters):
            with bucket._lock:  # Using the shared lock from the manager
                try:
                    current = int(bucket.get_object(key).decode())
                except FileNotFoundError:
                    current = 0
                bucket.put_object(key, str(current + 1).encode())

    def test_concurrent_shared_lock_behavior(self):
        """Verify that multiple processes can coordinate via the shared bucket lock."""
        key = "counter"
        num_procs = 4
        iters = 50
        bucket = self._helper_run_multiprocess_orchestration(self.worker_increment, num_processes=num_procs, args=(key, iters))

        final_val = int(bucket.get_object(key).decode())
        self.assertEqual(final_val, num_procs * iters)
