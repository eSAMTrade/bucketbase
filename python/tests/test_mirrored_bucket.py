import unittest
import tempfile
from bucketbase.fs_bucket import FSBucket
from bucketbase.memory_bucket import MemoryBucket
import threading
import queue
from bucketbase.mirrored_bucket import ReadQueue
from bucketbase.mirrored_bucket import ParallelStrategy, MirrorOperationError, TempFileStrategy, SuccessCriteria, \
    MirrorBucket, InMemoryStrategy, StreamingStrategy
from queue import Queue
import io
from pathlib import Path


class TestMirrorBucketInitialization(unittest.TestCase):
    def setUp(self):
        self.temp_dirs = [tempfile.TemporaryDirectory() for _ in range(3)]
        self.fs_buckets = [FSBucket(Path(temp_dir.name)) for temp_dir in self.temp_dirs]

    def tearDown(self):
        for temp_dir in self.temp_dirs:
            temp_dir.cleanup()

    def test_default_initialization(self):
        mirror = MirrorBucket(self.fs_buckets)
        self.assertEqual(mirror._read_bucket, self.fs_buckets[0])
        self.assertIsInstance(mirror._strategy, InMemoryStrategy)
        self.assertEqual(mirror._success_criteria, SuccessCriteria.ALL)

    def test_custom_initialization(self):
        mirror = MirrorBucket(self.fs_buckets, StreamingStrategy(), SuccessCriteria.ANY)
        self.assertIsInstance(mirror._strategy, StreamingStrategy)
        self.assertEqual(mirror._success_criteria, SuccessCriteria.ANY)

    def test_empty_buckets_list(self):
        with self.assertRaises(ValueError):
            MirrorBucket([])


class TestMirrorBucketStrategies(unittest.TestCase):
    def setUp(self):
        self.temp_dirs = [tempfile.TemporaryDirectory() for _ in range(3)]
        self.fs_buckets = [FSBucket(Path(temp_dir.name)) for temp_dir in self.temp_dirs]
        self.memory_buckets = [MemoryBucket() for _ in range(3)]
        self.test_content = b"sample content"

    def tearDown(self):
        for temp_dir in self.temp_dirs:
            temp_dir.cleanup()

    def _verify_buckets_content(self, buckets, name, content):
        for bucket in buckets:
            self.assertEqual(bucket.get_object(name), content)

    def test_inmemory_strategy_stream(self):
        name = "inmemory_test.txt"
        mirror = MirrorBucket(self.fs_buckets, InMemoryStrategy())
        stream = io.BytesIO(self.test_content)
        mirror.put_object_stream(name, stream)
        self._verify_buckets_content(self.fs_buckets, name, self.test_content)

    def test_tempfile_strategy_large_file(self):
        name = "tempfile_large.bin"
        mirror = MirrorBucket(self.fs_buckets, TempFileStrategy())
        large_content = b"A" * (1024 * 1024)  # 1MB
        stream = io.BytesIO(large_content)
        mirror.put_object_stream(name, stream)
        self._verify_buckets_content(self.fs_buckets, name, large_content)

    def test_streaming_strategy(self):
        name = "streaming_test.txt"
        all_buckets = self.fs_buckets + self.memory_buckets
        mirror = MirrorBucket(all_buckets, StreamingStrategy())
        large_content = b"B" * 500000
        stream = io.BytesIO(large_content)
        mirror.put_object_stream(name, stream)
        self._verify_buckets_content(self.fs_buckets, name, large_content)
        self._verify_buckets_content(self.memory_buckets, name, large_content)

    def test_parallel_strategy(self):
        name = "parallel_test.txt"
        mirror = MirrorBucket(self.fs_buckets, ParallelStrategy())
        stream = io.BytesIO(self.test_content)
        mirror.put_object_stream(name, stream)
        self._verify_buckets_content(self.fs_buckets, name, self.test_content)

    def test_put_object_method(self):
        name = "object_test.txt"
        mirror = MirrorBucket(self.fs_buckets, InMemoryStrategy())
        mirror.put_object(name, self.test_content)
        self._verify_buckets_content(self.fs_buckets, name, self.test_content)


class MockFailingBucket(MemoryBucket):
    def put_object(self, name, content):
        raise RuntimeError("Simulated failure")

    def put_object_stream(self, name, stream):
        raise RuntimeError("Simulated failure")


class TestSuccessCriteria(unittest.TestCase):
    def setUp(self):
        self.temp_dirs = [tempfile.TemporaryDirectory() for _ in range(2)]
        self.fs_buckets = [FSBucket(Path(temp_dir.name)) for temp_dir in self.temp_dirs]
        self.test_content = b"criteria test"

    def tearDown(self):
        for temp_dir in self.temp_dirs:
            temp_dir.cleanup()

    def test_all_success(self):
        name = "all_success.txt"
        mirror = MirrorBucket(self.fs_buckets, StreamingStrategy(), SuccessCriteria.ALL)
        stream = io.BytesIO(self.test_content)
        mirror.put_object_stream(name, stream)
        for bucket in self.fs_buckets:
            self.assertEqual(bucket.get_object(name), self.test_content)

    def test_all_failure(self):
        name = "all_failure.txt"
        failing_bucket = MockFailingBucket()
        buckets = [self.fs_buckets[0], failing_bucket, self.fs_buckets[1]]
        mirror = MirrorBucket(buckets, StreamingStrategy(), SuccessCriteria.ALL)
        stream = io.BytesIO(self.test_content)
        with self.assertRaises(RuntimeError):
            mirror.put_object_stream(name, stream)
        self.assertEqual(self.fs_buckets[0].get_object(name), self.test_content)
        self.assertEqual(self.fs_buckets[1].get_object(name), self.test_content)

    def test_any_success(self):
        name = "any_success.txt"
        failing_buckets = [MockFailingBucket() for _ in range(2)]
        buckets = [self.fs_buckets[0]] + failing_buckets
        mirror = MirrorBucket(buckets, StreamingStrategy(), SuccessCriteria.ANY)
        stream = io.BytesIO(self.test_content)
        mirror.put_object_stream(name, stream)
        self.assertEqual(self.fs_buckets[0].get_object(name), self.test_content)

    def test_any_failure(self):
        name = "any_failure.txt"
        failing_buckets = [MockFailingBucket() for _ in range(2)]
        buckets = failing_buckets
        mirror = MirrorBucket(buckets, StreamingStrategy(), SuccessCriteria.ANY)
        stream = io.BytesIO(self.test_content)
        with self.assertRaises(RuntimeError):
            mirror.put_object_stream(name, stream)

    def test_majority_success(self):
        name = "majority_success.txt"
        failing_bucket = MockFailingBucket()
        buckets = [self.fs_buckets[0], self.fs_buckets[1], failing_bucket]
        mirror = MirrorBucket(buckets, StreamingStrategy(), SuccessCriteria.MAJORITY)
        stream = io.BytesIO(self.test_content)
        mirror.put_object_stream(name, stream)
        self.assertEqual(self.fs_buckets[0].get_object(name), self.test_content)
        self.assertEqual(self.fs_buckets[1].get_object(name), self.test_content)

    def test_majority_failure(self):
        name = "majority_failure.txt"
        failing_buckets = [MockFailingBucket() for _ in range(2)]
        buckets = [self.fs_buckets[0]] + failing_buckets
        mirror = MirrorBucket(buckets, StreamingStrategy(), SuccessCriteria.MAJORITY)
        stream = io.BytesIO(self.test_content)
        with self.assertRaises(RuntimeError):
            mirror.put_object_stream(name, stream)
        self.assertEqual(self.fs_buckets[0].get_object(name), self.test_content)


class TestReadQueue(unittest.TestCase):
    def setUp(self):
        self.data_queue = queue.Queue()
        self.read_queue = ReadQueue(self.data_queue)

    def test_basic_read(self):
        self.data_queue.put(b"hello")
        self.data_queue.put(None)
        result = self.read_queue.read()
        self.assertEqual(result, b"hello")

    def test_sized_read(self):
        self.data_queue.put(b"hello world")
        self.data_queue.put(None)
        result = self.read_queue.read(5)
        self.assertEqual(result, b"hello")
        result = self.read_queue.read(6)
        self.assertEqual(result, b" world")

    def test_multiple_chunks(self):
        self.data_queue.put(b"chunk1")
        self.data_queue.put(b"chunk2")
        self.data_queue.put(b"chunk3")
        self.data_queue.put(None)
        result = self.read_queue.read()
        self.assertEqual(result, b"chunk1chunk2chunk3")

    def test_read_after_close(self):
        self.data_queue.put(b"test")
        self.read_queue.close()
        result = self.read_queue.read()
        self.assertEqual(result, b"")

    def test_context_manager(self):
        self.data_queue.put(b"context")
        self.data_queue.put(None)
        with self.read_queue as rq:
            result = rq.read()
            self.assertEqual(result, b"context")
        self.assertTrue(self.read_queue.closed)

    def test_threaded_reading(self):
        results = []

        def producer():
            for i in range(10):
                self.data_queue.put(f"chunk{i}".encode())
            self.data_queue.put(None)

        def consumer():
            while True:
                chunk = self.read_queue.read(5)
                if not chunk:
                    break
                results.append(chunk)

        producer_thread = threading.Thread(target=producer)
        consumer_thread = threading.Thread(target=consumer)
        producer_thread.start()
        consumer_thread.start()
        producer_thread.join(timeout=2)
        consumer_thread.join(timeout=2)
        full_content = b"".join(results)
        expected = b"".join(f"chunk{i}".encode() for i in range(10))
        self.assertEqual(full_content, expected)


class FailingBucket:
    def put_object(self, name, content):
        raise Exception("operation failed")


class TestBaseMirrorStrategyExecuteOnBucketsException(unittest.TestCase):
    def test_execute_on_buckets_all_fail(self):
        strategy = InMemoryStrategy()
        buckets = [FailingBucket(), FailingBucket()]

        def operation(bucket):
            bucket.put_object("test.txt", b"content")

        with self.assertRaises(MirrorOperationError) as cm:
            strategy.execute_on_buckets(buckets, operation, SuccessCriteria.ANY)
        self.assertIn("Failed to write to any bucket", str(cm.exception))


class FailingBucket:
    def put_object(self, name, content):
        raise Exception("parallel failure")

    # Provide a dummy implementation for put_object_stream to satisfy interface
    def put_object_stream(self, name, stream):
        raise Exception("parallel failure")


class TestParallelStrategyMirrorStreamException(unittest.TestCase):
    def test_parallel_strategy_failure(self):
        strategy = ParallelStrategy()
        stream = io.BytesIO(b"data")
        buckets = [FailingBucket(), FailingBucket()]
        with self.assertRaises(MirrorOperationError) as cm:
            strategy.mirror_stream("test.txt", stream, buckets, SuccessCriteria.ALL)
        self.assertIn("Failed to write", str(cm.exception))


# File: `python/tests/test_reader_thread_exception.py`


class FaultyStream:
    def read(self, size=-1):
        raise Exception("Reader error")


class TestReaderThreadException(unittest.TestCase):
    def test_reader_thread_exception(self):
        strategy = StreamingStrategy()
        errors = []
        data_queue = Queue()
        end_marker = object()
        faulty_stream = FaultyStream()
        reader_thread = strategy._create_reader_thread(faulty_stream, data_queue, end_marker, errors)
        reader_thread.join(timeout=2)
        self.assertTrue(any(tag == "reader" and "Reader error" in str(err) for tag, err in errors))


class FaultyQueue(Queue):
    def get(self, timeout=None):
        raise Exception("Distributor error")


class TestDistributorThreadException(unittest.TestCase):
    def test_distributor_thread_exception(self):
        strategy = StreamingStrategy()
        errors = []
        faulty_queue = FaultyQueue()
        bucket_queues = [Queue(), Queue()]
        end_marker = object()
        distributor_thread = strategy._create_distributor_thread(faulty_queue, bucket_queues, end_marker, errors)
        distributor_thread.join(timeout=2)
        self.assertTrue(any(tag == "distributor" and "Distributor error" in str(err) for tag, err in errors))


# Minimal fake implementation of IBucket for testing purposes
class FakeBucket:
    def __init__(self):
        self.objects = {}

    def put_object(self, name, content):
        self.objects[name] = content

    def put_object_stream(self, name, stream):
        self.objects[name] = stream.read()

    def get_object(self, name):
        return self.objects.get(name, b"")

    def get_object_stream(self, name):
        from io import BytesIO

        return BytesIO(self.objects.get(name, b""))

    def exists(self, name):
        return name in self.objects

    def get_size(self, name):
        return len(self.objects.get(name, b""))

    def list_objects(self, prefix=""):
        return [name for name in self.objects if name.startswith(prefix)]

    def shallow_list_objects(self, prefix="", delimiter="/"):
        return [name for name in self.objects if name.startswith(prefix)]

    def remove_objects(self, names):
        for name in names:
            if name in self.objects:
                del self.objects[name]


# Update _validate_name to strip both whitespace and leading '/'
def dummy_validate(name):
    return name.strip().lstrip("/")


MirrorBucket._validate_name = lambda self, n: dummy_validate(n)


class TestMirrorBucketCoverage(unittest.TestCase):
    def setUp(self):
        self.bucket = FakeBucket()
        self.mirror = MirrorBucket([self.bucket], InMemoryStrategy(), SuccessCriteria.ALL)

    def test_put_and_get_object(self):
        self.mirror.put_object(" /test.txt ", b"hello")
        self.assertEqual(self.mirror.get_object("test.txt"), b"hello")

    def test_put_object_stream(self):
        stream = io.BytesIO(b"stream data")
        self.mirror.put_object_stream(" /stream.txt ", stream)
        self.assertEqual(self.mirror.get_object("stream.txt"), b"stream data")

    def test_exists_and_get_size(self):
        self.mirror.put_object(" /exists.txt ", b"data")
        self.assertTrue(self.mirror.exists("exists.txt"))
        self.assertEqual(self.mirror.get_size("exists.txt"), 4)

    def test_list_objects(self):
        self.mirror.put_object(" /folder/file1.txt ", b"1")
        self.mirror.put_object(" /folder/file2.txt ", b"2")
        lst = self.mirror.list_objects("folder")
        self.assertTrue(all(name.startswith("folder") for name in lst))


if __name__ == "__main__":
    import unittest

    unittest.main()
