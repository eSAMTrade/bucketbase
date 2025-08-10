import io
import logging
import tempfile
import threading
import time
import traceback
import unittest
from io import BytesIO
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import BinaryIO, Iterator, Union
from unittest import TestCase
from unittest.mock import Mock

from bucketbase import AppendOnlyFSBucket, CachedImmutableBucket, IBucket, MemoryBucket
from bucketbase.ibucket import ObjectStream

loggerr = logging.getLogger(__name__)
logging.basicConfig(level=logging.ERROR, format="%(message)s")


class BlockingStream(BytesIO):
    """Stream that blocks on read until allowed to proceed"""

    def __init__(self, data: bytes, start_event: threading.Event, continue_event: threading.Event):
        super().__init__(data)
        self.start_event = start_event
        self.continue_event = continue_event
        self._read_called = False
        self._lock = threading.Lock()

    def read(self, size: int = -1) -> bytes:
        loggerr.error(f"BlockingStream.read called with size: {size}")
        with self._lock:
            loggerr.error("BlockingStream.read aquired lock")
            if not self._read_called:
                loggerr.error("BlockingStream.read first call")
                self._read_called = True
                self.start_event.set()  # Notify test we've started reading
                self.continue_event.wait()  # Block until test allows continuation
            else:
                loggerr.error("BlockingStream.read subsequent call")
            read_data = super().read(size)
            loggerr.error(f"BlockingStream.read returning {len(read_data)} bytes")
            return read_data


class MockMainBucket(IBucket):
    """Main bucket mock with call tracking and blocking streams"""

    class AtomicInteger(int):
        def __init__(self, value=0):
            self._value = value
            self._lock = threading.Lock()

        def __iadd__(self, other):
            with self._lock:
                self._value += other
            return self

        def __int__(self):
            with self._lock:
                return self._value

    def put_object(self, name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        raise NotImplementedError()

    def put_object_stream(self, name: PurePosixPath | str, stream: BinaryIO) -> None:
        raise NotImplementedError()

    def open_multipart_sink(self, name: PurePosixPath | str) -> Iterator[BinaryIO]:
        raise NotImplementedError()

    def __init__(self, content: bytes = b"test data"):
        self.get_object_stream_count = MockMainBucket.AtomicInteger(0)
        self.content = content
        self.start_event = threading.Event()
        self.continue_event = threading.Event()

    def get_object_stream(self, name: PurePosixPath) -> ObjectStream:
        loggerr.error(f"MockMainBucket.get_object_stream called from the thread: {threading.current_thread().name}")
        self.get_object_stream_count.__iadd__(1)
        blocking_stream = BlockingStream(self.content, self.start_event, self.continue_event)
        return ObjectStream(stream=blocking_stream, name=name)

    # Implement other required IBucket methods with default behavior
    get_object = Mock(side_effect=FileNotFoundError("Object not found in mock bucket"))
    list_objects = Mock(return_value=[], name="list_objects")
    shallow_list_objects = Mock(return_value=([], []), name="shallow_list_objects")
    exists = Mock(return_value=False, name="exists")
    remove_objects = Mock(return_value=[], name="remove_objects")
    get_size = Mock(side_effect=FileNotFoundError("Size not available for mock object"))


class TestCachedImmutableBucket(TestCase):
    def setUp(self):
        # Temporary directory for lock files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.locks_path = Path(self.temp_dir.name)

        # Setup the chain of buckets
        self.local_fs_cache_bucket = MemoryBucket()
        self.main_bucket = MemoryBucket()
        self.append_only_fs_bucket = AppendOnlyFSBucket(base=self.local_fs_cache_bucket, locks_path=self.locks_path)
        self.cached_bucket = CachedImmutableBucket(cache=self.append_only_fs_bucket, main=self.main_bucket)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_object_retrieval_populates_cache(self):
        test_object_name = "dir1/dir2/test_object"
        test_content = b"test content"

        # test assert raises FileNotFoundError when object is not found in the main bucket
        with self.assertRaises(FileNotFoundError):
            self.cached_bucket.get_object(test_object_name)

        # put the object in the main bucket
        self.main_bucket.put_object(test_object_name, test_content)

        # perform the actual test
        content = self.cached_bucket.get_object(test_object_name)

        # assert content and cache does contain the object
        self.assertEqual(content, test_content, "Content mismatch")
        self.assertTrue(self.local_fs_cache_bucket.exists(test_object_name), "Object not found in cache")

        # now remove the object from the main bucket
        self.main_bucket.remove_objects([test_object_name])

        # assert it does not exist in the main bucket
        self.assertFalse(self.main_bucket.exists(test_object_name), "Object found in main bucket")

        # assert it can be retrieved from the cached_bucket
        self.assertEqual(self.cached_bucket.get_object(test_object_name), test_content, "Content mismatch")

    def test_put_object_is_blocked(self):
        with self.assertRaises(io.UnsupportedOperation):
            self.cached_bucket.put_object("some_object", b"content")


class TestConcurrentCachedImmutableBucket(unittest.TestCase):
    """
    This test class is designed to verify the behavior of the CachedImmutableBucket when multiple threads attempt to access the same object concurrently.
    """

    TEST_DATA = b"test data"
    TEST_FILE_NAME = "test.txt"
    THREAD_TIMEOUT = 2.0
    THREAD_WAIT = 0.5

    def setUp(self):
        # Setup any necessary resources
        self.temp_dir = TemporaryDirectory()

    def tearDown(self):
        # Cleanup any temporary resources
        self.temp_dir.cleanup()

    def test_concurrent_stream_access(self):
        temp_dir_path = Path(self.temp_dir.name)
        print(temp_dir_path)
        main_bucket = MockMainBucket(self.TEST_DATA)
        fs_cache_bucket = AppendOnlyFSBucket.build(temp_dir_path)
        cached_bucket_in_test = CachedImmutableBucket(cache=fs_cache_bucket, main=main_bucket)

        results = []
        errors = []

        def get_stream_1():
            try:
                loggerr.error(f"get_stream_1 starting from thread: {threading.current_thread().name}")
                with cached_bucket_in_test.get_object_stream(self.TEST_FILE_NAME) as stream:
                    results.append(stream.read())
                loggerr.error("get_stream_1 done")
            except Exception as e:
                errors.append(e)
                loggerr.error(f"get_stream_1 error: {e}")

        def get_stream_2():
            try:
                loggerr.error(f"get_stream_2 starting from thread: {threading.current_thread().name}")
                with cached_bucket_in_test.get_object_stream(self.TEST_FILE_NAME) as stream:
                    results.append(stream.read())
                loggerr.error("get_stream_2 done")
            except Exception as e:
                errors.append(e)
                loggerr.error(f"get_stream_2 error: {e}")

        def get_stream_3():
            """This one is different from the other two, as it uses get_object instead of get_object_stream."""
            try:
                loggerr.error(f"get_stream_3 starting from thread: {threading.current_thread().name}")
                content = cached_bucket_in_test.get_object(self.TEST_FILE_NAME)
                results.append(content)
                loggerr.error("get_stream_3 done")
            except Exception as e:
                errors.append(e)
                loggerr.error(f"get_stream_3 error: {e}")
                traceback.print_exc()

        thread1 = threading.Thread(target=get_stream_1)
        thread2 = threading.Thread(target=get_stream_2)
        thread3 = threading.Thread(target=get_stream_3)

        thread1.start()

        # Wait for first thread to start reading from main bucket
        self.assertTrue(main_bucket.start_event.wait(timeout=self.THREAD_TIMEOUT), "Timeout waiting for first read to start")

        # Start second thread while first is blocked
        thread2.start()
        thread3.start()
        time.sleep(self.THREAD_WAIT)
        # Let the first thread complete its read
        main_bucket.continue_event.set()

        # Wait for completion
        thread1.join()
        thread2.join()
        thread3.join()

        # Verify results
        self.assertEqual(len(errors), 0, f"Unexpected errors: {errors}")
        self.assertListEqual(results, [self.TEST_DATA] * 3, "Both threads should get correct data")
        self.assertEqual(int(main_bucket.get_object_stream_count), 1, "Main bucket should only be accessed once")
        self.assertTrue(cached_bucket_in_test.exists(self.TEST_FILE_NAME), "Object should exist in cache after access")
