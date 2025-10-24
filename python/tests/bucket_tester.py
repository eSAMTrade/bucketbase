import csv
import gzip
import io
import tempfile
import threading
import time
from io import BytesIO
from pathlib import Path, PurePosixPath
from queue import Queue
from typing import BinaryIO
from unittest import TestCase

import pyarrow as pa
import pyarrow.parquet as pq
from streamerate import slist
from streamerate import stream as sstream
from tsx import iTSms

from bucketbase.ibucket import AsyncObjectWriter, IBucket


class MockException(Exception):
    pass


class FailingStream(io.IOBase):
    """A stream that fails after reading/writing a certain amount of data"""

    def __init__(self, data: bytes, fail_after_bytes: int, fail_on_read: bool):
        super().__init__()
        self.data = data
        self.fail_after_bytes = fail_after_bytes
        self.fail_on_read = fail_on_read
        self.bytes_processed = 0
        self._is_closed = False

    def read(self, size: int = -1) -> bytes:
        if self._is_closed:
            raise ValueError("I/O operation on closed stream")

        if self.fail_on_read and self.bytes_processed >= self.fail_after_bytes:
            raise MockException("Simulated stream read failure")

        if size == -1:
            remaining = self.data[self.bytes_processed :]
            self.bytes_processed = len(self.data)
        else:
            end_pos = min(self.bytes_processed + size, len(self.data))
            remaining = self.data[self.bytes_processed : end_pos]
            self.bytes_processed = end_pos

        if self.fail_on_read and self.bytes_processed > self.fail_after_bytes:
            raise MockException("Simulated stream read failure")

        return remaining

    def write(self, data: bytes) -> int:
        if self._is_closed:
            raise ValueError("I/O operation on closed stream")

        if not self.fail_on_read and self.bytes_processed >= self.fail_after_bytes:
            raise MockException("Simulated stream write failure")

        self.bytes_processed += len(data)
        return len(data)

    def close(self):
        self._is_closed = True

    @property
    def closed(self):
        return self._is_closed

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return True


class IBucketTester:  # pylint: disable=too-many-public-methods
    INVALID_PREFIXES = ["/", "/dir", "dir1//dir2", "dir1//", "star*1", "dir1/a\file.txt", "at@gmail", "sharp#1", "dollar$1", "comma,"]
    PATH_WITH_2025_KEYS = "test-dir-with-2025-keys/"

    def __init__(self, storage: IBucket, test_case: TestCase) -> None:
        self.storage = storage
        self.test_case = test_case
        # Next is a unique suffix to be used in the names of dirs and files, so they will be unique
        self.us = f"{iTSms.now() % 100_000_000:08d}"

    def cleanup(self) -> None:
        self.storage.remove_prefix(f"dir{self.us}")

    def test_put_and_get_object(self) -> None:
        unique_dir = f"dir{self.us}"
        # binary content
        path = PurePosixPath(f"{unique_dir}/file1.bin")
        b_content = b"Test content"
        self.storage.put_object(path, b_content)
        retrieved_content = self.storage.get_object(path)
        self.test_case.assertEqual(retrieved_content, b_content)

        # str content
        path = PurePosixPath(f"{unique_dir}/file1.txt")
        s_content = "Test content"
        self.storage.put_object(path, s_content)
        retrieved_content = self.storage.get_object(path)
        self.test_case.assertEqual(retrieved_content, bytes(s_content, "utf-8"))

        # bytearray content
        path = PurePosixPath(f"{unique_dir}/file1.ba")
        ba_content = bytearray(b"Test content")
        self.storage.put_object(path, ba_content)
        retrieved_content = self.storage.get_object(path)
        self.test_case.assertEqual(retrieved_content, b_content)

        # string path
        path = f"{unique_dir}/file1.txt"
        s_content = "Test content"
        self.storage.put_object(path, s_content)
        retrieved_content = self.storage.get_object(path)
        self.test_case.assertEqual(retrieved_content, bytes(s_content, "utf-8"))

        # inexistent path
        path = f"{unique_dir}/inexistent.txt"
        self.test_case.assertRaises(FileNotFoundError, self.storage.get_object, path)

    def validated_put_object_stream(self, name: PurePosixPath | str, stream: BinaryIO) -> None:
        assert isinstance(stream, io.IOBase), f"stream must be a BinaryIO, but got {type(stream)}"
        return self.storage.put_object_stream(name, stream)  # type: ignore[arg-type]

    def test_put_and_get_object_stream(self) -> None:
        unique_dir = f"dir{self.us}"
        # binary content
        path = PurePosixPath(f"{unique_dir}/file1.bin")
        b_content = b"Test\ncontent"
        b_gzipped_content = gzip.compress(b_content)
        gzipped_stream = BytesIO(b_gzipped_content)

        self.validated_put_object_stream(path, gzipped_stream)
        with self.storage.get_object_stream(path) as file:
            with gzip.open(file, "rt") as file:
                result = [file.readline() for _ in range(3)]
        self.test_case.assertEqual(result, ["Test\n", "content", ""])

        # string path
        path = f"{unique_dir}/file1.bin"
        retrieved_content = self.storage.get_object_stream(path)
        with retrieved_content as file:
            with gzip.open(file, "rt") as file:
                result = file.read()
        self.test_case.assertEqual(result, "Test\ncontent")

    def test_streaming_failure_atomicity(self) -> None:
        """
        Test that failed streaming operations don't leave partial objects in the bucket.
        This ensures atomicity - either the object is completely written or it doesn't exist.
        """
        unique_dir = f"dir{self.us}"

        # Test 1: put_object_stream with failing read stream
        path1 = PurePosixPath(f"{unique_dir}/failed_stream_read.bin")
        test_content = b"This is test content that should not be stored if stream fails"
        failing_stream = FailingStream(test_content, fail_after_bytes=10, fail_on_read=True)

        # Ensure object doesn't exist before test
        self.test_case.assertFalse(self.storage.exists(path1))

        # Attempt to put object with failing stream - should raise exception
        with self.test_case.assertRaises(MockException):
            self.storage.put_object_stream(path1, failing_stream)  # type: ignore[arg-type]

        # Object should NOT exist after failed operation
        self.test_case.assertFalse(self.storage.exists(path1), "Object should not exist after failed put_object_stream")

        # Test 2: open_write with failing write operation
        path2 = PurePosixPath(f"{unique_dir}/failed_open_write.bin")

        # Ensure object doesn't exist before test
        self.test_case.assertFalse(self.storage.exists(path2))

        # Attempt to write with failure during write operation
        try:
            with self.storage.open_write(path2, timeout_sec=1) as writer:
                writer.write(b"Some initial data")
                # Simulate failure during write operation
                raise MockException("Simulated write failure")
        except MockException:
            pass  # Expected exception

        # Object should NOT exist after failed operation
        self.test_case.assertFalse(self.storage.exists(path2), "Object should not exist after failed open_write")

        # Test 3: fput_object with non-existent file (should fail gracefully)
        path3 = PurePosixPath(f"{unique_dir}/failed_fput.bin")
        non_existent_file = Path("/non/existent/file.txt")

        # Ensure object doesn't exist before test
        self.test_case.assertFalse(self.storage.exists(path3))

        # Attempt to fput non-existent file - should raise exception
        with self.test_case.assertRaises((FileNotFoundError, IOError)):  # type: ignore[misc]
            self.storage.fput_object(path3, non_existent_file)

        # Object should NOT exist after failed operation
        self.test_case.assertFalse(self.storage.exists(path3), "Object should not exist after failed fput_object")

        # Test 4: Test with corrupted/partial stream that fails mid-read
        path4 = PurePosixPath(f"{unique_dir}/failed_partial_stream.bin")
        large_content = b"A" * 1000 + b"B" * 1000 + b"C" * 1000  # 3KB content
        partial_failing_stream = FailingStream(large_content, fail_after_bytes=1500, fail_on_read=True)

        # Ensure object doesn't exist before test
        self.test_case.assertFalse(self.storage.exists(path4))

        # Attempt to put object with stream that fails mid-read
        with self.test_case.assertRaises(MockException):
            self.storage.put_object_stream(path4, partial_failing_stream)  # type: ignore[arg-type]

        # Object should NOT exist after failed operation
        self.test_case.assertFalse(self.storage.exists(path4), "Object should not exist after failed partial stream read")

        # Test 5: Test successful operation after failures (ensure bucket state is clean)
        path5 = PurePosixPath(f"{unique_dir}/successful_after_failures.bin")
        success_content = b"This should succeed after previous failures"
        success_stream = BytesIO(success_content)

        # This should succeed
        self.storage.put_object_stream(path5, success_stream)

        # Object should exist and have correct content
        self.test_case.assertTrue(self.storage.exists(path5))
        retrieved_content = self.storage.get_object(path5)
        self.test_case.assertEqual(retrieved_content, success_content)

        # Test 6: Test fput_object with file that gets deleted during operation
        path6 = PurePosixPath(f"{unique_dir}/failed_fput_deleted_file.bin")

        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"Temporary file content")
            temp_file_path = Path(temp_file.name)

        # Delete the file before trying to fput it
        temp_file_path.unlink()

        # Ensure object doesn't exist before test
        self.test_case.assertFalse(self.storage.exists(path6))

        # Attempt to fput deleted file - should raise exception
        with self.test_case.assertRaises((FileNotFoundError, IOError)):  # type: ignore[misc]
            self.storage.fput_object(path6, temp_file_path)

        # Object should NOT exist after failed operation
        self.test_case.assertFalse(self.storage.exists(path6), "Object should not exist after failed fput_object with deleted file")

        # inexistent path
        path = f"{unique_dir}/inexistent.txt"
        self.test_case.assertRaises(FileNotFoundError, self.storage.get_object_stream, path)

    def test_list_objects(self) -> None:
        unique_dir = f"dir{self.us}"
        self.storage.put_object(PurePosixPath(f"{unique_dir}/file1.txt"), b"Content 1")
        self.storage.put_object(PurePosixPath(f"{unique_dir}/dir2/file2.txt"), b"Content 2")
        self.storage.put_object(PurePosixPath(f"{unique_dir}file1.txt"), b"Content 3")
        objects = self.storage.list_objects(PurePosixPath(f"{unique_dir}"))
        objects.sort()
        self.test_case.assertIsInstance(objects, slist)
        expected_objects_all = [
            PurePosixPath(f"{unique_dir}/dir2/file2.txt"),
            PurePosixPath(f"{unique_dir}/file1.txt"),
            PurePosixPath(f"{unique_dir}file1.txt"),
        ]
        self.test_case.assertListEqual(objects, expected_objects_all)

        objects = self.storage.list_objects(f"{unique_dir}/")
        objects.sort()
        self.test_case.assertIsInstance(objects, slist)
        expected_objects = [PurePosixPath(f"{unique_dir}/dir2/file2.txt"), PurePosixPath(f"{unique_dir}/file1.txt")]
        self.test_case.assertListEqual(objects, expected_objects)

        # here we expect that on Minio there will be other dirs/objects, so we just check of our objects do exist
        objects = self.storage.list_objects("")
        self.test_case.assertIsInstance(objects, slist)
        expected_objects_set = set(expected_objects_all)
        real_objects_set = objects.toSet()
        self.test_case.assertTrue(expected_objects_set.issubset(real_objects_set))

        # Invalid Prefix cases
        for prefix in self.INVALID_PREFIXES:
            self.test_case.assertRaises(ValueError, self.storage.list_objects, prefix)

    def test_list_objects_with_over1000keys(self) -> None:
        path_with2025_keys = self._ensure_dir_with_2025_keys()

        objects = self.storage.list_objects(path_with2025_keys)
        self.test_case.assertEqual(2025, objects.size())

    def test_shallow_list_objects(self) -> None:
        unique_dir = f"dir{self.us}"
        self.storage.put_object(PurePosixPath(f"{unique_dir}/file1.txt"), b"Content 1")
        self.storage.put_object(PurePosixPath(f"{unique_dir}/dir2/file2.txt"), b"Content 2")
        self.storage.put_object(PurePosixPath(f"{unique_dir}file1.txt"), b"Content 3")

        self.test_case.assertRaises(ValueError, self.storage.shallow_list_objects, "/")
        self.test_case.assertRaises(ValueError, self.storage.shallow_list_objects, "/d")

        objects = self.storage.shallow_list_objects(f"{unique_dir}/")
        expected_objects = [PurePosixPath(f"{unique_dir}/file1.txt")]
        expected_prefixes = [f"{unique_dir}/dir2/"]
        self.test_case.assertListEqual(objects.objects, expected_objects)
        self.test_case.assertEqual(objects.prefixes, expected_prefixes)

        shallow_listing = self.storage.shallow_list_objects(PurePosixPath(f"{unique_dir}"))
        expected_objects = [PurePosixPath(f"{unique_dir}file1.txt")]
        expected_prefixes = [f"{unique_dir}/"]
        self.test_case.assertIsInstance(shallow_listing.objects, slist)
        self.test_case.assertIsInstance(shallow_listing.prefixes, slist)
        self.test_case.assertListEqual(shallow_listing.objects, expected_objects)
        self.test_case.assertEqual(shallow_listing.prefixes, expected_prefixes)

        # here we expect that on Minio there will be other dirs/objects, since the bucket is shared, so we just check of our objects do exist
        shallow_listing = self.storage.shallow_list_objects("")
        expected_objects = {PurePosixPath(f"{unique_dir}file1.txt")}
        expected_prefixes = {f"{unique_dir}/"}
        self.test_case.assertTrue(expected_objects.issubset(shallow_listing.objects.toSet()))
        self.test_case.assertTrue(expected_prefixes.issubset(shallow_listing.prefixes.toSet()))

        # Invalid Prefix cases
        for prefix in self.INVALID_PREFIXES:
            self.test_case.assertRaises(ValueError, self.storage.shallow_list_objects, prefix)

    def test_shallow_list_objects_with_over1000keys(self) -> None:
        path_with2025_keys = self._ensure_dir_with_2025_keys()
        shallow_listing = self.storage.shallow_list_objects(path_with2025_keys)
        self.test_case.assertEqual(2025, shallow_listing.objects.size())
        self.test_case.assertEqual(0, shallow_listing.prefixes.size())

    def test_exists(self) -> None:
        unique_dir = f"dir{self.us}"
        path = PurePosixPath(f"{unique_dir}/file.txt")
        self.storage.put_object(path, b"Content")
        self.test_case.assertTrue(self.storage.exists(path))
        self.test_case.assertFalse(self.storage.exists(f"{unique_dir}"))
        self.test_case.assertRaises(ValueError, self.storage.exists, f"{unique_dir}/")

    def test_remove_objects(self) -> None:
        # Setup the test
        unique_dir = f"dir{self.us}"
        path1 = PurePosixPath(f"{unique_dir}/file1.txt")
        path2 = PurePosixPath(f"{unique_dir}/file2.txt")
        self.storage.put_object(path1, b"Content 1")
        self.storage.put_object(path2, b"Content 2")

        # perform removal action
        result = self.storage.remove_objects([path1, path2, f"{unique_dir}/inexistent.file"])

        # check that the files do not exist
        self.test_case.assertIsInstance(result, slist)
        self.test_case.assertEqual(result, [])
        self.test_case.assertFalse(self.storage.exists(path1))
        self.test_case.assertFalse(self.storage.exists(path2))
        self.test_case.assertRaises(FileNotFoundError, self.storage.get_object, f"{unique_dir}/file1.txt")
        self.test_case.assertRaises(ValueError, self.storage.remove_objects, [f"{unique_dir}/"])

        # check that the leftover empty directories are also removed, but bucket may contain leftovers from the other test runs
        shallow_listing = self.storage.shallow_list_objects("")
        prefixes = shallow_listing.prefixes.toSet()
        self.test_case.assertNotIn(f"{unique_dir}/", prefixes)

    def _ensure_dir_with_2025_keys(self) -> str:
        existing_keys = self.storage.list_objects(self.PATH_WITH_2025_KEYS)
        if not existing_keys:

            def upload_file(i):
                path = PurePosixPath(self.PATH_WITH_2025_KEYS) / f"file{i}.txt"
                content = f"Content {i}".encode("utf-8")
                self.storage.put_object(path, content)

            stream_obj = sstream(range(2025))
            stream_obj.fastmap(upload_file, poolSize=100).to_list()
        return self.PATH_WITH_2025_KEYS

    def test_get_size(self) -> None:
        # Setup the test
        unique_dir = f"dir{self.us}"
        path1 = PurePosixPath(f"{unique_dir}/file1.txt")

        content1 = b"Content 1"

        self.storage.put_object(path1, content1)

        self.test_case.assertEqual(len(content1), self.storage.get_size(path1))
        with self.test_case.assertRaises(FileNotFoundError):
            self.storage.get_size(f"{unique_dir}/NOT.FOUND")

        # update object -- new size
        content1 = b"Content 1 -- modified"
        self.storage.put_object(path1, content1)
        self.test_case.assertEqual(len(content1), self.storage.get_size(path1))

    def test_open_write(self) -> None:
        """Test the open_write method functionality."""
        unique_dir = f"dir{self.us}"

        # Test with larger content written in chunks
        path2 = PurePosixPath(f"{unique_dir}/multipart_large.csv.gz")
        header = ["id", "name", "value"]
        rows = [["1", "one", "1.1"], ["2", "two", "2.2"], ["3", "three", "3.3"]]

        test_object1 = self.storage.open_write(path2, timeout_sec=3)
        with test_object1 as sink:
            with gzip.GzipFile(fileobj=sink, mode="wb", filename="", mtime=0, compresslevel=1) as gzbin:  # type: ignore[arg-type]
                with io.TextIOWrapper(gzbin, encoding="utf-8", newline="") as gztext:  # type: ignore[arg-type]
                    w = csv.writer(gztext)
                    w.writerow(header)
                    for row in rows:
                        w.writerow(row)

        path = PurePosixPath(f"{unique_dir}/multipart_file.txt")
        test_content = b"Test content for multipart upload"

        test_object2 = self.storage.open_write(path, timeout_sec=3)
        # Test basic functionality
        with test_object2 as sink:
            sink.write(test_content)

        # Verify the object was stored correctly
        retrieved_content = self.storage.get_object(path)
        self.test_case.assertEqual(retrieved_content, test_content)

        obj_stream = self.storage.get_object_stream(path2)
        with obj_stream as _stream:
            with gzip.open(_stream, "rt") as gztext:
                reader = csv.reader(gztext)
                retrieved_rows = list(reader)
        self.test_case.assertEqual([header] + rows, retrieved_rows)

        # Test that the object exists and has correct size
        self.test_case.assertTrue(self.storage.exists(path2))
        self.test_case.assertEqual(76, self.storage.get_size(path2))

        # Test with string path
        path3 = f"{unique_dir}/multipart_string_path.txt"
        string_content = b"Content written using string path"

        test_object3 = self.storage.open_write(path3, timeout_sec=3)
        with test_object3 as sink:
            sink.write(string_content)

        retrieved_content = self.storage.get_object(path3)
        self.test_case.assertEqual(retrieved_content, string_content)
        if isinstance(test_object1, AsyncObjectWriter):
            test_object1._thread.join(timeout=1.0)  # pylint: disable=W0212
            self.test_case.assertFalse(test_object1._thread.is_alive())  # pylint: disable=W0212
        if isinstance(test_object2, AsyncObjectWriter):
            test_object2._thread.join(timeout=1.0)  # pylint: disable=W0212
            self.test_case.assertFalse(test_object2._thread.is_alive())  # pylint: disable=W0212
        if isinstance(test_object3, AsyncObjectWriter):
            test_object3._thread.join(timeout=1.0)  # pylint: disable=W0212
            self.test_case.assertFalse(test_object3._thread.is_alive())  # pylint: disable=W0212

    def test_open_write_timeout(self) -> None:
        # test timeout functionality in open_write:we write immediately, but the consumer doesn't consume in time;
        # The consumer is the bucketbase.ibucket.AsyncObjectWriter._write_to_bucket, which in turn calls _bucket.put_object_stream on its own thread
        # test timeout functionality in open_write: we write immediately, but the consumer doesn't consume in time.
        unique_dir = f"dir{self.us}"
        path_timeout = PurePosixPath(f"{unique_dir}/timeout_test.txt")
        test_content_timeout = b"Timeout test content"

        # Override _bucket.put_object_stream to simulate slow consumption in AsyncObjectWriter._write_to_bucket
        orig_put_object_stream = self.storage.put_object_stream
        background_threads = []

        successes = []
        try:

            def slow_put_object_stream(name, consumer_stream):  # pylint: disable=unused-argument
                # Track the current thread so we can wait for it to finish
                current_thread = threading.current_thread()
                background_threads.append(current_thread)
                try:
                    with consumer_stream as _stream:
                        time.sleep(1.0)  # Sleep for 1 second to cause timeout (test uses 0.5s timeout)
                        _stream.read()
                    successes.append("no_exception")
                except BaseException as exc:  # pylint: disable=broad-exception-caught
                    print(f"slow_put_object_stream exception: {exc}")
                    successes.append(exc)
                print("slow_put_object_stream done")

            self.storage.put_object_stream = slow_put_object_stream

            test_object: AsyncObjectWriter = self.storage.open_write(path_timeout, timeout_sec=0.5)  # type: ignore[assignment]
            with self.test_case.assertRaises(TimeoutError):
                with test_object as sink:
                    sink.write(test_content_timeout)
                    sink.write(test_content_timeout)

        finally:
            # Restore the original method to avoid side effects for other tests
            self.storage.put_object_stream = orig_put_object_stream
            # Wait for any background threads to complete to avoid race conditions
            print(f"Waiting for {len(background_threads)} background threads to complete...")
            for thread in background_threads:
                if thread.is_alive():
                    print(f"Waiting for thread {thread.name} to complete...")
                    thread.join(timeout=5.0)  # Wait up to 5 seconds
                    if thread.is_alive():
                        raise TimeoutError(f"Thread {thread.name} did not complete within timeout")  # type: ignore[misc]
            self.test_case.assertFalse(test_object._thread.is_alive())  # pylint: disable=W0212
            self.test_case.assertListEqual([TimeoutError], [e.__class__ for e in successes], f"Expected [TimeoutError], but got {successes}")

    def test_open_write_consumer_throws(self) -> None:
        # The consumer is the bucketbase.ibucket.AsyncObjectWriter._write_to_bucket, which in turn calls _bucket.put_object_stream on its own thread
        unique_dir = f"dir{self.us}"
        path_timeout = PurePosixPath(f"{unique_dir}/open_write_consumer_throws.txt")
        test_content_timeout = b"Timeout test content"

        background_threads = []
        orig_put_object_stream = self.storage.put_object_stream
        try:

            def throwing_put_object_stream(name, consumer_stream):  # pylint: disable=unused-argument
                # Track the current thread so we can wait for it to finish
                current_thread = threading.current_thread()
                background_threads.append(current_thread)
                with consumer_stream as _stream:
                    time.sleep(0.1)  # Sleep for 0.1 second to cause timeout (test uses 0.5s timeout)
                    _stream.read()
                raise MockException("test exception")

            self.storage.put_object_stream = throwing_put_object_stream

            test_object: AsyncObjectWriter = self.storage.open_write(path_timeout, timeout_sec=3)  # type: ignore[assignment]
            with self.test_case.assertRaises(MockException):
                with test_object as sink:
                    sink.write(test_content_timeout)
                    sink.write(test_content_timeout)
        finally:
            # Restore the original method to avoid side effects for other tests
            self.storage.put_object_stream = orig_put_object_stream
            # Wait for any background threads to complete to avoid race conditions
            print(f"Waiting for {len(background_threads)} background threads to complete...")
            for thread in background_threads:
                if thread.is_alive():
                    print(f"Waiting for thread {thread.name} to complete...")
                    thread.join(timeout=5.0)  # Wait up to 5 seconds
                    if thread.is_alive():
                        raise TimeoutError(f"Thread {thread.name} did not complete within timeout")  # type: ignore[misc]
            self.test_case.assertFalse(test_object._thread.is_alive())  # pylint: disable=W0212

    def test_open_write_feeder_throws(self) -> None:
        unique_dir = f"dir{self.us}"
        path_timeout = PurePosixPath(f"{unique_dir}/open_write_feeder_throws.txt")
        test_content_timeout = b"Timeout test content"

        background_threads = Queue()
        orig_put_object_stream = self.storage.put_object_stream
        expected_exc = Queue()

        def throwing_put_object_stream(name, consumer_stream):  # pylint: disable=unused-argument
            # Track the current thread so we can wait for it to finish
            current_thread = threading.current_thread()
            background_threads.put(current_thread)
            try:
                with consumer_stream as _stream:
                    try:
                        red = _stream.read(len(test_content_timeout))
                        self.test_case.assertEqual(test_content_timeout, red)
                        buf = _stream.read(1)
                        while buf != b"":
                            buf = _stream.read(1)
                    except BaseException as e:
                        expected_exc.put(e)
                        raise
            except BaseException as e:  # pylint: disable=broad-exception-caught
                expected_exc.put(e)

        test_object: AsyncObjectWriter = None  # type: ignore[assignment]
        self.storage.put_object_stream = throwing_put_object_stream
        try:
            test_object = self.storage.open_write(path_timeout, timeout_sec=1)  # type: ignore[assignment]
            try:
                with test_object as sink:
                    sink.write(test_content_timeout)
                    raise MockException("test exception")
            except MockException as e:
                self.test_case.assertEqual("test exception", str(e))
        finally:
            # Restore the original method to avoid side effects for other tests
            self.storage.put_object_stream = orig_put_object_stream
            # Wait for any background threads to complete to avoid race conditions
            print(f"Waiting for {background_threads.qsize()} background threads to complete...")
            first_thread = background_threads.get(timeout=2.0)
            first_thread.join(timeout=2.0)
            self.test_case.assertFalse(first_thread.is_alive())
            self.test_case.assertEqual(0, background_threads.qsize())
            self.test_case.assertTrue(expected_exc.qsize() == 2)
            if isinstance(test_object, AsyncObjectWriter):
                self.test_case.assertFalse(test_object._thread.is_alive())  # pylint: disable=W0212
            self.test_case.assertEqual("test exception", str(expected_exc.get_nowait()))

    def test_open_write_with_parquet(self) -> None:  # pylint: disable=too-many-locals
        """Test the open_write method with pyarrow parquet files using multiple batches."""
        unique_dir = f"dir{self.us}"
        parquet_path = PurePosixPath(f"{unique_dir}/open_write_test_data.parquet")

        # Define schema for our test data
        schema = pa.schema([("id", pa.int64()), ("name", pa.string()), ("value", pa.float64()), ("active", pa.bool_()), ("timestamp", pa.timestamp("ms"))])

        # Generate test data in 10 batches of 3 records each (or larger for multipart testing)
        batches = []
        # For MinioBucket with small PART_SIZE, use larger batches to trigger multipart upload
        # Check if this is a MinioBucket by looking at the class name
        is_minio = "MinioBucket" in str(type(self.storage))
        batch_size = 200000 if is_minio else 3  # 200k records per batch for minio, 3 for others
        num_batches = 3 if is_minio else 3  # 10 batches for others, 3 for minio

        for batch_num in range(num_batches):
            batch_data = {
                "id": [batch_num * batch_size + i for i in range(batch_size)],
                "name": [f"item_{batch_num}_{i}" * (10 if is_minio else 1) for i in range(batch_size)],  # Longer strings for minio
                "value": [batch_num * 10.5 + i * 1.1 for i in range(batch_size)],
                "active": [(batch_num * batch_size + i) % 2 == 0 for i in range(batch_size)],
                "timestamp": [pa.scalar(1640995200000 + batch_num * 1000 + i * 100, type=pa.timestamp("ms")) for i in range(batch_size)],
            }
            batch = pa.record_batch(batch_data, schema=schema)
            batches.append(batch)

        # Write parquet file using open_write
        tested_object: AsyncObjectWriter = self.storage.open_write(parquet_path, timeout_sec=3)  # type: ignore[assignment]
        with tested_object as sink:
            with pa.output_stream(sink) as arrow_sink:
                with pq.ParquetWriter(arrow_sink, schema) as writer:
                    for batch in batches:
                        writer.write_batch(batch)

        # Verify the file was created and has correct size
        self.test_case.assertTrue(self.storage.exists(parquet_path))
        file_size = self.storage.get_size(parquet_path)
        self.test_case.assertGreater(file_size, 0)

        # Read back the parquet file and verify data
        # Use get_object instead of get_object_stream to avoid seek issues with some implementations
        parquet_data = self.storage.get_object(parquet_path)
        table = pq.read_table(io.BytesIO(parquet_data))

        # Verify we have the expected number of rows
        expected_rows = num_batches * batch_size
        self.test_case.assertEqual(len(table), expected_rows)

        # Verify schema matches
        expected_columns = {"id", "name", "value", "active", "timestamp"}
        actual_columns = set(table.column_names)
        self.test_case.assertEqual(actual_columns, expected_columns)

        # Verify some data integrity
        ids = table.column("id").to_pylist()
        names = table.column("name").to_pylist()
        values = table.column("value").to_pylist()
        actives = table.column("active").to_pylist()

        # Check that we have sequential IDs from 0 to expected_rows-1
        self.test_case.assertEqual(ids, list(range(expected_rows)))

        # Check first few names to ensure pattern is correct
        name_multiplier = 10 if is_minio else 1
        expected_name_0 = "item_0_0" * name_multiplier
        expected_name_1 = "item_0_1" * name_multiplier
        self.test_case.assertEqual(names[0], expected_name_0)
        self.test_case.assertEqual(names[1], expected_name_1)

        # Check that values are calculated correctly
        self.test_case.assertAlmostEqual(values[0], 0.0, places=1)
        self.test_case.assertAlmostEqual(values[1], 1.1, places=1)
        if not is_minio:  # Only check this for smaller datasets
            self.test_case.assertEqual(names[2], "item_0_2")
            self.test_case.assertEqual(names[3], "item_1_0")
            self.test_case.assertAlmostEqual(values[3], 10.5, places=1)

        # Check boolean pattern
        expected_actives = [i % 2 == 0 for i in range(expected_rows)]
        self.test_case.assertEqual(actives, expected_actives)
        if isinstance(tested_object, AsyncObjectWriter):
            self.test_case.assertFalse(tested_object._thread.is_alive())  # pylint: disable=W0212

    def test_put_object_stream_exception_cleanup(self) -> None:
        """
        Test that when put_object_stream fails (stream raises exception during read),
        the file is NOT visible in list_objects.

        BUG: MinioBucket may fail this test because partial uploads might be visible.
        """
        unique_dir = f"dir{self.us}"
        test_path = PurePosixPath(f"{unique_dir}/stream_exception_test.txt")
        test_content = b"A" * 1000  # 1KB of data

        # Create a stream that fails after reading some data
        failing_stream = FailingStream(test_content, fail_after_bytes=500, fail_on_read=True)

        # Ensure the object doesn't exist before the test
        self.test_case.assertFalse(self.storage.exists(test_path))

        # Attempt to put object with failing stream
        with self.test_case.assertRaises(MockException):
            self.storage.put_object_stream(test_path, failing_stream)  # type: ignore[arg-type]

        # The critical assertion: object should NOT exist after failed stream
        self.test_case.assertFalse(self.storage.exists(test_path), "Object should not exist after put_object_stream failure")

        # Also verify it's not in list_objects
        objects = self.storage.list_objects(unique_dir)
        self.test_case.assertFalse(objects, "Object should not appear in list_objects after failed write")

    def test_open_write_partial_write_exception_cleanup(self) -> None:
        """
        Test that when an exception occurs after writing some data (but not closing properly),
        the file is NOT visible in list_objects.

        This simulates a scenario where:
        1. Writer starts writing data
        2. Some data is written successfully
        3. An exception occurs before the write completes
        4. The object should NOT be visible in listings

        BUG: MinioBucket and MemoryBucket fail this test.
        """
        unique_dir = f"dir{self.us}"
        test_path = PurePosixPath(f"{unique_dir}/partial_write_test.txt")

        # Ensure the object doesn't exist before the test
        self.test_case.assertFalse(self.storage.exists(test_path))

        # Write some data, then raise an exception
        with self.test_case.assertRaises(MockException):
            with self.storage.open_write(test_path, timeout_sec=5) as writer:
                # Write multiple chunks
                writer.write(b"First chunk of data\n")
                writer.write(b"Second chunk of data\n")
                writer.write(b"Third chunk of data\n")
                # Simulate an error during writing
                raise MockException("Error during multi-chunk write")

        # Object should NOT exist
        self.test_case.assertFalse(self.storage.exists(test_path), "Object should not exist after exception during multi-chunk write")

        # Verify not in list_objects
        objects = self.storage.list_objects(unique_dir)
        self.test_case.assertFalse(objects, "Object should not appear in list_objects after failed write")

    def test_open_write_without_proper_close(self) -> None:
        """
        Test that simulates not properly closing the file during open_write.
        This is closer to the real-world scenario where a file handle is not closed properly.

        The test verifies that if we don't exit the context manager normally (e.g., due to an exception),
        the file should NOT be visible in list_objects.

        BUG: This is the main bug reported - MinioBucket may leave partial files visible.
        """
        unique_dir = f"dir{self.us}"
        test_path = PurePosixPath(f"{unique_dir}/not_properly_closed.txt")

        # Ensure the object doesn't exist before the test
        self.test_case.assertFalse(self.storage.exists(test_path))

        # Simulate not properly closing by raising exception
        writer_manager = self.storage.open_write(test_path, timeout_sec=5)
        writer = writer_manager.__enter__()  # pylint: disable=C2801
        # Write multiple chunks
        writer.write(b"First chunk of data\n")
        writer.write(b"Second chunk of data\n")
        writer.write(b"Third chunk of data\n")
        writer.flush()
        writer.flush()

        # Object should NOT exist after failed write
        self.test_case.assertFalse(self.storage.exists(test_path), "Object should not exist when file was not properly closed due to exception")

        # Verify not in list_objects
        objects = self.storage.list_objects(unique_dir)
        self.test_case.assertFalse(objects, f"list_objects should return empty list, got {objects}")
        writer_manager.__exit__(None, None, None)

    def test_open_write_sync_exception_cleanup(self) -> None:
        """
        Test that when an exception is raised during open_write_sync, the file is NOT visible in list_objects.
        This tests the synchronous version of open_write (if available).

        BUG: MemoryBucket.open_write_sync has a bug where it stores content even when an exception occurs.
        """
        # Check if the storage has open_write_sync method
        self.test_case.assertTrue(hasattr(self.storage, "open_write_sync"), "Storage does not have open_write_sync method")

        unique_dir = f"dir{self.us}"
        test_path = PurePosixPath(f"{unique_dir}/sync_exception_test.txt")
        test_content = b"This content should not be stored due to exception"

        # Ensure the object doesn't exist before the test
        self.test_case.assertFalse(self.storage.exists(test_path))

        # Attempt to write with an exception raised during writing
        with self.test_case.assertRaises(MockException):
            with self.storage.open_write_sync(test_path) as writer:  # type: ignore[attr-defined]
                writer.write(test_content)
                # Raise an exception before the context manager exits normally
                raise MockException("Simulated write failure in sync mode")

        # The critical assertion: object should NOT exist after failed write
        self.test_case.assertFalse(self.storage.exists(test_path), "Object should not exist after exception during open_write_sync")

        # Also verify it's not in list_objects
        objects = self.storage.list_objects(unique_dir)
        self.test_case.assertFalse(objects, f"list_objects should return empty list, got {objects}")

    def test_regression_parquet_by_AMX(self) -> None:
        parquet_schema = pa.schema([("a", pa.int64())])
        unique_dir = f"dir{self.us}"
        test_path = PurePosixPath(f"{unique_dir}/test_regression.parquet")

        self.storage.remove_objects([test_path])

        writer_ctx = self.storage.open_write(test_path, timeout_sec=5.0)
        writer = writer_ctx.__enter__()  # pylint: disable=C2801
        arrow_sink: pa.NativeFile = pa.output_stream(writer)
        parquet_writer = pq.ParquetWriter(writer, parquet_schema)

        batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], schema=parquet_schema)  # type: ignore[misc]
        parquet_writer.write_batch(batch)

        existing = list(str(x) for x in self.storage.list_objects(test_path))
        self.test_case.assertEqual(0, len(existing), f"exists (and must not; no flush yet): {existing}")

        arrow_sink.close()

        existing = list(str(x) for x in self.storage.list_objects(test_path))
        self.test_case.assertEqual(0, len(existing), f"exists (after arrow close): {existing}")
        with self.test_case.assertRaises(RuntimeError):
            writer_ctx.__exit__(RuntimeError, RuntimeError("test"), None)
        existing = list(str(x) for x in self.storage.list_objects(test_path))
        self.test_case.assertEqual(0, len(existing), f"exists (and must not, because of __exit__ with error): {existing}")

    def test_regression_exception_thrown_in_parquet_writer_context_doesnt_save_object(self) -> None:
        """credits to AMX (amaximciuc@esamtrade.com)"""
        parquet_schema = pa.schema([("a", pa.int64())])
        unique_dir = f"dir{self.us}"
        test_path = PurePosixPath(f"{unique_dir}/test_regression.parquet")

        self.storage.remove_objects([test_path])
        with self.test_case.assertRaises(RuntimeError):
            with self.storage.open_write(test_path, timeout_sec=5.0) as writer:
                with pa.output_stream(writer) as arrow_sink:
                    arrow_sink: pa.NativeFile
                    with pq.ParquetWriter(arrow_sink, parquet_schema) as parquet_writer:
                        batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], schema=parquet_schema)  # type: ignore[misc]
                        parquet_writer.write_batch(batch)

                        existing = self.storage.list_objects(test_path)
                        self.test_case.assertFalse(self.storage.list_objects(test_path), f"objects exists (and must not; no flush yet): {existing}")
                        raise RuntimeError("test")

        existing = self.storage.list_objects(test_path)
        self.test_case.assertFalse(self.storage.list_objects(test_path), f"objects exists (after arrow close): {existing}")

    def test_regression_exception_thrown_in_arrow_sink_context_doesnt_save_object(self) -> None:
        """credits to AMX (amaximciuc@esamtrade.com)"""
        parquet_schema = pa.schema([("a", pa.int64())])
        unique_dir = f"dir{self.us}"
        test_path = PurePosixPath(f"{unique_dir}/test_regression.parquet")

        self.storage.remove_objects([test_path])
        with self.test_case.assertRaises(RuntimeError):
            with self.storage.open_write(test_path, timeout_sec=5.0) as writer:
                with pa.output_stream(writer) as arrow_sink:
                    arrow_sink: pa.NativeFile
                    with pq.ParquetWriter(arrow_sink, parquet_schema) as parquet_writer:
                        batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], schema=parquet_schema)  # type: ignore[misc]
                        parquet_writer.write_batch(batch)

                    existing = self.storage.list_objects(test_path)
                    self.test_case.assertFalse(self.storage.list_objects(test_path), f"objects exists (and must not; no flush yet): {existing}")
                    raise RuntimeError("test")

        existing = self.storage.list_objects(test_path)
        self.test_case.assertFalse(self.storage.list_objects(test_path), f"objects exists (after arrow close): {existing}")

    def test_regression_exception_thrown_in_open_write_context_doesnt_save_object(self) -> None:
        """credits to AMX (amaximciuc@esamtrade.com)"""
        parquet_schema = pa.schema([("a", pa.int64())])
        unique_dir = f"dir{self.us}"
        test_path = PurePosixPath(f"{unique_dir}/test_regression.parquet")

        self.storage.remove_objects([test_path])
        with self.test_case.assertRaises(RuntimeError):
            with self.storage.open_write(test_path, timeout_sec=5.0) as writer:
                with pa.output_stream(writer) as arrow_sink:
                    arrow_sink: pa.NativeFile
                    with pq.ParquetWriter(arrow_sink, parquet_schema) as parquet_writer:
                        batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], schema=parquet_schema)  # type: ignore[misc]
                        parquet_writer.write_batch(batch)

                existing = self.storage.list_objects(test_path)
                self.test_case.assertFalse(self.storage.list_objects(test_path), f"objects exists (and must not; no flush yet): {existing}")
                raise RuntimeError("test")

        existing = self.storage.list_objects(test_path)
        self.test_case.assertFalse(self.storage.list_objects(test_path), f"objects exists (after arrow close): {existing}")
