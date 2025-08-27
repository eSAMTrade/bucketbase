import csv
import gzip
import io
import threading
import time
from io import BytesIO
from pathlib import PurePosixPath
from queue import Queue
from typing import BinaryIO
from unittest import TestCase

import pyarrow as pa
import pyarrow.parquet as pq
from streamerate import slist, stream
from tsx import iTSms

from bucketbase.ibucket import AsyncObjectWriter, IBucket


class MockException(Exception):
    pass


class IBucketTester:
    INVALID_PREFIXES = ["/", "/dir", "dir1//dir2", "dir1//", "star*1", "dir1/a\file.txt", "at@gmail", "sharp#1", "dollar$1", "comma,"]
    PATH_WITH_2025_KEYS = "test-dir-with-2025-keys/"

    def __init__(self, storage: IBucket, test_case: TestCase) -> None:
        self.storage = storage
        self.test_case = test_case
        # Next is a unique suffix to be used in the names of dirs and files, so they will be unique
        self.us = f"{iTSms.now() % 100_000_000:08d}"

    def cleanup(self):
        self.storage.remove_prefix(f"dir{self.us}")

    def test_put_and_get_object(self):
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
        return self.storage.put_object_stream(name, stream)

    def test_put_and_get_object_stream(self):
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

        # Here we validate that we can put_object_stream directly from get_object_stream
        path_out = PurePosixPath(f"{unique_dir}/file1_out.bin")
        with self.storage.get_object_stream(path) as file:
            self.validated_put_object_stream(path_out, file)

        with self.storage.get_object_stream(path_out) as file:
            with gzip.open(file, "rt") as file:
                result = file.read()
                self.test_case.assertEqual(result, "Test\ncontent")

        # inexistent path
        path = f"{unique_dir}/inexistent.txt"
        self.test_case.assertRaises(FileNotFoundError, self.storage.get_object_stream, path)

    def test_list_objects(self):
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

    def test_list_objects_with_over1000keys(self):
        path_with2025_keys = self._ensure_dir_with_2025_keys()

        objects = self.storage.list_objects(path_with2025_keys)
        self.test_case.assertEqual(2025, objects.size())

    def test_shallow_list_objects(self):
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

    def test_shallow_list_objects_with_over1000keys(self):
        path_with2025_keys = self._ensure_dir_with_2025_keys()
        shallow_listing = self.storage.shallow_list_objects(path_with2025_keys)
        self.test_case.assertEqual(2025, shallow_listing.objects.size())
        self.test_case.assertEqual(0, shallow_listing.prefixes.size())

    def test_exists(self):
        unique_dir = f"dir{self.us}"
        path = PurePosixPath(f"{unique_dir}/file.txt")
        self.storage.put_object(path, b"Content")
        self.test_case.assertTrue(self.storage.exists(path))
        self.test_case.assertFalse(self.storage.exists(f"{unique_dir}"))
        self.test_case.assertRaises(ValueError, self.storage.exists, f"{unique_dir}/")

    def test_remove_objects(self):
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

            stream(range(2025)).fastmap(upload_file, poolSize=100).to_list()
        return self.PATH_WITH_2025_KEYS

    def test_get_size(self):
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

    def test_open_write(self):
        """Test the open_write method functionality."""
        unique_dir = f"dir{self.us}"

        # Test with larger content written in chunks
        path2 = PurePosixPath(f"{unique_dir}/multipart_large.csv.gz")
        header = ["id", "name", "value"]
        rows = [["1", "one", "1.1"], ["2", "two", "2.2"], ["3", "three", "3.3"]]

        test_object1 = self.storage.open_write(path2, timeout_sec=3)
        with test_object1 as sink:
            with gzip.GzipFile(fileobj=sink, mode="wb", filename="", mtime=0, compresslevel=1) as gzbin:
                with io.TextIOWrapper(gzbin, encoding="utf-8", newline="") as gztext:
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
        with obj_stream as stream:
            with gzip.open(stream, "rt") as gztext:
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
            test_object1._thread.join(timeout=1.0)
            self.test_case.assertFalse(test_object1._thread.is_alive())
        if isinstance(test_object2, AsyncObjectWriter):
            test_object2._thread.join(timeout=1.0)
            self.test_case.assertFalse(test_object2._thread.is_alive())
        if isinstance(test_object3, AsyncObjectWriter):
            test_object3._thread.join(timeout=1.0)
            self.test_case.assertFalse(test_object3._thread.is_alive())

    def test_open_write_timeout(self):
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

            def slow_put_object_stream(name, consumer_stream):
                # Track the current thread so we can wait for it to finish
                current_thread = threading.current_thread()
                background_threads.append(current_thread)
                try:
                    with consumer_stream as _stream:
                        time.sleep(1.0)  # Sleep for 1 second to cause timeout (test uses 0.5s timeout)
                        _stream.read()
                    successes.append("no_exception")
                except BaseException as e:
                    print(f"slow_put_object_stream exception: {e}")
                    successes.append(e)
                print("slow_put_object_stream done")

            self.storage.put_object_stream = slow_put_object_stream

            test_object: AsyncObjectWriter = self.storage.open_write(path_timeout, timeout_sec=0.5)
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
                        raise TimeoutError(f"Thread {thread.name} did not complete within timeout")
            self.test_case.assertFalse(test_object._thread.is_alive())
            self.test_case.assertListEqual([TimeoutError], [e.__class__ for e in successes], f"Expected [TimeoutError], but got {successes}")

    def test_open_write_consumer_throws(self):
        # The consumer is the bucketbase.ibucket.AsyncObjectWriter._write_to_bucket, which in turn calls _bucket.put_object_stream on its own thread
        unique_dir = f"dir{self.us}"
        path_timeout = PurePosixPath(f"{unique_dir}/open_write_consumer_throws.txt")
        test_content_timeout = b"Timeout test content"

        background_threads = []
        orig_put_object_stream = self.storage.put_object_stream
        try:

            def throwing_put_object_stream(name, consumer_stream):
                # Track the current thread so we can wait for it to finish
                current_thread = threading.current_thread()
                background_threads.append(current_thread)
                with consumer_stream as _stream:
                    time.sleep(0.1)  # Sleep for 0.1 second to cause timeout (test uses 0.5s timeout)
                    _stream.read()
                raise MockException("test exception")

            self.storage.put_object_stream = throwing_put_object_stream

            test_object: AsyncObjectWriter = self.storage.open_write(path_timeout, timeout_sec=3)
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
                        raise TimeoutError(f"Thread {thread.name} did not complete within timeout")
            self.test_case.assertFalse(test_object._thread.is_alive())

    def test_open_write_feeder_throws(self):
        unique_dir = f"dir{self.us}"
        path_timeout = PurePosixPath(f"{unique_dir}/open_write_feeder_throws.txt")
        test_content_timeout = b"Timeout test content"

        background_threads = Queue()
        orig_put_object_stream = self.storage.put_object_stream
        expected_exc = Queue()

        def throwing_put_object_stream(name, consumer_stream):
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
            except BaseException as e:
                expected_exc.put(e)

        test_object: AsyncObjectWriter = None
        self.storage.put_object_stream = throwing_put_object_stream
        try:
            test_object = self.storage.open_write(path_timeout, timeout_sec=1)
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
                self.test_case.assertFalse(test_object._thread.is_alive())
            self.test_case.assertEqual("test exception", str(expected_exc.get_nowait()))

    def test_open_write_with_parquet(self):
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
        tested_object: AsyncObjectWriter = self.storage.open_write(parquet_path, timeout_sec=3)
        with tested_object as sink:
            arrow_sink = pa.output_stream(sink)
            with pq.ParquetWriter(arrow_sink, schema) as writer:
                for batch in batches:
                    writer.write_batch(batch)
            arrow_sink.close()

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
            self.test_case.assertFalse(tested_object._thread.is_alive())
