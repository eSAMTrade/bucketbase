import gzip
import io
from io import BytesIO
from pathlib import PurePosixPath
from typing import BinaryIO
from unittest import TestCase

import pyarrow as pa
import pyarrow.parquet as pq
from streamerate import slist, stream
from tsx import iTSms

from bucketbase.ibucket import IBucket


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

    def test_open_multipart_sink(self):
        """Test the open_multipart_sink method functionality."""
        unique_dir = f"dir{self.us}"
        path = PurePosixPath(f"{unique_dir}/multipart_file.txt")
        test_content = b"Test content for multipart upload"

        # Test basic functionality
        with self.storage.open_multipart_sink(path) as sink:
            sink.write(test_content)

        # Verify the object was stored correctly
        retrieved_content = self.storage.get_object(path)
        self.test_case.assertEqual(retrieved_content, test_content)

        # Test with larger content written in chunks
        path2 = PurePosixPath(f"{unique_dir}/multipart_large.txt")
        chunk1 = b"First chunk of data. "
        chunk2 = b"Second chunk of data. "
        chunk3 = b"Third and final chunk."
        expected_content = chunk1 + chunk2 + chunk3

        with self.storage.open_multipart_sink(path2) as sink:
            sink.write(chunk1)
            sink.write(chunk2)
            sink.write(chunk3)

        retrieved_content = self.storage.get_object(path2)
        self.test_case.assertEqual(retrieved_content, expected_content)

        # Test that the object exists and has correct size
        self.test_case.assertTrue(self.storage.exists(path2))
        self.test_case.assertEqual(self.storage.get_size(path2), len(expected_content))

        # Test with string path
        path3 = f"{unique_dir}/multipart_string_path.txt"
        string_content = b"Content written using string path"

        with self.storage.open_multipart_sink(path3) as sink:
            sink.write(string_content)

        retrieved_content = self.storage.get_object(path3)
        self.test_case.assertEqual(retrieved_content, string_content)

    def test_open_multipart_sink_with_parquet(self):
        """Test the open_multipart_sink method with pyarrow parquet files using multiple batches."""
        unique_dir = f"dir{self.us}"
        parquet_path = PurePosixPath(f"{unique_dir}/test_data.parquet")

        # Define schema for our test data
        schema = pa.schema([("id", pa.int64()), ("name", pa.string()), ("value", pa.float64()), ("active", pa.bool_()), ("timestamp", pa.timestamp("ms"))])

        # Generate test data in 10 batches of 3 records each (or larger for multipart testing)
        batches = []
        # For MinioBucket with small PART_SIZE, use larger batches to trigger multipart upload
        # Check if this is a MinioBucket by looking at the class name
        is_minio = "MinioBucket" in str(type(self.storage))
        batch_size = 200000 if is_minio else 3  # 200k records per batch for minio, 3 for others
        num_batches = 10 if not is_minio else 3  # 10 batches for others, 3 for minio

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

        # Write parquet file using open_multipart_sink
        with self.storage.open_multipart_sink(parquet_path) as sink:
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
