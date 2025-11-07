# mypy: disable-error-code="no-untyped-def"
import gc
import os
import tempfile
import threading
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO
from unittest import TestCase

import psutil
from exceptiongroup import ExceptionGroup
from minio import Minio

from bucketbase import MemoryBucket, MinioBucket, ShallowListing
from bucketbase.backup_multi_bucket import BackupMultiBucket


class TestBackupMultiBucketMemoryMemLeak(TestCase):

    @staticmethod
    def _get_current_process_memory_MB() -> float:
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024

    class MockMinioClient(Minio):

        def __init__(self, do_fail: bool, part_size: int):
            self._do_fail = do_fail
            self._part_size = part_size
            self._chunks_written = [0]

        def put_object(self, bucket_name: str, object_name: str, data: BinaryIO, length: int, **kwargs: Any) -> None:
            if self._do_fail:
                while data.read(self._part_size):
                    self._chunks_written[0] += 1
                    if self._chunks_written[0] > 1:
                        raise TimeoutError("test: timeout error after 1 chunk")
            else:
                while data.read(self._part_size):
                    pass
                return None

    def test_regression_fput_object_memory_leak_with_minio_timeout(self) -> None:
        """
        Regression test for memory leak in BackupMultiBucket.

        Issue summary:
        - One of the clients in the BackupMultiBucket timed out repeatedly
        - The memory usage of the process grew linearly with each retry

        Investiagtion results:
        - when using MagicMock objects instead of Minio Clients (extending Minio class), like in this test, the leak is reproduced through
            the _put_object_stream_to_missing() which sends to the context.__exit__(e.__class__, e, e.__traceback__)
            the __traceback__ object with the buffer from the while loop in _put_object_stream_to_missing(), since the tracebacks contain references to locals
            The buffers gets accumulated in the MagicMock calls (calls are registered in a list in MagicMock).
            If MagickMock was reset between calls to put, the leak was not reproducing.

        This test just ensures that we won't have a memory leak in the future, which might be induced by BackupMultiBucket methods,
            and ensures that ALL active threads after put_object_stream() returns.
        """

        part_size = 5 * 1024 * 1024
        file_size = 3 * part_size
        file_content = b"x" * file_size
        test_obj_path = PurePosixPath("test.bin")

        mock_client_success = self.MockMinioClient(do_fail=False, part_size=part_size)
        mock_minio_timeout = self.MockMinioClient(do_fail=True, part_size=part_size)

        bucket_success = MinioBucket("test-bucket-success", mock_client_success)
        bucket_timeout = MinioBucket("test-bucket-timeout", mock_minio_timeout)

        multi_bucket = BackupMultiBucket([bucket_success, bucket_timeout], timeout_sec=0.5)

        num_retries = 5
        gc.collect()
        baseline_memory = self._get_current_process_memory_MB()
        memory_samples = [("baseline", baseline_memory)]
        thread_ids_before = {t.ident for t in threading.enumerate()}

        for i in range(1, num_retries + 1):
            with self.assertRaises(TimeoutError):
                current_memory = self._get_current_process_memory_MB()
                print(f"  retry_{i}: {current_memory:6.1f}MB (growth: {current_memory - baseline_memory:+6.1f}MB)")
                stream = BytesIO(file_content)
                multi_bucket.put_object_stream(test_obj_path, stream)

            gc.collect()
            current_memory = self._get_current_process_memory_MB()
            memory_samples.append((f"retry_{i}", current_memory))

        active_thread_ids = {t.ident for t in threading.enumerate()}
        new_threads = active_thread_ids - thread_ids_before
        self.assertEqual(0, len(new_threads))

        final_memory = self._get_current_process_memory_MB()
        final_growth = final_memory - baseline_memory

        print("\n{'=' * 70}\nFPUT_OBJECT MEMORY LEAK TEST (production scenario)\n{'=' * 70}")
        print(f"Total memory growth: {final_growth:.1f}MB ({final_growth / num_retries:.1f}MB per retry)")
        for label, memory in memory_samples:
            growth = memory - baseline_memory
            print(f"  {label:10s}: {memory:6.1f}MB (growth: {growth:+6.1f}MB)")
        print(f"Final: {final_memory:6.1f}MB (growth: {final_growth:+6.1f}MB)")

        threshold_mb = part_size * (num_retries - 1) / 1024 / 1024
        leaks_present = final_growth > threshold_mb
        self.assertFalse(leaks_present, f"regression: we have mem-leaks; {final_growth:.1f}MB > {threshold_mb:.1f}MB threshold")
        print(f"  [OK] NO LEAK: {final_growth:.1f}MB <= {threshold_mb:.1f}MB threshold\n{'=' * 70}\n")


class TestBackupMultiBucketComprehensive(TestCase):
    """
    Note!!!

    These tests were created by Claude 4.5, and have not been reviewed by human yet. They just pass
    """

    class FailingMemoryBucket(MemoryBucket):
        """MemoryBucket that can be configured to fail on specific operations"""

        def __init__(self, fail_on_open_write: bool = False, fail_on_write: bool = False, fail_after_bytes: int = -1, fail_on_exit: bool = False):
            super().__init__()
            self.fail_on_open_write = fail_on_open_write
            self.fail_on_write = fail_on_write
            self.fail_after_bytes = fail_after_bytes
            self.fail_on_exit = fail_on_exit
            self.bytes_written = 0
            self.open_write_called = False
            self.write_called = False
            self.exit_called = False

        def open_write(self, name: PurePosixPath | str, timeout_sec: float | None = None):
            self.open_write_called = True
            if self.fail_on_open_write:
                raise RuntimeError("Simulated open_write failure")

            # Return a custom context manager that tracks writes
            parent_self = self
            original_context = super().open_write(name, timeout_sec)

            class FailingContextManager:
                def __enter__(self):
                    self.writer = original_context.__enter__()
                    return self

                def write(self, data: bytes) -> int:
                    parent_self.write_called = True
                    parent_self.bytes_written += len(data)

                    if parent_self.fail_on_write:
                        raise RuntimeError("Simulated write failure")

                    if parent_self.fail_after_bytes >= 0 and parent_self.bytes_written > parent_self.fail_after_bytes:
                        raise RuntimeError(f"Simulated write failure after {parent_self.fail_after_bytes} bytes")

                    return self.writer.write(data)

                def __exit__(self, exc_type, exc_val, exc_tb):
                    parent_self.exit_called = True
                    if parent_self.fail_on_exit:
                        raise RuntimeError("Simulated exit failure")
                    return self.writer.__exit__(exc_type, exc_val, exc_tb)

            return FailingContextManager()

    def test_put_object_stream_success_all_buckets(self):
        """Test successful write to all buckets"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()
        bucket3 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2, bucket3], timeout_sec=1.0)

        content = b"Hello, World!" * 1000
        stream = BytesIO(content)

        multi_bucket.put_object_stream("test.txt", stream)

        # Verify all buckets have the content
        self.assertEqual(content, bucket1.get_object("test.txt"))
        self.assertEqual(content, bucket2.get_object("test.txt"))
        self.assertEqual(content, bucket3.get_object("test.txt"))

    def test_put_object_stream_one_bucket_fails_on_open(self):
        """Test write continues when one bucket fails on open_write, but raises exception at end"""
        bucket1 = MemoryBucket()
        bucket2 = self.FailingMemoryBucket(fail_on_open_write=True)
        bucket3 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2, bucket3], timeout_sec=1.0)

        content = b"Hello, World!" * 1000
        stream = BytesIO(content)

        # Should raise exception because one bucket failed, but successful buckets should have content
        with self.assertRaises(RuntimeError) as ctx:
            multi_bucket.put_object_stream("test.txt", stream)

        self.assertIn("Simulated open_write failure", str(ctx.exception))

        # Verify successful buckets have the content
        self.assertEqual(content, bucket1.get_object("test.txt"))
        self.assertEqual(content, bucket3.get_object("test.txt"))

        # Verify failed bucket doesn't have the content
        with self.assertRaises(FileNotFoundError):
            bucket2.get_object("test.txt")

    def test_put_object_stream_one_bucket_fails_on_write(self):
        """Test write continues when one bucket fails during write, but raises exception at end"""
        bucket1 = MemoryBucket()
        bucket2 = self.FailingMemoryBucket(fail_on_write=True)
        bucket3 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2, bucket3], timeout_sec=1.0)

        content = b"Hello, World!" * 1000
        stream = BytesIO(content)

        # Should raise exception because one bucket failed, but successful buckets should have content
        with self.assertRaises(RuntimeError) as ctx:
            multi_bucket.put_object_stream("test.txt", stream)

        self.assertIn("Simulated write failure", str(ctx.exception))

        # Verify successful buckets have the content
        self.assertEqual(content, bucket1.get_object("test.txt"))
        self.assertEqual(content, bucket3.get_object("test.txt"))

        # Verify failed bucket doesn't have the content
        with self.assertRaises(FileNotFoundError):
            bucket2.get_object("test.txt")

    def test_put_object_stream_all_buckets_fail_on_open(self):
        """Test exception raised when all buckets fail on open_write"""
        bucket1 = self.FailingMemoryBucket(fail_on_open_write=True)
        bucket2 = self.FailingMemoryBucket(fail_on_open_write=True)

        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        content = b"Hello, World!" * 1000
        stream = BytesIO(content)

        # Should raise ExceptionGroup with 2 exceptions
        with self.assertRaises(ExceptionGroup) as ctx:
            multi_bucket.put_object_stream("test.txt", stream)

        self.assertEqual(2, len(ctx.exception.exceptions))

    def test_put_object_stream_all_buckets_fail_on_write(self):
        """Test exception raised when all buckets fail during write"""
        bucket1 = self.FailingMemoryBucket(fail_on_write=True)
        bucket2 = self.FailingMemoryBucket(fail_on_write=True)

        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        content = b"Hello, World!" * 1000
        stream = BytesIO(content)

        # Should raise ExceptionGroup with 2 exceptions
        with self.assertRaises(ExceptionGroup) as ctx:
            multi_bucket.put_object_stream("test.txt", stream)

        self.assertEqual(2, len(ctx.exception.exceptions))

    def test_put_object_stream_large_file(self):
        """Test write with file larger than buffer size (5MB)"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        # Create 15MB file (3 chunks of 5MB)
        content = b"x" * (15 * 1024 * 1024)
        stream = BytesIO(content)

        multi_bucket.put_object_stream("large.bin", stream)

        # Verify both buckets have the content
        self.assertEqual(content, bucket1.get_object("large.bin"))
        self.assertEqual(content, bucket2.get_object("large.bin"))

    def test_put_object_stream_one_bucket_fails_mid_write(self):
        """Test write continues when one bucket fails in the middle of writing, but raises exception at end"""
        bucket1 = MemoryBucket()
        bucket2 = self.FailingMemoryBucket(fail_after_bytes=1024 * 1024)  # Fail after 1MB
        bucket3 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2, bucket3], timeout_sec=1.0)

        # Create 10MB file
        content = b"x" * (10 * 1024 * 1024)
        stream = BytesIO(content)

        # Should raise exception because one bucket failed, but successful buckets should have content
        with self.assertRaises(RuntimeError) as ctx:
            multi_bucket.put_object_stream("large.bin", stream)

        self.assertIn("Simulated write failure after", str(ctx.exception))

        # Verify successful buckets have the content
        self.assertEqual(content, bucket1.get_object("large.bin"))
        self.assertEqual(content, bucket3.get_object("large.bin"))

        # Verify failed bucket doesn't have the complete content
        with self.assertRaises(FileNotFoundError):
            bucket2.get_object("large.bin")

    def test_fput_object_success(self):
        """Test fput_object with successful write to all buckets"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
            content = b"File content" * 1000
            tmp.write(content)
            tmp_path = Path(tmp.name)

        try:
            multi_bucket.fput_object("test.txt", tmp_path)

            # Verify both buckets have the content
            self.assertEqual(content, bucket1.get_object("test.txt"))
            self.assertEqual(content, bucket2.get_object("test.txt"))
        finally:
            tmp_path.unlink()

    def test_fput_object_file_not_found(self):
        """Test fput_object raises FileNotFoundError for non-existent file"""
        bucket1 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1], timeout_sec=1.0)

        with self.assertRaises(FileNotFoundError) as ctx:
            multi_bucket.fput_object("test.txt", Path("/nonexistent/file.txt"))

        self.assertIn("Source file not found", str(ctx.exception))

    def test_fput_object_skip_existing_same_size(self):
        """Test fput_object skips upload when file already exists with same size"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
            content = b"File content" * 1000
            tmp.write(content)
            tmp_path = Path(tmp.name)

        try:
            # First upload
            multi_bucket.fput_object("test.txt", tmp_path)

            # Modify bucket1 to track if put is called again
            original_put = bucket1.put_object_stream
            put_called = [False]

            def tracking_put(name, stream):
                put_called[0] = True
                return original_put(name, stream)

            bucket1.put_object_stream = tracking_put  # type: ignore[method-assign]

            # Second upload - should skip
            multi_bucket.fput_object("test.txt", tmp_path)

            # Verify put was not called
            self.assertFalse(put_called[0])
        finally:
            tmp_path.unlink()

    def test_fput_object_existing_different_size_raises(self):
        """Test fput_object raises FileExistsError when file exists with different size"""
        bucket1 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1], timeout_sec=1.0)

        # Put an object with different size
        bucket1.put_object("test.txt", b"short")

        # Create a temporary file with different size
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
            content = b"File content" * 1000
            tmp.write(content)
            tmp_path = Path(tmp.name)

        try:
            with self.assertRaises(FileExistsError) as ctx:
                multi_bucket.fput_object("test.txt", tmp_path)

            self.assertIn("already exists with different size", str(ctx.exception))
        finally:
            tmp_path.unlink()

    def test_get_object_from_first_bucket(self):
        """Test get_object retrieves from first available bucket"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()
        bucket3 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2, bucket3], timeout_sec=1.0)

        content = b"Hello, World!"
        bucket1.put_object("test.txt", content)

        # Should retrieve from bucket1
        result = multi_bucket.get_object("test.txt")
        self.assertEqual(content, result)

    def test_get_object_from_second_bucket_when_first_missing(self):
        """Test get_object retrieves from second bucket when first doesn't have it"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()
        bucket3 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2, bucket3], timeout_sec=1.0)

        content = b"Hello, World!"
        bucket2.put_object("test.txt", content)

        # Should retrieve from bucket2
        result = multi_bucket.get_object("test.txt")
        self.assertEqual(content, result)

    def test_get_object_not_found_in_any_bucket(self):
        """Test get_object raises FileNotFoundError when object not in any bucket"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        with self.assertRaises(FileNotFoundError):
            multi_bucket.get_object("nonexistent.txt")

    def test_get_object_stream_from_first_bucket(self):
        """Test get_object_stream retrieves from first available bucket"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        content = b"Hello, World!"
        bucket1.put_object("test.txt", content)

        # Should retrieve from bucket1
        with multi_bucket.get_object_stream("test.txt") as stream:
            result = stream.read()

        self.assertEqual(content, result)

    def test_get_object_stream_not_found(self):
        """Test get_object_stream raises FileNotFoundError when object not in any bucket"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        with self.assertRaises(FileNotFoundError):
            multi_bucket.get_object_stream("nonexistent.txt")

    def test_exists_true_when_in_first_bucket(self):
        """Test exists returns True when object is in first bucket"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        bucket1.put_object("test.txt", b"content")

        self.assertTrue(multi_bucket.exists("test.txt"))

    def test_exists_true_when_in_second_bucket(self):
        """Test exists returns True when object is in second bucket"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        bucket2.put_object("test.txt", b"content")

        self.assertTrue(multi_bucket.exists("test.txt"))

    def test_exists_false_when_not_in_any_bucket(self):
        """Test exists returns False when object is not in any bucket"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        self.assertFalse(multi_bucket.exists("nonexistent.txt"))

    def test_shallow_list_objects_merges_from_all_buckets(self):
        """Test shallow_list_objects merges results from all buckets"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()
        bucket3 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2, bucket3], timeout_sec=1.0)

        # Put different objects in different buckets
        bucket1.put_object("dir1/file1.txt", b"content1")
        bucket2.put_object("dir1/file2.txt", b"content2")
        bucket3.put_object("dir1/file3.txt", b"content3")

        # Also put some in subdirectories
        bucket1.put_object("dir1/subdir/file4.txt", b"content4")

        result = multi_bucket.shallow_list_objects("dir1/")

        # Should have all 3 files from dir1
        self.assertEqual(3, len(result.objects))
        object_names = {obj.name for obj in result.objects}
        self.assertIn("file1.txt", object_names)
        self.assertIn("file2.txt", object_names)
        self.assertIn("file3.txt", object_names)

        # Should have subdir prefix
        self.assertEqual(1, len(result.prefixes))
        self.assertIn("dir1/subdir/", result.prefixes)

    def test_shallow_list_objects_empty_when_no_objects(self):
        """Test shallow_list_objects returns empty when no objects match"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        result = multi_bucket.shallow_list_objects("nonexistent")

        self.assertEqual(0, len(result.objects))
        self.assertEqual(0, len(result.prefixes))

    def test_copy_object_from_success(self):
        """Test copy_object_from copies from source bucket to all destination buckets"""
        src_bucket = MemoryBucket()
        dst_bucket1 = MemoryBucket()
        dst_bucket2 = MemoryBucket()

        multi_bucket = BackupMultiBucket([dst_bucket1, dst_bucket2], timeout_sec=1.0)

        content = b"Source content" * 1000
        src_bucket.put_object("source.txt", content)

        multi_bucket.copy_object_from(src_bucket, "source.txt", "destination.txt")

        # Verify both destination buckets have the content
        self.assertEqual(content, dst_bucket1.get_object("destination.txt"))
        self.assertEqual(content, dst_bucket2.get_object("destination.txt"))

    def test_copy_object_from_source_not_found(self):
        """Test copy_object_from raises FileNotFoundError when source doesn't exist"""
        src_bucket = MemoryBucket()
        dst_bucket = MemoryBucket()

        multi_bucket = BackupMultiBucket([dst_bucket], timeout_sec=1.0)

        with self.assertRaises(FileNotFoundError) as ctx:
            multi_bucket.copy_object_from(src_bucket, "nonexistent.txt", "destination.txt")

        self.assertIn("Source file not found", str(ctx.exception))

    def test_copy_object_from_skip_existing_same_size(self):
        """Test copy_object_from skips when destination already has same size"""
        src_bucket = MemoryBucket()
        dst_bucket1 = MemoryBucket()
        dst_bucket2 = MemoryBucket()

        multi_bucket = BackupMultiBucket([dst_bucket1, dst_bucket2], timeout_sec=1.0)

        content = b"Source content" * 1000
        src_bucket.put_object("source.txt", content)

        # Pre-populate destination
        dst_bucket1.put_object("destination.txt", content)
        dst_bucket2.put_object("destination.txt", content)

        # Should not raise and should skip
        multi_bucket.copy_object_from(src_bucket, "source.txt", "destination.txt")

        # Verify content is still there
        self.assertEqual(content, dst_bucket1.get_object("destination.txt"))
        self.assertEqual(content, dst_bucket2.get_object("destination.txt"))

    def test_not_implemented_methods(self):
        """Test that NotImplementedError methods raise correctly"""
        bucket = MemoryBucket()
        multi_bucket = BackupMultiBucket([bucket], timeout_sec=1.0)

        with self.assertRaises(NotImplementedError):
            multi_bucket.put_object("test.txt", b"content")

        with self.assertRaises(NotImplementedError):
            multi_bucket.get_size("test.txt")

        with self.assertRaises(NotImplementedError):
            multi_bucket.list_objects()

        with self.assertRaises(NotImplementedError):
            multi_bucket.remove_objects(["test.txt"])


class TestBackupMultiBucketBoundaryConditions(TestCase):
    """Tests for boundary conditions and edge cases"""

    def test_empty_bucket_list_put_object_stream(self):
        """Test put_object_stream with empty bucket list raises ExceptionGroup"""
        multi_bucket = BackupMultiBucket([], timeout_sec=1.0)

        content = b"Hello, World!"
        stream = BytesIO(content)

        # Should raise because no buckets available - but what exception?
        # Looking at implementation: if no active writers after open_write loop, raises exceptions
        # With empty list, exceptions list is empty, so _raise_exc_if_fail([]) does nothing
        # Then stream context enters, but no writers exist, so assertion fails
        with self.assertRaises(AssertionError) as ctx:
            multi_bucket.put_object_stream("test.txt", stream)

        self.assertIn("Should have at least one active writer", str(ctx.exception))

    def test_empty_bucket_list_get_object(self):
        """Test get_object with empty bucket list raises assertion"""
        multi_bucket = BackupMultiBucket([], timeout_sec=1.0)

        # With empty bucket list, last_not_found and last_exception remain None
        # Line 134: assert last_exception is not None - will fail
        with self.assertRaises(AssertionError):
            multi_bucket.get_object("test.txt")

    def test_empty_bucket_list_exists(self):
        """Test exists with empty bucket list returns False"""
        multi_bucket = BackupMultiBucket([], timeout_sec=1.0)

        # With empty bucket list, loop doesn't execute, last_exc is None, returns False
        result = multi_bucket.exists("test.txt")
        self.assertFalse(result)

    def test_empty_bucket_list_shallow_list_objects(self):
        """Test shallow_list_objects with empty bucket list raises assertion"""
        multi_bucket = BackupMultiBucket([], timeout_sec=1.0)

        # at_least_one_bucket remains False, line 170: assert last_exc is not None - will fail
        with self.assertRaises(AssertionError):
            multi_bucket.shallow_list_objects("prefix/")

    def test_single_bucket_success(self):
        """Test single bucket (N=1) works correctly for all operations"""
        bucket = MemoryBucket()
        multi_bucket = BackupMultiBucket([bucket], timeout_sec=1.0)

        content = b"Single bucket content"
        stream = BytesIO(content)

        multi_bucket.put_object_stream("test.txt", stream)
        self.assertEqual(content, bucket.get_object("test.txt"))
        self.assertEqual(content, multi_bucket.get_object("test.txt"))
        self.assertTrue(multi_bucket.exists("test.txt"))

    def test_single_bucket_failure_raises_single_exception(self):
        """Test single bucket failure raises single exception, not ExceptionGroup"""

        class FailingBucket(MemoryBucket):
            def open_write(self, name: PurePosixPath | str, timeout_sec: float | None = None):
                raise RuntimeError("Single bucket failure")

        bucket = FailingBucket()
        multi_bucket = BackupMultiBucket([bucket], timeout_sec=1.0)

        content = b"Content"
        stream = BytesIO(content)

        # With single bucket, should raise RuntimeError directly, not ExceptionGroup
        with self.assertRaises(RuntimeError) as ctx:
            multi_bucket.put_object_stream("test.txt", stream)

        self.assertEqual("Single bucket failure", str(ctx.exception))

    def test_put_object_stream_exact_buffer_size(self):
        """Test put_object_stream with content exactly equal to buffer size (5MB)"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()
        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        # Exactly 5MB - should be read in one chunk
        content = b"x" * (5 * 1024 * 1024)
        stream = BytesIO(content)

        multi_bucket.put_object_stream("exact.bin", stream)

        self.assertEqual(content, bucket1.get_object("exact.bin"))
        self.assertEqual(content, bucket2.get_object("exact.bin"))

    def test_put_object_stream_buffer_size_plus_one(self):
        """Test put_object_stream with content = buffer size + 1 byte"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()
        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        # 5MB + 1 byte - should be read in two chunks
        content = b"x" * (5 * 1024 * 1024 + 1)
        stream = BytesIO(content)

        multi_bucket.put_object_stream("plus_one.bin", stream)

        self.assertEqual(content, bucket1.get_object("plus_one.bin"))
        self.assertEqual(content, bucket2.get_object("plus_one.bin"))

    def test_put_object_stream_empty_content(self):
        """Test put_object_stream with empty (0-byte) content"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()
        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        content = b""
        stream = BytesIO(content)

        multi_bucket.put_object_stream("empty.bin", stream)

        self.assertEqual(content, bucket1.get_object("empty.bin"))
        self.assertEqual(content, bucket2.get_object("empty.bin"))

    def test_fput_object_empty_file(self):
        """Test fput_object with empty (0-byte) file"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()
        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
            tmp_path = Path(tmp.name)

        try:
            multi_bucket.fput_object("empty.txt", tmp_path)

            self.assertEqual(b"", bucket1.get_object("empty.txt"))
            self.assertEqual(b"", bucket2.get_object("empty.txt"))
        finally:
            tmp_path.unlink()


class TestBackupMultiBucketErrorScenarios(TestCase):
    """Tests for error scenarios and exception handling paths"""

    class ExceptionRaisingBucket(MemoryBucket):
        """Bucket that raises custom exceptions for testing"""

        def __init__(self, exception_to_raise: Exception | None = None):
            super().__init__()
            self.exception_to_raise = exception_to_raise

        def get_object(self, name: PurePosixPath | str) -> bytes:
            if self.exception_to_raise:
                raise self.exception_to_raise
            return super().get_object(name)

        def exists(self, name: PurePosixPath | str) -> bool:
            if self.exception_to_raise:
                raise self.exception_to_raise
            return super().exists(name)

        def shallow_list_objects(self, prefix: PurePosixPath | str = "") -> ShallowListing:
            if self.exception_to_raise:
                raise self.exception_to_raise
            return super().shallow_list_objects(prefix)

    def test_get_object_all_buckets_raise_non_file_not_found(self):
        """Test get_object when all buckets raise non-FileNotFoundError exceptions"""
        bucket1 = self.ExceptionRaisingBucket(RuntimeError("Connection error 1"))
        bucket2 = self.ExceptionRaisingBucket(RuntimeError("Connection error 2"))
        bucket3 = self.ExceptionRaisingBucket(RuntimeError("Connection error 3"))

        multi_bucket = BackupMultiBucket([bucket1, bucket2, bucket3], timeout_sec=1.0)

        # Should raise the last exception (line 135)
        with self.assertRaises(RuntimeError) as ctx:
            multi_bucket.get_object("test.txt")

        self.assertEqual("Connection error 3", str(ctx.exception))

    def test_get_object_mixed_exceptions_prefers_file_not_found(self):
        """Test get_object with mix of exceptions prefers FileNotFoundError"""
        bucket1 = self.ExceptionRaisingBucket(RuntimeError("Connection error"))
        bucket2 = self.ExceptionRaisingBucket(FileNotFoundError("Not found"))
        bucket3 = self.ExceptionRaisingBucket(RuntimeError("Another error"))

        multi_bucket = BackupMultiBucket([bucket1, bucket2, bucket3], timeout_sec=1.0)

        # Should raise FileNotFoundError (line 133)
        with self.assertRaises(FileNotFoundError) as ctx:
            multi_bucket.get_object("test.txt")

        self.assertEqual("Not found", str(ctx.exception))

    def test_shallow_list_objects_all_buckets_fail(self):
        """Test shallow_list_objects when all buckets raise exceptions"""
        bucket1 = self.ExceptionRaisingBucket(RuntimeError("List error 1"))
        bucket2 = self.ExceptionRaisingBucket(RuntimeError("List error 2"))

        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        # Should raise the last exception (line 171)
        with self.assertRaises(RuntimeError) as ctx:
            multi_bucket.shallow_list_objects("prefix/")

        self.assertEqual("List error 2", str(ctx.exception))

    def test_exists_all_buckets_raise_exceptions(self):
        """Test exists when all buckets raise exceptions"""
        bucket1 = self.ExceptionRaisingBucket(RuntimeError("Exists error 1"))
        bucket2 = self.ExceptionRaisingBucket(RuntimeError("Exists error 2"))

        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=1.0)

        # Should raise the last exception (line 186)
        with self.assertRaises(RuntimeError) as ctx:
            multi_bucket.exists("test.txt")

        self.assertEqual("Exists error 2", str(ctx.exception))

    def test_fput_object_some_buckets_matching_size_some_different(self):
        """Test fput_object when some buckets have matching size, others have different size"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()
        bucket3 = MemoryBucket()

        multi_bucket = BackupMultiBucket([bucket1, bucket2, bucket3], timeout_sec=1.0)

        # Pre-populate bucket1 with matching size, bucket2 with different size
        matching_content = b"File content" * 1000
        bucket1.put_object("test.txt", matching_content)
        bucket2.put_object("test.txt", b"Different size content")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
            tmp.write(matching_content)
            tmp_path = Path(tmp.name)

        try:
            # Should raise FileExistsError when checking bucket2
            with self.assertRaises(FileExistsError) as ctx:
                multi_bucket.fput_object("test.txt", tmp_path)

            self.assertIn("already exists with different size", str(ctx.exception))
        finally:
            tmp_path.unlink()

    def test_copy_object_from_some_buckets_matching_size_some_different(self):
        """Test copy_object_from when some buckets have matching size, others have different size"""
        src_bucket = MemoryBucket()
        dst_bucket1 = MemoryBucket()
        dst_bucket2 = MemoryBucket()
        dst_bucket3 = MemoryBucket()

        multi_bucket = BackupMultiBucket([dst_bucket1, dst_bucket2, dst_bucket3], timeout_sec=1.0)

        content = b"Source content" * 1000
        src_bucket.put_object("source.txt", content)

        # Pre-populate dst_bucket1 with matching size, dst_bucket2 with different size
        dst_bucket1.put_object("destination.txt", content)
        dst_bucket2.put_object("destination.txt", b"Different")

        # Should raise FileExistsError when checking dst_bucket2
        with self.assertRaises(FileExistsError) as ctx:
            multi_bucket.copy_object_from(src_bucket, "source.txt", "destination.txt")

        self.assertIn("already exists with different size", str(ctx.exception))


class TestBackupMultiBucketConcurrency(TestCase):
    """Tests for concurrent access and thread safety"""

    def test_concurrent_put_object_stream_different_files(self):
        """Test concurrent writes to different files work correctly"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()
        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=5.0)

        num_threads = 5
        file_size = 1024 * 1024  # 1MB per file
        exceptions = []
        results = {}

        def write_file(file_index: int):
            try:
                content = bytes([file_index % 256]) * file_size
                stream = BytesIO(content)
                file_name = f"file_{file_index}.bin"
                multi_bucket.put_object_stream(file_name, stream)
                results[file_index] = content
            except Exception as e:
                exceptions.append(e)

        threads = [threading.Thread(target=write_file, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify no exceptions occurred
        self.assertEqual(0, len(exceptions), f"Concurrent writes failed: {exceptions}")

        # Verify all files were written correctly to both buckets
        for i in range(num_threads):
            file_name = f"file_{i}.bin"
            expected_content = results[i]
            self.assertEqual(expected_content, bucket1.get_object(file_name))
            self.assertEqual(expected_content, bucket2.get_object(file_name))

    def test_concurrent_get_object_same_file(self):
        """Test concurrent reads of the same file work correctly"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()
        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=5.0)

        content = b"Shared content" * 10000
        bucket1.put_object("shared.txt", content)

        num_threads = 10
        exceptions = []
        results = []

        def read_file():
            try:
                data = multi_bucket.get_object("shared.txt")
                results.append(data)
            except Exception as e:
                exceptions.append(e)

        threads = [threading.Thread(target=read_file) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify no exceptions occurred
        self.assertEqual(0, len(exceptions), f"Concurrent reads failed: {exceptions}")

        # Verify all reads returned correct content
        self.assertEqual(num_threads, len(results))
        for data in results:
            self.assertEqual(content, data)

    def test_concurrent_mixed_operations(self):
        """Test concurrent mix of read/write/exists operations"""
        bucket1 = MemoryBucket()
        bucket2 = MemoryBucket()
        multi_bucket = BackupMultiBucket([bucket1, bucket2], timeout_sec=5.0)

        # Pre-populate some files
        for i in range(5):
            bucket1.put_object(f"existing_{i}.txt", f"Content {i}".encode())

        num_threads = 15
        exceptions = []
        operation_counts = {"write": 0, "read": 0, "exists": 0}
        lock = threading.Lock()

        def mixed_operations(thread_id: int):
            try:
                op_type = thread_id % 3
                if op_type == 0:  # Write
                    content = f"Thread {thread_id} content".encode()
                    stream = BytesIO(content)
                    multi_bucket.put_object_stream(f"new_{thread_id}.txt", stream)
                    with lock:
                        operation_counts["write"] += 1
                elif op_type == 1:  # Read
                    file_index = thread_id % 5
                    multi_bucket.get_object(f"existing_{file_index}.txt")
                    with lock:
                        operation_counts["read"] += 1
                else:  # Exists
                    file_index = thread_id % 5
                    multi_bucket.exists(f"existing_{file_index}.txt")
                    with lock:
                        operation_counts["exists"] += 1
            except Exception as e:
                exceptions.append(e)

        threads = [threading.Thread(target=mixed_operations, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify no exceptions occurred
        self.assertEqual(0, len(exceptions), f"Concurrent mixed operations failed: {exceptions}")

        # Verify operation counts
        self.assertEqual(5, operation_counts["write"])
        self.assertEqual(5, operation_counts["read"])
        self.assertEqual(5, operation_counts["exists"])

        # Verify written files exist in both buckets
        for i in range(0, num_threads, 3):  # Every 3rd thread wrote
            file_name = f"new_{i}.txt"
            expected_content = f"Thread {i} content".encode()
            self.assertEqual(expected_content, bucket1.get_object(file_name))
            self.assertEqual(expected_content, bucket2.get_object(file_name))
