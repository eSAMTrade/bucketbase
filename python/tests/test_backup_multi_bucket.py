# mypy: disable-error-code="no-untyped-def"
import gc
import os
import threading
import time
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any, BinaryIO
from unittest import TestCase
from unittest.mock import MagicMock

import psutil
from exceptiongroup import ExceptionGroup

from bucketbase import MinioBucket
from bucketbase.backup_multi_bucket import BackupMultiBucket


class TestBackupMultiBucketMemoryLeakRegression(TestCase):
    """
    Regression test for memory leak in BackupMultiBucket.

    Production issue:
    - File: updates-20251103T135959Z-az2_0__v2_4_2.jsonl.gz (351MB)
    - 108 failed upload attempts over 12 hours
    - Memory grew from 100MB to 2.6GB
    - One bucket (MinIO-backup) timed out repeatedly

    Root cause (confirmed):
    - When signaling timeout in AsyncObjectWriter.__exit__, we passed the traceback
    - Exception traceback holds references to AsyncObjectWriter, preventing garbage collection
    - Memory accumulates: ~5MB per bucket per retry (1 chunk of _DEFAULT_BUF_SIZE)

    """

    @staticmethod
    def _get_memory_mb() -> float:
        """Get current process memory in MB"""
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024

    def test_fput_object_memory_leak_with_minio_timeout(self) -> None:
        """
        Test memory leak using fput_object (as used in production via RawDataStore).

        Production scenario:
        - 2 buckets: one succeeds (Backblaze), one times out (MinIO)

        Behavior:
        - With leak: Memory grows ~5MB per retry (from failing bucket)
        """

        part_size = 5 * 1024 * 1024
        file_size = 3 * part_size
        file_content = b"x" * file_size
        test_name = PurePosixPath("test.bin")

        # Mock Minio client 1: SUCCESS (like Backblaze in production)
        def mock_put_object_success(bucket_name: str, object_name: str, data: BinaryIO, length: int, **kwargs: Any) -> None:
            while data.read(part_size):
                pass
            return None

        mock_client_success = MagicMock()
        mock_client_success.put_object = MagicMock(side_effect=mock_put_object_success)

        # Mock Minio client 2: TIMEOUT (like MinIO in production)
        chunks_written = [0]

        def mock_put_object_raises_timeout(bucket_name: str, object_name: str, data: BinaryIO, length: int, **kwargs: Any) -> None:
            while data.read(part_size):
                chunks_written[0] += 1
                if chunks_written[0] > 1:
                    raise TimeoutError("test: timeout error after 1 chunk")

        mock_client_timeout = MagicMock()
        mock_client_timeout.put_object = MagicMock(side_effect=mock_put_object_raises_timeout)

        bucket_success = MinioBucket("test-bucket-success", mock_client_success)
        bucket_timeout = MinioBucket("test-bucket-timeout", mock_client_timeout)

        # Use BackupMultiBucket (production code path)
        # multi_bucket = BackupMultiBucket([bucket_success, bucket_timeout], timeout_sec=0.5)

        num_retries = 5
        gc.collect()
        baseline_memory = self._get_memory_mb()
        memory_samples = [("baseline", baseline_memory)]

        for i in range(1, num_retries + 1):
            try:
                multi_bucket = BackupMultiBucket([bucket_success, bucket_timeout], timeout_sec=0.5)
                gc.collect()
                current_memory = self._get_memory_mb()
                print(f"  retry_{i}: {current_memory:6.1f}MB (growth: {current_memory - baseline_memory:+6.1f}MB)")
                stream = BytesIO(file_content)
                multi_bucket.put_object_stream(test_name, stream)
            except (TimeoutError, ExceptionGroup):
                pass  # Expected - one bucket times out
            del multi_bucket
            # list all threads heere:
            for thread in threading.enumerate():
                if thread.is_alive():
                    print(
                        "Thread: ",
                        thread.name,
                    )
                else:
                    print("Dead Thread: ", thread.name, " is not alive")
                time.sleep(0.5)

            gc.collect()
            current_memory = self._get_memory_mb()
            memory_samples.append((f"retry_{i}", current_memory))

        final_memory = self._get_memory_mb()
        final_growth = final_memory - baseline_memory

        print(f"\n{'=' * 70}")
        print("FPUT_OBJECT MEMORY LEAK TEST (production scenario)")
        print(f"{'=' * 70}")
        print(f"File: {file_size / 1024 / 1024:.1f}MB × {num_retries} retries × 2 buckets (1 success, 1 timeout)")
        print(f"Total memory growth: {final_growth:.1f}MB ({final_growth / num_retries:.1f}MB per retry)")
        print("")
        for label, memory in memory_samples:
            growth = memory - baseline_memory
            print(f"  {label:10s}: {memory:6.1f}MB (growth: {growth:+6.1f}MB)")
        print(f"Final: {final_memory:6.1f}MB (growth: {final_growth:+6.1f}MB)")
        print("")

        threshold_mb = part_size * (num_retries - 1) / 1024 / 1024
        leaks_present = final_growth > threshold_mb
        self.assertFalse(leaks_present, f"regression: we have mem-leaks; {final_growth:.1f}MB > {threshold_mb:.1f}MB threshold")
        print(f"  [OK] NO LEAK: {final_growth:.1f}MB <= {threshold_mb:.1f}MB threshold")
        print(f"{'=' * 70}\n")
