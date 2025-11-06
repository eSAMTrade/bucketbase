# mypy: disable-error-code="no-untyped-def"
import gc
import os
import threading
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any, BinaryIO
from unittest import TestCase

import psutil
from minio import Minio

from bucketbase import MinioBucket
from bucketbase.backup_multi_bucket import BackupMultiBucket


class TestBackupMultiBucketMemory(TestCase):

    @staticmethod
    def _get_current_process_memory_MB() -> float:
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024

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
        test_name = PurePosixPath("test.bin")

        class MockMinioClient(Minio):

            def __init__(self, do_fail: bool):
                self._do_fail = do_fail
                self._chunks_written = [0]

            def put_object(self, bucket_name: str, object_name: str, data: BinaryIO, length: int, **kwargs: Any) -> None:
                if self._do_fail:
                    while data.read(part_size):
                        self._chunks_written[0] += 1
                        if self._chunks_written[0] > 1:
                            raise TimeoutError("test: timeout error after 1 chunk")
                else:
                    while data.read(part_size):
                        pass
                    return None

        mock_client_success = MockMinioClient(do_fail=False)
        mock_minio_timeout = MockMinioClient(do_fail=True)

        bucket_success = MinioBucket("test-bucket-success", mock_client_success)
        bucket_timeout = MinioBucket("test-bucket-timeout", mock_minio_timeout)

        multi_bucket = BackupMultiBucket([bucket_success, bucket_timeout], timeout_sec=0.5)

        num_retries = 5
        gc.collect()
        baseline_memory = self._get_current_process_memory_MB()
        memory_samples = [("baseline", baseline_memory)]

        for i in range(1, num_retries + 1):
            with self.assertRaises(TimeoutError):
                current_memory = self._get_current_process_memory_MB()
                print(f"  retry_{i}: {current_memory:6.1f}MB (growth: {current_memory - baseline_memory:+6.1f}MB)")
                stream = BytesIO(file_content)
                multi_bucket.put_object_stream(test_name, stream)

            gc.collect()
            current_memory = self._get_current_process_memory_MB()
            memory_samples.append((f"retry_{i}", current_memory))

        active_threads = threading.enumerate()
        self.assertEqual(1, len(active_threads))

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
