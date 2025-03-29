import tempfile
import threading
import time
import unittest
from pathlib import Path

from bucketbase import MemoryBucket
from bucketbase.fs_bucket import AppendOnlyFSBucket


class TestAppendOnlyFSBucket(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for lock files
        self.temp_dir = tempfile.TemporaryDirectory()

        base_bucket_path = Path(self.temp_dir.name)
        self.locks_path = base_bucket_path / AppendOnlyFSBucket.BUCKETBASE_TMP_DIR_NAME / "__locks__"
        self.base_bucket = MemoryBucket()

    def tearDown(self):
        # Cleanup the temporary directory
        self.temp_dir.cleanup()

    def test_put_object_creates_lock(self):
        base_bucket_put_calls = []
        object_name = "dir1/dir2/test_object"
        lock_file_path = self.locks_path / (object_name.replace(AppendOnlyFSBucket.SEP, "$") + ".lock")
        content = b"test content"

        class MockMemBucket(MemoryBucket):
            def __init__(self):
                super().__init__()

            def put_object(self2, name, content):
                print(f"put_object: {name}")
                self.assertTrue(lock_file_path.exists())
                super().put_object(name, content)
                base_bucket_put_calls.append((name, content))

        mock_base_bucket = MockMemBucket()
        bucket = AppendOnlyFSBucket(mock_base_bucket, self.locks_path)

        # put object is expected to create a lock file before calling base_bucket.put_object, and remove it after
        bucket.put_object(object_name, content)

        self.assertFalse(lock_file_path.exists())
        self.assertEqual(base_bucket_put_calls, [(object_name, content)])

    def test_lock_object_creates_lock(self):
        bucket = AppendOnlyFSBucket(self.base_bucket, self.locks_path)
        object_name = "dir1/dir2/test_object"

        # Attempt to lock the object
        bucket._lock_object(object_name)

        # Check if the lock file was created
        lock_file_path = self.locks_path / (object_name.replace(bucket.SEP, "$") + ".lock")
        self.assertTrue(lock_file_path)
        bucket._unlock_object(object_name)

    def test_unlock_object_releases_lock(self):
        bucket = AppendOnlyFSBucket(self.base_bucket, self.locks_path)
        object_name = "test_object"

        # Lock and then unlock the object
        bucket._lock_object(object_name)
        bucket._unlock_object(object_name)

        # Check if the lock was released (file exists but is unlocked)
        # Note: This might need adjustment based on how FileLockForPath implements release
        lock_file_path = self.locks_path / (object_name.replace(bucket.SEP, "$") + ".lock")
        self.assertFalse(lock_file_path.exists())  # Adjust this assertion based on your lock mechanism

    def test_unlocking_unlocked_object_raises_assertion(self):
        bucket = AppendOnlyFSBucket(self.base_bucket, self.locks_path)
        object_name = "dir1/dir2/non_locked_object"

        # Expect an assertion error when trying to unlock an object that wasn't locked
        with self.assertRaises(RuntimeError) as e:
            bucket._unlock_object(object_name)
        self.assertEqual(str(e.exception), "Object dir1/dir2/non_locked_object is not locked")

    def test_lock_object_with_threads(self):
        bucket = AppendOnlyFSBucket(self.base_bucket, self.locks_path)
        object_name = "shared_object"
        lock_acquired = [False, False]  # To track lock acquisition in threads

        def lock_and_release_first():
            bucket._lock_object(object_name)
            lock_acquired[0] = True
            time.sleep(0.1)  # Simulate work by sleeping
            bucket._unlock_object(object_name)
            lock_acquired[0] = False

        def wait_and_lock_second():
            time.sleep(0.001)  # Ensure this runs after the first thread has acquired the lock
            t1 = time.time()
            bucket._lock_object(object_name)
            t2 = time.time()
            print(f"Time taken to acquire lock: {t2 - t1}")
            self.assertTrue(t2 - t1 > 0.1, "The second thread should have waited for the first thread to release the lock")
            lock_acquired[1] = True  # Should only reach here after the first thread releases the lock
            bucket._unlock_object(object_name)

        # Create threads
        thread1 = threading.Thread(target=lock_and_release_first)
        thread1.start()

        thread2 = threading.Thread(target=wait_and_lock_second)
        thread2.start()

        # Wait for both threads to complete
        thread1.join()
        thread2.join()

        # Verify that both threads were able to acquire the lock
        self.assertFalse(lock_acquired[0], "The first thread should have released the lock")
        self.assertTrue(lock_acquired[1], "The second thread should have acquired the lock after the first thread released it")

    def test_get_size(self):
        bucket = AppendOnlyFSBucket(self.base_bucket, self.locks_path)
        object_name = "test_object"
        content = b"test content"

        bucket.put_object(object_name, content)

        size = bucket.get_size(object_name)
        self.assertEqual(size, len(content))

        with self.assertRaises(FileNotFoundError):
            bucket.get_size(f"non_existent_object")

