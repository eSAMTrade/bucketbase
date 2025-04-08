import tempfile
import threading
import time
import unittest
from io import BytesIO
from pathlib import Path

from bucketbase.fs_bucket import AppendOnlyFSBucket, FSBucket

from bucketbase import MemoryBucket


class TestAppendOnlyFSBucket(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for lock files
        self.temp_dir = tempfile.TemporaryDirectory()

        base_bucket_path = Path(self.temp_dir.name)
        self.locks_path = base_bucket_path / FSBucket.BUCKETBASE_TMP_DIR_NAME / "__locks__"
        self.base_bucket = MemoryBucket()

    def tearDown(self):
        # Cleanup the temporary directory
        self.temp_dir.cleanup()

    def test_put_object_creates_lock(self):
        base_bucket_put_calls = []
        object_name = "dir1/dir2/test_object"
        lock_file_path = self.locks_path / (object_name.replace(AppendOnlyFSBucket.SEP, FSBucket.TEMP_SEP) + ".lock")
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
        bucket_in_test = AppendOnlyFSBucket(mock_base_bucket, self.locks_path)

        # put object is expected to create a lock file before calling base_bucket.put_object, and remove it after
        bucket_in_test.put_object(object_name, content)

        self.assertFalse(lock_file_path.exists())
        self.assertEqual(base_bucket_put_calls, [(object_name, content)])

    def test_put_object_twice_raises_exception(self):
        bucket_in_test = AppendOnlyFSBucket(self.base_bucket, self.locks_path)
        object_name = "dir1/dir2/test_object"
        content = b"test content"
        bucket_in_test.put_object(object_name, content)
        with self.assertRaises(IOError):
            bucket_in_test.put_object(object_name, content)
        with self.assertRaises(IOError):
            bucket_in_test.put_object_stream(object_name, content)

    def test_put_object_stream_twice_raises_exception(self):
        bucket_in_test = AppendOnlyFSBucket(self.base_bucket, self.locks_path)
        object_name = "dir1/dir2/test_object"
        content = b"test content"
        stream = BytesIO(content)
        bucket_in_test.put_object_stream(object_name, stream)
        with self.assertRaises(IOError):
            bucket_in_test.put_object(object_name, content)
        with self.assertRaises(IOError):
            bucket_in_test.put_object_stream(object_name, stream)

    def test_put_objects_on_existing_object_raises_exception(self):
        bucket_in_test = AppendOnlyFSBucket(self.base_bucket, self.locks_path)
        object_name = "dir1/dir2/test_object"
        content = b"test content"
        stream = BytesIO(content)
        self.base_bucket.put_object(object_name, content)
        with self.assertRaises(IOError):
            bucket_in_test.put_object(object_name, content)
        with self.assertRaises(IOError):
            bucket_in_test.put_object_stream(object_name, stream)

    def test_lock_object_creates_lock_and_unlock_releases(self):
        bucket_in_test = AppendOnlyFSBucket(self.base_bucket, self.locks_path)
        object_name = "dir1/dir2/test_object"

        # Attempt to lock the object
        bucket_in_test._lock_object(object_name)

        # Check if the lock file was created
        lock_file_path = self.locks_path / (object_name.replace(bucket_in_test.SEP, FSBucket.TEMP_SEP) + ".lock")
        self.assertTrue(lock_file_path.exists())

        bucket_in_test._unlock_object(object_name)
        self.assertFalse(lock_file_path.exists())

    def test_unlocking_unlocked_object_raises_assertion(self):
        bucket_in_test = AppendOnlyFSBucket(self.base_bucket, self.locks_path)
        object_name = "dir1/dir2/non_locked_object"

        # Expect an assertion error when trying to unlock an object that wasn't locked
        with self.assertRaises(RuntimeError) as e:
            bucket_in_test._unlock_object(object_name)
        self.assertEqual(str(e.exception), "Object dir1/dir2/non_locked_object is not locked")

    def test_get_after_put_object(self):
        bucket_in_test = AppendOnlyFSBucket(self.base_bucket, self.locks_path)
        object_name = "dir1/dir2/test_object"
        content = b"test content"
        bucket_in_test.put_object(object_name, content)
        retrieved_content = bucket_in_test.get_object(object_name)
        self.assertEqual(retrieved_content, content)
        obj_stream = bucket_in_test.get_object_stream(object_name)
        with obj_stream as stream:
            retrieved_content_stream_content = stream.read()
        self.assertEqual(retrieved_content_stream_content, content)

    def test_lock_object_with_threads(self):
        bucket_in_test = AppendOnlyFSBucket(self.base_bucket, self.locks_path)
        object_name = "shared_object"
        lock_acquired = [False, False]  # To track lock acquisition in threads

        def lock_and_release_first():
            bucket_in_test._lock_object(object_name)
            lock_acquired[0] = True
            time.sleep(0.1)  # Simulate work by sleeping
            bucket_in_test._unlock_object(object_name)
            lock_acquired[0] = False

        def wait_and_lock_second():
            time.sleep(0.001)  # Ensure this runs after the first thread has acquired the lock
            t1 = time.time()
            bucket_in_test._lock_object(object_name)
            t2 = time.time()
            print(f"Time taken to acquire lock: {t2 - t1}")
            self.assertTrue(t2 - t1 > 0.1, "The second thread should have waited for the first thread to release the lock")
            lock_acquired[1] = True  # Should only reach here after the first thread releases the lock
            bucket_in_test._unlock_object(object_name)

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

        with self.assertRaises(FileNotFoundError):
            bucket.get_size("non_existent_object")

        bucket.put_object(object_name, content)

        size = bucket.get_size(object_name)
        self.assertEqual(size, len(content))

    def test_exists(self):
        bucket_in_test = AppendOnlyFSBucket(self.base_bucket, self.locks_path)
        object_name = "test_object"
        content = b"test content"
        self.assertFalse(bucket_in_test.exists(object_name))
        self.base_bucket.put_object(object_name, content)
        self.assertTrue(bucket_in_test.exists(object_name))
