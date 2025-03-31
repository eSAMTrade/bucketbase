import unittest
import tempfile
import threading
import time
from pathlib import Path
import concurrent.futures

from bucketbase.namedlock import ThreadLockManager, FileLockManager


class ThreadLockManagerTests(unittest.TestCase):
    def setUp(self):
        self.lock_manager = ThreadLockManager()

    def test_get_lock_returns_same_lock_for_same_name(self):
        lock1 = self.lock_manager.get_lock("test_lock")
        lock2 = self.lock_manager.get_lock("test_lock")
        self.assertIs(lock1, lock2)

    def test_get_lock_returns_different_locks_for_different_names(self):
        lock1 = self.lock_manager.get_lock("test_lock1")
        lock2 = self.lock_manager.get_lock("test_lock2")
        self.assertIsNot(lock1, lock2)

    def test_lock_actually_locks(self):
        lock = self.lock_manager.get_lock("test_lock")
        shared_counter = [0]
        num_threads = 10
        iterations = 1000

        def increment():
            for _ in range(iterations):
                with lock:
                    value = shared_counter[0]
                    time.sleep(0.0000001)
                    shared_counter[0] = value + 1

        threads = []
        for _ in range(num_threads):
            thread = threading.Thread(target=increment)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        self.assertEqual(shared_counter[0], num_threads * iterations)

    def test_concurrent_lock_access(self):
        # Test that multiple threads can get the same lock safely
        def get_lock():
            return self.lock_manager.get_lock("concurrent_test_lock")

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            locks = list(executor.map(lambda _: get_lock(), range(10)))

        self.assertEqual(len(locks), 10)
        # All locks should be the same object
        for lock in locks:
            self.assertIs(lock, locks[0])


class FileLockManagerTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.lock_manager = FileLockManager(self.temp_dir)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_get_lock_returns_same_lock_for_same_name(self):
        lock1 = self.lock_manager.get_lock("test_lock")
        lock2 = self.lock_manager.get_lock("test_lock")
        self.assertIs(lock1, lock2)

    def test_get_lock_returns_different_locks_for_different_names(self):
        lock1 = self.lock_manager.get_lock("test_lock1")
        lock2 = self.lock_manager.get_lock("test_lock2")
        self.assertIsNot(lock1, lock2)

    def test_sanitizes_path_characters(self):
        # These should map to the same lock file
        name1 = "path/with/slashes"
        name2 = "path\\with\\backslashes"

        lock1 = self.lock_manager.get_lock(name1)
        lock2 = self.lock_manager.get_lock(name2)

        # The locks should be different objects because the names are different
        self.assertIsNot(lock1, lock2)

        lock1.acquire()
        lock2.acquire()

        # Verify both lock files were created with sanitized names
        files = list(self.temp_dir.glob("*"))
        sanitized_names = [f.name for f in files]

        self.assertIn("path#with#slashes.lock", sanitized_names)
        self.assertIn("path#with#backslashes.lock", sanitized_names)
        lock1.release()
        lock2.release()

        self.assertEqual(0, len(list(self.temp_dir.glob("*"))))


    def test_lock_directory_created(self):
        # Delete the directory to test creation
        import shutil
        shutil.rmtree(self.temp_dir)

        # Re-create the manager, which should recreate the directory
        self.lock_manager = FileLockManager(self.temp_dir)

        self.assertTrue(self.temp_dir.exists())
        self.assertTrue(self.temp_dir.is_dir())