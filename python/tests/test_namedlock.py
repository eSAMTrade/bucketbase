import concurrent.futures
import tempfile
import unittest
from pathlib import Path

from bucketbase.named_lock_manager import FileLockManager, ThreadLockManager


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
        name2 = "path/with/slashes2"
        name_invalid = "path\\with\\backslashes"

        lock1 = self.lock_manager.get_lock(name1)
        lock2 = self.lock_manager.get_lock(name2)

        self.assertRaises(ValueError, self.lock_manager.get_lock, name_invalid, only_existing=True)

        # The locks should be different objects because the names are different
        self.assertIsNot(lock1, lock2)

        lock1.acquire()
        lock2.acquire()

        lock_verifier = FileLockManager(self.temp_dir)
        try:
            # Attempting to acquire already-held locks should timeout, raising TimeoutError
            # (filelock.Timeout is a subclass of TimeoutError).
            with self.assertRaises(TimeoutError):
                lock_verifier.get_lock(name1).acquire(timeout=0.1)
            with self.assertRaises(TimeoutError):
                lock_verifier.get_lock(name2).acquire(timeout=0.1)
        finally:
            lock1.release()
            lock2.release()

        v_lock1 = lock_verifier.get_lock(name1)
        self.assertTrue(v_lock1.acquire(timeout=0.1))
        v_lock1.release()
        v_lock2 = lock_verifier.get_lock(name2)
        self.assertTrue(v_lock2.acquire(timeout=0.1))
        v_lock2.release()

    def test_lock_directory_created(self):
        # Delete the directory to test creation
        import shutil

        shutil.rmtree(self.temp_dir)

        # Re-create the manager, which should recreate the directory
        self.lock_manager = FileLockManager(self.temp_dir)

        self.assertTrue(self.temp_dir.exists())
        self.assertTrue(self.temp_dir.is_dir())

    def test_released_lock_can_be_reacquired_by_new_manager(self):
        name = "some_lock"
        lock1 = self.lock_manager.get_lock(name)
        lock1.acquire()
        lock1.release()

        new_manager = FileLockManager(self.temp_dir)
        lock2 = new_manager.get_lock(name)
        self.assertTrue(lock2.acquire(timeout=0.1))
        lock2.release()


if __name__ == "__main__":
    unittest.main()
