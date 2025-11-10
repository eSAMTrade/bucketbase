import io
import logging
import tempfile
import threading
from pathlib import Path, PurePosixPath
from unittest import TestCase
from unittest.mock import MagicMock

from bucketbase import (
    AppendOnlyFSBucket,
    CachedImmutableBucket,
    FSBucket,
    IBucket,
    MemoryBucket,
)
from bucketbase.ibucket import AbstractAppendOnlySynchronizedBucket
from bucketbase.named_lock_manager import ThreadLockManager
from tests.bucket_tester import IBucketTester

loggerr = logging.getLogger(__name__)
logging.basicConfig(level=logging.ERROR, format="%(message)s")


class SynchronizedMockMemoryBucket(AbstractAppendOnlySynchronizedBucket):
    def __init__(self, base_bucket: IBucket):
        super().__init__(base_bucket)
        self._lock_manager = ThreadLockManager()

    def _lock_object(self, name: PurePosixPath | str) -> None:
        name = str(name)
        lock = self._lock_manager.get_lock(name)
        lock.acquire()

    def _unlock_object(self, name: PurePosixPath | str) -> None:
        name = str(name)
        lock = self._lock_manager.get_lock(name, only_existing=True)
        lock.release()


class TestIntegratedCachedImmutableBucket(TestCase):
    def setUp(self) -> None:
        self.cache = SynchronizedMockMemoryBucket(MemoryBucket())
        self.main = MemoryBucket()
        self.bucket_in_test = CachedImmutableBucket(self.cache, self.main)
        self.tester = IBucketTester(self.bucket_in_test, self)

    def test_get_object_content_happy_path(self):
        """
        Here we test that the object is retrieved from the main storage, and then cached into the cache.
        We test that initially the object is not in the cache, but it is in the main storage.
        Then we retrieve the object, and check that it is in the cache.
        Then we remove the object from the main storage, and check that it is still in the cache, and that it can be retrieved from the storage in test.
        """
        unique_dir = f"dir{self.tester.us}"
        # binary content
        path = PurePosixPath(f"{unique_dir}/file1.bin")
        b_content = b"Test content"
        self.main.put_object(path, b_content)

        self.assertFalse(self.cache.exists(path))
        self.assertTrue(self.bucket_in_test.exists(path))
        retrieved_content = self.bucket_in_test.get_object(path)
        self.assertEqual(retrieved_content, b_content)
        self.assertTrue(self.bucket_in_test.exists(path))
        self.assertTrue(self.cache.exists(path))

        list_results = self.bucket_in_test.list_objects("")
        self.assertEqual(list_results, [path])
        self.main.remove_objects([path])
        self.assertEqual(self.bucket_in_test.list_objects(""), [])
        self.assertRaises(FileNotFoundError, self.main.get_object, path)

        retrieved_content = self.bucket_in_test.get_object(path)
        self.assertEqual(retrieved_content, b_content)

    def test_get_object_stream_happy_path(self):
        """
        Here we test that the object is retrieved from the main storage, and then cached into the cache.
        We test that initially the object is not in the cache, but it is in the main storage.
        Then we retrieve the object, and check that it is in the cache.
        Then we remove the object from the main storage, and check that it is still in the cache, and that it can be retrieved from the storage in test.
        """
        unique_dir = f"dir{self.tester.us}"
        # binary content
        path = PurePosixPath(f"{unique_dir}/file1.bin")
        b_content = b"Test content"
        self.main.put_object(path, b_content)

        self.assertFalse(self.cache.exists(path))
        self.assertTrue(self.bucket_in_test.exists(path))
        with self.bucket_in_test.get_object_stream(path) as stream:
            retrieved_content = stream.read()
        self.assertEqual(retrieved_content, b_content)
        self.assertTrue(self.bucket_in_test.exists(path))
        self.assertTrue(self.cache.exists(path))

        list_results = self.bucket_in_test.list_objects("")
        self.assertEqual(list_results, [path])
        self.main.remove_objects([path])
        self.assertEqual(self.bucket_in_test.list_objects(""), [])
        self.assertRaises(FileNotFoundError, self.main.get_object, path)

        with self.bucket_in_test.get_object_stream(path) as stream:
            retrieved_content = stream.read()
        self.assertEqual(retrieved_content, b_content)

    def test_putobject(self):
        self.assertRaises(io.UnsupportedOperation, self.bucket_in_test.put_object, "test", "test")

    def test_list_objects(self):
        cache = MagicMock(spec=AbstractAppendOnlySynchronizedBucket)
        cache.list_objects.return_value = ["cache_list"]
        main = MagicMock(spec=IBucket)
        main.list_objects.return_value = ["main_list"]
        storage = CachedImmutableBucket(cache, main)
        result = storage.list_objects("test")
        self.assertEqual(result, ["main_list"])
        cache.list_objects.assert_not_called()

    def test_shallow_list_objects(self):
        cache = MagicMock(spec=AbstractAppendOnlySynchronizedBucket)
        cache.shallow_list_objects.return_value = ["cache_list"]
        main = MagicMock(spec=IBucket)
        main.shallow_list_objects.return_value = ["main_list"]
        storage = CachedImmutableBucket(cache, main)
        result = storage.shallow_list_objects("test")
        self.assertEqual(result, ["main_list"])
        cache.shallow_list_objects.assert_not_called()

    def test_exists(self):
        cache = MagicMock(spec=AbstractAppendOnlySynchronizedBucket)
        main = MagicMock(spec=IBucket)
        storage = CachedImmutableBucket(cache, main)

        main.exists.return_value = False
        cache.exists.return_value = True
        self.assertTrue(storage.exists("test"))

        main.exists.return_value = True
        cache.exists.return_value = False
        self.assertTrue(storage.exists("test"))

        main.exists.return_value = False
        cache.exists.return_value = False
        self.assertFalse(storage.exists("test"))

    def test_remove_objects(self):
        self.assertRaises(io.UnsupportedOperation, self.bucket_in_test.remove_objects, ["test"])

    def test_get_size(self):
        # due to assert_called_once_with, we need to reinit each one.
        # this could be written as different test functions, but this is more concise, IMO
        with self.subTest("local"):
            cache = MagicMock(spec=AbstractAppendOnlySynchronizedBucket)
            main = MagicMock(spec=IBucket)
            storage = CachedImmutableBucket(cache, main)

            cache.get_size.return_value = 10

            self.assertEqual(storage.get_size("test"), 10)
            cache.get_size.assert_called_once_with("test")
            main.get_size.assert_not_called()

        with self.subTest("remote-only"):
            cache = MagicMock(spec=AbstractAppendOnlySynchronizedBucket)
            main = MagicMock(spec=IBucket)
            storage = CachedImmutableBucket(cache, main)

            cache.get_size.side_effect = FileNotFoundError
            main.get_size.return_value = 200

            self.assertEqual(storage.get_size("test"), 200)
            cache.get_size.assert_called_once_with("test")
            main.get_size.assert_called_once_with("test")

        with self.subTest("non-existent"):
            cache = MagicMock(spec=AbstractAppendOnlySynchronizedBucket)
            main = MagicMock(spec=IBucket)
            storage = CachedImmutableBucket(cache, main)

            cache.get_size.side_effect = FileNotFoundError
            main.get_size.side_effect = FileNotFoundError

            with self.assertRaises(FileNotFoundError):
                storage.get_size("test")
            cache.get_size.assert_called_once_with("test")
            main.get_size.assert_called_once_with("test")

    def test_put_not_allowed(self):
        # Test that put_object raises an exception
        with self.assertRaises(io.UnsupportedOperation):
            self.bucket_in_test.put_object("test", b"content")

        # Test that put_object_stream raises an exception
        with self.assertRaises(io.UnsupportedOperation):
            self.bucket_in_test.put_object_stream("test", io.BytesIO(b"content"))

    def test_build_from_fs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_cache_dir = Path(temp_dir) / "cache"
            main_dir = Path(temp_dir) / "main"
            main_dir.mkdir(parents=True, exist_ok=True)

            # put some files/dirs in the path
            (main_dir / "dir").mkdir(parents=True)
            (main_dir / "dir-no-files--ignored").mkdir(parents=True)
            (main_dir / "file1.txt").write_bytes(b"test content /file1.txt")
            (main_dir / "dir" / "file2.txt").write_bytes(b"test content /dir/file2.txt")

            cache_root = Path(temp_cache_dir)
            main = FSBucket(main_dir)

            cached_bucket = CachedImmutableBucket.build_from_fs(cache_root, main)
            # at first - no files in the cache
            lst = [x for x in cache_root.iterdir() if not str(x).endswith(main.BUCKETBASE_TMP_DIR_NAME)]
            self.assertEqual(lst, [])

            cached_bucket.get_object("dir/file2.txt")
            self.assertTrue(cached_bucket._cache.exists("dir/file2.txt"))
            self.assertTrue((cache_root / "dir" / "file2.txt").exists())
            on_disk_cache = sorted(str(Path(x).relative_to(cache_root)) for x in cache_root.iterdir())
            self.assertEqual(on_disk_cache, [FSBucket.BUCKETBASE_TMP_DIR_NAME, "dir"])

            self.assertEqual(list([str(x) for x in cached_bucket.list_objects("")]), ["file1.txt", "dir/file2.txt"])

    def test_cachedappendonly_one_fetch_from_main_bucket(self):
        # Test concurrent access to CachedImmutableBucket to verify object is only fetched once from main bucket
        num_threads = 29

        obj_name = "concurrent_test_object"
        content = b"test concurrent content"

        # Create a mock main bucket to track calls
        get_object_calls = []

        class MockMainBucket(MemoryBucket):
            def get_object(self, name):
                get_object_calls.append(name)
                return super().get_object(name)

        mock_main = MockMainBucket()
        mock_main.put_object(obj_name, content)

        base_bucket = MemoryBucket()
        with tempfile.TemporaryDirectory() as fs_dir:
            cache_bucket = AppendOnlyFSBucket(base_bucket, locks_path=Path(fs_dir) / "locks")
            bucket_in_test = CachedImmutableBucket(cache=cache_bucket, main=mock_main)

            results_lock = threading.Lock()
            results = []
            threads = []

            def get_object_from_thread():
                result = bucket_in_test.get_object(obj_name)
                with results_lock:
                    results.append(result)

            for _ in range(num_threads):
                thread = threading.Thread(target=get_object_from_thread)
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join(timeout=2.0)
                self.assertFalse(thread.is_alive(), "Thread did not complete within timeout")

            # Verify results
            self.assertEqual(len(get_object_calls), 1, "Main bucket's get_object should be called exactly once")
            self.assertEqual(len(results), num_threads, "All threads should have retrieved the content")
            for result in results:
                self.assertEqual(result, content, "All threads should get the same content")
