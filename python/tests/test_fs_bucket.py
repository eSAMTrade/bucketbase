import threading
from io import BytesIO
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from threading import Barrier
from unittest import TestCase

from bucketbase import FSBucket, IBucket
from tests.bucket_tester import IBucketTester
from tests.chunkedstream import ChunkedCallbackStream


class TestFSBucket(TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.temp_dir_path = Path(self.temp_dir.name)
        self.storage = FSBucket(self.temp_dir_path)
        self.tester = IBucketTester(self.storage, self)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_put_and_get_object(self):
        self.tester.test_put_and_get_object()

    def test_put_and_get_object_stream(self):
        self.tester.test_put_and_get_object_stream()

        # check no remaining files in temp dir
        self.assertEqual(0, len(list((self.storage._root / self.storage.BUCKETBASE_TMP_DIR_NAME).iterdir())))

    def test_list_objects(self):
        self.tester.test_list_objects()

    def test_shallow_list_objects(self):
        self.tester.test_shallow_list_objects()

    def test_exists(self):
        self.tester.test_exists()

    def test_remove_objects(self):
        self.tester.test_remove_objects()

    def test_get_size(self):
        self.tester.test_get_size()

    def test_open_write(self):
        self.tester.test_open_write()

    def test_open_write_consumer_throws(self):
        self.tester.test_open_write_consumer_throws()

    def test_open_write_feeder_throws(self):
        self.tester.test_open_write_feeder_throws()

    def test_open_write_with_parquet(self):
        self.tester.test_open_write_with_parquet()

    def test_broken_stream_upload(self):
        """on broken stream upload, the file should not be "uploaded" i.e. not shown in list and shallow_list"""
        test_content = b"This is some test content that should not be completely uploaded"
        test_object_name = "broken_upload_test.txt"

        def break_10th_chunk(chunk, position, size):
            self.assertFalse(self.storage.exists(test_object_name))

            if position >= 10:
                raise IOError("Simulated error during upload")

        broken_stream = ChunkedCallbackStream(test_content, callback=break_10th_chunk, chunk_size=10)

        with self.assertRaises(IOError):
            # noinspection PyTypeChecker
            self.storage.put_object_stream(test_object_name, broken_stream)

        temp_dir = self.storage._root / self.storage.BUCKETBASE_TMP_DIR_NAME

        self.assertFalse(self.storage.exists(test_object_name))
        self.assertEqual(1, len(list(temp_dir.iterdir())))
        self.assertEqual(1, len(list(self.storage._root.iterdir())))
        self.assertEqual(0, len(list(self.storage.list_objects(""))))
        slist = self.storage.shallow_list_objects("")
        self.assertEqual(0, len(list(slist.prefixes)))
        self.assertEqual(0, len(list(slist.objects)))

    def test_multiple_fsbuckets_same_local_put_get(self):
        # 2 instances of FSBucket pointing to the same directory should work normally
        temp_dir = Path(self.temp_dir.name)
        bucket1 = FSBucket(temp_dir)
        bucket2 = FSBucket(temp_dir)
        test_object_name = "concurrent_test.txt"
        test_content = b"Test content for concurrent access"

        bucket1.put_object(test_object_name, test_content)

        self.assertTrue(bucket2.exists(test_object_name))
        self.assertEqual(bucket2.get_object(test_object_name), test_content)

        bucket1.remove_objects([test_object_name])

        self.assertFalse(bucket2.exists(test_object_name))
        self.assertEqual(0, len(list(bucket2.list_objects(""))))

    def test_listing_our_dir(self):
        obj_name = PurePosixPath("dir1/file.txt")
        self.storage.put_object_stream(obj_name, BytesIO(b"Test content"))
        tmp_file = self.temp_dir_path / FSBucket.BUCKETBASE_TMP_DIR_NAME / f"{obj_name.as_posix().replace('/', '#')}.tmp"
        self.assertTrue((self.temp_dir_path / FSBucket.BUCKETBASE_TMP_DIR_NAME).exists())
        with open(tmp_file, "wb") as f:
            f.write(b"Test content")

        with self.assertRaises(ValueError) as context:
            self.storage.list_objects(self.storage.BUCKETBASE_TMP_DIR_NAME)
        self.assertRegex(str(context.exception), r"^Invalid S3 prefix: \S+")
        with self.assertRaises(ValueError) as context:
            self.storage.list_objects(self.storage.BUCKETBASE_TMP_DIR_NAME + "/something")
        self.assertRegex(str(context.exception), r"^Invalid S3 prefix: \S+")
        with self.assertRaises(ValueError) as context:
            self.storage.list_objects(self.storage.BUCKETBASE_TMP_DIR_NAME + IBucket.SEP)
        self.assertRegex(str(context.exception), r"^Invalid S3 prefix: \S+")
        with self.assertRaises(ValueError) as context:
            self.storage.list_objects(self.storage.BUCKETBASE_TMP_DIR_NAME + ".something")
        self.assertRegex(str(context.exception), r"^Invalid S3 prefix: \S+")

        self.assertListEqual([obj_name], self.storage.list_objects(""))
        self.assertListEqual([obj_name], self.storage.list_objects("d"))

    def test_shallow_listing_our_dir(self):
        obj_name = PurePosixPath("dir1/file.txt")
        self.storage.put_object_stream(obj_name, BytesIO(b"Test content"))
        tmp_file = self.temp_dir_path / FSBucket.BUCKETBASE_TMP_DIR_NAME / f"{obj_name.as_posix().replace('/', '#')}.tmp"
        self.assertTrue((self.temp_dir_path / FSBucket.BUCKETBASE_TMP_DIR_NAME).exists())
        with open(tmp_file, "wb") as f:
            f.write(b"Test content")

        with self.assertRaises(ValueError) as context:
            self.storage.shallow_list_objects(self.storage.BUCKETBASE_TMP_DIR_NAME + "/something")
        self.assertRegex(str(context.exception), r"^Invalid S3 prefix: \S+")
        with self.assertRaises(ValueError) as context:
            self.storage.shallow_list_objects(self.storage.BUCKETBASE_TMP_DIR_NAME + IBucket.SEP)
        self.assertRegex(str(context.exception), r"^Invalid S3 prefix: \S+")
        with self.assertRaises(ValueError) as context:
            self.storage.shallow_list_objects(self.storage.BUCKETBASE_TMP_DIR_NAME + ".something")
        self.assertRegex(str(context.exception), r"^Invalid S3 prefix: \S+")

        s_listing = self.storage.shallow_list_objects("")
        self.assertListEqual([], s_listing.objects)
        self.assertListEqual(["dir1/"], s_listing.prefixes)

        s_listing = self.storage.shallow_list_objects("d")
        self.assertListEqual([], s_listing.objects)
        self.assertListEqual(["dir1/"], s_listing.prefixes)

    def test_get_size_during_writing(self):
        """Test that get_size raises FileNotFoundError while writing an object one byte at a time until complete."""
        test_file = "size_during_write_test.txt"
        content = b"Testing byte-by-byte writing with size checks"
        size_checks = 0
        not_found_checks = 0

        def check_size_callback(chunk, position, size):
            nonlocal size_checks, not_found_checks
            size_checks += 1
            not_found_checks += 1
            self.assertFalse(self.storage.exists(test_file))
            with self.assertRaises(FileNotFoundError, msg=f"File should not exist during writing -- pos {position}"):
                self.storage.get_size(test_file)

        def final_check(chunk, position, size):
            with self.assertRaises(FileNotFoundError, msg=f"File should not exist during writing -- pos {position}"):
                self.storage.get_size(test_file)

        stream = ChunkedCallbackStream(content, callback=check_size_callback, chunk_size=1, final_callback=final_check)

        self.storage.put_object_stream(test_file, stream)

        self.assertTrue(self.storage.exists(test_file))
        final_size = self.storage.get_size(test_file)
        self.assertEqual(len(content), final_size)

        self.assertTrue(size_checks > 0, "No size checks were performed during writing")
        self.assertTrue(not_found_checks > 0, "No FileNotFoundError exceptions were caught")

    def test_concurrent_put_streams(self):
        test_cases = [
            {"name": "same_file", "filename": "test/dir/for/concurrent_test.txt", "wanted_num_files": 1, "num_threads": 9},
            {"name": "distinct_files", "filename": "", "wanted_num_files": 9, "num_threads": 9},
        ]

        for test_case in test_cases:
            self.setUp()  # this will run redundantly, but it's ok
            with self.subTest(name=test_case["name"]):
                self._run_concurrent_put_stream(
                    fname=test_case["filename"], wanted_num_files=test_case["wanted_num_files"], num_threads=test_case["num_threads"]
                )
            self.tearDown()

    def _run_concurrent_put_stream(self, fname=None, wanted_num_files=1, num_threads=6):
        storage_workdir = self.storage._root / self.storage.BUCKETBASE_TMP_DIR_NAME

        barrier_started = Barrier(num_threads + 1)
        barrier_finished_streams = Barrier(num_threads + 1)
        barrier_move_on_close = Barrier(num_threads + 1)

        def _finalizer(chunk, position, size):
            barrier_finished_streams.wait()
            barrier_move_on_close.wait()

        def _writer(ident: int, filename=""):
            storage = FSBucket(self.storage._root)
            content = b"Testing concurrent stream {id}"
            if not filename:
                filename = f"test/dir/for/concurrent_test_{ident}.txt"
            barrier_started.wait()
            content_stream = ChunkedCallbackStream(content, chunk_size=1, final_callback=_finalizer)
            storage.put_object_stream(filename, content_stream)

        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=_writer, args=(i, fname))
            thread.start()
            threads.append(thread)

        barrier_started.wait()

        barrier_finished_streams.wait()

        self.assertEqual(
            0, len(list(self.storage.list_objects(""))), f"Expected 0 files in {self.storage._root}, but found {len(list(self.storage.list_objects('')))}"
        )
        self.assertEqual(
            num_threads,
            len(list(storage_workdir.iterdir())),
            f"Expected {num_threads} files in {storage_workdir}, but found {len(list(storage_workdir.iterdir()))}",
        )

        barrier_move_on_close.wait()

        for thread in threads:
            thread.join()

        self.assertEqual(0, len(list(storage_workdir.iterdir())), f"Expected 0 files in {storage_workdir}, but found {len(list(storage_workdir.iterdir()))}")
        self.assertEqual(
            wanted_num_files,
            len(list(self.storage.list_objects(""))),
            f"Expected {wanted_num_files} files in at list_objects(''), but found {len(list(self.storage.list_objects('')))}",
        )
