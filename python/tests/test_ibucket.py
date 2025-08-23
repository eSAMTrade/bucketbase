from pathlib import PurePosixPath
from unittest.case import TestCase

from bucketbase import MemoryBucket
from bucketbase.ibucket import IBucket
from tests.bucket_tester import IBucketTester


class TestIBucket(TestCase):
    def test_split_prefix(self) -> None:
        res = IBucket._split_prefix("dir1/dir2/file.txt")
        self.assertEqual(res, ("dir1/dir2/", "file.txt"))

        res = IBucket._split_prefix("dir1/dir2/")
        self.assertEqual(res, ("dir1/dir2/", ""))

        res = IBucket._split_prefix("dir1/dir2")
        self.assertEqual(res, ("dir1/", "dir2"))

        res = IBucket._split_prefix("dir1/")
        self.assertEqual(res, ("dir1/", ""))

        res = IBucket._split_prefix("dir1")
        self.assertEqual(res, ("", "dir1"))

        # Valid characters
        res = IBucket._split_prefix("!'(file)'-1_2.txt")
        self.assertEqual(res, ("", "!'(file)'-1_2.txt"))

        # Invalid Prefix cases
        for prefix in IBucketTester.INVALID_PREFIXES:
            self.assertRaises(ValueError, IBucket._split_prefix, prefix)

    def test_copy_prefix_to_prefix(self) -> None:
        src_bucket = MemoryBucket()
        dst_bucket = MemoryBucket()

        src_bucket.put_object("dir1/dir2/file1.txt", b"content1")
        src_bucket.put_object("dir1/dir2/file2.txt", b"content2")
        src_bucket.put_object("dir1/file4.txt", b"content4")
        src_bucket.put_object("directory_file.txt", b"content5")

        src_bucket.copy_prefix(dst_bucket, src_prefix="dir", dst_prefix="copy_prefix/dir")

        objects = dst_bucket.list_objects(prefix="copy_prefix")
        expected_objects = [
            PurePosixPath("copy_prefix/dir1/dir2/file1.txt"),
            PurePosixPath("copy_prefix/dir1/dir2/file2.txt"),
            PurePosixPath("copy_prefix/dir1/file4.txt"),
            PurePosixPath("copy_prefix/directory_file.txt"),
        ]
        self.assertEqual(objects, expected_objects)

    def test_copy_prefix_to_prefix_mthreaded(self) -> None:
        src_bucket = MemoryBucket()
        dst_bucket = MemoryBucket()

        src_bucket.put_object("dir1/dir2/file1.txt", b"content1")
        src_bucket.put_object("dir1/dir2/file2.txt", b"content2")
        src_bucket.put_object("dir1/file4.txt", b"content4")
        src_bucket.put_object("directory_file.txt", b"content5")

        src_bucket.copy_prefix(dst_bucket, src_prefix="dir", dst_prefix="copy_prefix/dir", threads=4)

        objects = dst_bucket.list_objects(prefix="copy_prefix")
        expected_objects = [
            PurePosixPath("copy_prefix/dir1/dir2/file1.txt"),
            PurePosixPath("copy_prefix/dir1/dir2/file2.txt"),
            PurePosixPath("copy_prefix/dir1/file4.txt"),
            PurePosixPath("copy_prefix/directory_file.txt"),
        ]
        self.assertEqual(objects, expected_objects)

    def test_copy_prefix_to_root(self) -> None:
        src_bucket = MemoryBucket()
        dst_bucket = MemoryBucket()

        src_bucket.put_object("dir1/dir2/file1.txt", b"content1")
        src_bucket.put_object("dir1/dir2/file2.txt", b"content2")
        src_bucket.put_object("dir1/file4.txt", b"content4")
        src_bucket.put_object("directory_file.txt", b"content5")

        src_bucket.copy_prefix(dst_bucket, src_prefix="dir")

        objects = dst_bucket.list_objects()
        expected_objects = [
            PurePosixPath("1/dir2/file1.txt"),
            PurePosixPath("1/dir2/file2.txt"),
            PurePosixPath("1/file4.txt"),
            PurePosixPath("ectory_file.txt"),
        ]
        self.assertEqual(objects, expected_objects)

    def test_copy_dir_prefix_to_root(self) -> None:
        """
        This covers a regression where the destination object name starts with "/", like /dir2/file1.txt
        as "dir1/dir2" - "dir1" = "/dir2"
        """
        src_bucket = MemoryBucket()
        dst_bucket = MemoryBucket()

        src_bucket.put_object("dir1/dir2/file1.txt", b"content1")
        src_bucket.put_object("dir1/dir2/file2.txt", b"content2")
        src_bucket.put_object("dir1/file4.txt", b"content4")
        src_bucket.put_object("directory_file.txt", b"content5")

        src_bucket.copy_prefix(dst_bucket, src_prefix="dir1")

        objects = dst_bucket.list_objects()
        expected_objects = [PurePosixPath("dir2/file1.txt"), PurePosixPath("dir2/file2.txt"), PurePosixPath("file4.txt")]
        self.assertEqual(objects, expected_objects)

    def test_copy_prefix_from_dir_to_root(self) -> None:
        src_bucket = MemoryBucket()
        dst_bucket = MemoryBucket()

        src_bucket.put_object("dir1/dir2/file1.txt", b"content1")
        src_bucket.put_object("dir1/dir2/file2.txt", b"content2")
        src_bucket.put_object("dir1/file4.txt", b"content4")
        src_bucket.put_object("directory_file.txt", b"content5")

        src_bucket.copy_prefix(dst_bucket, src_prefix="dir1/")

        objects = dst_bucket.list_objects()
        expected_objects = [PurePosixPath("dir2/file1.txt"), PurePosixPath("dir2/file2.txt"), PurePosixPath("file4.txt")]
        self.assertEqual(objects, expected_objects)

    def test_move_prefix(self) -> None:
        src_bucket = MemoryBucket()
        dst_bucket = MemoryBucket()

        src_bucket.put_object("dir1/dir2/file1.txt", b"content1")
        src_bucket.put_object("dir1/dir2/file2.txt", b"content2")
        src_bucket.put_object("dir1/file4.txt", b"content4")
        src_bucket.put_object("directory_file.txt", b"content5")

        src_bucket.move_prefix(dst_bucket, src_prefix="dir1", dst_prefix="dir_copy")

        objects = dst_bucket.list_objects()
        expected_objects = [PurePosixPath("dir_copy/dir2/file1.txt"), PurePosixPath("dir_copy/dir2/file2.txt"), PurePosixPath("dir_copy/file4.txt")]
        self.assertEqual(objects, expected_objects)

        remained_objects = src_bucket.list_objects()
        self.assertEqual(remained_objects, [PurePosixPath("directory_file.txt")])

    def test_open_write(self) -> None:
        """Test open_write method using MemoryBucket."""
        bucket = MemoryBucket()
        tester = IBucketTester(bucket, self)
        try:
            tester.test_open_write()
        finally:
            tester.cleanup()

    def test_open_write_with_parquet(self) -> None:
        """Test open_write with parquet files using MemoryBucket."""
        bucket = MemoryBucket()
        tester = IBucketTester(bucket, self)
        try:
            tester.test_open_write_with_parquet()
        finally:
            tester.cleanup()

    def test_open_write_timeout(self) -> None:
        """Test open_write timeout functionality using MemoryBucket."""
        bucket = MemoryBucket()
        tester = IBucketTester(bucket, self)
        try:
            tester.test_open_write_timeout()
        finally:
            tester.cleanup()
