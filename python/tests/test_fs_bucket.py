from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from bucketbase import FSBucket
from tests.bucket_tester import IBucketTester


class TestFSBucket(TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        temp_dir_path = Path(self.temp_dir.name)
        self.storage = FSBucket(temp_dir_path)
        self.tester = IBucketTester(self.storage, self)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_put_and_get_object(self):
        self.tester.test_put_and_get_object()

    def test_put_and_get_object_stream(self):
        self.tester.test_put_and_get_object_stream()

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