import threading
import time
import unittest
from unittest import TestCase

from bucketbase.queue_binary_io import BytesQueue, QueueBinaryIO


class TestQueueBinaryIO(unittest.TestCase):
    def test_stream_with_producer_thread(self):
        s = QueueBinaryIO()

        payload_parts = [b"hello", b" ", b"world", b"!", b" " * 1024, b"tail"]
        total = b"".join(payload_parts)

        def producer():
            for part in payload_parts:
                s.feed(part)
                time.sleep(0.01)  # simulate staggered production
            s.wait_finish()

        t = threading.Thread(target=producer, daemon=True)
        t.start()

        # Consumer pattern like MinIO's multipart uploader: fixed-size reads
        received = bytearray()
        while True:
            chunk = s.read(512)  # MinIO will call read(part_size) repeatedly
            if not chunk:
                break
            received.extend(chunk)

        t.join(timeout=1)
        self.assertEqual(bytes(received), total)

        # After EOF: further reads must return b""
        self.assertEqual(s.read(10), b"")
        self.assertEqual(s.read(), b"")

    def test_read_all(self):
        s = QueueBinaryIO()

        def producer():
            s.feed(b"a" * 1000)
            s.wait_finish()

        threading.Thread(target=producer, daemon=True).start()
        data = s.read()  # size=-1 -> read all until EOF
        self.assertEqual(len(data), 1000)
        self.assertTrue(all(c == ord("a") for c in data))

    def test_readinto(self):
        s = QueueBinaryIO()

        def producer():
            s.feed(b"abcdef")
            s.wait_finish()

        threading.Thread(target=producer, daemon=True).start()
        buf = bytearray(4)
        n1 = s.readinto(buf)
        self.assertEqual(n1, 4)
        self.assertEqual(bytes(buf), b"abcd")
        buf2 = bytearray(4)
        n2 = s.readinto(buf2)
        self.assertEqual(n2, 2)
        self.assertEqual(bytes(buf2[:2]), b"ef")
        self.assertEqual(s.readinto(bytearray(1)), 0)

    def test_timeout_returns_partial_then_empty(self):
        s = QueueBinaryIO(get_timeout=0.05)

        # now feed some and let it return partial if not enough
        def producer():
            time.sleep(0.01)
            print("feeding xyz")
            s.feed(b"xyz")
            print("xyz fed")
            time.sleep(0.01)
            s.notify_finish_write()

        t = threading.Thread(target=producer, daemon=True)
        t.start()
        first = s.read(10)  # will get 'xyz' after a short wait
        self.assertEqual(first, b"xyz")
        rest = s.read(10)  # after finish(), returns b""
        self.assertEqual(rest, b"")
        t.join(timeout=1)

    def test_finish_idempotent_and_eof(self):
        s = QueueBinaryIO()

        def producer():
            s.feed(b"123")
            s.wait_finish()
            s.wait_finish()  # extra finish should be harmless

        t = threading.Thread(target=producer, daemon=True)
        t.start()
        a12 = s.read(2)
        self.assertEqual(a12, b"12")
        a3 = s.read(1)
        self.assertEqual(a3, b"3")
        eof = s.read(1)  # should return b"" immediately
        self.assertEqual(eof, b"")  # EOF reached
        # further reads must return b""
        eof2 = s.read(1)  # should return b"" immediately
        self.assertEqual(eof2, b"")
        t.join(timeout=1)

    def test_close_wakes_reader_and_prevents_feed(self):
        s = QueueBinaryIO()
        got = {"chunk": None}

        def reader():
            got["chunk"] = s.read(4)  # will be unblocked by close()

        rt = threading.Thread(target=reader)
        rt.start()
        time.sleep(0.02)
        s.notify_finish_write()
        rt.join(timeout=1)
        self.assertFalse(s.closed)
        self.assertEqual(got["chunk"], b"")  # close -> EOF to reader
        with self.assertRaises(ValueError):
            s.feed(b"nope")

    def test_close_with_buffered_data(self):
        s = QueueBinaryIO()
        s.feed(b"abc")
        succes = threading.Event()

        def consumer():
            time.sleep(0.02)
            self.assertEqual(s.read(10), b"abc")
            self.assertEqual(s.read(1), b"")  # EOF
            succes.set()

        ct = threading.Thread(target=consumer)
        ct.start()
        time.sleep(0.02)
        s.notify_finish_write()
        self.assertTrue(succes.wait(timeout=0.1))

    def test_exception_propagation_immediate(self):
        s = QueueBinaryIO()
        s.fail_reader(RuntimeError("boom"))
        with self.assertRaisesRegex(RuntimeError, "boom"):
            s.read(1)
        # subsequent reads keep raising the same exception
        with self.assertRaisesRegex(RuntimeError, "boom"):
            s.read(1)

    def test_exception_after_some_data(self):
        s = QueueBinaryIO()

        def producer():
            s.feed(b"hello")
            time.sleep(0.02)
            s.fail_reader(ValueError("bad"))

        threading.Thread(target=producer, daemon=True).start()
        # First read may return some data before the error is observed
        chunk = s.read(5)
        self.assertIn(chunk, (b"hello", b""))  # depending on timing
        with self.assertRaisesRegex(ValueError, "bad"):
            s.read(1)

    def test_exception_with_read_all(self):
        s = QueueBinaryIO()

        def producer():
            s.feed(b"abc")
            s.fail_reader(IOError("net down"))

        threading.Thread(target=producer, daemon=True).start()
        _ = s.read(3)  # read one byte to unblock producer
        with self.assertRaisesRegex(IOError, "net down"):
            _ = s.read(1)  # size=-1 should raise if an error arrives

    def test_fail_requires_exception_instance(self):
        s = QueueBinaryIO()
        with self.assertRaises(TypeError):
            s.fail_reader("not-exception")  # type: ignore[arg-type]

    def test_flags(self):
        s = QueueBinaryIO()
        self.assertTrue(s.readable())
        self.assertFalse(s.writable())
        self.assertFalse(s.seekable())
        succes = threading.Event()

        def consumer():
            time.sleep(0.02)
            while s.read() != b"":
                print("reading new byte")
            succes.set()

        t = threading.Thread(target=consumer)
        t.start()
        s.wait_finish()
        self.assertEqual(s.read(1), b"")
        s.close()
        self.assertTrue(s.closed)


class TestBytesQueue(TestCase):
    """Test BytesQueue with orthogonal scenarios using boundary analysis and equivalence partitioning."""

    def test_single_buffer_complete_read(self):
        """Partition: Single buffer, read all at once."""
        q = BytesQueue()
        data = b"Hello, world!"
        q.append_bytes(data)
        self.assertEqual(q.get_next(-1), data)
        self.assertEqual(q.get_next(-1), b"")

    def test_single_buffer_partial_reads(self):
        """Partition: Single buffer, multiple partial reads."""
        q = BytesQueue()
        q.append_bytes(b"Hello, world!")

        self.assertEqual(q.get_next(5), b"Hello")
        self.assertEqual(q.get_next(2), b", ")
        self.assertEqual(q.get_next(6), b"world!")
        self.assertEqual(q.get_next(-1), b"")

    def test_single_buffer_boundary_reads(self):
        """Boundary: Read exactly buffer size, then empty."""
        q = BytesQueue()
        data = b"ABC"
        q.append_bytes(data)

        self.assertEqual(q.get_next(3), data)
        self.assertEqual(q.get_next(-1), b"")

    def test_multiple_buffers_complete_read(self):
        """Partition: Multiple buffers, read all at once."""
        q = BytesQueue()
        q.append_bytes(b"ABC")
        q.append_bytes(b"DEF")
        q.append_bytes(b"GHI")

        self.assertEqual(q.get_next(-1), b"ABCDEFGHI")

    def test_multiple_buffers_cross_boundary_reads(self):
        """Boundary: Reads spanning multiple buffer boundaries."""
        q = BytesQueue()
        q.append_bytes(b"ABC")
        q.append_bytes(b"DEF")
        q.append_bytes(b"GHI")

        self.assertEqual(q.get_next(5), b"ABCDE")
        self.assertEqual(q.get_next(4), b"FGHI")

    def test_interleaved_append_read_operations(self):
        """Partition: Mixed append/read operations."""
        q = BytesQueue()
        q.append_bytes(b"Hello")
        self.assertEqual(q.get_next(3), b"Hel")

        q.append_bytes(b" World")
        self.assertEqual(q.get_next(4), b"lo W")
        self.assertEqual(q.get_next(-1), b"orld")

    def test_oversized_requests(self):
        """Boundary: Request more data than available."""
        q = BytesQueue()
        q.append_bytes(b"Hello")

        result = q.get_next(100)
        self.assertEqual(result, b"Hello")
        self.assertEqual(q.get_next(-1), b"")

    def test_empty_bytes_handling(self):
        """Edge case: Empty bytes operations."""
        q = BytesQueue()
        q.append_bytes(b"")
        self.assertEqual(q.get_next(-1), b"")

        q.append_bytes(b"Hello")
        q.append_bytes(b"")
        q.append_bytes(b"World")
        self.assertEqual(q.get_next(-1), b"HelloWorld")

    def test_single_byte_reads(self):
        """Performance: Many single-byte reads."""
        q = BytesQueue()
        data = b"ABCDEFGHIJ"
        q.append_bytes(data)

        result = []
        for i in range(len(data)):
            chunk = q.get_next(1)
            result.append(chunk)
            self.assertEqual(chunk, data[i : i + 1])

        self.assertEqual(b"".join(result), data)
        self.assertEqual(q.get_next(-1), b"")

    def test_data_copied_behavior(self):
        """Behavior: append_bytes creates independent copies."""
        original = bytearray(b"Hello")
        q = BytesQueue()
        q.append_bytes(original)

        original[0] = ord("X")
        self.assertEqual(q.get_next(-1), b"Hello")

    def test_type_validation(self):
        """Edge case: Type validation for append_bytes."""
        q = BytesQueue()

        q.append_bytes(bytearray(b"test"))
        self.assertEqual(q.get_next(-1), b"test")

        q.append_bytes(memoryview(b"view"))
        self.assertEqual(q.get_next(-1), b"view")

    def test_large_buffer_handling(self):
        """Performance: Large single buffer operations."""
        large_size = 1024 * 1024
        large_data = b"X" * large_size
        q = BytesQueue()
        q.append_bytes(large_data)

        chunk = q.get_next(1024)
        self.assertEqual(len(chunk), 1024)
        self.assertEqual(chunk, b"X" * 1024)

        remainder = q.get_next(-1)
        self.assertEqual(len(remainder), large_size - 1024)


if __name__ == "__main__":
    unittest.main()
