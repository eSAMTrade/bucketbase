import threading
import time
import unittest
from unittest import TestCase

from bucketbase._queue_binary_io import BytesQueue, QueueBinaryReadable


class MockException(Exception):
    pass


class TestQueueBinaryIO(unittest.TestCase):
    def test_stream_with_producer_thread(self):
        s = QueueBinaryReadable()

        payload_parts = [b"hello", b" ", b"world", b"!", b" " * 1024, b"tail"]
        total = b"".join(payload_parts)

        def producer():
            for part in payload_parts:
                s.feed(part)
                time.sleep(0.01)  # simulate staggered production
            s.send_eof()
            s.wait_finish_state()

        t = threading.Thread(target=producer, daemon=True)
        t.start()

        # Consumer pattern like MinIO's multipart uploader: fixed-size reads
        received = bytearray()
        while True:
            chunk = s.read(512)  # MinIO will call read(part_size) repeatedly
            if not chunk:
                break
            received.extend(chunk)
        self.assertEqual(b"", s.read(10))
        self.assertEqual(b"", s.read(10))
        self.assertEqual(b"", s.read())
        self.assertEqual(b"", s.read())
        s.notify_upload_success()

        t.join(timeout=1)
        self.assertEqual(bytes(received), total)

        # After EOF: further reads must return b""
        self.assertEqual(s.read(10), b"")
        self.assertEqual(s.read(), b"")

    def test_read_all(self):
        s = QueueBinaryReadable()

        def producer():
            for i in range(10):
                s.feed(b"a" * i)
            s.send_eof()
            s.wait_finish_state()

        threading.Thread(target=producer, daemon=True).start()
        data = s.read()  # size=-1 -> read all until EOF
        self.assertEqual(len(data), sum(range(10)))
        self.assertTrue(all(c == ord("a") for c in data))

    def test_readinto(self):
        s = QueueBinaryReadable()

        def producer():
            s.feed(b"abcdef")
            s.send_eof()
            s.wait_finish_state()

        t = threading.Thread(target=producer, daemon=True)
        t.start()
        buf = bytearray(4)
        n1 = s.readinto(buf)
        self.assertEqual(n1, 4)
        self.assertEqual(bytes(buf), b"abcd")
        buf2 = bytearray(4)
        n2 = s.readinto(buf2)
        self.assertEqual(n2, 2)
        self.assertEqual(bytes(buf2[:2]), b"ef")
        self.assertEqual(s.readinto(bytearray(1)), 0)
        s.notify_upload_success()
        t.join(timeout=1)

    def test_timeout_returns_partial_then_empty(self):
        s = QueueBinaryReadable()

        # now feed some and let it return partial if not enough
        def producer():
            time.sleep(0.01)
            print("feeding xyz")
            s.feed(b"xyz")
            print("xyz fed")
            time.sleep(0.01)
            s.send_eof()

        t = threading.Thread(target=producer, daemon=True)
        t.start()
        first = s.read(10)  # will get 'xyz' after a short wait
        self.assertEqual(first, b"xyz")
        rest = s.read(10)  # after finish(), returns b""
        self.assertEqual(rest, b"")
        t.join(timeout=1)

    def test_finish_idempotent_and_eof(self):
        s = QueueBinaryReadable()

        def producer():
            s.feed(b"123")
            s.send_eof()
            s.wait_finish_state()
            s.wait_finish_state()  # extra finish should be harmless

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
        s.notify_upload_success()
        t.join(timeout=1)

    def test_close_wakes_reader_and_prevents_feed(self):
        s = QueueBinaryReadable()
        got = {"chunk": None}

        def reader():
            got["chunk"] = s.read(4)  # will be unblocked by close()

        rt = threading.Thread(target=reader)
        rt.start()
        time.sleep(0.02)
        s.send_eof()
        rt.join(timeout=1)
        self.assertFalse(s.closed)
        self.assertEqual(got["chunk"], b"")  # close -> EOF to reader
        with self.assertRaises(ValueError):
            s.feed(b"nope")

    def test_close_with_buffered_data(self):
        s = QueueBinaryReadable()
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
        s.send_eof()
        self.assertTrue(succes.wait(timeout=0.1))

    def test_exception_propagation_to_reader_immediate(self):
        s = QueueBinaryReadable()
        s.send_exception_to_reader(RuntimeError("boom"))
        with self.assertRaisesRegex(RuntimeError, "boom"):
            s.read(1)
        # subsequent reads keep raising the same exception
        with self.assertRaisesRegex(RuntimeError, "boom"):
            s.read(1)

    def test_exception_to_reader_after_some_data(self):
        s = QueueBinaryReadable()

        def producer():
            s.feed(b"hello")
            time.sleep(0.02)
            s.send_exception_to_reader(ValueError("bad"))

        threading.Thread(target=producer, daemon=True).start()
        # First read may return some data before the error is observed
        chunk = s.read(5)
        self.assertIn(chunk, (b"hello", b""))  # depending on timing
        with self.assertRaisesRegex(ValueError, "bad"):
            s.read(1)

    def test_exception_to_reader_with_read_all(self):
        s = QueueBinaryReadable()

        def producer():
            s.feed(b"abc")
            s.send_exception_to_reader(IOError("net down"))

        threading.Thread(target=producer, daemon=True).start()
        with self.assertRaisesRegex(IOError, "net down"):
            _ = s.read(4)  # size=-1 should raise if an error arrives

    def test_exceptioN_to_reader_requires_exception_instance(self):
        s = QueueBinaryReadable()
        with self.assertRaises(TypeError):
            s.send_exception_to_reader("not-exception")  # type: ignore[arg-type]

    def test_exc_to_feeder_nominal(self):
        s = QueueBinaryReadable()
        s.feed(b"abc")

        def consumer():
            self.assertEqual(s.read(1), b"a")
            s.on_consumer_fail(MockException("bad"))

        ct = threading.Thread(target=consumer)
        ct.start()
        self.assertIsInstance(s.wait_finish_state(timeout_sec=1), MockException)
        ct.join(timeout=1)

    def test_exc_to_feeder_after_all_read_by_parts(self):
        s = QueueBinaryReadable()
        s.feed(b"abc")

        def consumer():
            self.assertEqual(s.read(10), b"abc")
            self.assertEqual(s.read(10), b"")
            s.on_consumer_fail(MockException("bad"))

        ct = threading.Thread(target=consumer, daemon=True)
        ct.start()
        s.send_eof()
        self.assertIsInstance(s.wait_finish_state(timeout_sec=1), MockException)
        ct.join(timeout=1)

    def test_exc_to_feeder_after_readall(self):
        s = QueueBinaryReadable()
        s.feed(b"abc")

        def consumer():
            self.assertEqual(s.read(), b"abc")
            s.on_consumer_fail(MockException("bad"))

        ct = threading.Thread(target=consumer)
        ct.start()
        self.assertIsInstance(s.wait_finish_state(timeout_sec=1), MockException)
        ct.join(timeout=1)

    def test_exc_to_feeder_after_successful_close(self):
        s = QueueBinaryReadable()
        s.feed(b"abc")

        def consumer():
            self.assertEqual(s.read(), b"abc")
            s.close()
            s.on_consumer_fail(MockException("bad"))

        ct = threading.Thread(target=consumer)
        ct.start()
        s.send_eof()
        self.assertIsInstance(s.wait_finish_state(timeout_sec=1), MockException)
        ct.join(timeout=1)

    def test_flags(self):
        s = QueueBinaryReadable()
        self.assertTrue(s.readable())
        self.assertFalse(s.writable())
        self.assertFalse(s.seekable())
        succes = threading.Event()

        def consumer():
            time.sleep(0.02)
            while s.read() != b"":
                print("reading new byte")
            s.notify_upload_success()
            succes.set()

        t = threading.Thread(target=consumer)
        t.start()
        s.send_eof()
        self.assertIs(QueueBinaryReadable.SUCCESS_FLAG, s.wait_finish_state())
        self.assertEqual(s.read(1), b"")
        s.close()
        self.assertTrue(s.closed)

    def test_readall_when_multiple_chunks_in_queue(self):
        s = QueueBinaryReadable()

        def producer():
            s.feed(b"hello")
            s.feed(b"world")
            s.send_eof()

        t = threading.Thread(target=producer, daemon=True)
        t.start()
        all_bytes = s.read(-1)
        self.assertEqual(all_bytes, b"helloworld")
        t.join(timeout=1)


class TestBytesQueue(TestCase):
    """Test BytesQueue with orthogonal scenarios using boundary analysis and equivalence partitioning."""

    def test_single_buffer_complete_read(self):
        """Partition: Single buffer, read all at once."""
        q = BytesQueue()
        data = b"Hello, world!"
        q.append(data)
        self.assertEqual(q.get_next(-1), data)
        self.assertEqual(q.get_next(-1), b"")

    def test_single_buffer_partial_reads(self):
        """Partition: Single buffer, multiple partial reads."""
        q = BytesQueue()
        q.append(b"Hello, world!")

        self.assertEqual(q.get_next(5), b"Hello")
        self.assertEqual(q.get_next(2), b", ")
        self.assertEqual(q.get_next(6), b"world!")
        self.assertEqual(q.get_next(-1), b"")

    def test_single_buffer_boundary_reads(self):
        """Boundary: Read exactly buffer size, then empty."""
        q = BytesQueue()
        data = b"ABC"
        q.append(data)

        self.assertEqual(q.get_next(3), data)
        self.assertEqual(q.get_next(-1), b"")

    def test_multiple_buffers_complete_read(self):
        """Partition: Multiple buffers, read all at once."""
        q = BytesQueue()
        q.append(b"ABC")
        q.append(b"DEF")
        q.append(b"GHI")

        self.assertEqual(q.get_next(-1), b"ABCDEFGHI")

    def test_multiple_buffers_cross_boundary_reads(self):
        """Boundary: Reads spanning multiple buffer boundaries."""
        q = BytesQueue()
        q.append(b"ABC")
        q.append(b"DEF")
        q.append(b"GHI")

        self.assertEqual(q.get_next(5), b"ABCDE")
        self.assertEqual(q.get_next(4), b"FGHI")

    def test_interleaved_append_read_operations(self):
        """Partition: Mixed append/read operations."""
        q = BytesQueue()
        q.append(b"Hello")
        self.assertEqual(q.get_next(3), b"Hel")

        q.append(b" World")
        self.assertEqual(q.get_next(4), b"lo W")
        self.assertEqual(q.get_next(-1), b"orld")

    def test_oversized_requests(self):
        """Boundary: Request more data than available."""
        q = BytesQueue()
        q.append(b"Hello")

        result = q.get_next(100)
        self.assertEqual(result, b"Hello")
        self.assertEqual(q.get_next(-1), b"")

    def test_empty_bytes_handling(self):
        """Edge case: Empty bytes operations."""
        q = BytesQueue()
        q.append(b"")
        self.assertEqual(q.get_next(-1), b"")

        q.append(b"Hello")
        q.append(b"")
        q.append(b"World")
        self.assertEqual(q.get_next(-1), b"HelloWorld")

    def test_single_byte_reads(self):
        """Performance: Many single-byte reads."""
        q = BytesQueue()
        data = b"ABCDEFGHIJ"
        q.append(data)

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
        q.append(original)

        original[0] = ord("X")
        self.assertEqual(q.get_next(-1), b"Hello")

    def test_type_validation(self):
        """Edge case: Type validation for append_bytes."""
        q = BytesQueue()

        q.append(bytearray(b"test"))
        self.assertEqual(q.get_next(-1), b"test")

        q.append(memoryview(b"view"))
        self.assertEqual(q.get_next(-1), b"view")

    def test_large_buffer_handling(self):
        """Performance: Large single buffer operations."""
        large_size = 1024 * 1024
        large_data = b"X" * large_size
        q = BytesQueue()
        q.append(large_data)

        chunk = q.get_next(1024)
        self.assertEqual(len(chunk), 1024)
        self.assertEqual(chunk, b"X" * 1024)

        remainder = q.get_next(-1)
        self.assertEqual(len(remainder), large_size - 1024)


if __name__ == "__main__":
    unittest.main()
