import threading
import time
import unittest

from bucketbase.queue_binary_io import QueueBinaryIO


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
        # no producer yet -> should return empty after timeout
        self.assertEqual(s.read(1024), b"")

        # now feed some and let it return partial if not enough
        def producer():
            time.sleep(0.02)
            s.feed(b"xyz")
            time.sleep(0.1)
            s.wait_finish()

        threading.Thread(target=producer, daemon=True).start()
        first = s.read(10)  # will get 'xyz' after a short wait
        self.assertEqual(first, b"xyz")
        rest = s.read(10)  # after finish(), returns b""
        self.assertEqual(rest, b"")

    def test_finish_idempotent_and_eof(self):
        s = QueueBinaryIO()

        def producer():
            s.feed(b"123")
            s.wait_finish()
            s.wait_finish()  # extra finish should be harmless

        threading.Thread(target=producer, daemon=True).start()
        self.assertEqual(s.read(2), b"12")
        self.assertEqual(s.read(2), b"3")
        self.assertEqual(s.read(2), b"")  # EOF reached
        self.assertEqual(s.read(), b"")

    def test_close_wakes_reader_and_prevents_feed(self):
        s = QueueBinaryIO()
        got = {"chunk": None}

        def reader():
            got["chunk"] = s.read(4)  # will be unblocked by close()

        rt = threading.Thread(target=reader)
        rt.start()
        time.sleep(0.02)
        s.close()
        rt.join(timeout=1)
        self.assertTrue(s.closed)
        self.assertEqual(got["chunk"], b"")  # close -> EOF to reader
        with self.assertRaises(ValueError):
            s.feed(b"nope")

    def test_close_with_buffered_data(self):
        s = QueueBinaryIO()
        s.feed(b"abc")
        s.close()
        # buffered data should still be readable once; then EOF
        self.assertEqual(s.read(10), b"abc")
        self.assertEqual(s.read(1), b"")

    def test_exception_propagation_immediate(self):
        s = QueueBinaryIO()
        s.fail_reader(RuntimeError("boom"))
        # def producer():
        #     s.fail(RuntimeError("boom"))
        # threading.Thread(target=producer, daemon=True).start()
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
        with self.assertRaisesRegex(IOError, "net down"):
            _ = s.read()  # size=-1 should raise if an error arrives

    def test_fail_requires_exception_instance(self):
        s = QueueBinaryIO()
        with self.assertRaises(TypeError):
            s.fail_reader("not-exception")  # type: ignore[arg-type]

    def test_flags(self):
        s = QueueBinaryIO()
        self.assertTrue(s.readable())
        self.assertFalse(s.writable())
        self.assertFalse(s.seekable())
        s.wait_finish()
        self.assertEqual(s.read(1), b"")
        s.close()
        self.assertTrue(s.closed)


if __name__ == "__main__":
    unittest.main()
