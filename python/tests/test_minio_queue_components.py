# mypy: disable-error-code="no-untyped-def"
import threading
import time
import unittest
from queue import Queue
from typing import Optional

from bucketbase.minio_bucket import _QueueReader, _QueueWriter


class TestQueueWriter(unittest.TestCase):
    def setUp(self):
        self.queue: Queue[Optional[bytes]] = Queue()
        self.closed_flag = [0]
        self.writer = _QueueWriter(self.queue)

    def test_writable(self):
        self.assertTrue(self.writer.writable())

    def test_write_small_data_buffered(self):
        """Test that small writes are buffered until CHUNK_SIZE is reached."""
        small_data = b"hello"
        bytes_written = self.writer.write(small_data)

        self.assertEqual(bytes_written, len(small_data))
        self.assertTrue(self.queue.empty())  # Should be buffered, not in queue yet
        self.assertEqual(len(self.writer._buffer), len(small_data))

    def test_write_exact_chunk_size(self):
        """Test that writing exactly CHUNK_SIZE bytes puts one chunk in queue."""
        chunk_data = b"x" * _QueueWriter.CHUNK_SIZE
        bytes_written = self.writer.write(chunk_data)

        self.assertEqual(bytes_written, _QueueWriter.CHUNK_SIZE)
        self.assertFalse(self.queue.empty())
        self.assertEqual(len(self.writer._buffer), 0)

        # Verify the chunk in queue
        chunk = self.queue.get_nowait()
        self.assertEqual(chunk, chunk_data)
        self.assertEqual(len(chunk), _QueueWriter.CHUNK_SIZE)

    def test_write_larger_than_chunk_size(self):
        """Test that writing more than CHUNK_SIZE creates multiple chunks."""
        large_data = b"x" * (_QueueWriter.CHUNK_SIZE * 2 + 100)
        bytes_written = self.writer.write(large_data)

        self.assertEqual(bytes_written, len(large_data))

        # Should have 2 complete chunks in queue
        chunk1 = self.queue.get_nowait()
        chunk2 = self.queue.get_nowait()
        self.assertTrue(self.queue.empty())

        self.assertEqual(len(chunk1), _QueueWriter.CHUNK_SIZE)
        self.assertEqual(len(chunk2), _QueueWriter.CHUNK_SIZE)
        self.assertEqual(len(self.writer._buffer), 100)  # Remainder buffered

    def test_multiple_small_writes_accumulate(self):
        """Test that multiple small writes accumulate until CHUNK_SIZE."""
        write_size = _QueueWriter.CHUNK_SIZE // 4

        # Write 3 times, should still be buffered
        for i in range(3):
            self.writer.write(b"x" * write_size)
            self.assertTrue(self.queue.empty())

        self.assertEqual(len(self.writer._buffer), write_size * 3)

        # 4th write should trigger chunk emission
        self.writer.write(b"x" * write_size)
        self.assertFalse(self.queue.empty())
        self.assertEqual(len(self.writer._buffer), 0)

        chunk = self.queue.get_nowait()
        self.assertEqual(len(chunk), _QueueWriter.CHUNK_SIZE)

    def test_flush_sends_remaining_data(self):
        """Test that flush sends any buffered data regardless of size."""
        small_data = b"remaining_data"
        self.writer.write(small_data)
        self.assertTrue(self.queue.empty())

        self.writer.flush()
        self.assertFalse(self.queue.empty())
        self.assertEqual(len(self.writer._buffer), 0)

        chunk = self.queue.get_nowait()
        self.assertEqual(chunk, small_data)

    def test_flush_on_empty_buffer(self):
        """Test that flush on empty buffer doesn't send anything."""
        self.writer.flush()
        self.assertTrue(self.queue.empty())

    def test_flush_on_closed_writer(self):
        """Test that flush on closed writer does nothing."""
        self.writer.close()
        self.writer.flush()  # Should not raise

    def test_close_flushes_and_sends_eof(self):
        """Test that close flushes remaining data and sends EOF marker."""
        small_data = b"final_data"
        self.writer.write(small_data)

        self.writer.close()

        # Should have the data chunk and EOF marker
        chunk = self.queue.get_nowait()
        eof_marker = self.queue.get_nowait()

        self.assertEqual(chunk, small_data)
        self.assertIsNone(eof_marker)
        self.assertTrue(self.writer.closed)

    def test_write_after_close_raises(self):
        """Test that writing after close raises ValueError."""
        self.writer.close()

        with self.assertRaises(ValueError) as cm:
            self.writer.write(b"data")
        self.assertIn("I/O operation on closed file", str(cm.exception))

    def test_write_different_data_types(self):
        """Test writing different data types (bytes, bytearray, memoryview)."""
        test_data = b"test_data"

        # Test bytes
        self.assertEqual(self.writer.write(test_data), len(test_data))

        # Test bytearray
        self.assertEqual(self.writer.write(bytearray(test_data)), len(test_data))

        # Test memoryview
        self.assertEqual(self.writer.write(memoryview(test_data)), len(test_data))

        # Test string encoded as bytes
        string_data = "string".encode("utf-8")
        self.assertEqual(self.writer.write(string_data), 6)

    def test_empty_write(self):
        """Test that writing empty data works correctly."""
        bytes_written = self.writer.write(b"")
        self.assertEqual(bytes_written, 0)
        self.assertEqual(len(self.writer._buffer), 0)
        self.assertTrue(self.queue.empty())


class TestQueueReader(unittest.TestCase):
    def setUp(self):
        self.queue: Queue[Optional[bytes]] = Queue()
        self.closed_flag = [0]
        self.reader = _QueueReader(self.queue, self.closed_flag)

    def test_readable(self):
        self.assertTrue(self.reader.readable())

    def test_readinto_with_data_in_queue(self):
        """Test reading when data is available in queue."""
        test_data = b"hello world"
        self.queue.put(test_data)

        buffer = bytearray(20)
        bytes_read = self.reader.readinto(buffer)

        self.assertEqual(bytes_read, len(test_data))
        self.assertEqual(buffer[:bytes_read], test_data)

    def test_readinto_multiple_chunks(self):
        """Test reading multiple chunks from queue."""
        chunk1 = b"first_chunk"
        chunk2 = b"second_chunk"
        self.queue.put(chunk1)
        self.queue.put(chunk2)

        buffer = bytearray(50)
        bytes_read = self.reader.readinto(buffer)

        # Should read first chunk completely
        self.assertEqual(bytes_read, len(chunk1))
        self.assertEqual(buffer[:bytes_read], chunk1)

        # Second read should get second chunk
        bytes_read2 = self.reader.readinto(buffer)
        self.assertEqual(bytes_read2, len(chunk2))
        self.assertEqual(buffer[:bytes_read2], chunk2)

    def test_readinto_partial_buffer_fill(self):
        """Test reading when buffer is smaller than available data."""
        large_chunk = b"x" * 100
        self.queue.put(large_chunk)

        small_buffer = bytearray(10)
        bytes_read = self.reader.readinto(small_buffer)

        self.assertEqual(bytes_read, 10)
        self.assertEqual(small_buffer, b"x" * 10)

        # Remaining data should still be in reader's buffer
        self.assertEqual(len(self.reader._buffer), 90)

    def test_readinto_eof_marker(self):
        """Test that EOF marker (None) terminates reading."""
        self.queue.put(b"data")
        self.queue.put(None)  # EOF marker

        buffer = bytearray(20)
        bytes_read = self.reader.readinto(buffer)

        self.assertEqual(bytes_read, 4)
        self.assertEqual(buffer[:bytes_read], b"data")

        # Next read should return 0 (EOF)
        bytes_read2 = self.reader.readinto(buffer)
        self.assertEqual(bytes_read2, 0)
        self.assertTrue(self.reader._eof)

    def test_readinto_after_eof(self):
        """Test that reading after EOF returns 0."""
        self.reader._eof = True

        buffer = bytearray(10)
        bytes_read = self.reader.readinto(buffer)

        self.assertEqual(bytes_read, 0)

    def test_readinto_timeout_with_closed_flag(self):
        """Test timeout behavior when writer is closed but queue is empty."""
        self.closed_flag[0] = 1  # Simulate writer closed

        buffer = bytearray(10)
        bytes_read = self.reader.readinto(buffer)

        self.assertEqual(bytes_read, 0)
        self.assertTrue(self.reader._eof)

    def test_readinto_timeout_without_closed_flag(self):
        """Test timeout behavior when writer is not closed."""
        # This test simulates a timeout scenario
        buffer = bytearray(20)  # Make buffer larger than data

        # Start reading in a separate thread to avoid blocking the test
        result = [None]
        exception = [None]

        def read_with_timeout():
            try:
                # This should timeout and continue waiting
                result[0] = self.reader.readinto(buffer)
            except Exception as e:
                exception[0] = e

        thread = threading.Thread(target=read_with_timeout)
        thread.daemon = True
        thread.start()

        # Wait a bit, then add data
        time.sleep(0.1)
        test_data = b"delayed_data"  # This is 12 bytes
        self.queue.put(test_data)

        thread.join(timeout=2.0)

        self.assertIsNone(exception[0])
        self.assertEqual(result[0], len(test_data))  # Should be 12
        self.assertEqual(buffer[: result[0]], test_data)

    def test_readinto_non_bytes_chunk(self):
        """Test handling of non-bytes chunks (should be converted)."""
        self.queue.put(bytearray(b"bytearray_data"))

        buffer = bytearray(20)
        bytes_read = self.reader.readinto(buffer)

        self.assertEqual(bytes_read, 14)
        self.assertEqual(buffer[:bytes_read], b"bytearray_data")


class TestQueueWriterReaderIntegration(unittest.TestCase):
    def setUp(self):
        self.queue: Queue[Optional[bytes]] = Queue()
        self.closed_flag = [0]
        self.writer = _QueueWriter(self.queue)
        self.reader = _QueueReader(self.queue, self.closed_flag)

    def test_write_read_cycle(self):
        """Test complete write-read cycle."""
        test_data = b"integration_test_data"

        # Write data
        self.writer.write(test_data)
        self.writer.flush()

        # Read data
        buffer = bytearray(50)
        bytes_read = self.reader.readinto(buffer)

        self.assertEqual(bytes_read, len(test_data))
        self.assertEqual(buffer[:bytes_read], test_data)

    def test_large_data_streaming(self):
        """Test streaming large data through writer/reader."""
        chunk_size = _QueueWriter.CHUNK_SIZE
        large_data = b"x" * (chunk_size * 3 + 100)

        # Write large data
        self.writer.write(large_data)
        self.writer.close()

        # Read all data back
        result = bytearray()
        buffer = bytearray(chunk_size // 2)  # Smaller read buffer

        while True:
            bytes_read = self.reader.readinto(buffer)
            if bytes_read == 0:
                break
            result.extend(buffer[:bytes_read])

        self.assertEqual(result, large_data)


if __name__ == "__main__":
    unittest.main()
