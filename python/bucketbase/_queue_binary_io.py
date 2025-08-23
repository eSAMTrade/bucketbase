# _queue_binary_io.py
import io
import queue
from collections import deque
from threading import Event
from typing import BinaryIO, Optional, Union

try:
    from typing import override  # type: ignore
except ImportError:
    from typing_extensions import override  # type: ignore


class _EOFSentinel:
    pass


_EOF = _EOFSentinel()


class _ErrorWrapper:
    __slots__ = ("exc",)

    def __init__(self, exc: BaseException):
        self.exc = exc


class BytesQueue:
    def __init__(self):
        self._buffers = deque()
        self._read_pos = 0

    def append(self, data: bytes):
        self._buffers.append(bytes(data))  # copy bytes

    def get_next(self, size=-1) -> bytes:
        result = []
        while self._buffers and (size != 0):
            buf = self._buffers[0]
            start = self._read_pos
            available = len(buf) - start

            if size == -1 or size >= available:
                result.append(buf[start:])
                self._buffers.popleft()
                self._read_pos = 0
                if size != -1:
                    size -= available
            else:
                result.append(buf[start : start + size])
                self._read_pos += size
                size = 0
        if result:
            return b"".join(result)
        return b""


class QueueBinaryReadable(io.RawIOBase, BinaryIO):
    """
    A BinaryIO-compatible, read-only stream where another thread feeds bytes.
    Use .feed(b"...") to push data, .finish() to signal EOF, or .fail(exc) to propagate an error.
    """

    def __init__(self, *, max_queue_size: int = 1):
        super().__init__()
        self._q: queue.Queue[bytes | _EOFSentinel | _ErrorWrapper] = queue.Queue(maxsize=max_queue_size)
        self._closed_flag: bool = False
        self._writing_closed: bool = False
        self._exc_to_feeder: Optional[BaseException] = None
        self._exc_to_consumer: Optional[BaseException] = None
        self._buffer = BytesQueue()
        self._finished_reading = Event()
        self._upload_thread_success = Event()

    def feed(self, data: Union[bytes, bytearray, memoryview], timeout_sec: Optional[float] = None) -> None:
        if self._exc_to_feeder is not None:
            raise self._exc_to_feeder
        if self._closed_flag:
            raise ValueError("Stream is closed")
        if self._writing_closed:
            raise ValueError("Stream is closed")
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("feed() expects bytes-like data")
        # copy to immutable bytes to avoid external mutation
        if len(data) > 0:
            try:
                self._q.put(bytes(data), timeout=timeout_sec)
            except queue.Full:
                raise TimeoutError(f"Timeout after {timeout_sec} seconds waiting to write data")

    def send_eof(self, timeout_sec: Optional[float] = None) -> None:
        """Called from feeder to signal EOF"""
        if self._writing_closed:
            raise ValueError("Stream already closed")
        self._writing_closed = True
        self._q.put(_EOF, timeout=timeout_sec)

    def wait_upload_success(self, timeout_sec: Optional[float] = None) -> None:
        success = self._upload_thread_success.wait(timeout=timeout_sec)
        if not success:
            raise TimeoutError("wait_upload_success timed out")

    def wait_finish(self, timeout_sec: Optional[float] = None) -> None:
        assert self._writing_closed, "wait_finish() called before EOF"
        finished = self._finished_reading.wait(timeout=timeout_sec)
        if not finished:
            raise TimeoutError("wait_finish timed out")

    def send_exception_to_reader(self, exc: BaseException) -> None:
        """Propagate an exception to readers."""
        if not isinstance(exc, BaseException):
            raise TypeError("fail() expects an exception instance")
        self._exc_to_consumer = exc
        try:
            self._q.put_nowait(_ErrorWrapper(exc))
        except queue.Full:
            pass

    def on_consumer_fail(self, exc: BaseException):
        self._exc_to_feeder = exc
        self._closed_flag = True

    def _get_no_wait_next_chunk_or_none(self) -> Optional[bytes]:
        try:
            item = self._q.get_nowait()
            return item
        except queue.Empty:
            return None

    def notify_upload_success(self):
        temp = self._get_no_wait_next_chunk_or_none()
        while temp is _EOF:
            temp = self._get_no_wait_next_chunk_or_none()
        assert temp is None, f"notify_read_finish() called before EOF, and queue contains {temp}"
        assert self._q.empty(), "notify_read_finish() called before EOF"
        assert self._buffer.get_next() == b""
        self._upload_thread_success.set()

    # ---- io.RawIOBase overrides ----
    @override
    def readable(self) -> bool:
        return True

    @override
    def writable(self) -> bool:
        return False

    @override
    def seekable(self) -> bool:
        return False

    @override
    def close(self) -> None:
        self._closed_flag = True
        super().close()

    @override
    def read(self, size: int = -1) -> bytes:
        if self._closed_flag:
            raise ValueError("Stream is closed")
        if self._exc_to_consumer is not None:
            raise self._exc_to_consumer
        if self._finished_reading.is_set():
            return b""
        if size == 0:
            raise ValueError("read called with size 0")

        if size is None or size < 0:
            while True:
                next_el = self._q.get()
                if next_el is _EOF:
                    self._finished_reading.set()
                    assert self._writing_closed, "notify_read_finish() called before EOF"
                    self._writing_closed = True
                    return self._buffer.get_next(-1)
                if isinstance(next_el, _ErrorWrapper):
                    self._exc_to_consumer = next_el.exc
                    raise next_el.exc
                assert isinstance(next_el, (bytes, bytearray, memoryview))
                self._buffer.append(next_el)

        assert size > 0, f"read called with size: {size}"
        remain_in_buffer = self._buffer.get_next(size)
        if len(remain_in_buffer) > 0:
            return remain_in_buffer
        assert len(remain_in_buffer) == 0
        next_el = self._q.get()
        if next_el is _EOF:
            self._finished_reading.set()
            assert self._writing_closed, "notify_read_finish() called before EOF"
            self._writing_closed = True
            return b""
        if isinstance(next_el, _ErrorWrapper):
            self._exc_to_consumer = next_el.exc
            raise next_el.exc
        assert isinstance(next_el, (bytes, bytearray, memoryview))
        assert len(next_el) > 0, f"read called with size: {size} and next_el is 0 bytes"
        if len(next_el) <= size:
            return next_el
        self._buffer.append(next_el[size:])
        return next_el[:size]

    @override
    def readinto(self, b) -> int:
        if self._exc_to_consumer is not None:
            raise self._exc_to_consumer
        # Optional fast-path; RawIOBase.read() would call this if we didn't override read()
        chunk = self.read(len(b))
        n = len(chunk)
        if n:
            b[:n] = chunk
        return n


class QueueBinaryWritable(io.RawIOBase, BinaryIO):
    CHUNK_SIZE = 1024 * 1024  # 1 MiB per queue item

    def __init__(self, queue_binary_io: QueueBinaryReadable, timeout_sec: Optional[float] = None) -> None:
        super().__init__()
        self._consumer_stream = queue_binary_io
        self._closed = False
        self._timeout_sec = timeout_sec

    @override
    def writable(self) -> bool:
        return True

    @override
    def write(self, b) -> int:
        if len(b) == 0:
            return 0
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        self._consumer_stream.feed(b, timeout_sec=self._timeout_sec)
        return len(b)

    @override
    def flush(self) -> None:
        pass

    @override
    def close(self) -> None:
        if not self.closed:
            self._closed = True
            self._consumer_stream.send_eof()  # Signal EOF first
            self._consumer_stream.wait_upload_success(timeout_sec=self._timeout_sec)  # Then wait for upload
        super().close()
