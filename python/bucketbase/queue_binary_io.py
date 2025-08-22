# queue_binary_io.py
import io
import queue
from threading import Event, Lock
from typing import BinaryIO, Optional, Union

try:
    from typing import override  # type: ignore
except ImportError:
    from typing_extensions import override  # type: ignore

_Sentinel = object()


class _ErrorWrapper:
    __slots__ = ("exc",)

    def __init__(self, exc: BaseException):
        self.exc = exc


from collections import deque


class BytesQueue2:
    def __init__(self):
        self._chunks: deque[bytes] = deque()
        self._offset = 0  # offset into the first chunk
        self._total_size = 0

    def __len__(self) -> int:
        return self._total_size

    def append_bytes(self, data: bytes) -> None:
        if data:
            self._chunks.append(data)
            self._total_size += len(data)

    def get_next(self, size: int = -1) -> bytes:
        if size == 0:
            return b""

        if size < 0:  # read all
            size = self._total_size

        if size >= self._total_size:
            # Return everything and clear
            result = self._get_all_remaining()
            self.clear()
            return result

        # Collect chunks until we have enough data
        result_chunks = []
        remaining = size

        while remaining > 0 and self._chunks:
            chunk = self._chunks[0]
            available_in_chunk = len(chunk) - self._offset

            if remaining >= available_in_chunk:
                # Take the rest of this chunk
                if self._offset == 0:
                    result_chunks.append(chunk)
                else:
                    result_chunks.append(chunk[self._offset :])
                remaining -= available_in_chunk
                self._total_size -= available_in_chunk
                self._chunks.popleft()
                self._offset = 0
            else:
                # Take part of this chunk
                end_offset = self._offset + remaining
                result_chunks.append(chunk[self._offset : end_offset])
                self._offset = end_offset
                self._total_size -= remaining
                remaining = 0

        return b"".join(result_chunks)

    def _get_all_remaining(self) -> bytes:
        if not self._chunks:
            return b""

        if len(self._chunks) == 1:
            return self._chunks[0][self._offset :]

        result_chunks = []
        first_chunk = self._chunks[0]
        if self._offset < len(first_chunk):
            result_chunks.append(first_chunk[self._offset :])

        for chunk in list(self._chunks)[1:]:
            result_chunks.append(chunk)

        return b"".join(result_chunks)

    def clear(self) -> None:
        self._chunks.clear()
        self._offset = 0
        self._total_size = 0

    def extend(self, data: Union[bytes, bytearray, memoryview]) -> None:
        self.append_bytes(bytes(data))


class BytesQueue:
    def __init__(self):
        self._buffers = deque()
        self._read_pos = 0

    def append_bytes(self, data: bytes):
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


class QueueBinaryIO(io.RawIOBase, BinaryIO):
    """
    A BinaryIO-compatible, read-only stream where another thread feeds bytes.
    Use .feed(b"...") to push data, .finish() to signal EOF, or .fail(exc) to propagate an error.
    """

    def __init__(self, *, max_queue_size: int = 1, get_timeout: Optional[float] = None):
        super().__init__()
        self._q: "queue.Queue[Union[bytes, bytearray, memoryview, object]]" = queue.Queue(maxsize=max_queue_size)
        self._closed_flag = Event()
        self._feeder_exc: Optional[BaseException] = None
        self._get_timeout = get_timeout
        self._lock = Lock()
        self._buffer = BytesQueue()
        self._error: Optional[BaseException] = None
        self._finished_reading = Event()
        self._writing_closed: bool = False
        self._upload_thread_success = Event()

    # ---- producer API ----
    def feed(self, data: Union[bytes, bytearray, memoryview], timeout_sec: Optional[float] = None) -> None:
        print(f"feed called with {len(data)} bytes")
        with self._lock:
            print(f"feed lock acquired with {len(data)} bytes")
            if self._feeder_exc is not None:
                raise self._feeder_exc
            if self._closed_flag.is_set():
                raise ValueError("Stream is closed")
            if self._writing_closed:
                raise ValueError("Stream is closed")
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("feed() expects bytes-like data")
        # copy to immutable bytes to avoid external mutation
        if len(data) == 0:
            print("feed called with 0 bytes")
        else:
            self._q.put(bytes(data), timeout=timeout_sec)

    def notify_finish_write(self, timeout_sec: Optional[float] = None) -> None:
        with self._lock:
            if self._writing_closed:
                raise ValueError("Stream already closed")
            self._writing_closed = True
        self._q.put(_Sentinel, timeout=timeout_sec)

    def wait_upload_success(self, timeout_sec: Optional[float] = None) -> None:
        success = self._upload_thread_success.wait(timeout=timeout_sec)
        if not success:
            raise TimeoutError("wait_upload_success timed out")

    def wait_finish(self, timeout_sec: Optional[float] = None) -> None:
        # Signal EOF
        self._q.put(_Sentinel)
        print("wait_finish called")
        finished = self._finished_reading.wait(timeout=timeout_sec)
        print("wait_finish done")
        if not finished:
            raise TimeoutError("wait_finish timed out")

    def fail_reader(self, exc: BaseException) -> None:
        """Propagate an exception to readers."""
        if not isinstance(exc, BaseException):
            raise TypeError("fail() expects an exception instance")
        self._error = exc
        try:
            self._q.put_nowait(_ErrorWrapper(exc))
            print("fail_reader called")
        except queue.Full:
            print("fail_reader called, but queue is full")

    def fail_feeder(self, exc: BaseException):
        print("fail_feeder called:", exc)
        with self._lock:
            if self._feeder_exc is not None:
                self._feeder_exc = exc
            self._closed_flag.set()

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
        print("close called")
        if not self._closed_flag.is_set():
            self._closed_flag.set()
        super().close()

    @override
    def read(self, size: int = -1) -> bytes:
        assert not self._closed_flag.is_set()
        print(f"read called with size: {size}")
        with self._lock:
            if self._error is not None:
                raise self._error
        if self._closed_flag.is_set():
            raise ValueError("Stream is closed")
        if self._finished_reading.is_set():
            return b""
        if size == 0:
            raise ValueError("read called with size 0")

        # size < 0 => read all until EOF
        if size is None or size < 0:
            remain_in_buffer = self._buffer.get_next()
            if not remain_in_buffer:
                next_el = self._q.get()
                if next_el is _Sentinel:
                    self._finished_reading.set()
                    self._writing_closed = True
                    return b""
                if isinstance(next_el, _ErrorWrapper):
                    self._error = next_el.exc
                    raise next_el.exc
                assert isinstance(next_el, (bytes, bytearray, memoryview))
                assert len(next_el) > 0, f"read called with size: {size} and next_el is 0 bytes"
                return next_el
            return remain_in_buffer
        assert size > 0, f"read called with size: {size}"
        remain_in_buffer = self._buffer.get_next(size)
        if len(remain_in_buffer) > 0:
            return remain_in_buffer
        assert len(remain_in_buffer) == 0
        next_el = self._q.get()
        if next_el is _Sentinel:
            self._finished_reading.set()
            self._writing_closed = True
            return b""
        if isinstance(next_el, _ErrorWrapper):
            self._error = next_el.exc
            raise next_el.exc
        assert isinstance(next_el, (bytes, bytearray, memoryview))
        assert len(next_el) > 0, f"read called with size: {size} and next_el is 0 bytes"
        if len(next_el) <= size:
            return next_el
        self._buffer.append_bytes(next_el[size:])
        return next_el[:size]

    @override
    def readinto(self, b) -> int:
        if self._error is not None:
            raise self._error
        # Optional fast-path; RawIOBase.read() would call this if we didn't override read()
        chunk = self.read(len(b))
        n = len(chunk)
        if n:
            b[:n] = chunk
        return n

    def _get_next_el_from_queue(self) -> Optional[bytes]:
        try:
            item = self._q.get_nowait()
            return item
        except queue.Empty:
            return None

    def notify_upload_success(self):
        temp = self._get_next_el_from_queue()
        while temp is _Sentinel:
            temp = self._get_next_el_from_queue()
        assert temp is None, f"notify_read_finish() called before EOF, and queue contains {temp}"
        assert self._q.empty(), "notify_read_finish() called before EOF"
        assert self._buffer.get_next() == b""
        self._upload_thread_success.set()
