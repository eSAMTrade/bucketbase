# _queue_binary_io.py
import io
import queue
import threading
from collections import deque
from threading import Event
from typing import Any, BinaryIO, Generic, Optional, TypeVar, Union

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


T = TypeVar("T")


class StatefulEvent(Generic[T]):
    def __init__(self):
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._state: Optional[T] = None

    def set(self, state: T) -> None:
        """Sets the event flag and stores a state object. The state is immutable, so if called with another state, it will raise an exception."""
        with self._lock:
            if self._state is not None:
                if id(state) != id(self._state):
                    raise ValueError(f"StatefulEvent.set() called with different state: {state} != {self._state}")
            self._state = state
            self._event.set()

    def wait(self, timeout: Optional[float] = None) -> Optional[Any]:
        """Waits for the event to be set and returns the state object. Returns None on timeout."""
        if self._event.wait(timeout):
            return self._state
        return None

    def get_nowait(self) -> Optional[T]:
        """Returns the state object if the event is set, otherwise returns None."""
        if self._event.is_set():
            return self._state
        return None

    def is_set(self) -> bool:
        return self._event.is_set()


class QueueBinaryReadable(io.RawIOBase, BinaryIO):
    """
    A BinaryIO-compatible, read-only stream where another thread feeds bytes.
    Use .feed(b"...") to push data, .finish() to signal EOF, or .fail(exc) to propagate an error.
    """

    SUCCESS_FLAG = object()

    def __init__(self, *, max_queue_size: int = 1):
        super().__init__()
        self._q: queue.Queue[bytes | _EOFSentinel | _ErrorWrapper] = queue.Queue(maxsize=max_queue_size)
        self._closed_flag: bool = False
        self._writing_closed: bool = False
        self._exc_to_consumer: StatefulEvent[BaseException] = StatefulEvent()
        self._buffer = BytesQueue()
        self._finished_reading = Event()
        self._finish_event: StatefulEvent[BaseException | object | None] = StatefulEvent()
        self._read_lock = threading.RLock()
        self._write_lock = threading.RLock()

    def feed(self, data: Union[bytes, bytearray, memoryview], timeout_sec: Optional[float] = None) -> None:
        with self._write_lock:
            state = self._finish_event.get_nowait()
            if state is not None:
                if isinstance(state, BaseException):
                    raise state
                if state is self.SUCCESS_FLAG:
                    raise ValueError("Feed operation on an already closed read-stream")
                raise RuntimeError(f"Unknown state: {state}")
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
                    raise TimeoutError(f"Timeout after {timeout_sec} seconds waiting to write data")  # pylint: disable=raise-missing-from

    def send_eof(self, timeout_sec: Optional[float] = None) -> None:
        """Called from feeder to signal EOF"""
        with self._write_lock:
            self._writing_closed = True
        self._q.put(_EOF, timeout=timeout_sec)

    def wait_upload_success_or_raise(self, timeout_sec: Optional[float] = None) -> None:
        state = self._finish_event.wait(timeout_sec)
        if state is None:
            raise TimeoutError(f"Timeout after {timeout_sec} seconds waiting for upload success")
        if isinstance(state, BaseException):
            raise state
        if state is self.SUCCESS_FLAG:
            return
        raise RuntimeError(f"Unknown finish state: {state}")

    def wait_finish_state(self, timeout_sec: Optional[float] = None) -> BaseException | object | None:
        return self._finish_event.wait(timeout=timeout_sec)

    def send_exception_to_reader(self, exc: BaseException) -> None:
        """Propagate an exception to readers."""
        if not isinstance(exc, BaseException):
            raise TypeError("fail() expects an exception instance")
        with self._write_lock:
            self._writing_closed = True
            self._exc_to_consumer.set(exc)

        # draining the queue first
        while self._get_no_wait_next_chunk_or_none() is not None:
            pass
        # we expect no one is writing to the queue at this point from our thread
        try:
            self._q.put_nowait(_ErrorWrapper(exc))
        except queue.Full:
            # pylint: disable=raise-missing-from
            raise RuntimeError(f"Failed to propagate exception to reader. Someone unexpected thread is putting data in the queue: {self._q.qsize()}")

    def on_consumer_fail(self, exc: BaseException):
        self._finish_event.set(exc)
        # we intentionally don't aquire the lock here, as on fail we don't care about consistency anymore
        self._closed_flag = True

    def _get_no_wait_next_chunk_or_none(self) -> Optional[bytes]:
        try:
            item = self._q.get_nowait()
            return item
        except queue.Empty:
            return None

    def notify_upload_success(self):
        with self._read_lock:
            temp = self._get_no_wait_next_chunk_or_none()
            while temp is _EOF:
                temp = self._get_no_wait_next_chunk_or_none()

            # If there's an exception set, we might have remaining data due to early termination
            if self._exc_to_consumer.is_set():
                # Clear any remaining data from queue and buffer when there's an exception
                while temp is not None:
                    temp = self._get_no_wait_next_chunk_or_none()
                self._buffer.get_next(-1)  # Clear buffer
                raise self._exc_to_consumer.get_nowait()
            # Normal case - should be empty
            assert temp is None, f"notify_upload_success() called before EOF, and queue contains {temp}"
            assert self._q.empty(), "notify_upload_success() called before EOF"
            assert self._buffer.get_next() == b""

            self._finish_event.set(QueueBinaryReadable.SUCCESS_FLAG)

    def _wait_finished_reading(self, timeout_sec: Optional[float] = None) -> None:
        """This method is used only for debug and testing purposes"""
        self._finished_reading.wait(timeout_sec)

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
        with self._read_lock:
            if self._closed_flag:
                return
            self._closed_flag = True
        super().close()

    @override
    def read(self, size: int = -1) -> bytes:  # pylint: disable=too-many-branches
        with self._read_lock:
            if self._closed_flag:
                raise ValueError("Stream is closed")
            # Check for exceptions even after reading is finished
            if self._exc_to_consumer.is_set():
                raise self._exc_to_consumer.get_nowait()
            if self._finished_reading.is_set():
                return b""
            if size == 0:
                raise ValueError("read called with size 0")

            if size is None or size < 0:
                while True:
                    next_el = self._q.get()
                    if self._exc_to_consumer.is_set():
                        raise self._exc_to_consumer.get_nowait()
                    if next_el is _EOF:
                        self._finished_reading.set()
                        assert self._writing_closed, "EOF observed before send_eof() flagged _writing_closed"
                        self._writing_closed = True
                        # Check for exceptions even after EOF
                        return self._buffer.get_next(-1)
                    if isinstance(next_el, _ErrorWrapper):
                        self._exc_to_consumer.set(next_el.exc)
                        raise next_el.exc
                    assert isinstance(next_el, (bytes, bytearray, memoryview))
                    self._buffer.append(next_el)

            assert size > 0, f"read called with size: {size}"
            remain_in_buffer = self._buffer.get_next(size)
            if len(remain_in_buffer) > 0:
                return remain_in_buffer
            assert len(remain_in_buffer) == 0
            next_el = self._q.get()
            # Check for exceptions even after EOF
            if self._exc_to_consumer.is_set():
                raise self._exc_to_consumer.get_nowait()
            if next_el is _EOF:
                self._finished_reading.set()
                assert self._writing_closed, "EOF observed before send_eof() flagged _writing_closed"
                self._writing_closed = True
                if self._exc_to_consumer.is_set():
                    raise self._exc_to_consumer.get_nowait()
                return b""
            if isinstance(next_el, _ErrorWrapper):
                self._exc_to_consumer.set(next_el.exc)
                raise next_el.exc
            assert isinstance(next_el, (bytes, bytearray, memoryview))
            assert len(next_el) > 0, f"read called with size: {size} and next_el is 0 bytes"
            if len(next_el) <= size:
                return next_el
            self._buffer.append(next_el[size:])
            return next_el[:size]

    @override
    def readinto(self, b) -> int:
        with self._read_lock:
            if self._exc_to_consumer.is_set():
                raise self._exc_to_consumer.get_nowait()
            # Optional fast-path; RawIOBase.read() would call this if we didn't override read()
            chunk = self.read(len(b))
            n = len(chunk)
            if n:
                b[:n] = chunk
            return n

    def __del__(self):
        """This method is here to avoid calling super().__del__(), as it calls close(), and it leads to inconsistencies when exit with exception, and deadlocks
        See tests.bucket_tester.IBucketTester.test_regression_infinite_cycle_on_unentered_open_write_context for details
        """


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
            self._consumer_stream.send_eof(self._timeout_sec)  # Signal EOF first
            self._consumer_stream.wait_upload_success_or_raise(timeout_sec=self._timeout_sec)  # Then wait for upload
        super().close()

    def __del__(self):
        """This method is here to avoid calling super().__del__(), as it calls close(), and it leads to inconsistencies when exit with exception, and deadlocks
        See tests.bucket_tester.IBucketTester.test_regression_infinite_cycle_on_unentered_open_write_context for details
        """
