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


class QueueBinaryIO(io.RawIOBase, BinaryIO):
    """
    A BinaryIO-compatible, read-only stream where another thread feeds bytes.
    Use .feed(b"...") to push data, .finish() to signal EOF, or .fail(exc) to propagate an error.
    """

    def __init__(self, *, max_queue_size: int = 1, get_timeout: Optional[float] = None):
        super().__init__()
        self._q: "queue.Queue[Union[bytes, bytearray, memoryview, object]]" = queue.Queue(maxsize=max_queue_size)
        self._buffer = bytearray()
        self._eof = False
        self._closed_flag = Event()
        self._feeder_exc: Optional[BaseException] = None
        self._get_timeout = get_timeout
        self._lock = Lock()
        self._error: Optional[BaseException] = None

    # ---- producer API ----
    def feed(self, data: Union[bytes, bytearray, memoryview]) -> None:
        with self._lock:
            if self._feeder_exc is not None:
                raise self._feeder_exc
            if self._closed_flag.is_set():
                raise ValueError("Stream is closed")
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("feed() expects bytes-like data")
        # copy to immutable bytes to avoid external mutation
        self._q.put(bytes(data))

    def wait_finish(self, timeout_sec: Optional[float] = None) -> None:
        # Signal EOF
        self._q.put(_Sentinel)
        self._closed_flag.wait(timeout=timeout_sec)

    def fail_reader(self, exc: BaseException) -> None:
        """Propagate an exception to readers."""
        if not isinstance(exc, BaseException):
            raise TypeError("fail() expects an exception instance")
        self._q.put(_ErrorWrapper(exc))

    def fail_feeder(self, exc: BaseException):
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
        if not self._closed_flag.is_set():
            self._closed_flag.set()
            # ensure a waiting reader wakes up
            try:
                self._q.put_nowait(_Sentinel)
            except queue.Full:
                pass
        super().close()

    @override
    def read(self, size: int = -1) -> bytes:
        with self._lock:
            if self._error is not None:
                raise self._error
        if self._closed_flag.is_set() and not self._buffer and self._q.empty():
            return b""
        if size == 0:
            return b""

        # size < 0 => read all until EOF
        if size is None or size < 0:
            self._drain_until_eof_or_error()
            with self._lock:
                if self._error is not None:
                    raise self._error
                out = bytes(self._buffer)
                self._buffer.clear()
                return out

        # size > 0
        while True:
            with self._lock:
                if self._error is not None:
                    raise self._error
                if len(self._buffer) >= size:
                    out = bytes(self._buffer[:size])
                    del self._buffer[:size]
                    return out
                if self._eof:
                    out = bytes(self._buffer)
                    self._buffer.clear()
                    return out

            # Need more data: block for next chunk or EOF
            try:
                item = self._q.get(timeout=self._get_timeout)
            except queue.Empty:
                # timeout: return whatever we have (possibly empty)
                with self._lock:
                    out = bytes(self._buffer)
                    self._buffer.clear()
                    return out

            if item is _Sentinel:
                with self._lock:
                    self._eof = True
                # loop again to flush remaining buffer
                continue
            if isinstance(item, _ErrorWrapper):
                with self._lock:
                    self._error = item.exc
                # raise immediately; any remaining buffer is discarded
                raise item.exc

            # normal bytes chunk
            with self._lock:
                self._buffer.extend(item)

    @override
    def readinto(self, b) -> int:
        # Optional fast-path; RawIOBase.read() would call this if we didn't override read()
        chunk = self.read(len(b))
        n = len(chunk)
        if n:
            b[:n] = chunk
        return n

    # ---- helpers ----
    def _drain_until_eof_or_error(self) -> None:
        while True:
            with self._lock:
                if self._eof or self._error is not None:
                    return
            try:
                item = self._q.get(timeout=self._get_timeout)
            except queue.Empty:
                # No EOF yet and timeout reached; return with what we have
                return
            if item is _Sentinel:
                with self._lock:
                    self._eof = True
                return
            if isinstance(item, _ErrorWrapper):
                with self._lock:
                    self._error = item.exc
                return
            with self._lock:
                self._buffer.extend(item)
