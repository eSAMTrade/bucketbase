from __future__ import annotations

from io import IOBase
from types import TracebackType
from typing import BinaryIO, Iterable

try:
    from typing import (  # type: ignore[attr-defined, attr-defined]
        Any,
        Buffer,
        final,
        override,
    )
except ImportError:
    from typing_extensions import Buffer, final, override  # type: ignore


@final
class NonClosingStream(IOBase, BinaryIO):
    """
    A delegating wrapper for file-like objects that prevents the underlying stream from being closed by callers
    that don't know about transport-side finalization semantics (for example, writers like ArrowSink used with S3).

    Purpose:
        - Prevents premature finalization/commit of remote uploads when a writer fails or calls `close()` in error paths.
        - Lets the owning component decide when the underlying stream should be actually finalized/closed (use `force_base_close()` for that).

    Behavior summary:
        - `close()` marks only the wrapper as closed; the base stream stays open.
        - After the wrapper is closed, I/O operations raise `ValueError`.
        - This wrapper does not provide atomic rename/commit guarantees for remote stores (e.g., S3); it only avoids accidental finalization.
        - Not thread-safe: concurrent access requires external synchronization.
    """

    def __init__(self, base: IOBase) -> None:
        if not isinstance(base, IOBase):
            raise TypeError("base must be an IOBase instance")
        self._base = base
        self._closed: bool = False

    @override
    def close(self) -> None:
        self._closed = True

    def __del__(self) -> None:
        """This method is here to avoid calling super().__del__(), as it calls close(), and it leads to inconsistencies when exit with exception, and deadlocks
        See tests.bucket_tester.IBucketTester.test_regression_infinite_cycle_on_unentered_open_write_context for details
        """

    @property
    @override
    def closed(self) -> bool:
        """Return True if the object is closed or its base is closed."""
        return self._closed or self._base.closed

    def _raise_if_stream_is_closed(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file")

    @override
    def __iter__(self) -> NonClosingStream:
        self._raise_if_stream_is_closed()
        return self

    @override
    def __next__(self) -> bytes:
        self._raise_if_stream_is_closed()
        return next(self._base)

    @override
    def __enter__(self) -> NonClosingStream:
        self._raise_if_stream_is_closed()
        return self

    @override
    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None) -> None:
        self.close()

    def close_base(self) -> None:
        self._base.close()

    def write(self, *args: Any, **kwargs: Any) -> int:
        self._raise_if_stream_is_closed()
        return self._base.write(*args, **kwargs)

    @override
    def flush(self) -> None:
        self._raise_if_stream_is_closed()
        return self._base.flush()

    def read(self, size: int = -1) -> bytes:
        self._raise_if_stream_is_closed()
        return self._base.read(size)

    @override
    def fileno(self) -> int:
        self._raise_if_stream_is_closed()
        return self._base.fileno()

    @override
    def isatty(self) -> bool:
        self._raise_if_stream_is_closed()
        return self._base.isatty()

    @override
    def readable(self) -> bool:
        self._raise_if_stream_is_closed()
        return self._base.readable()

    @override
    def readline(self, size: int | None = -1) -> bytes:
        self._raise_if_stream_is_closed()
        return self._base.readline(size)

    @override
    def readlines(self, hint: int = -1) -> list[bytes]:
        self._raise_if_stream_is_closed()
        return self._base.readlines(hint)

    @override
    def seek(self, offset: int, whence: int = 0) -> int:
        self._raise_if_stream_is_closed()
        return self._base.seek(offset, whence)

    @override
    def seekable(self) -> bool:
        self._raise_if_stream_is_closed()
        return self._base.seekable()

    @override
    def tell(self) -> int:
        self._raise_if_stream_is_closed()
        return self._base.tell()

    @override
    def truncate(self, size: int | None = None) -> int:
        self._raise_if_stream_is_closed()
        return self._base.truncate(size)

    @override
    def writable(self) -> bool:
        self._raise_if_stream_is_closed()
        return self._base.writable()

    @override
    def writelines(self, lines: Iterable[Buffer]) -> None:
        self._raise_if_stream_is_closed()
        return self._base.writelines(lines)
