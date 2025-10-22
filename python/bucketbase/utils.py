from __future__ import annotations

from typing import final


@final
class NoOwnershipIO:
    """
    A thin delegating wrapper around a file-like object that prevents
    closing the underlying stream.

    Why:
        - Solves the problem of multiple ownership.
        - The component with this wrapper has no say on closing the underlying stream.

    Rules:
        - Closing the wrapper marks *only the wrapper* as closed; the base stays open.
        - When the wrapper is closed, write/flush/seek/tell/read operations raise ValueError.
    """

    def __init__(self, base, promise_not_to_cache: bool = False):
        # Duck typing: ensure base has the required meth    ods and attributes
        required_attrs = ["close", "write", "flush", "closed"]
        for attr in required_attrs:
            if not hasattr(base, attr):
                raise TypeError(f"base must be a stream-like object with '{attr}' method/attribute")
        self._base = base
        self._closed: bool = False
        self._promise_not_to_cache = promise_not_to_cache

    def close(self) -> None:  # swallow close
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed or self._base.closed

    def _if_open(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file")

    # these 2 are looked up on the class only, not instance attributes
    def __iter__(self):
        self._if_open()
        return self

    def __next__(self):
        self._if_open()
        return next(self._base)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __getattr__(self, name):
        """Delegate attribute access to base stream with closed-state guard.

        Returns a wrapper that checks if the stream is closed on every call,
        even if the method reference was cached before the wrapper was closed.
        """
        attr = getattr(self._base, name)  # Raises AttributeError if missing

        # pylint: disable=no-else-return
        if not self._promise_not_to_cache and callable(attr):
            # Return a lambda that checks closed state on EVERY invocation
            def guarded_call(*args, **kwargs):
                self._if_open()
                return attr(*args, **kwargs)

            return guarded_call
        else:
            # For non-callable attributes (e.g., name, mode), check now
            self._if_open()
            return attr
