from collections.abc import Callable
from io import BytesIO


class ChunkedCallbackStream:
    """
    reads in chunks
    uses a callback so that we can do the checks and/or throw exceptions
    """

    def __init__(
        self,
        content: bytes,
        callback: Callable[[bytes, int, int], None] | None = None,
        final_callback: Callable[[bytes, int, int], None] | None = None,
        chunk_size: int = 1,
    ) -> None:
        """
        chunk_size: size of each chunk to read
        content: content to read from
        callback: function to call after reading each chunk
        final_callback: function to call after reading all chunks
        """
        self._stream = BytesIO(content)
        self._position = 0
        self._content_length = len(content)
        self._callback = callback
        self._final_callback = final_callback
        self._chunk_size = chunk_size

    def read(self, size: int = -1) -> bytes:
        if self._position >= self._content_length:
            if self._final_callback:
                self._final_callback(b"", self._position, size)
            return b""
        chunk = self._stream.read(self._chunk_size)
        self._position += len(chunk)

        # Call the callback if provided
        if chunk and self._callback:
            self._callback(chunk, self._position, size)
        return chunk
