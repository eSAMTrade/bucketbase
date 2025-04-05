from io import BytesIO

class ChunkedCallbackStream:
    """
    reads in chunks
    uses a callback so that we can do the checks and/or throw exceptions
    """
    def __init__(self, content:bytes|str, callback=None, final_callback=None, chunk_size=1):
        """
        chunk_size: size of each chunk to read
        content: content to read from
        callback: function to call after reading each chunk
        final_callback: function to call after reading all chunks
        """
        self.stream = BytesIO(content)
        self.position = 0
        self.content_length = len(content)
        self.callback = callback
        self.final_callback = final_callback
        self.chunk_size = chunk_size

    def read(self, size=-1):
        if self.position >= self.content_length:
            if self.final_callback:
                self.final_callback(b"", self.position, size)
            return b""
        chunk = self.stream.read(self.chunk_size)
        self.position += len(chunk)

        # Call the callback if provided
        if chunk and self.callback:
            self.callback(chunk, self.position, size)
        return chunk
